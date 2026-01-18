#![allow(dead_code)]
// Модуль ещё не встроен в рабочий поток бота, поэтому временно подавляем
// предупреждения о неиспользуемых элементах до его подключения.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::time::sleep;

const TMDB_BASE_URL: &str = "https://api.themoviedb.org/3";
const DIGITAL_RELEASE_TYPE: &str = "4";
const SORTING: &str = "primary_release_date.asc";
const RELEVANT_PRODUCTION_COUNTRIES: [&str; 10] =
    ["US", "GB", "CA", "AU", "FR", "DE", "IT", "ES", "JP", "KR"];
const EXCLUDED_GENRES: [&str; 4] = ["Documentary", "TV Movie", "Music", "Reality"];
const RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(5 * 60),
    Duration::from_secs(15 * 60),
    Duration::from_secs(30 * 60),
];

#[derive(Debug, Error)]
pub enum TmdbError {
    #[error("некорректное окно релизов: начало позже конца")]
    InvalidWindow,
    #[error("ошибка HTTP: {0}")]
    Http(#[from] reqwest::Error),
    #[error("неожиданный статус ответа: {0}")]
    UnexpectedStatus(StatusCode),
    #[error("ошибка парсинга даты: {0}")]
    DateParse(#[from] chrono::ParseError),
    #[error("предел повторных попыток исчерпан")]
    RetryLimitExceeded,
}

#[derive(Debug, Clone, Copy)]
pub struct ReleaseWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MovieRelease {
    pub id: u64,
    pub title: String,
    pub release_date: NaiveDate,
    pub original_language: String,
    pub popularity: f64,
    pub homepage: Option<String>,
    pub watch_providers: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TmdbClient {
    http: Client,
    api_key: String,
    history: HashSet<u64>,
}

impl TmdbClient {
    pub fn new<S, I>(api_key: S, history: I) -> Self
    where
        S: Into<String>,
        I: IntoIterator<Item = u64>,
    {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("не удалось создать HTTP-клиент");

        Self {
            http,
            api_key: api_key.into(),
            history: history.into_iter().collect(),
        }
    }

    pub fn append_history<I>(&mut self, ids: I)
    where
        I: IntoIterator<Item = u64>,
    {
        self.history.extend(ids);
    }

    pub async fn fetch_digital_releases(
        &self,
        window: ReleaseWindow,
    ) -> Result<Vec<MovieRelease>, TmdbError> {
        if window.start > window.end {
            return Err(TmdbError::InvalidWindow);
        }

        let url = format!("{TMDB_BASE_URL}/discover/movie");
        let client = self.http.clone();
        let api_key = self.api_key.clone();
        let start = window.start.to_rfc3339_opts(SecondsFormat::Secs, true);
        let end = window.end.to_rfc3339_opts(SecondsFormat::Secs, true);

        let request_factory = |page| {
            let client = client.clone();
            let url = url.clone();
            let api_key = api_key.clone();
            let start = start.clone();
            let end = end.clone();

            move || {
                discover_request(
                    client.clone(),
                    url.clone(),
                    api_key.clone(),
                    start.clone(),
                    end.clone(),
                    page,
                )
            }
        };

        let mut response: DiscoverResponse = self.fetch_json(request_factory(1)).await?;
        let mut movies = response.results;

        if response.total_pages > 1 {
            for page in 2..=response.total_pages {
                response = self.fetch_json(request_factory(page)).await?;
                movies.extend(response.results);
            }
        }

        let mut releases = Vec::new();
        for movie in movies.into_iter() {
            if self.history.contains(&movie.id) {
                continue;
            }

            if movie.release_date.is_empty() {
                continue;
            }

            let release_date = NaiveDate::parse_from_str(&movie.release_date, "%Y-%m-%d")?;
            let details = self.fetch_movie_details(movie.id).await?;

            if !is_relevant_release(&details) {
                continue;
            }

            releases.push(MovieRelease {
                id: movie.id,
                title: movie.title,
                release_date,
                original_language: movie.original_language,
                popularity: movie.popularity,
                homepage: details.homepage,
                watch_providers: details.watch_providers,
            });
        }

        Ok(releases)
    }

    pub async fn fetch_movie_details(&self, movie_id: u64) -> Result<MovieDetails, TmdbError> {
        let url = format!("{TMDB_BASE_URL}/movie/{movie_id}");
        let client = self.http.clone();
        let api_key = self.api_key.clone();

        let request_factory = move || movie_request(client.clone(), url.clone(), api_key.clone());

        let payload: MovieDetailsResponse = self.fetch_json(request_factory).await?;

        let watch_providers = payload
            .watch_providers
            .map(|providers| collect_providers(providers.results))
            .unwrap_or_default();

        Ok(MovieDetails {
            homepage: payload.homepage,
            watch_providers,
            production_countries: payload.production_countries,
            vote_average: payload.vote_average,
            vote_count: payload.vote_count,
            genres: payload.genres,
            runtime: payload.runtime,
        })
    }

    async fn fetch_json<T, F>(&self, request_factory: F) -> Result<T, TmdbError>
    where
        T: DeserializeOwned,
        F: Fn() -> RequestBuilder,
    {
        let response = self.execute_with_retry(request_factory).await?;
        let status = response.status();
        if !status.is_success() {
            return Err(TmdbError::UnexpectedStatus(status));
        }

        Ok(response.json().await?)
    }

    async fn execute_with_retry<F>(&self, request_factory: F) -> Result<Response, TmdbError>
    where
        F: Fn() -> RequestBuilder,
    {
        let mut delays = RETRY_DELAYS.iter().copied();

        loop {
            match request_factory().send().await {
                Ok(resp) if resp.status().is_server_error() => {
                    if let Some(delay) = delays.next() {
                        sleep(delay).await;
                        continue;
                    }

                    break;
                }
                Ok(resp) => return Ok(resp),
                Err(err)
                    if err
                        .status()
                        .map(|status| status.is_server_error())
                        .unwrap_or(false) =>
                {
                    if let Some(delay) = delays.next() {
                        sleep(delay).await;
                        continue;
                    }

                    break;
                }
                Err(err) => return Err(TmdbError::Http(err)),
            }
        }

        Err(TmdbError::RetryLimitExceeded)
    }
}

#[derive(Debug, Deserialize)]
struct DiscoverResponse {
    total_pages: u32,
    results: Vec<DiscoverMovie>,
}

#[derive(Debug, Deserialize)]
struct DiscoverMovie {
    id: u64,
    title: String,
    #[serde(default)]
    release_date: String,
    #[serde(default)]
    original_language: String,
    #[serde(default)]
    popularity: f64,
}

#[derive(Debug, Deserialize)]
struct MovieDetailsResponse {
    homepage: Option<String>,
    #[serde(rename = "watch/providers")]
    watch_providers: Option<WatchProvidersEnvelope>,
    #[serde(default)]
    production_countries: Vec<ProductionCountry>,
    #[serde(default)]
    vote_average: f64,
    #[serde(default)]
    vote_count: u32,
    #[serde(default)]
    genres: Vec<Genre>,
    #[serde(default)]
    runtime: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct WatchProvidersEnvelope {
    results: HashMap<String, WatchProviderRegion>,
}

#[derive(Debug, Deserialize)]
struct WatchProviderRegion {
    #[serde(default)]
    flatrate: Vec<WatchProviderInfo>,
    #[serde(default)]
    rent: Vec<WatchProviderInfo>,
    #[serde(default)]
    buy: Vec<WatchProviderInfo>,
}

#[derive(Debug, Deserialize)]
struct WatchProviderInfo {
    #[serde(rename = "provider_name")]
    provider_name: String,
}

#[derive(Debug)]
pub struct MovieDetails {
    homepage: Option<String>,
    watch_providers: Vec<String>,
    production_countries: Vec<ProductionCountry>,
    vote_average: f64,
    vote_count: u32,
    genres: Vec<Genre>,
    runtime: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ProductionCountry {
    #[serde(rename = "iso_3166_1")]
    code: String,
}

#[derive(Debug, Deserialize)]
struct Genre {
    name: String,
}

fn discover_request(
    client: Client,
    url: String,
    api_key: String,
    start: String,
    end: String,
    page: u32,
) -> RequestBuilder {
    let query = vec![
        ("api_key".to_string(), api_key),
        ("sort_by".to_string(), SORTING.to_string()),
        (
            "with_release_type".to_string(),
            DIGITAL_RELEASE_TYPE.to_string(),
        ),
        ("release_date.gte".to_string(), start),
        ("release_date.lte".to_string(), end),
        ("include_adult".to_string(), "false".to_string()),
        ("page".to_string(), page.to_string()),
    ];

    client.get(url).query(&query)
}

fn movie_request(client: Client, url: String, api_key: String) -> RequestBuilder {
    let query = vec![
        ("api_key".to_string(), api_key),
        (
            "append_to_response".to_string(),
            "watch/providers".to_string(),
        ),
    ];

    client.get(url).query(&query)
}

fn collect_providers(regions: HashMap<String, WatchProviderRegion>) -> Vec<String> {
    let mut providers = BTreeSet::new();
    for region in regions.into_values() {
        for info in region
            .flatrate
            .into_iter()
            .chain(region.rent)
            .chain(region.buy)
        {
            providers.insert(info.provider_name);
        }
    }

    providers.into_iter().collect()
}

pub fn is_relevant_release(details: &MovieDetails) -> bool {
    let has_relevant_country = details
        .production_countries
        .iter()
        .any(|country| RELEVANT_PRODUCTION_COUNTRIES.contains(&country.code.as_str()));

    let has_excluded_genre = details
        .genres
        .iter()
        .any(|genre| EXCLUDED_GENRES.contains(&genre.name.as_str()));

    let has_required_runtime = details
        .runtime
        .map(|minutes| minutes >= 60)
        .unwrap_or(false);

    has_relevant_country && !has_excluded_genre && has_required_runtime
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_details<F>(mut transform: F) -> MovieDetails
    where
        F: FnMut(&mut MovieDetails),
    {
        let mut details = MovieDetails {
            homepage: None,
            watch_providers: Vec::new(),
            production_countries: vec![ProductionCountry {
                code: "US".to_string(),
            }],
            vote_average: 6.5,
            vote_count: 50,
            genres: vec![Genre {
                name: "Drama".to_string(),
            }],
            runtime: Some(95),
        };

        transform(&mut details);
        details
    }

    #[test]
    fn relevant_release_passes_all_checks() {
        let details = make_details(|_| {});

        assert!(is_relevant_release(&details));
    }

    #[test]
    fn release_without_relevant_country_is_filtered_out() {
        let details = make_details(|details| {
            details.production_countries = vec![ProductionCountry {
                code: "BR".to_string(),
            }];
        });

        assert!(!is_relevant_release(&details));
    }

    #[test]
    fn release_without_rating_or_votes_is_still_allowed() {
        let details = make_details(|details| {
            details.vote_average = 0.0;
            details.vote_count = 0;
        });

        assert!(is_relevant_release(&details));
    }

    #[test]
    fn release_with_excluded_genre_is_filtered_out() {
        let details = make_details(|details| {
            details.genres.push(Genre {
                name: "Documentary".to_string(),
            });
        });

        assert!(!is_relevant_release(&details));
    }

    #[test]
    fn release_with_short_runtime_is_filtered_out() {
        let details = make_details(|details| {
            details.runtime = Some(55);
        });

        assert!(!is_relevant_release(&details));
    }

    #[test]
    fn release_without_runtime_is_filtered_out() {
        let details = make_details(|details| {
            details.runtime = None;
        });

        assert!(!is_relevant_release(&details));
    }
}
