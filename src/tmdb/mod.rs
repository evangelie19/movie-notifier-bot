#![allow(dead_code)]
// Модуль ещё не встроен в рабочий поток бота, поэтому временно подавляем
// предупреждения о неиспользуемых элементах до его подключения.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::time::Duration;

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::time::sleep;
use tracing::info;

const TMDB_BASE_URL: &str = "https://api.themoviedb.org/3";
const DIGITAL_RELEASE_TYPE: &str = "4";
const SORTING: &str = "popularity.desc";
const DEFAULT_PRIORITY_REGIONS: [&str; 6] = ["US", "GB", "CA", "AU", "DE", "FR"];
const MAX_MOVIES_PER_REGION: usize = 40;
const MAX_DISCOVER_RESULTS_TOTAL: usize = 120;
const RELEVANT_PRODUCTION_COUNTRIES: [&str; 23] = [
    "US", "GB", "CA", "AU", "FR", "DE", "IT", "ES", "JP", "KR", "RU", "NL", "SE", "NO", "DK", "FI",
    "BE", "IE", "CH", "PL", "CZ", "AT", "HU",
];
const EXCLUDED_GENRES: [&str; 4] = ["Documentary", "TV Movie", "Music", "Reality"];
const RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(5 * 60),
    Duration::from_secs(15 * 60),
    Duration::from_secs(30 * 60),
];
const MAX_DISCOVER_PAGES: u32 = 5;
const MIN_VOTE_COUNT: u32 = 10;
const MIN_VOTE_AVERAGE: f64 = 6.3;
const MIN_POPULARITY: f64 = 0.0;
const MOVIE_DISCOVER_WINDOW_DAYS: i64 = 14;
const DISCOVER_WINDOW_EXPAND_THRESHOLD_DAYS: i64 = 7;
const MOVIE_DEBUG_CANDIDATES_LIMIT: usize = 25;
const PRIORITY_REGIONS_ENV: &str = "TMDB_PRIORITY_REGIONS";

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
    pub digital_release_date: NaiveDate,
    pub original_language: String,
    pub popularity: f64,
    pub vote_average: Option<f64>,
    pub vote_count: Option<u32>,
    pub homepage: Option<String>,
    pub watch_providers: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum TvEventKind {
    Premiere,
    Season { season_number: u32 },
}

#[derive(Debug, Clone)]
pub struct TvEvent {
    pub show_id: u64,
    pub show_name: String,
    pub original_language: String,
    pub event_date: NaiveDate,
    pub kind: TvEventKind,
    pub vote_average: Option<f64>,
    pub vote_count: Option<u32>,
    pub popularity: Option<f64>,
}

impl TvEvent {
    pub fn event_key(&self) -> String {
        match self.kind {
            TvEventKind::Premiere => format!("tv:{}:premiere", self.show_id),
            TvEventKind::Season { season_number } => {
                format!("tv:{}:season:{}", self.show_id, season_number)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TmdbClient {
    http: Client,
    api_key: String,
    priority_regions: Vec<String>,
}

impl TmdbClient {
    pub fn new<S>(api_key: S) -> Self
    where
        S: Into<String>,
    {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("не удалось создать HTTP-клиент");

        Self {
            http,
            api_key: api_key.into(),
            priority_regions: resolve_priority_regions(),
        }
    }

    pub async fn fetch_digital_releases(
        &self,
        window: ReleaseWindow,
    ) -> Result<Vec<MovieRelease>, TmdbError> {
        if window.start > window.end {
            return Err(TmdbError::InvalidWindow);
        }
        let movie_window = movie_discover_window(window);

        let url = format!("{TMDB_BASE_URL}/discover/movie");
        let client = self.http.clone();
        let api_key = self.api_key.clone();
        let start = format_discover_date(movie_window.start);
        let end = format_discover_date(movie_window.end);

        let mut regional_candidates = HashMap::new();
        let mut movie_regions: HashMap<u64, BTreeSet<String>> = HashMap::new();
        let mut movies = Vec::new();

        for region in &self.priority_regions {
            let region_movies = self
                .fetch_discover_movies_for_region(
                    client.clone(),
                    url.clone(),
                    api_key.clone(),
                    start.clone(),
                    end.clone(),
                    region,
                )
                .await?;
            regional_candidates.insert(region.clone(), region_movies.len());
            for region_movie in &region_movies {
                movie_regions
                    .entry(region_movie.id)
                    .or_default()
                    .insert(region.clone());
            }

            movies.extend(region_movies.into_iter().take(MAX_MOVIES_PER_REGION));
        }

        let before_dedup = movies.len();
        let mut seen_ids = HashSet::new();
        movies.retain(|movie| seen_ids.insert(movie.id));
        let after_dedup = movies.len();
        if movies.len() > MAX_DISCOVER_RESULTS_TOTAL {
            movies.truncate(MAX_DISCOVER_RESULTS_TOTAL);
        }

        info!(
            target: "tmdb",
            regions = ?self.priority_regions,
            discover_start = %start,
            discover_end = %end,
            regional_candidates = ?regional_candidates,
            before_dedup,
            after_dedup,
            limited_total = movies.len(),
            "Собраны кандидаты discover по приоритетным регионам"
        );

        let raw_movies = movies.len();
        let mut releases = Vec::new();
        let mut details_enriched = 0usize;
        let mut logged_candidates = 0usize;
        let mut skipped_missing_date = 0usize;
        let mut skipped_missing_original_date = 0usize;
        let mut skipped_missing_digital_date = 0usize;
        let mut skipped_outside_window = 0usize;
        let mut skipped_old = 0usize;
        let mut after_basic_filter = 0usize;
        let mut skipped_by_imdb = 0usize;
        let mut skipped_by_country = 0usize;
        let mut skipped_by_genre = 0usize;
        let mut skipped_by_quality = 0usize;
        let mut skipped_by_runtime = 0usize;
        let mut skipped_by_other = 0usize;
        let current_year = window.end.date_naive().year();
        for movie in movies.into_iter() {
            let discover_regions = movie_regions
                .get(&movie.id)
                .map(|regions| regions.iter().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            let release_date_value = if movie.release_date.trim().is_empty() {
                "<missing>".to_string()
            } else {
                movie.release_date.clone()
            };
            if movie.release_date.is_empty() {
                skipped_missing_date += 1;
                skipped_by_other += 1;
                if logged_candidates < MOVIE_DEBUG_CANDIDATES_LIMIT {
                    info!(
                        target: "tmdb",
                        title = %movie.title,
                        movie_id = movie.id,
                        release_date = %release_date_value,
                        discover_regions = ?discover_regions,
                        production_countries = ?Vec::<String>::new(),
                        genres = ?Vec::<String>::new(),
                        vote_average = "missing",
                        vote_count = "missing",
                        runtime = "missing",
                        imdb_id = "missing",
                        verdict = %MovieFilterVerdict::rejected(
                            MovieRejectionReason::Other,
                            "missing_release_date"
                        ).kind(),
                        reason = "missing_release_date",
                        "movie_candidate_diagnostic"
                    );
                    logged_candidates += 1;
                }
                continue;
            }
            let Some(original_release_date) =
                parse_original_release_date(&movie.original_release_date)
            else {
                skipped_missing_original_date += 1;
                skipped_by_other += 1;
                if logged_candidates < MOVIE_DEBUG_CANDIDATES_LIMIT {
                    info!(
                        target: "tmdb",
                        title = %movie.title,
                        movie_id = movie.id,
                        release_date = %release_date_value,
                        discover_regions = ?discover_regions,
                        production_countries = ?Vec::<String>::new(),
                        genres = ?Vec::<String>::new(),
                        vote_average = "missing",
                        vote_count = "missing",
                        runtime = "missing",
                        imdb_id = "missing",
                        verdict = %MovieFilterVerdict::rejected(
                            MovieRejectionReason::Other,
                            "missing_original_release_date"
                        ).kind(),
                        reason = "missing_original_release_date",
                        "movie_candidate_diagnostic"
                    );
                    logged_candidates += 1;
                }
                continue;
            };
            let original_year = original_release_date.year();
            if !is_recent_original_release(original_year, current_year) {
                skipped_old += 1;
                info!(
                    target: "tmdb",
                    title = %movie.title,
                    original_year,
                    current_year,
                    "skip_old_movie"
                );
                continue;
            }
            let release_date = parse_release_date(&movie.release_date)
                .ok()
                .unwrap_or(original_release_date);
            let details = self.fetch_movie_details(movie.id).await?;
            details_enriched += 1;
            let today = window.end.date_naive();
            let Some(digital_release_date) =
                self.fetch_digital_release_date(movie.id, today).await?
            else {
                skipped_missing_digital_date += 1;
                skipped_by_other += 1;
                if logged_candidates < MOVIE_DEBUG_CANDIDATES_LIMIT {
                    log_movie_candidate_diagnostic(
                        &movie,
                        &discover_regions,
                        &release_date_value,
                        &details,
                        MovieFilterVerdict::rejected(
                            MovieRejectionReason::Other,
                            "missing_digital_release_date",
                        ),
                    );
                    logged_candidates += 1;
                }
                continue;
            };

            if !date_in_window(digital_release_date, window) {
                skipped_outside_window += 1;
                skipped_by_other += 1;
                if logged_candidates < MOVIE_DEBUG_CANDIDATES_LIMIT {
                    log_movie_candidate_diagnostic(
                        &movie,
                        &discover_regions,
                        &release_date_value,
                        &details,
                        MovieFilterVerdict::rejected(
                            MovieRejectionReason::Other,
                            "digital_release_outside_window",
                        ),
                    );
                    logged_candidates += 1;
                }
                continue;
            }

            after_basic_filter += 1;
            let verdict = movie_filter_verdict(&details);
            if logged_candidates < MOVIE_DEBUG_CANDIDATES_LIMIT {
                log_movie_candidate_diagnostic(
                    &movie,
                    &discover_regions,
                    &release_date_value,
                    &details,
                    verdict,
                );
                logged_candidates += 1;
            }
            if verdict.rejection_reason == Some(MovieRejectionReason::ImdbMissing) {
                skipped_by_imdb += 1;
                continue;
            }
            if verdict.rejection_reason == Some(MovieRejectionReason::Country) {
                skipped_by_country += 1;
                continue;
            }
            if verdict.rejection_reason == Some(MovieRejectionReason::Genre) {
                skipped_by_genre += 1;
                continue;
            }
            if verdict.rejection_reason == Some(MovieRejectionReason::RatingVoteCount) {
                skipped_by_quality += 1;
                continue;
            }
            if verdict.rejection_reason == Some(MovieRejectionReason::Runtime) {
                skipped_by_runtime += 1;
                continue;
            }

            releases.push(MovieRelease {
                id: movie.id,
                title: movie.title,
                release_date,
                digital_release_date,
                original_language: movie.original_language,
                popularity: movie.popularity,
                vote_average: details.vote_average,
                vote_count: details.vote_count,
                homepage: details.homepage,
                watch_providers: details.watch_providers,
            });
        }

        info!(
            target: "tmdb",
            fetched = releases.len(),
            skipped_missing_date,
            skipped_missing_original_date,
            skipped_missing_digital_date,
            skipped_outside_window,
            skipped_old,
            "Сформирован список цифровых релизов после фильтров"
        );
        info!(
            target: "tmdb",
            raw_movies,
            after_basic_filter,
            skipped_by_country,
            skipped_by_genre,
            skipped_by_quality,
            skipped_by_runtime,
            skipped_by_imdb,
            final_movies = releases.len(),
            "Диагностика фильтрации фильмов"
        );
        info!(
            target: "tmdb",
            raw_movies,
            details_enriched,
            accepted_movies = releases.len(),
            rejected_country = skipped_by_country,
            rejected_genre = skipped_by_genre,
            rejected_rating_vote_count = skipped_by_quality,
            rejected_runtime = skipped_by_runtime,
            rejected_imdb_missing = skipped_by_imdb,
            rejected_other = skipped_by_other,
            detailed_logged = logged_candidates,
            debug_limit = MOVIE_DEBUG_CANDIDATES_LIMIT,
            discover_start = %start,
            discover_end = %end,
            "Сводка movie-пайплайна"
        );

        Ok(releases)
    }

    pub async fn fetch_tv_events(&self, window: ReleaseWindow) -> Result<Vec<TvEvent>, TmdbError> {
        if window.start > window.end {
            return Err(TmdbError::InvalidWindow);
        }

        let mut show_ids = HashSet::new();
        for filter in [TvDiscoverFilter::FirstAirDate, TvDiscoverFilter::AirDate] {
            let shows = self.fetch_tv_discover(window, filter).await?;
            for show in shows {
                show_ids.insert(show.id);
            }
        }

        let mut events = Vec::new();
        let mut skipped_missing_date = 0usize;
        let mut skipped_outside_window = 0usize;
        let mut skipped_quality = 0usize;

        for show_id in show_ids {
            let details = self.fetch_tv_details(show_id).await?;
            let mut has_premiere = false;

            if let Some(first_air_date) = details.first_air_date.as_deref() {
                let date = parse_release_date(first_air_date)?;
                if date_in_window(date, window) {
                    if passes_quality_filters(
                        details.vote_average,
                        details.vote_count,
                        details.popularity,
                    ) {
                        events.push(TvEvent {
                            show_id,
                            show_name: details.name.clone(),
                            original_language: details.original_language.clone(),
                            event_date: date,
                            kind: TvEventKind::Premiere,
                            vote_average: details.vote_average,
                            vote_count: details.vote_count,
                            popularity: details.popularity,
                        });
                        has_premiere = true;
                    } else {
                        skipped_quality += 1;
                    }
                } else {
                    skipped_outside_window += 1;
                }
            } else {
                skipped_missing_date += 1;
            }

            for season in details.seasons.iter() {
                let Some(air_date) = season.air_date.as_deref() else {
                    skipped_missing_date += 1;
                    continue;
                };
                let date = parse_release_date(air_date)?;
                if !date_in_window(date, window) {
                    skipped_outside_window += 1;
                    continue;
                }
                if !passes_quality_filters(
                    details.vote_average,
                    details.vote_count,
                    details.popularity,
                ) {
                    skipped_quality += 1;
                    continue;
                }
                if has_premiere && season.season_number == 1 {
                    continue;
                }
                events.push(TvEvent {
                    show_id,
                    show_name: details.name.clone(),
                    original_language: details.original_language.clone(),
                    event_date: date,
                    kind: TvEventKind::Season {
                        season_number: season.season_number,
                    },
                    vote_average: details.vote_average,
                    vote_count: details.vote_count,
                    popularity: details.popularity,
                });
            }
        }

        info!(
            target: "tmdb",
            fetched = events.len(),
            skipped_missing_date,
            skipped_outside_window,
            skipped_quality,
            "Сформирован список событий сериалов"
        );

        Ok(events)
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
            imdb_id: payload.imdb_id,
            watch_providers,
            production_countries: payload.production_countries,
            vote_average: payload.vote_average,
            vote_count: payload.vote_count,
            genres: payload.genres,
            runtime: payload.runtime,
            popularity: payload.popularity,
        })
    }

    async fn fetch_tv_details(&self, show_id: u64) -> Result<TvShowDetails, TmdbError> {
        let url = format!("{TMDB_BASE_URL}/tv/{show_id}");
        let client = self.http.clone();
        let api_key = self.api_key.clone();

        let request_factory = move || tv_request(client.clone(), url.clone(), api_key.clone());

        let payload: TvShowDetailsResponse = self.fetch_json(request_factory).await?;

        Ok(TvShowDetails {
            name: payload.name,
            original_language: payload.original_language,
            first_air_date: payload.first_air_date,
            seasons: payload.seasons,
            vote_average: payload.vote_average,
            vote_count: payload.vote_count,
            popularity: payload.popularity,
        })
    }

    async fn fetch_tv_discover(
        &self,
        window: ReleaseWindow,
        filter: TvDiscoverFilter,
    ) -> Result<Vec<DiscoverTvShow>, TmdbError> {
        let url = format!("{TMDB_BASE_URL}/discover/tv");
        let client = self.http.clone();
        let api_key = self.api_key.clone();
        let start = format_discover_date(window.start);
        let end = format_discover_date(window.end);

        let request_factory = |page| {
            let client = client.clone();
            let url = url.clone();
            let api_key = api_key.clone();
            let start = start.clone();
            let end = end.clone();

            move || {
                discover_tv_request(
                    client.clone(),
                    url.clone(),
                    api_key.clone(),
                    start.clone(),
                    end.clone(),
                    page,
                    filter,
                )
            }
        };

        let mut response: DiscoverTvResponse = self.fetch_json(request_factory(1)).await?;
        let mut shows = response.results;
        let total_pages = limit_total_pages(response.total_pages);

        info!(
            target: "tmdb",
            start = %start,
            end = %end,
            total_pages = response.total_pages,
            total_results = response.total_results,
            limited_pages = total_pages,
            "Получен ответ TMDB discover tv"
        );

        if total_pages > 1 {
            for page in 2..=total_pages {
                response = self.fetch_json(request_factory(page)).await?;
                shows.extend(response.results);
            }
        }

        Ok(shows)
    }

    pub async fn fetch_digital_release_date(
        &self,
        movie_id: u64,
        today: NaiveDate,
    ) -> Result<Option<NaiveDate>, TmdbError> {
        let url = format!("{TMDB_BASE_URL}/movie/{movie_id}/release_dates");
        let client = self.http.clone();
        let api_key = self.api_key.clone();

        let request_factory =
            move || release_dates_request(client.clone(), url.clone(), api_key.clone());

        let payload: ReleaseDatesResponse = self.fetch_json(request_factory).await?;
        select_digital_release_date(&payload.results, today, &self.priority_regions)
    }

    async fn fetch_discover_movies_for_region(
        &self,
        client: Client,
        url: String,
        api_key: String,
        start: String,
        end: String,
        region: &str,
    ) -> Result<Vec<DiscoverMovie>, TmdbError> {
        let request_factory = |page| {
            let client = client.clone();
            let url = url.clone();
            let api_key = api_key.clone();
            let start = start.clone();
            let end = end.clone();
            let region = region.to_string();

            move || {
                discover_request(
                    client.clone(),
                    url.clone(),
                    api_key.clone(),
                    start.clone(),
                    end.clone(),
                    page,
                    region.clone(),
                )
            }
        };

        let mut response: DiscoverResponse = self.fetch_json(request_factory(1)).await?;
        let mut movies = response.results;
        let total_pages = limit_total_pages(response.total_pages);

        info!(
            target: "tmdb",
            region,
            start = %start,
            end = %end,
            total_pages = response.total_pages,
            total_results = response.total_results,
            limited_pages = total_pages,
            "Получен ответ TMDB discover"
        );

        if total_pages > 1 {
            for page in 2..=total_pages {
                response = self.fetch_json(request_factory(page)).await?;
                movies.extend(response.results);
            }
        }

        Ok(movies)
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
    total_results: u32,
    results: Vec<DiscoverMovie>,
}

#[derive(Debug, Clone, Copy)]
enum TvDiscoverFilter {
    FirstAirDate,
    AirDate,
}

#[derive(Debug, Deserialize)]
struct DiscoverMovie {
    id: u64,
    title: String,
    #[serde(
        default,
        rename = "primary_release_date",
        alias = "original_release_date"
    )]
    original_release_date: String,
    #[serde(default)]
    release_date: String,
    #[serde(default)]
    original_language: String,
    #[serde(default)]
    popularity: f64,
}

#[derive(Debug, Deserialize)]
struct DiscoverTvResponse {
    total_pages: u32,
    total_results: u32,
    results: Vec<DiscoverTvShow>,
}

#[derive(Debug, Deserialize)]
struct DiscoverTvShow {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct MovieDetailsResponse {
    homepage: Option<String>,
    imdb_id: Option<String>,
    #[serde(rename = "watch/providers")]
    watch_providers: Option<WatchProvidersEnvelope>,
    #[serde(default)]
    production_countries: Vec<ProductionCountry>,
    vote_average: Option<f64>,
    vote_count: Option<u32>,
    #[serde(default)]
    genres: Vec<Genre>,
    #[serde(default)]
    runtime: Option<u32>,
    popularity: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct TvShowDetailsResponse {
    name: String,
    #[serde(default)]
    original_language: String,
    first_air_date: Option<String>,
    #[serde(default)]
    seasons: Vec<TvSeason>,
    vote_average: Option<f64>,
    vote_count: Option<u32>,
    popularity: Option<f64>,
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

#[derive(Debug, Deserialize)]
struct ReleaseDatesResponse {
    results: Vec<ReleaseDatesRegion>,
}

#[derive(Debug, Deserialize)]
struct ReleaseDatesRegion {
    #[serde(rename = "iso_3166_1")]
    region: String,
    #[serde(default)]
    release_dates: Vec<ReleaseDateEntry>,
}

#[derive(Debug, Deserialize)]
struct ReleaseDateEntry {
    #[serde(rename = "release_date")]
    release_date: String,
    #[serde(rename = "type")]
    release_type: u8,
}

#[derive(Debug)]
pub struct MovieDetails {
    homepage: Option<String>,
    imdb_id: Option<String>,
    watch_providers: Vec<String>,
    production_countries: Vec<ProductionCountry>,
    vote_average: Option<f64>,
    vote_count: Option<u32>,
    genres: Vec<Genre>,
    runtime: Option<u32>,
    popularity: Option<f64>,
}

#[derive(Debug)]
struct TvShowDetails {
    name: String,
    original_language: String,
    first_air_date: Option<String>,
    seasons: Vec<TvSeason>,
    vote_average: Option<f64>,
    vote_count: Option<u32>,
    popularity: Option<f64>,
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

#[derive(Debug, Deserialize)]
struct TvSeason {
    air_date: Option<String>,
    season_number: u32,
}

fn discover_request(
    client: Client,
    url: String,
    api_key: String,
    start: String,
    end: String,
    page: u32,
    region: String,
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
        ("region".to_string(), region),
        ("vote_count.gte".to_string(), MIN_VOTE_COUNT.to_string()),
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

fn tv_request(client: Client, url: String, api_key: String) -> RequestBuilder {
    let query = vec![("api_key".to_string(), api_key)];

    client.get(url).query(&query)
}

fn release_dates_request(client: Client, url: String, api_key: String) -> RequestBuilder {
    let query = vec![("api_key".to_string(), api_key)];

    client.get(url).query(&query)
}

fn discover_tv_request(
    client: Client,
    url: String,
    api_key: String,
    start: String,
    end: String,
    page: u32,
    filter: TvDiscoverFilter,
) -> RequestBuilder {
    let mut query = vec![
        ("api_key".to_string(), api_key),
        ("sort_by".to_string(), "first_air_date.asc".to_string()),
        ("include_adult".to_string(), "false".to_string()),
        ("page".to_string(), page.to_string()),
    ];

    match filter {
        TvDiscoverFilter::FirstAirDate => {
            query.push(("first_air_date.gte".to_string(), start));
            query.push(("first_air_date.lte".to_string(), end));
        }
        TvDiscoverFilter::AirDate => {
            query.push(("air_date.gte".to_string(), start));
            query.push(("air_date.lte".to_string(), end));
        }
    }

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

fn select_digital_release_date(
    results: &[ReleaseDatesRegion],
    today: NaiveDate,
    priority_regions: &[String],
) -> Result<Option<NaiveDate>, TmdbError> {
    let mut by_region: HashMap<String, Vec<NaiveDate>> = HashMap::new();

    for region in results {
        let mut dates = Vec::new();
        for entry in &region.release_dates {
            if entry.release_type != 4 {
                continue;
            }
            if entry.release_date.is_empty() {
                continue;
            }
            dates.push(parse_release_date(&entry.release_date)?);
        }

        if !dates.is_empty() {
            by_region.insert(region.region.clone(), dates);
        }
    }

    for region in priority_regions {
        if let Some(dates) = by_region.get(region) {
            return Ok(select_preferred_date(dates.iter().copied(), today));
        }
    }

    Ok(select_preferred_date(
        by_region.into_values().flatten(),
        today,
    ))
}

fn resolve_priority_regions() -> Vec<String> {
    resolve_priority_regions_from(env::var(PRIORITY_REGIONS_ENV).ok().as_deref())
}

fn resolve_priority_regions_from(raw: Option<&str>) -> Vec<String> {
    let from_env = raw.map(parse_priority_regions);

    from_env
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| {
            DEFAULT_PRIORITY_REGIONS
                .iter()
                .map(|region| (*region).to_string())
                .collect()
        })
}

fn parse_priority_regions(raw: &str) -> Vec<String> {
    let mut unique = HashSet::new();
    raw.split(',')
        .filter_map(|region| {
            let normalized = region.trim().to_ascii_uppercase();
            if normalized.len() == 2
                && normalized.chars().all(|ch| ch.is_ascii_alphabetic())
                && unique.insert(normalized.clone())
            {
                return Some(normalized);
            }

            None
        })
        .collect()
}

fn select_preferred_date<I>(dates: I, today: NaiveDate) -> Option<NaiveDate>
where
    I: Iterator<Item = NaiveDate>,
{
    let mut past_max: Option<NaiveDate> = None;
    let mut future_min: Option<NaiveDate> = None;

    for date in dates {
        if date <= today {
            past_max = Some(past_max.map_or(date, |current| current.max(date)));
        } else {
            future_min = Some(future_min.map_or(date, |current| current.min(date)));
        }
    }

    past_max.or(future_min)
}

fn parse_release_date(raw: &str) -> Result<NaiveDate, TmdbError> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Ok(parsed.naive_utc().date());
    }

    Ok(NaiveDate::parse_from_str(raw, "%Y-%m-%d")?)
}

fn parse_original_release_date(raw: &str) -> Option<NaiveDate> {
    if raw.trim().is_empty() {
        return None;
    }

    parse_release_date(raw).ok()
}

fn is_recent_original_release(original_year: i32, current_year: i32) -> bool {
    original_year >= current_year - 1
}

fn format_discover_date(date: DateTime<Utc>) -> String {
    date.date_naive().format("%Y-%m-%d").to_string()
}

fn date_in_window(date: NaiveDate, window: ReleaseWindow) -> bool {
    let start = window.start.date_naive();
    let end = window.end.date_naive();
    date >= start && date <= end
}

fn movie_discover_window(window: ReleaseWindow) -> ReleaseWindow {
    let days = (window.end.date_naive() - window.start.date_naive()).num_days();
    if days <= DISCOVER_WINDOW_EXPAND_THRESHOLD_DAYS {
        return ReleaseWindow {
            start: window.end - chrono::Duration::days(MOVIE_DISCOVER_WINDOW_DAYS),
            end: window.end,
        };
    }

    window
}

fn limit_total_pages(total_pages: u32) -> u32 {
    total_pages.clamp(1, MAX_DISCOVER_PAGES)
}

fn passes_quality_filters(
    vote_average: Option<f64>,
    vote_count: Option<u32>,
    popularity: Option<f64>,
) -> bool {
    let votes_ok = match (vote_average, vote_count) {
        (Some(average), Some(count)) => average >= MIN_VOTE_AVERAGE && count >= MIN_VOTE_COUNT,
        _ => true,
    };
    let popularity_ok = popularity.is_none_or(|value| value >= MIN_POPULARITY);

    votes_ok && popularity_ok
}

pub fn is_relevant_release(details: &MovieDetails) -> bool {
    movie_filter_verdict(details).is_accepted()
}

#[derive(Debug, Clone, Copy)]
struct MovieFilterVerdict {
    rejection_reason: Option<MovieRejectionReason>,
    note: &'static str,
}

impl MovieFilterVerdict {
    fn accepted() -> Self {
        Self {
            rejection_reason: None,
            note: "passed_all_filters",
        }
    }

    fn rejected(reason: MovieRejectionReason, note: &'static str) -> Self {
        Self {
            rejection_reason: Some(reason),
            note,
        }
    }

    fn is_accepted(self) -> bool {
        self.rejection_reason.is_none()
    }

    fn kind(self) -> &'static str {
        match self.rejection_reason {
            None => "accepted",
            Some(reason) => reason.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MovieRejectionReason {
    Country,
    Genre,
    RatingVoteCount,
    Runtime,
    ImdbMissing,
    Other,
}

impl MovieRejectionReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Country => "rejected: country",
            Self::Genre => "rejected: genre",
            Self::RatingVoteCount => "rejected: rating_vote_count",
            Self::Runtime => "rejected: runtime",
            Self::ImdbMissing => "rejected: imdb_missing",
            Self::Other => "rejected: other",
        }
    }
}

fn movie_filter_verdict(details: &MovieDetails) -> MovieFilterVerdict {
    let has_imdb_id = details
        .imdb_id
        .as_deref()
        .map(|id| !id.trim().is_empty())
        .unwrap_or(false);
    let has_relevant_country = details
        .production_countries
        .iter()
        .any(|country| RELEVANT_PRODUCTION_COUNTRIES.contains(&country.code.as_str()));
    let has_excluded_genre = details
        .genres
        .iter()
        .any(|genre| EXCLUDED_GENRES.contains(&genre.name.as_str()));
    let passes_runtime_filter = details.runtime.is_none_or(|minutes| minutes >= 60);
    let passes_quality_filters =
        passes_quality_filters(details.vote_average, details.vote_count, details.popularity);

    if !has_imdb_id {
        return MovieFilterVerdict::rejected(MovieRejectionReason::ImdbMissing, "imdb_id_missing");
    }
    if !has_relevant_country {
        return MovieFilterVerdict::rejected(
            MovieRejectionReason::Country,
            "no_relevant_production_country",
        );
    }
    if has_excluded_genre {
        return MovieFilterVerdict::rejected(
            MovieRejectionReason::Genre,
            "contains_excluded_genre",
        );
    }
    if !passes_quality_filters {
        return MovieFilterVerdict::rejected(
            MovieRejectionReason::RatingVoteCount,
            "quality_filters_failed",
        );
    }
    if !passes_runtime_filter {
        return MovieFilterVerdict::rejected(
            MovieRejectionReason::Runtime,
            "runtime_less_than_minimum",
        );
    }

    MovieFilterVerdict::accepted()
}

fn log_movie_candidate_diagnostic(
    movie: &DiscoverMovie,
    discover_regions: &[String],
    release_date: &str,
    details: &MovieDetails,
    verdict: MovieFilterVerdict,
) {
    let production_countries = details
        .production_countries
        .iter()
        .map(|country| country.code.clone())
        .collect::<Vec<_>>();
    let genres = details
        .genres
        .iter()
        .map(|genre| genre.name.clone())
        .collect::<Vec<_>>();

    info!(
        target: "tmdb",
        title = %movie.title,
        movie_id = movie.id,
        release_date = %release_date,
        discover_regions = ?discover_regions,
        production_countries = ?production_countries,
        genres = ?genres,
        vote_average = ?details.vote_average,
        vote_count = ?details.vote_count,
        runtime = ?details.runtime,
        imdb_id = %if details
            .imdb_id
            .as_deref()
            .is_some_and(|id| !id.trim().is_empty())
        {
            "present"
        } else {
            "missing"
        },
        verdict = %verdict.kind(),
        reason = %verdict.note,
        "movie_candidate_diagnostic"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;

    fn make_details<F>(mut transform: F) -> MovieDetails
    where
        F: FnMut(&mut MovieDetails),
    {
        let mut details = MovieDetails {
            homepage: None,
            imdb_id: Some("tt1234567".to_string()),
            watch_providers: Vec::new(),
            production_countries: vec![ProductionCountry {
                code: "US".to_string(),
            }],
            vote_average: Some(7.0),
            vote_count: Some(50),
            genres: vec![Genre {
                name: "Drama".to_string(),
            }],
            runtime: Some(95),
            popularity: Some(1.0),
        };

        transform(&mut details);
        details
    }

    fn make_release_entry(date: &str, release_type: u8) -> ReleaseDateEntry {
        ReleaseDateEntry {
            release_date: date.to_string(),
            release_type,
        }
    }

    fn make_region(region: &str, dates: Vec<ReleaseDateEntry>) -> ReleaseDatesRegion {
        ReleaseDatesRegion {
            region: region.to_string(),
            release_dates: dates,
        }
    }

    fn default_priority_regions() -> Vec<String> {
        DEFAULT_PRIORITY_REGIONS
            .iter()
            .map(|region| (*region).to_string())
            .collect()
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
        let verdict = movie_filter_verdict(&details);
        assert_eq!(
            verdict.rejection_reason,
            Some(MovieRejectionReason::Country)
        );
    }

    #[test]
    fn release_without_imdb_id_is_filtered_out() {
        let details = make_details(|details| {
            details.imdb_id = None;
        });

        assert!(!is_relevant_release(&details));
        let verdict = movie_filter_verdict(&details);
        assert_eq!(
            verdict.rejection_reason,
            Some(MovieRejectionReason::ImdbMissing)
        );
    }

    #[test]
    fn release_with_low_vote_count_is_filtered_out() {
        let details = make_details(|details| {
            details.vote_count = Some(9);
        });

        assert!(!is_relevant_release(&details));
        let verdict = movie_filter_verdict(&details);
        assert_eq!(
            verdict.rejection_reason,
            Some(MovieRejectionReason::RatingVoteCount)
        );
    }

    #[test]
    fn release_with_low_vote_average_is_filtered_out() {
        let details = make_details(|details| {
            details.vote_average = Some(6.0);
        });

        assert!(!is_relevant_release(&details));
    }

    #[test]
    fn release_with_low_popularity_is_filtered_out() {
        let details = make_details(|details| {
            details.popularity = Some(-1.0);
        });

        assert!(!is_relevant_release(&details));
    }

    #[test]
    fn release_with_missing_votes_is_not_filtered() {
        let details = make_details(|details| {
            details.vote_average = None;
            details.vote_count = None;
        });

        assert!(is_relevant_release(&details));
    }

    #[test]
    fn release_with_missing_average_is_not_filtered() {
        let details = make_details(|details| {
            details.vote_average = None;
            details.vote_count = Some(1);
        });

        assert!(is_relevant_release(&details));
    }

    #[test]
    fn quality_filters_skip_missing_values() {
        assert!(passes_quality_filters(None, None, None));
        assert!(passes_quality_filters(Some(MIN_VOTE_AVERAGE), None, None));
        assert!(passes_quality_filters(None, Some(1), None));
        assert!(!passes_quality_filters(
            Some(MIN_VOTE_AVERAGE - 0.1),
            Some(MIN_VOTE_COUNT),
            None
        ));
    }

    #[test]
    fn tv_event_key_is_generated() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).expect("валидная дата");
        let premiere = TvEvent {
            show_id: 42,
            show_name: "Тест".to_string(),
            original_language: "en".to_string(),
            event_date: date,
            kind: TvEventKind::Premiere,
            vote_average: None,
            vote_count: None,
            popularity: None,
        };
        let season = TvEvent {
            show_id: 42,
            show_name: "Тест".to_string(),
            original_language: "en".to_string(),
            event_date: date,
            kind: TvEventKind::Season { season_number: 3 },
            vote_average: None,
            vote_count: None,
            popularity: None,
        };

        assert_eq!(premiere.event_key(), "tv:42:premiere");
        assert_eq!(season.event_key(), "tv:42:season:3");
    }

    #[test]
    fn release_with_excluded_genre_is_filtered_out() {
        let details = make_details(|details| {
            details.genres.push(Genre {
                name: "Documentary".to_string(),
            });
        });

        assert!(!is_relevant_release(&details));
        let verdict = movie_filter_verdict(&details);
        assert_eq!(verdict.rejection_reason, Some(MovieRejectionReason::Genre));
    }

    #[test]
    fn release_with_short_runtime_is_filtered_out() {
        let details = make_details(|details| {
            details.runtime = Some(55);
        });

        assert!(!is_relevant_release(&details));
        let verdict = movie_filter_verdict(&details);
        assert_eq!(
            verdict.rejection_reason,
            Some(MovieRejectionReason::Runtime)
        );
    }

    #[test]
    fn release_without_runtime_is_not_filtered_out() {
        let details = make_details(|details| {
            details.runtime = None;
        });

        assert!(is_relevant_release(&details));
        let verdict = movie_filter_verdict(&details);
        assert!(verdict.is_accepted());
    }

    #[test]
    fn digital_release_date_prefers_priority_region() {
        let today = NaiveDate::from_ymd_opt(2026, 2, 14).expect("валидная дата");
        let results = vec![
            make_region("US", vec![make_release_entry("2024-03-10", 4)]),
            make_region(
                "RU",
                vec![
                    make_release_entry("2024-02-01", 4),
                    make_release_entry("2024-02-05", 4),
                ],
            ),
        ];

        let selected =
            select_digital_release_date(&results, today, &["RU".to_string(), "US".to_string()])
                .expect("дата выбирается");
        assert_eq!(
            selected,
            Some(NaiveDate::from_ymd_opt(2024, 2, 5).expect("валидная дата"))
        );
    }

    #[test]
    fn digital_release_date_falls_back_to_any_region() {
        let today = NaiveDate::from_ymd_opt(2026, 2, 14).expect("валидная дата");
        let results = vec![
            make_region("BR", vec![make_release_entry("2024-01-02", 4)]),
            make_region("CA", vec![make_release_entry("2024-01-05", 4)]),
        ];

        let selected = select_digital_release_date(&results, today, &default_priority_regions())
            .expect("дата выбирается");
        assert_eq!(
            selected,
            Some(NaiveDate::from_ymd_opt(2024, 1, 5).expect("валидная дата"))
        );
    }

    #[test]
    fn digital_release_date_ignores_non_digital_entries() {
        let today = NaiveDate::from_ymd_opt(2026, 2, 14).expect("валидная дата");
        let results = vec![make_region(
            "RU",
            vec![
                make_release_entry("2024-01-01", 3),
                make_release_entry("2024-01-04", 4),
            ],
        )];

        let selected = select_digital_release_date(&results, today, &default_priority_regions())
            .expect("дата выбирается");
        assert_eq!(
            selected,
            Some(NaiveDate::from_ymd_opt(2024, 1, 4).expect("валидная дата"))
        );
    }

    #[test]
    fn select_preferred_date_chooses_latest_past_or_today() {
        let today = NaiveDate::from_ymd_opt(2026, 2, 14).expect("валидная дата");
        let old_past = NaiveDate::from_ymd_opt(2019, 1, 1).expect("валидная дата");
        let latest_past = NaiveDate::from_ymd_opt(2026, 2, 13).expect("валидная дата");

        let selected = select_preferred_date([old_past, latest_past].into_iter(), today);
        assert_eq!(selected, Some(latest_past));
    }

    #[test]
    fn select_preferred_date_chooses_earliest_future_when_no_past_dates() {
        let today = NaiveDate::from_ymd_opt(2026, 2, 14).expect("валидная дата");
        let earliest_future = NaiveDate::from_ymd_opt(2026, 2, 15).expect("валидная дата");
        let later_future = NaiveDate::from_ymd_opt(2026, 2, 20).expect("валидная дата");

        let selected = select_preferred_date([later_future, earliest_future].into_iter(), today);
        assert_eq!(selected, Some(earliest_future));
    }

    #[test]
    fn select_preferred_date_returns_none_for_empty_input() {
        let today = NaiveDate::from_ymd_opt(2026, 2, 14).expect("валидная дата");

        let selected = select_preferred_date(std::iter::empty(), today);
        assert_eq!(selected, None);
    }

    #[test]
    fn digital_release_date_prefers_past_or_today() {
        let today = NaiveDate::from_ymd_opt(2026, 2, 14).expect("валидная дата");
        let past = NaiveDate::from_ymd_opt(2026, 2, 13).expect("валидная дата");
        let future = NaiveDate::from_ymd_opt(2026, 2, 15).expect("валидная дата");
        let results = vec![make_region(
            "RU",
            vec![
                make_release_entry(&past.to_string(), 4),
                make_release_entry(&future.to_string(), 4),
            ],
        )];

        let selected = select_digital_release_date(&results, today, &default_priority_regions())
            .expect("дата выбирается");
        assert_eq!(selected, Some(past));
    }

    #[test]
    fn digital_release_date_selects_nearest_future_when_only_future() {
        let today = NaiveDate::from_ymd_opt(2026, 2, 14).expect("валидная дата");
        let first_future = NaiveDate::from_ymd_opt(2026, 2, 15).expect("валидная дата");
        let second_future = NaiveDate::from_ymd_opt(2026, 2, 16).expect("валидная дата");
        let results = vec![make_region(
            "RU",
            vec![
                make_release_entry(&second_future.to_string(), 4),
                make_release_entry(&first_future.to_string(), 4),
            ],
        )];

        let selected = select_digital_release_date(&results, today, &default_priority_regions())
            .expect("дата выбирается");
        assert_eq!(selected, Some(first_future));
    }

    #[test]
    fn discover_dates_formatted_as_yyyy_mm_dd() {
        let date = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2024, 3, 5)
                .expect("валидная дата")
                .and_hms_opt(12, 30, 0)
                .expect("валидное время"),
            Utc,
        );
        let formatted = format_discover_date(date);
        assert_eq!(formatted, "2024-03-05");
    }

    #[test]
    fn discover_request_includes_formatted_dates() {
        let client = Client::new();
        let request = discover_request(
            client,
            "https://example.com".to_string(),
            "token".to_string(),
            "2024-01-02".to_string(),
            "2024-01-05".to_string(),
            1,
            "US".to_string(),
        )
        .build()
        .expect("request build");

        let query = request
            .url()
            .query()
            .expect("query string should be present");
        assert!(query.contains("sort_by=popularity.desc"));
        assert!(query.contains("with_release_type=4"));
        assert!(query.contains("release_date.gte=2024-01-02"));
        assert!(query.contains("release_date.lte=2024-01-05"));
        assert!(query.contains("region=US"));
        assert!(query.contains("vote_count.gte=10"));
    }

    #[test]
    fn movie_discover_window_expands_week_to_two_weeks() {
        let end = DateTime::parse_from_rfc3339("2026-03-20T12:00:00Z")
            .expect("валидная дата")
            .with_timezone(&Utc);
        let input = ReleaseWindow {
            start: end - chrono::Duration::days(7),
            end,
        };

        let expanded = movie_discover_window(input);

        assert_eq!(expanded.end, end);
        assert_eq!(expanded.start, end - chrono::Duration::days(14));
    }

    #[test]
    fn movie_discover_window_keeps_longer_ranges() {
        let end = DateTime::parse_from_rfc3339("2026-03-20T12:00:00Z")
            .expect("валидная дата")
            .with_timezone(&Utc);
        let input = ReleaseWindow {
            start: end - chrono::Duration::days(14),
            end,
        };

        let expanded = movie_discover_window(input);

        assert_eq!(expanded.start, input.start);
        assert_eq!(expanded.end, input.end);
    }

    #[test]
    fn date_in_window_is_inclusive() {
        let start = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2024, 3, 1)
                .expect("валидная дата")
                .and_hms_opt(0, 0, 0)
                .expect("валидное время"),
            Utc,
        );
        let end = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2024, 3, 5)
                .expect("валидная дата")
                .and_hms_opt(23, 59, 59)
                .expect("валидное время"),
            Utc,
        );
        let window = ReleaseWindow { start, end };

        let inside = NaiveDate::from_ymd_opt(2024, 3, 3).expect("валидная дата");
        let boundary_start = NaiveDate::from_ymd_opt(2024, 3, 1).expect("валидная дата");
        let boundary_end = NaiveDate::from_ymd_opt(2024, 3, 5).expect("валидная дата");
        let outside = NaiveDate::from_ymd_opt(2024, 3, 6).expect("валидная дата");

        assert!(date_in_window(inside, window));
        assert!(date_in_window(boundary_start, window));
        assert!(date_in_window(boundary_end, window));
        assert!(!date_in_window(outside, window));
    }

    #[test]
    fn discover_pagination_is_limited() {
        assert_eq!(limit_total_pages(1), 1);
        assert_eq!(limit_total_pages(MAX_DISCOVER_PAGES), MAX_DISCOVER_PAGES);
        assert_eq!(
            limit_total_pages(MAX_DISCOVER_PAGES + 4),
            MAX_DISCOVER_PAGES
        );
    }

    #[test]
    fn original_release_date_missing_is_not_parsed() {
        assert!(parse_original_release_date("").is_none());
        assert!(parse_original_release_date("  ").is_none());
        assert!(parse_original_release_date("not-a-date").is_none());
    }

    #[test]
    fn original_release_date_parses_valid_date() {
        let parsed = parse_original_release_date("2024-02-10").expect("дата парсится");
        assert_eq!(
            parsed,
            NaiveDate::from_ymd_opt(2024, 2, 10).expect("валидная дата")
        );
    }

    #[test]
    fn parse_priority_regions_normalizes_and_deduplicates() {
        let parsed = parse_priority_regions("us, gb,US,xx1, ,fr");

        assert_eq!(parsed, vec!["US", "GB", "FR"]);
    }

    #[test]
    fn resolve_priority_regions_uses_defaults_for_empty_env() {
        let parsed = parse_priority_regions(" ,123");

        assert!(parsed.is_empty());
        let defaults = default_priority_regions();
        assert_eq!(defaults, resolve_priority_regions_from(None));
    }

    #[test]
    fn recent_original_release_allows_current_and_previous_years() {
        let current_year = 2026;
        assert!(is_recent_original_release(2026, current_year));
        assert!(is_recent_original_release(2025, current_year));
        assert!(!is_recent_original_release(2024, current_year));
    }
}
