use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use tracing::info;

use crate::config::TelegramConfig;
use crate::formatter::{
    DigitalRelease, ReleaseKind, build_empty_messages, build_messages, sort_releases_by_priority,
};
use crate::state::{SentEventHistory, SentHistory, StateError};
use crate::telegram::TelegramDispatcher;
use crate::tmdb::{MovieRelease, ReleaseWindow, TmdbClient, TvEvent, TvEventKind};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
pub trait ReleaseProvider {
    async fn fetch_releases(&self, window: ReleaseWindow) -> Result<ReleaseBatch, BoxError>;
}

#[async_trait]
pub trait MessageDispatcher {
    async fn send_messages(&self, chat_id: i64, messages: Vec<String>) -> Result<(), BoxError>;
}

#[derive(Debug, Default, Clone)]
pub struct ReleaseBatch {
    pub movies: Vec<MovieRelease>,
    pub tv_events: Vec<TvEvent>,
}

pub struct Orchestrator<C: crate::github::artifacts::ArtifactStore, P, D>
where
    P: ReleaseProvider,
    D: MessageDispatcher,
{
    movie_history: SentHistory<C>,
    tv_history: SentEventHistory<C>,
    release_provider: P,
    dispatcher: D,
    telegram_config: TelegramConfig,
}

const MAX_RELEASES_PER_RUN: usize = 10;

impl<C: crate::github::artifacts::ArtifactStore, P, D> Orchestrator<C, P, D>
where
    P: ReleaseProvider,
    D: MessageDispatcher,
{
    pub fn new(
        movie_history: SentHistory<C>,
        tv_history: SentEventHistory<C>,
        release_provider: P,
        dispatcher: D,
        telegram_config: TelegramConfig,
    ) -> Self {
        Self {
            movie_history,
            tv_history,
            release_provider,
            dispatcher,
            telegram_config,
        }
    }

    pub fn release_window(now: DateTime<Utc>) -> ReleaseWindow {
        let start = now - Duration::days(7);
        ReleaseWindow { start, end: now }
    }

    pub async fn run(&mut self, now: DateTime<Utc>) -> Result<RunSummary, OrchestratorError> {
        if let Err(err) = self.movie_history.restore() {
            eprintln!(
                "WARN: не удалось восстановить историю фильмов, продолжаю с пустой историей: {err}"
            );
        }
        if let Err(err) = self.tv_history.restore() {
            eprintln!(
                "WARN: не удалось восстановить историю сериалов, продолжаю с пустой историей: {err}"
            );
        }

        let window = Self::release_window(now);
        let batch = self
            .release_provider
            .fetch_releases(window)
            .await
            .map_err(OrchestratorError::Releases)?;

        let fetched = batch.movies.len() + batch.tv_events.len();
        let (movie_releases, movie_duplicates) = self.filter_new_movies(batch.movies);
        let (tv_events, tv_duplicates) = self.filter_new_tv_events(batch.tv_events);
        let duplicates = movie_duplicates + tv_duplicates;

        let mut combined = Vec::new();
        combined.extend(Self::convert_movies(&movie_releases));
        combined.extend(Self::convert_tv_events(&tv_events));

        sort_releases_by_priority(&mut combined);
        let candidate_count = combined.len();
        if combined.len() > MAX_RELEASES_PER_RUN {
            combined.truncate(MAX_RELEASES_PER_RUN);
        }

        info!(
            target: "orchestrator",
            fetched,
            after_history = candidate_count,
            duplicates,
            sent = combined.len(),
            "Отфильтрованы релизы после истории"
        );

        if combined.is_empty() {
            let empty_messages = build_empty_messages(&self.telegram_config);
            let messages_sent = empty_messages.len();
            for message in empty_messages {
                self.dispatcher
                    .send_messages(message.chat_id, vec![message.text])
                    .await
                    .map_err(OrchestratorError::Dispatch)?;
            }
            return Ok(RunSummary {
                fetched,
                new_releases: candidate_count,
                sent_releases: 0,
                duplicates,
                messages_sent,
                movie_history_appended: 0,
                tv_history_appended: 0,
                truncated: candidate_count,
            });
        }

        let messages = build_messages(&combined, &self.telegram_config);

        let mut grouped: std::collections::HashMap<i64, Vec<String>> =
            std::collections::HashMap::new();
        for message in messages {
            grouped
                .entry(message.chat_id)
                .or_default()
                .push(message.text);
        }

        let mut messages_sent = 0;
        for (chat_id, messages) in grouped {
            messages_sent += messages.len();
            self.dispatcher
                .send_messages(chat_id, messages)
                .await
                .map_err(OrchestratorError::Dispatch)?;
        }

        let movie_ids: Vec<u64> = combined
            .iter()
            .filter_map(|release| match release.kind {
                ReleaseKind::Movie => Some(release.id),
                _ => None,
            })
            .collect();
        let tv_keys: Vec<String> = combined
            .iter()
            .filter_map(|release| match &release.kind {
                ReleaseKind::TvPremiere | ReleaseKind::TvSeason { .. } => {
                    Some(release.event_key.clone())
                }
                _ => None,
            })
            .collect();

        let movie_inserted = self.movie_history.append(&movie_ids);
        let tv_inserted = self.tv_history.append(&tv_keys);

        if movie_inserted > 0
            && let Err(err) = self.movie_history.persist()
        {
            eprintln!("WARN: не удалось сохранить историю фильмов, продолжаю без ошибки: {err}");
        }
        if tv_inserted > 0
            && let Err(err) = self.tv_history.persist()
        {
            eprintln!("WARN: не удалось сохранить историю сериалов, продолжаю без ошибки: {err}");
        }

        Ok(RunSummary {
            fetched,
            new_releases: candidate_count,
            sent_releases: combined.len(),
            duplicates,
            messages_sent,
            movie_history_appended: movie_inserted,
            tv_history_appended: tv_inserted,
            truncated: candidate_count.saturating_sub(combined.len()),
        })
    }

    fn filter_new_movies(&self, releases: Vec<MovieRelease>) -> (Vec<MovieRelease>, usize) {
        let mut unique = Vec::new();
        let mut duplicates = 0usize;
        let mut seen = std::collections::HashSet::new();

        for release in releases.into_iter() {
            if self.movie_history.contains(release.id) || !seen.insert(release.id) {
                duplicates += 1;
                continue;
            }
            unique.push(release);
        }

        (unique, duplicates)
    }

    fn filter_new_tv_events(&self, events: Vec<TvEvent>) -> (Vec<TvEvent>, usize) {
        let mut unique = Vec::new();
        let mut duplicates = 0usize;
        let mut seen = std::collections::HashSet::new();

        for event in events.into_iter() {
            let key = event.event_key();
            if self.tv_history.contains(&key) || !seen.insert(key.clone()) {
                duplicates += 1;
                continue;
            }
            unique.push(event);
        }

        (unique, duplicates)
    }

    fn convert_movies(releases: &[MovieRelease]) -> Vec<DigitalRelease> {
        releases
            .iter()
            .map(|release| DigitalRelease {
                id: release.id,
                title: release.title.clone(),
                event_date: release.digital_release_date,
                locale: release.original_language.clone(),
                kind: ReleaseKind::Movie,
                vote_average: release.vote_average,
                vote_count: release.vote_count,
                event_key: format!("movie:{}", release.id),
            })
            .collect()
    }

    fn convert_tv_events(events: &[TvEvent]) -> Vec<DigitalRelease> {
        events
            .iter()
            .map(|event| {
                let kind = match event.kind {
                    TvEventKind::Premiere => ReleaseKind::TvPremiere,
                    TvEventKind::Season { season_number } => {
                        ReleaseKind::TvSeason { season_number }
                    }
                };
                DigitalRelease {
                    id: event.show_id,
                    title: event.show_name.clone(),
                    event_date: event.event_date,
                    locale: event.original_language.clone(),
                    kind,
                    vote_average: event.vote_average,
                    vote_count: event.vote_count,
                    event_key: event.event_key(),
                }
            })
            .collect()
    }
}

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error(transparent)]
    State(#[from] StateError),
    #[error("ошибка загрузки релизов: {0}")]
    Releases(BoxError),
    #[error("ошибка отправки уведомлений: {0}")]
    Dispatch(BoxError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub fetched: usize,
    pub new_releases: usize,
    pub sent_releases: usize,
    pub duplicates: usize,
    pub messages_sent: usize,
    pub movie_history_appended: usize,
    pub tv_history_appended: usize,
    pub truncated: usize,
}

impl RunSummary {
    pub fn render_markdown(&self) -> String {
        format!(
            "*Итоги прогона:*\\n- загружено релизов: {}\\n- новых релизов после истории: {}\\n- отправлено релизов: {}\\n- дубликатов: {}\\n- отправлено сообщений: {}\\n- добавлено в историю фильмов: {}\\n- добавлено в историю сериалов: {}\\n- отброшено из-за лимита: {}",
            self.fetched,
            self.new_releases,
            self.sent_releases,
            self.duplicates,
            self.messages_sent,
            self.movie_history_appended,
            self.tv_history_appended,
            self.truncated
        )
    }
}

#[async_trait]
impl ReleaseProvider for TmdbClient {
    async fn fetch_releases(&self, window: ReleaseWindow) -> Result<ReleaseBatch, BoxError> {
        let movies = self
            .fetch_digital_releases(window)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
        let tv_events = self
            .fetch_tv_events(window)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;

        Ok(ReleaseBatch { movies, tv_events })
    }
}

#[async_trait]
impl MessageDispatcher for TelegramDispatcher {
    async fn send_messages(&self, chat_id: i64, messages: Vec<String>) -> Result<(), BoxError> {
        self.send_batch(chat_id, messages)
            .await
            .map_err(|err| Box::new(err) as BoxError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_window_covers_last_week() {
        let now = Utc::now();
        let window = Orchestrator::<
            crate::github::artifacts::GitHubArtifactsClient,
            TmdbClient,
            TelegramDispatcher,
        >::release_window(now);

        assert_eq!(window.end, now);
        assert_eq!(window.start, now - Duration::days(7));
    }
}
