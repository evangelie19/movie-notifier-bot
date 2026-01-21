use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

use crate::config::TelegramConfig;
use crate::formatter::{DigitalRelease, build_empty_messages, build_messages};
use crate::state::{MovieId, SentHistory, StateError};
use crate::telegram::TelegramDispatcher;
use crate::tmdb::{MovieRelease, ReleaseWindow, TmdbClient};
use tracing::info;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
pub trait ReleaseProvider {
    async fn fetch_releases(
        &mut self,
        window: ReleaseWindow,
    ) -> Result<Vec<MovieRelease>, BoxError>;

    fn preload_history<I>(&mut self, ids: I)
    where
        I: IntoIterator<Item = MovieId>;
}

#[async_trait]
pub trait MessageDispatcher {
    async fn send_messages(&self, chat_id: i64, messages: Vec<String>) -> Result<(), BoxError>;
}

pub struct Orchestrator<C: crate::github::artifacts::ArtifactStore, P, D>
where
    P: ReleaseProvider,
    D: MessageDispatcher,
{
    history: SentHistory<C>,
    release_provider: P,
    dispatcher: D,
    telegram_config: TelegramConfig,
}

impl<C: crate::github::artifacts::ArtifactStore, P, D> Orchestrator<C, P, D>
where
    P: ReleaseProvider,
    D: MessageDispatcher,
{
    pub fn new(
        history: SentHistory<C>,
        release_provider: P,
        dispatcher: D,
        telegram_config: TelegramConfig,
    ) -> Self {
        Self {
            history,
            release_provider,
            dispatcher,
            telegram_config,
        }
    }

    pub fn release_window(now: DateTime<Utc>) -> ReleaseWindow {
        let start = now - Duration::hours(48) - Duration::minutes(5);
        ReleaseWindow { start, end: now }
    }

    pub async fn run(&mut self, now: DateTime<Utc>) -> Result<RunSummary, OrchestratorError> {
        if let Err(err) = self.history.restore() {
            eprintln!(
                "WARN: не удалось восстановить историю отправок, продолжаю с пустой историей: {err}"
            );
        }
        self.release_provider
            .preload_history(self.history.iter().copied());

        let window = Self::release_window(now);
        let releases = self
            .release_provider
            .fetch_releases(window)
            .await
            .map_err(OrchestratorError::Releases)?;

        let (unique_releases, duplicates) = self.filter_new_releases(releases);
        info!(
            target: "orchestrator",
            fetched = unique_releases.len() + duplicates,
            after_history = unique_releases.len(),
            duplicates,
            "Отфильтрованы релизы после истории"
        );
        if unique_releases.is_empty() {
            let empty_messages = build_empty_messages(&self.telegram_config);
            let messages_sent = empty_messages.len();
            for message in empty_messages {
                self.dispatcher
                    .send_messages(message.chat_id, vec![message.text])
                    .await
                    .map_err(OrchestratorError::Dispatch)?;
            }
            return Ok(RunSummary {
                fetched: duplicates,
                new_releases: 0,
                duplicates,
                messages_sent,
                history_appended: 0,
            });
        }

        let digital_releases = Self::convert_releases(&unique_releases);
        let messages = build_messages(&digital_releases, &self.telegram_config);

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

        let inserted = self.history.append(
            &unique_releases
                .iter()
                .map(|release| release.id)
                .collect::<Vec<_>>(),
        );
        if inserted > 0
            && let Err(err) = self.history.persist()
        {
            eprintln!("WARN: не удалось сохранить историю отправок, продолжаю без ошибки: {err}");
        }

        Ok(RunSummary {
            fetched: unique_releases.len() + duplicates,
            new_releases: unique_releases.len(),
            duplicates,
            messages_sent,
            history_appended: inserted,
        })
    }

    fn filter_new_releases(&self, releases: Vec<MovieRelease>) -> (Vec<MovieRelease>, usize) {
        let mut unique = Vec::new();
        let mut duplicates = 0usize;

        for release in releases.into_iter() {
            if self.history.contains(release.id) {
                duplicates += 1;
                continue;
            }
            unique.push(release);
        }

        (unique, duplicates)
    }

    fn convert_releases(releases: &[MovieRelease]) -> Vec<DigitalRelease> {
        releases
            .iter()
            .map(|release| {
                let naive_time = release
                    .digital_release_date
                    .and_hms_opt(0, 0, 0)
                    .expect("корректная дата релиза");
                let datetime = DateTime::<Utc>::from_naive_utc_and_offset(naive_time, Utc);
                DigitalRelease {
                    id: release.id,
                    title: release.title.clone(),
                    release_time: SystemTime::from(datetime),
                    display_date: release.digital_release_date.format("%Y-%m-%d").to_string(),
                    locale: release.original_language.clone(),
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
    pub duplicates: usize,
    pub messages_sent: usize,
    pub history_appended: usize,
}

impl RunSummary {
    pub fn render_markdown(&self) -> String {
        format!(
            "*Итоги прогона:*\\n- загружено релизов: {}\\n- новых релизов: {}\\n- дубликатов: {}\\n- отправлено сообщений: {}\\n- добавлено в историю: {}",
            self.fetched,
            self.new_releases,
            self.duplicates,
            self.messages_sent,
            self.history_appended
        )
    }
}

#[async_trait]
impl ReleaseProvider for TmdbClient {
    async fn fetch_releases(
        &mut self,
        window: ReleaseWindow,
    ) -> Result<Vec<MovieRelease>, BoxError> {
        self.fetch_digital_releases(window)
            .await
            .map_err(|err| Box::new(err) as BoxError)
    }

    fn preload_history<I>(&mut self, ids: I)
    where
        I: IntoIterator<Item = MovieId>,
    {
        self.append_history(ids);
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
    fn release_window_covers_48h_with_overlap() {
        let now = Utc::now();
        let window = Orchestrator::<
            crate::github::artifacts::GitHubArtifactsClient,
            TmdbClient,
            TelegramDispatcher,
        >::release_window(now);

        assert_eq!(window.end, now);
        assert_eq!(
            window.start,
            now - Duration::hours(48) - Duration::minutes(5)
        );
    }
}
