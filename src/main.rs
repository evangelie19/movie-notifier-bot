#![allow(dead_code)]

mod config;
mod formatter;
mod github;
mod state;
mod telegram;
mod tmdb;

use std::{future::Future, pin::Pin, time::SystemTime};

use formatter::{DigitalRelease, TelegramMessage, build_messages};
use state::{MovieId, SentHistory, StateError};
use telegram::TelegramDispatcher;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("ошибка состояния: {0}")]
    State(#[from] StateError),
    #[error("ошибка Telegram: {0}")]
    Telegram(#[from] telegram::TelegramError),
    #[error("ошибка источника данных: {0}")]
    Source(String),
}

pub trait HistoryAccess {
    fn restore(&mut self) -> Result<(), PipelineError>;
    fn append(&mut self, ids: &[MovieId]) -> usize;
    fn persist(&mut self) -> Result<(), PipelineError>;
    fn contains(&self, id: MovieId) -> bool;
}

impl HistoryAccess for SentHistory {
    fn restore(&mut self) -> Result<(), PipelineError> {
        SentHistory::restore(self).map_err(PipelineError::from)
    }

    fn append(&mut self, ids: &[MovieId]) -> usize {
        SentHistory::append(self, ids)
    }

    fn persist(&mut self) -> Result<(), PipelineError> {
        SentHistory::persist(self).map_err(PipelineError::from)
    }

    fn contains(&self, id: MovieId) -> bool {
        SentHistory::contains(self, id)
    }
}

pub trait ReleaseFetcher {
    fn fetch<'a>(
        &'a self,
        history: &'a dyn HistoryAccess,
        now: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PipelineRelease>, PipelineError>> + Send + 'a>>;
}

pub trait MessageDispatcher {
    fn send_messages<'a>(
        &'a self,
        messages: &'a [TelegramMessage],
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>>;
}

impl MessageDispatcher for TelegramDispatcher {
    fn send_messages<'a>(
        &'a self,
        messages: &'a [TelegramMessage],
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
        Box::pin(async move {
            for message in messages {
                self.send_batch(message.chat_id, [message.text.clone()])
                    .await?;
            }
            Ok(())
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelineRelease {
    pub id: MovieId,
    pub release: DigitalRelease,
}

#[tokio::main]
async fn main() -> Result<(), PipelineError> {
    // Основной сценарий предполагает создание реальных зависимостей через окружение.
    // Для локального запуска оставляем заглушки, чтобы подчеркнуть порядок пайплайна.
    let mut history = EmptyHistory;
    let fetcher = NoopFetcher;
    let dispatcher = NoopDispatcher;
    let config = config::TelegramConfig::default();
    let now = SystemTime::now();

    run_pipeline(&mut history, &fetcher, &dispatcher, &config, now).await
}

pub async fn run_pipeline(
    history: &mut dyn HistoryAccess,
    fetcher: &dyn ReleaseFetcher,
    dispatcher: &dyn MessageDispatcher,
    config: &config::TelegramConfig,
    now: SystemTime,
) -> Result<(), PipelineError> {
    restore_history(history)?;
    let releases = fetch_updates(fetcher, history, now).await?;
    let messages = prepare_payloads(&releases, config, now);

    if messages.is_empty() {
        return Ok(());
    }

    dispatch_notifications(dispatcher, &messages).await?;
    persist_history(history, &releases)?;

    Ok(())
}

fn restore_history(history: &mut dyn HistoryAccess) -> Result<(), PipelineError> {
    history.restore()
}

async fn fetch_updates(
    fetcher: &dyn ReleaseFetcher,
    history: &dyn HistoryAccess,
    now: SystemTime,
) -> Result<Vec<PipelineRelease>, PipelineError> {
    fetcher.fetch(history, now).await
}

fn prepare_payloads(
    releases: &[PipelineRelease],
    config: &config::TelegramConfig,
    now: SystemTime,
) -> Vec<TelegramMessage> {
    let digital: Vec<DigitalRelease> = releases.iter().map(|item| item.release.clone()).collect();
    build_messages(&digital, config, now)
}

async fn dispatch_notifications(
    dispatcher: &dyn MessageDispatcher,
    messages: &[TelegramMessage],
) -> Result<(), PipelineError> {
    dispatcher.send_messages(messages).await
}

fn persist_history(
    history: &mut dyn HistoryAccess,
    releases: &[PipelineRelease],
) -> Result<(), PipelineError> {
    let new_ids: Vec<MovieId> = releases.iter().map(|item| item.id).collect();
    history.append(&new_ids);
    history.persist()?;
    Ok(())
}

#[derive(Default)]
struct EmptyHistory;

impl HistoryAccess for EmptyHistory {
    fn restore(&mut self) -> Result<(), PipelineError> {
        Ok(())
    }

    fn append(&mut self, _ids: &[MovieId]) -> usize {
        0
    }

    fn persist(&mut self) -> Result<(), PipelineError> {
        Ok(())
    }

    fn contains(&self, _id: MovieId) -> bool {
        false
    }
}

struct NoopFetcher;

impl ReleaseFetcher for NoopFetcher {
    fn fetch<'a>(
        &'a self,
        _history: &'a dyn HistoryAccess,
        _now: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PipelineRelease>, PipelineError>> + Send + 'a>>
    {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct NoopDispatcher;

impl MessageDispatcher for NoopDispatcher {
    fn send_messages<'a>(
        &'a self,
        _messages: &'a [TelegramMessage],
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct MemoryHistory {
        restored: bool,
        appended: Arc<Mutex<Vec<MovieId>>>,
        persisted: Arc<Mutex<Vec<Vec<MovieId>>>>,
    }

    impl HistoryAccess for MemoryHistory {
        fn restore(&mut self) -> Result<(), PipelineError> {
            self.restored = true;
            Ok(())
        }

        fn append(&mut self, ids: &[MovieId]) -> usize {
            self.appended.lock().unwrap().extend_from_slice(ids);
            ids.len()
        }

        fn persist(&mut self) -> Result<(), PipelineError> {
            let current = self.appended.lock().unwrap().clone();
            self.persisted.lock().unwrap().push(current);
            Ok(())
        }

        fn contains(&self, id: MovieId) -> bool {
            self.appended.lock().unwrap().contains(&id)
        }
    }

    struct MemoryFetcher {
        releases: Vec<PipelineRelease>,
    }

    impl ReleaseFetcher for MemoryFetcher {
        fn fetch<'a>(
            &'a self,
            _history: &'a dyn HistoryAccess,
            _now: SystemTime,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<PipelineRelease>, PipelineError>> + Send + 'a>>
        {
            let releases = self.releases.clone();
            Box::pin(async move { Ok(releases) })
        }
    }

    #[derive(Default)]
    struct MemoryDispatcher {
        should_fail: bool,
        delivered: Arc<Mutex<Vec<TelegramMessage>>>,
    }

    impl MessageDispatcher for MemoryDispatcher {
        fn send_messages<'a>(
            &'a self,
            messages: &'a [TelegramMessage],
        ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
            let should_fail = self.should_fail;
            let delivered = self.delivered.clone();
            let messages = messages.to_vec();
            Box::pin(async move {
                if should_fail {
                    return Err(PipelineError::Source(
                        "тестовая ошибка отправки".to_string(),
                    ));
                }
                delivered.lock().unwrap().extend(messages);
                Ok(())
            })
        }
    }

    fn sample_release(id: MovieId) -> PipelineRelease {
        let now = SystemTime::now();
        let release = DigitalRelease {
            id,
            title: format!("Фильм {id}"),
            release_time: now,
            display_date: "01.01.2024 10:00".to_string(),
            locale: "ru".to_string(),
            platforms: vec!["Кинотеатр".to_string()],
        };
        PipelineRelease { id, release }
    }

    #[tokio::test]
    async fn history_not_persisted_on_failed_dispatch() {
        let mut history = MemoryHistory::default();
        let fetcher = MemoryFetcher {
            releases: vec![sample_release(10)],
        };
        let dispatcher = MemoryDispatcher {
            should_fail: true,
            delivered: Arc::new(Mutex::new(Vec::new())),
        };
        let config = config::TelegramConfig::single_global_chat(1);

        let result = run_pipeline(
            &mut history,
            &fetcher,
            &dispatcher,
            &config,
            SystemTime::now(),
        )
        .await;

        assert!(result.is_err(), "ожидалась ошибка отправки");
        assert!(history.persisted.lock().unwrap().is_empty());
        assert!(history.appended.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn history_persisted_after_successful_dispatch() {
        let mut history = MemoryHistory::default();
        let fetcher = MemoryFetcher {
            releases: vec![sample_release(20), sample_release(30)],
        };
        let dispatcher = MemoryDispatcher::default();
        let config = config::TelegramConfig::single_global_chat(1);

        run_pipeline(
            &mut history,
            &fetcher,
            &dispatcher,
            &config,
            SystemTime::now(),
        )
        .await
        .expect("поток должен завершиться успехом");

        let appended = history.appended.lock().unwrap().clone();
        assert_eq!(appended, vec![20, 30]);
        let persisted = history.persisted.lock().unwrap().clone();
        assert_eq!(persisted, vec![vec![20, 30]]);
    }
}
