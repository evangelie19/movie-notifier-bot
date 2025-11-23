#![allow(dead_code)]

mod config;
mod formatter;

use std::env;

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

mod github;
mod state;
mod telegram;
mod tmdb;

use crate::config::TelegramConfig;
use crate::formatter::{DigitalRelease, TelegramMessage, build_messages};
use crate::github::artifacts::{ArtifactStore, GitHubArtifactsClient, GitHubCredentials};
use crate::state::SentHistory;
use crate::telegram::{TelegramDispatcher, dispatcher_from_env};
use crate::tmdb::{MovieRelease, ReleaseWindow, TmdbClient};

const HISTORY_FILE_PATH: &str = "state/sent_movie_ids.txt";
const HISTORY_ARTIFACT_NAME: &str = "sent-movie-ids";
const LEGACY_HISTORY_ARTIFACT_NAME: &str = "sent_movie_ids";

#[derive(Debug, Error)]
enum AppError {
    #[error("не задана переменная окружения {0}")]
    MissingEnv(String),
    #[error("некорректное значение GITHUB_REPOSITORY: {0}")]
    InvalidRepositoryFormat(String),
    #[error("некорректное значение TELEGRAM_CHAT_ID: {0}")]
    InvalidChatId(String),
    #[error(transparent)]
    State(#[from] state::StateError),
    #[error(transparent)]
    Tmdb(#[from] tmdb::TmdbError),
    #[error(transparent)]
    Telegram(#[from] telegram::TelegramError),
    #[error(transparent)]
    TelegramConfig(#[from] telegram::ConfigError),
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let mut history = restore_history()?;
    let tmdb_api_key = required_env("TMDB_API_KEY")?;
    let tmdb_client = TmdbClient::new(tmdb_api_key, history.iter().copied());
    let chat_id = parse_chat_id(required_env("TELEGRAM_CHAT_ID")?)?;
    let telegram_config = TelegramConfig::single_global_chat(chat_id);
    let dispatcher = build_dispatcher(chat_id)?;

    let now = Utc::now();
    let window = release_window(now);
    let releases = tmdb_client.fetch_digital_releases(window).await?;

    dispatch_notifications(
        &dispatcher,
        &telegram_messages(&releases, &telegram_config, now),
    )
    .await?;

    persist_history(&mut history, &releases)?;

    Ok(())
}

fn build_dispatcher(chat_id: i64) -> Result<TelegramDispatcher, AppError> {
    dispatcher_from_env(vec![chat_id]).map_err(AppError::from)
}

fn release_window(now: DateTime<Utc>) -> ReleaseWindow {
    let start = now - Duration::hours(24) - Duration::minutes(5);
    ReleaseWindow { start, end: now }
}

fn parse_chat_id(raw: String) -> Result<i64, AppError> {
    raw.trim()
        .parse::<i64>()
        .map_err(|_| AppError::InvalidChatId(raw))
}

fn persist_history<C: ArtifactStore>(
    history: &mut SentHistory<C>,
    releases: &[MovieRelease],
) -> Result<usize, AppError> {
    if releases.is_empty() {
        return Ok(0);
    }

    let ids: Vec<u64> = releases.iter().map(|release| release.id).collect();
    let inserted = history.append(&ids);
    history.persist()?;
    Ok(inserted)
}

fn telegram_messages(
    releases: &[MovieRelease],
    config: &TelegramConfig,
    now: DateTime<Utc>,
) -> Vec<TelegramMessage> {
    let now_std = now.into();
    let digital: Vec<DigitalRelease> = releases
        .iter()
        .map(|release| DigitalRelease {
            id: release.id,
            title: release.title.clone(),
            release_time: release
                .release_date
                .and_hms_opt(0, 0, 0)
                .expect("корректная дата")
                .and_utc()
                .into(),
            display_date: release.release_date.format("%d.%m.%Y 00:00").to_string(),
            locale: release.original_language.clone(),
            platforms: release.watch_providers.clone(),
        })
        .collect();

    build_messages(&digital, config, now_std)
}

async fn dispatch_notifications(
    dispatcher: &TelegramDispatcher,
    messages: &[TelegramMessage],
) -> Result<(), AppError> {
    for message in messages {
        dispatcher
            .send_batch(message.chat_id, [message.text.clone()])
            .await?;
    }

    Ok(())
}

fn restore_history() -> Result<SentHistory<GitHubArtifactsClient>, AppError> {
    let creds = github_credentials_from_env()?;
    let mut history = SentHistory::new(HISTORY_FILE_PATH, HISTORY_ARTIFACT_NAME, creds.clone())?;
    history.restore()?;
    if history.iter().next().is_some() || std::path::Path::new(HISTORY_FILE_PATH).exists() {
        return Ok(history);
    }

    let mut legacy = SentHistory::new(HISTORY_FILE_PATH, LEGACY_HISTORY_ARTIFACT_NAME, creds)?;
    legacy.restore()?;
    if legacy.iter().next().is_some() || std::path::Path::new(HISTORY_FILE_PATH).exists() {
        let ids: Vec<u64> = legacy.iter().copied().collect();
        history.append(&ids);
        history.persist()?;
        return Ok(history);
    }

    Ok(history)
}

fn github_credentials_from_env() -> Result<GitHubCredentials, AppError> {
    let repo = required_env("GITHUB_REPOSITORY")?;
    let (owner, name) = repo
        .split_once('/')
        .ok_or_else(|| AppError::InvalidRepositoryFormat(repo.clone()))?;
    let token = required_env("GITHUB_TOKEN")?;

    Ok(GitHubCredentials::new(owner, name, token))
}

fn required_env(name: &str) -> Result<String, AppError> {
    env::var(name).map_err(|_| AppError::MissingEnv(name.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::fs;
    use std::rc::Rc;

    use chrono::NaiveDate;
    use tempfile::tempdir;

    use crate::github::artifacts::ArtifactError;

    #[derive(Clone, Default)]
    struct MemoryStore {
        uploaded: Rc<RefCell<Vec<(String, String, Vec<u8>)>>>,
    }

    impl ArtifactStore for MemoryStore {
        fn download_artifact(
            &self,
            _artifact_name: &str,
        ) -> Result<Option<Vec<u8>>, ArtifactError> {
            Ok(None)
        }

        fn upload_artifact(
            &self,
            artifact_name: &str,
            file_name: &str,
            content: &[u8],
        ) -> Result<(), ArtifactError> {
            self.uploaded.borrow_mut().push((
                artifact_name.to_string(),
                file_name.to_string(),
                content.to_vec(),
            ));
            Ok(())
        }
    }

    fn sample_release(id: u64) -> MovieRelease {
        MovieRelease {
            id,
            title: format!("Релиз {id}"),
            release_date: NaiveDate::from_ymd_opt(2024, 1, 1).expect("валидная дата"),
            original_language: "en".to_string(),
            popularity: 0.0,
            homepage: None,
            watch_providers: Vec::new(),
        }
    }

    #[test]
    fn release_window_covers_24h_with_overlap() {
        let now = Utc::now();
        let window = release_window(now);

        assert_eq!(window.end, now);
        assert_eq!(
            window.start,
            now - Duration::hours(24) - Duration::minutes(5)
        );
    }

    #[test]
    fn history_is_persisted_after_new_releases() {
        let dir = tempdir().expect("временная директория создаётся");
        let file_path = dir.path().join("history.txt");
        fs::write(&file_path, b"1\n").expect("история должна записываться");

        let store = MemoryStore::default();
        let mut history = SentHistory::with_store(&file_path, "artifact", store.clone());
        history.restore().expect("история должна читаться");

        let releases = vec![sample_release(2)];
        let inserted = persist_history(&mut history, &releases).expect("персист должен работать");

        assert_eq!(inserted, 1);

        let saved = fs::read_to_string(&file_path).expect("файл истории должен существовать");
        assert!(saved.contains('2'));

        let uploads = store.uploaded.borrow();
        assert_eq!(uploads.len(), 1);
        let (artifact_name, file_name, payload) =
            uploads.first().expect("должна быть одна загрузка");
        assert_eq!(artifact_name, "artifact");
        assert_eq!(file_name, "history.txt");
        assert!(String::from_utf8_lossy(payload).contains('2'));
    }

    #[test]
    fn parse_chat_id_rejects_invalid_values() {
        let err = parse_chat_id("not-a-number".to_string()).unwrap_err();

        match err {
            AppError::InvalidChatId(raw) => assert_eq!(raw, "not-a-number"),
            other => panic!("получено неожиданное значение ошибки: {other:?}"),
        }
    }
}
