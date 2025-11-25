#![allow(dead_code)]

mod config;
mod formatter;

use std::{
    collections::HashSet,
    env,
    time::{Duration as StdDuration, SystemTime},
};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

mod github;
mod state;
mod telegram;
mod tmdb;

use crate::github::artifacts::{ArtifactStore, GitHubArtifactsClient, GitHubCredentials};
use crate::state::SentHistory;
use crate::tmdb::{MovieRelease, ReleaseWindow, TmdbClient};
use crate::{
    config::TelegramConfig,
    formatter::{DigitalRelease, TelegramMessage, build_messages},
    telegram::TelegramDispatcher,
};

const HISTORY_FILE_PATH: &str = "state/sent_movie_ids.txt";
const HISTORY_ARTIFACT_NAME: &str = "sent_movie_ids";

#[derive(Debug, Error)]
enum AppError {
    #[error("не задана переменная окружения {0}")]
    MissingEnv(String),
    #[error("некорректное значение GITHUB_REPOSITORY: {0}")]
    InvalidRepositoryFormat(String),
    #[error("некорректный идентификатор чата Telegram: {0}")]
    InvalidChatId(String),
    #[error(transparent)]
    State(#[from] state::StateError),
    #[error(transparent)]
    Tmdb(#[from] tmdb::TmdbError),
    #[error(transparent)]
    Telegram(#[from] telegram::TelegramError),
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let mut history = restore_history()?;
    let mut tmdb_client = initialize_tmdb(&history)?;
    let telegram_dispatcher = initialize_telegram_dispatcher()?;
    let telegram_config = telegram_config_from_env()?;

    let now = Utc::now();
    let window = release_window(now);

    let updates = fetch_updates(&tmdb_client, window).await?;
    let (unique_releases, duplicates) = filter_duplicates(&history, updates);
    tmdb_client.append_history(unique_releases.iter().map(|release| release.id));

    let payloads = prepare_payloads(&unique_releases, &telegram_config, now);
    let sent_messages = dispatch_notifications(&telegram_dispatcher, &payloads).await?;

    let persisted = persist_history(&mut history, &unique_releases)?;
    summarize_run(unique_releases.len(), duplicates, sent_messages, persisted);

    Ok(())
}

async fn fetch_updates(
    tmdb_client: &TmdbClient,
    window: ReleaseWindow,
) -> Result<Vec<MovieRelease>, AppError> {
    Ok(tmdb_client.fetch_digital_releases(window).await?)
}

fn filter_duplicates(
    history: &SentHistory,
    releases: Vec<MovieRelease>,
) -> (Vec<MovieRelease>, usize) {
    let mut seen = HashSet::new();
    let mut filtered = Vec::new();
    let mut duplicates = 0usize;

    for release in releases.into_iter() {
        if !seen.insert(release.id) || history.contains(release.id) {
            duplicates += 1;
            continue;
        }

        filtered.push(release);
    }

    (filtered, duplicates)
}

fn prepare_payloads(
    releases: &[MovieRelease],
    config: &TelegramConfig,
    now: DateTime<Utc>,
) -> Vec<TelegramMessage> {
    let now_system = SystemTime::UNIX_EPOCH + StdDuration::from_secs(now.timestamp() as u64);
    let formatted: Vec<DigitalRelease> = releases
        .iter()
        .map(|release| DigitalRelease {
            id: release.id,
            title: release.title.clone(),
            release_time: release_time(&release.release_date),
            display_date: release.release_date.format("%d.%m.%Y 00:00").to_string(),
            locale: release.original_language.clone(),
            platforms: release.watch_providers.clone(),
        })
        .collect();

    build_messages(&formatted, config, now_system)
}

async fn dispatch_notifications(
    dispatcher: &TelegramDispatcher,
    messages: &[TelegramMessage],
) -> Result<usize, AppError> {
    let mut sent = 0usize;

    for message in messages {
        dispatcher
            .send_batch(message.chat_id, [message.text.clone()])
            .await?;
        sent += 1;
    }

    Ok(sent)
}

fn summarize_run(found: usize, duplicates: usize, sent: usize, persisted: usize) {
    println!(
        "Итоги прогона: найдено {found} релиз(ов), дубликатов {duplicates}, отправлено сообщений {sent}, сохранено в истории {persisted}"
    );
}

fn release_window(now: DateTime<Utc>) -> ReleaseWindow {
    let start = now - Duration::hours(24) - Duration::minutes(5);
    ReleaseWindow { start, end: now }
}

fn release_time(date: &chrono::NaiveDate) -> SystemTime {
    date.and_hms_opt(0, 0, 0)
        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp())
        .map(|ts| SystemTime::UNIX_EPOCH + StdDuration::from_secs(ts as u64))
        .unwrap_or(SystemTime::UNIX_EPOCH)
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

fn restore_history() -> Result<SentHistory<GitHubArtifactsClient>, AppError> {
    let creds = github_credentials_from_env()?;
    let mut history = SentHistory::new(HISTORY_FILE_PATH, HISTORY_ARTIFACT_NAME, creds)?;
    history.restore()?;
    Ok(history)
}

fn initialize_tmdb(history: &SentHistory) -> Result<TmdbClient, AppError> {
    let tmdb_api_key = required_env("TMDB_API_KEY")?;
    Ok(TmdbClient::new(tmdb_api_key, history.iter().copied()))
}

fn initialize_telegram_dispatcher() -> Result<TelegramDispatcher, AppError> {
    let token = required_env("TELEGRAM_BOT_TOKEN")?;
    let chat_id = telegram_chat_id_from_env()?;
    Ok(TelegramDispatcher::new(token, vec![chat_id]))
}

fn telegram_config_from_env() -> Result<TelegramConfig, AppError> {
    let chat_id = telegram_chat_id_from_env()?;
    Ok(TelegramConfig::single_global_chat(chat_id))
}

fn telegram_chat_id_from_env() -> Result<i64, AppError> {
    let chat_id = required_env("TELEGRAM_CHAT_ID")?;
    chat_id
        .parse()
        .map_err(|_| AppError::InvalidChatId(chat_id))
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
}
