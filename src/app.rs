use std::env;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

use crate::github::artifacts::{ArtifactStore, GitHubArtifactsClient, GitHubCredentials};
use crate::state::SentHistory;
use crate::tmdb::{MovieRelease, ReleaseWindow, TmdbClient};

const HISTORY_FILE_PATH: &str = "state/sent_movie_ids.txt";
const HISTORY_ARTIFACT_NAME: &str = "sent_movie_ids";

#[derive(Debug, Error)]
pub enum AppError {
    #[error("не задана переменная окружения {0}")]
    MissingEnv(String),
    #[error("некорректное значение GITHUB_REPOSITORY: {0}")]
    InvalidRepositoryFormat(String),
    #[error(transparent)]
    State(#[from] crate::state::StateError),
    #[error(transparent)]
    Tmdb(#[from] crate::tmdb::TmdbError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("ошибка отправки уведомлений: {0}")]
    Generic(String),
}

impl From<crate::telegram::TelegramError> for DispatchError {
    fn from(err: crate::telegram::TelegramError) -> Self {
        Self::Generic(err.to_string())
    }
}

#[async_trait]
pub trait ReleaseDispatcher {
    async fn dispatch(&self, releases: &[MovieRelease])
    -> Result<Vec<MovieRelease>, DispatchError>;
}

pub struct NoopDispatcher;

#[async_trait]
impl ReleaseDispatcher for NoopDispatcher {
    async fn dispatch(
        &self,
        releases: &[MovieRelease],
    ) -> Result<Vec<MovieRelease>, DispatchError> {
        Ok(releases.to_vec())
    }
}

pub fn release_window(now: DateTime<Utc>) -> ReleaseWindow {
    let start = now - Duration::hours(24) - Duration::minutes(5);
    ReleaseWindow { start, end: now }
}

pub async fn fetch_releases(
    history: &mut SentHistory<GitHubArtifactsClient>,
) -> Result<Vec<MovieRelease>, AppError> {
    let tmdb_api_key = required_env("TMDB_API_KEY")?;
    let tmdb_client = TmdbClient::new(tmdb_api_key, history.iter().copied());

    let now = Utc::now();
    let window = release_window(now);
    Ok(tmdb_client.fetch_digital_releases(window).await?)
}

pub async fn dispatch_and_persist<D: ReleaseDispatcher, C: ArtifactStore>(
    dispatcher: &D,
    history: &mut SentHistory<C>,
    releases: &[MovieRelease],
) -> Result<usize, AppError> {
    let dispatched = dispatcher.dispatch(releases).await?;
    persist_history(history, &dispatched)
}

pub fn persist_history<C: ArtifactStore>(
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

pub fn restore_history() -> Result<SentHistory<GitHubArtifactsClient>, AppError> {
    let creds = github_credentials_from_env()?;
    let mut history = SentHistory::new(HISTORY_FILE_PATH, HISTORY_ARTIFACT_NAME, creds)?;
    history.restore()?;
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

pub fn required_env(name: &str) -> Result<String, AppError> {
    env::var(name).map_err(|_| AppError::MissingEnv(name.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::fs;
    use std::rc::Rc;

    type UploadedArtifact = (String, String, Vec<u8>);
    type UploadedArtifacts = Rc<RefCell<Vec<UploadedArtifact>>>;

    use chrono::NaiveDate;
    use tempfile::tempdir;

    use crate::github::artifacts::{ArtifactError, ArtifactStore};

    #[derive(Clone, Default)]
    struct MemoryStore {
        uploaded: UploadedArtifacts,
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

    #[tokio::test]
    async fn dispatched_subset_is_used_for_persistence() {
        struct PartialDispatcher;

        #[async_trait]
        impl ReleaseDispatcher for PartialDispatcher {
            async fn dispatch(
                &self,
                releases: &[MovieRelease],
            ) -> Result<Vec<MovieRelease>, DispatchError> {
                Ok(releases
                    .iter()
                    .filter(|release| release.id % 2 == 0)
                    .cloned()
                    .collect())
            }
        }

        let dir = tempdir().expect("временная директория создаётся");
        let file_path = dir.path().join("history.txt");

        let store = MemoryStore::default();
        let mut history = SentHistory::with_store(&file_path, "artifact", store.clone());
        history.restore().expect("история должна читаться");

        let releases = vec![sample_release(1), sample_release(2)];
        let inserted = dispatch_and_persist(&PartialDispatcher, &mut history, &releases)
            .await
            .unwrap();

        assert_eq!(inserted, 1);
        let saved = fs::read_to_string(&file_path).expect("файл истории должен существовать");
        assert!(saved.contains('2'));
        assert!(!saved.contains('1'));
    }
}
