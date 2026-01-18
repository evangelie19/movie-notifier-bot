use std::cell::RefCell;
use std::fs;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::NaiveDate;
use movie_notifier_bot::app::{DispatchError, ReleaseDispatcher, dispatch_and_persist};
use movie_notifier_bot::github::artifacts::{ArtifactError, ArtifactStore};
use movie_notifier_bot::state::SentHistory;
use movie_notifier_bot::tmdb::MovieRelease;
use tempfile::tempdir;

type UploadedArtifact = (String, String, Vec<u8>);
type UploadedArtifacts = Arc<RefCell<Vec<UploadedArtifact>>>;

#[derive(Clone, Default)]
struct MemoryStore {
    uploaded: UploadedArtifacts,
}

impl ArtifactStore for MemoryStore {
    fn download_artifact(&self, _artifact_name: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
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
    let release_date = NaiveDate::from_ymd_opt(2024, 1, 1).expect("валидная дата");
    MovieRelease {
        id,
        title: format!("Релиз {id}"),
        release_date,
        digital_release_date: release_date,
        original_language: "en".to_string(),
        popularity: 0.0,
        homepage: None,
        watch_providers: Vec::new(),
    }
}

#[derive(Default)]
struct TestDispatcher {
    fail: bool,
    to_send: Vec<u64>,
}

#[async_trait]
impl ReleaseDispatcher for TestDispatcher {
    async fn dispatch(
        &self,
        releases: &[MovieRelease],
    ) -> Result<Vec<MovieRelease>, DispatchError> {
        if self.fail {
            return Err(DispatchError::Generic("искусственная ошибка".to_string()));
        }

        Ok(releases
            .iter()
            .filter(|release| self.to_send.is_empty() || self.to_send.contains(&release.id))
            .cloned()
            .collect())
    }
}

#[tokio::test]
async fn history_updates_only_with_dispatched_items() {
    let dir = tempdir().expect("временная директория создаётся");
    let file_path = dir.path().join("history.txt");
    fs::write(&file_path, b"1\n").expect("история должна записываться");

    let store = MemoryStore::default();
    let mut history = SentHistory::with_store(&file_path, "artifact", store.clone());
    history.restore().expect("история должна читаться");

    let dispatcher = TestDispatcher {
        fail: false,
        to_send: vec![2],
    };

    let releases = vec![sample_release(2), sample_release(3)];
    let inserted = dispatch_and_persist(&dispatcher, &mut history, &releases)
        .await
        .expect("отправка должна быть успешной");

    assert_eq!(inserted, 1);
    let saved = fs::read_to_string(&file_path).expect("файл истории должен существовать");
    assert!(saved.contains('1'));
    assert!(saved.contains('2'));
    assert!(!saved.contains('3'));

    let uploads = store.uploaded.borrow();
    assert_eq!(uploads.len(), 1);
}

#[tokio::test]
async fn history_is_not_updated_on_dispatch_failure() {
    let dir = tempdir().expect("временная директория создаётся");
    let file_path = dir.path().join("history.txt");
    fs::write(&file_path, b"10\n").expect("история должна записываться");

    let store = MemoryStore::default();
    let mut history = SentHistory::with_store(&file_path, "artifact", store.clone());
    history.restore().expect("история должна читаться");

    let dispatcher = TestDispatcher {
        fail: true,
        to_send: vec![20],
    };

    let releases = vec![sample_release(20)];
    let result = dispatch_and_persist(&dispatcher, &mut history, &releases).await;

    assert!(result.is_err());
    let saved = fs::read_to_string(&file_path).expect("файл истории должен существовать");
    assert!(saved.contains("10"));
    assert!(!saved.contains("20"));

    let uploads = store.uploaded.borrow();
    assert!(uploads.is_empty());
}
