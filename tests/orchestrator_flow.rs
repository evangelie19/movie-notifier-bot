use async_trait::async_trait;
use chrono::{NaiveDate, Utc};
use movie_notifier_bot::config::{ChatConfig, TelegramConfig};
use movie_notifier_bot::github::artifacts::{ArtifactError, ArtifactStore};
use movie_notifier_bot::orchestrator::{
    BoxError, MessageDispatcher, Orchestrator, ReleaseProvider,
};
use movie_notifier_bot::state::{MovieId, SentHistory};
use movie_notifier_bot::tmdb::{MovieRelease, ReleaseWindow};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

type UploadEntry = (String, String, Vec<u8>);
type UploadLog = Arc<Mutex<Vec<UploadEntry>>>;
type SentEntry = (i64, Vec<String>);
type SentMessages = Arc<Mutex<Vec<SentEntry>>>;

#[derive(Default, Clone)]
struct MemoryStore {
    uploads: UploadLog,
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
        self.uploads.lock().expect("блокировка доступна").push((
            artifact_name.to_string(),
            file_name.to_string(),
            content.to_vec(),
        ));
        Ok(())
    }
}

#[derive(Clone)]
struct StubProvider {
    releases: Vec<MovieRelease>,
    preloaded: Arc<Mutex<Vec<MovieId>>>,
    last_window: Arc<Mutex<Option<ReleaseWindow>>>,
}

impl StubProvider {
    fn new(releases: Vec<MovieRelease>) -> Self {
        Self {
            releases,
            preloaded: Arc::new(Mutex::new(Vec::new())),
            last_window: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl ReleaseProvider for StubProvider {
    async fn fetch_releases(
        &mut self,
        window: ReleaseWindow,
    ) -> Result<Vec<MovieRelease>, BoxError> {
        self.last_window
            .lock()
            .expect("блокировка доступна")
            .replace(window);
        Ok(self.releases.clone())
    }

    fn preload_history<I>(&mut self, ids: I)
    where
        I: IntoIterator<Item = MovieId>,
    {
        self.preloaded
            .lock()
            .expect("блокировка доступна")
            .extend(ids);
    }
}

#[derive(Default, Clone)]
struct StubDispatcher {
    sent: SentMessages,
}

#[async_trait]
impl MessageDispatcher for StubDispatcher {
    async fn send_messages(&self, chat_id: i64, messages: Vec<String>) -> Result<(), BoxError> {
        self.sent
            .lock()
            .expect("блокировка доступна")
            .push((chat_id, messages));
        Ok(())
    }
}

fn sample_release(id: u64, title: &str) -> MovieRelease {
    let release_date = NaiveDate::from_ymd_opt(2024, 1, 1).expect("валидная дата");
    MovieRelease {
        id,
        title: title.to_string(),
        release_date,
        digital_release_date: release_date,
        original_language: "ru".to_string(),
        popularity: 10.0,
        homepage: Some("https://example.org".to_string()),
        watch_providers: vec!["Kinopoisk".to_string()],
    }
}

#[tokio::test]
async fn orchestrator_runs_full_cycle() {
    let dir = tempdir().expect("временная директория создаётся");
    let file_path = dir.path().join("history.txt");
    std::fs::write(&file_path, b"1\n").expect("история должна записываться");

    let store = MemoryStore::default();
    let history = SentHistory::with_store(&file_path, "artifact", store.clone());

    let releases = vec![
        sample_release(1, "Дубликат"),
        sample_release(2, "Новый релиз"),
    ];
    let provider = StubProvider::new(releases);
    let dispatcher = StubDispatcher::default();
    let telegram_config = TelegramConfig {
        chats: vec![ChatConfig {
            chat_id: 99,
            locales: vec!["ru".to_string()],
        }],
    };

    let mut orchestrator = Orchestrator::new(
        history,
        provider.clone(),
        dispatcher.clone(),
        telegram_config,
    );

    let now = Utc::now();
    let summary = orchestrator
        .run(now)
        .await
        .expect("оркестратор должен завершиться успешно");

    assert_eq!(summary.fetched, 2);
    assert_eq!(summary.new_releases, 1);
    assert_eq!(summary.duplicates, 1);
    assert_eq!(summary.messages_sent, 1);
    assert_eq!(summary.history_appended, 1);

    let preloaded = provider
        .preloaded
        .lock()
        .expect("блокировка доступна")
        .clone();
    assert_eq!(preloaded, vec![1]);

    let window = provider
        .last_window
        .lock()
        .expect("блокировка доступна")
        .as_ref()
        .copied()
        .expect("окно должно быть установлено");
    assert_eq!(window.end.timestamp(), now.timestamp());
    assert_eq!(
        window.start.timestamp(),
        (now - chrono::Duration::hours(48) - chrono::Duration::minutes(5)).timestamp()
    );

    let sent = dispatcher.sent.lock().expect("блокировка доступна").clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, 99);
    assert!(sent[0].1[0].contains("Новый релиз"));

    let saved = std::fs::read_to_string(&file_path).expect("файл истории должен существовать");
    assert!(saved.contains('2'));

    let uploads = store.uploads.lock().expect("блокировка доступна");
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].0, "artifact");
    assert_eq!(uploads[0].1, "history.txt");
    assert!(String::from_utf8_lossy(&uploads[0].2).contains('2'));
}
