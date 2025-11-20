#![allow(dead_code)]

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;

use thiserror::Error;

use crate::github::artifacts::{
    ArtifactError, ArtifactStore, GitHubArtifactsClient, GitHubCredentials,
};

/// TMDB идентификатор фильма.
pub type MovieId = u64;

/// Хранилище отправленных идентификаторов релизов.
///
/// Структура отвечает за загрузку артефакта с GitHub, чтение локального файла
/// и синхронизацию обновлённого списка идентификаторов.
pub struct SentHistory<C: ArtifactStore = GitHubArtifactsClient> {
    file_path: PathBuf,
    artifact_name: String,
    ids: BTreeSet<MovieId>,
    artifact_store: C,
}

impl SentHistory<GitHubArtifactsClient> {
    /// Создаёт продовую реализацию, работающую с GitHub Artifacts API.
    pub fn new(
        file_path: impl Into<PathBuf>,
        artifact_name: impl Into<String>,
        credentials: GitHubCredentials,
    ) -> Result<Self, StateError> {
        let client = GitHubArtifactsClient::new(credentials)?;
        Ok(Self::with_store(file_path, artifact_name, client))
    }
}

impl<C: ArtifactStore> SentHistory<C> {
    /// Конструктор, позволяющий подменить источник артефактов (например, в тестах).
    pub fn with_store(
        file_path: impl Into<PathBuf>,
        artifact_name: impl Into<String>,
        artifact_store: C,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            artifact_name: artifact_name.into(),
            ids: BTreeSet::new(),
            artifact_store,
        }
    }

    /// Проверяет, присутствует ли идентификатор в истории.
    pub fn contains(&self, id: MovieId) -> bool {
        self.ids.contains(&id)
    }

    /// Добавляет список новых идентификаторов, возвращая количество реально вставленных значений.
    pub fn append(&mut self, new_ids: &[MovieId]) -> usize {
        let mut inserted = 0;
        for id in new_ids {
            if self.ids.insert(*id) {
                inserted += 1;
            }
        }
        inserted
    }

    /// Восстанавливает историю: пытается скачать артефакт и обновить локальный файл.
    pub fn restore(&mut self) -> Result<(), StateError> {
        if let Some(artifact_bytes) = self.artifact_store.download_artifact(&self.artifact_name)? {
            self.save_raw(&artifact_bytes)?;
            self.apply_raw(&artifact_bytes)?;
            return Ok(());
        }

        if self.file_path.exists() {
            let bytes = fs::read(&self.file_path)?;
            self.apply_raw(&bytes)?;
        }
        Ok(())
    }

    /// Сохраняет текущий список идентификаторов в файл и публикует его как артефакт.
    pub fn persist(&mut self) -> Result<(), StateError> {
        let raw = self.render_file();
        self.save_raw(raw.as_bytes())?;
        let file_name = self
            .file_path
            .file_name()
            .ok_or_else(|| StateError::MissingFileName(self.file_path.clone()))?
            .to_string_lossy()
            .to_string();
        self.artifact_store
            .upload_artifact(&self.artifact_name, &file_name, raw.as_bytes())?;
        Ok(())
    }

    fn apply_raw(&mut self, data: &[u8]) -> Result<(), StateError> {
        if data.is_empty() {
            self.ids.clear();
            return Ok(());
        }
        let text = String::from_utf8(data.to_vec())?;
        let mut parsed = BTreeSet::new();
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let id = MovieId::from_str(trimmed).map_err(|err| StateError::InvalidId {
                value: trimmed.to_string(),
                source: err,
            })?;
            parsed.insert(id);
        }
        self.ids = parsed;
        Ok(())
    }

    fn render_file(&self) -> String {
        self.ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn save_raw(&self, data: &[u8]) -> Result<(), StateError> {
        if let Some(parent) = self.file_path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.file_path, data)?;
        Ok(())
    }
}

/// Ошибки работы с историей отправленных релизов.
#[derive(Debug, Error)]
pub enum StateError {
    #[error("ошибка GitHub Artifacts API: {0}")]
    Artifact(#[from] ArtifactError),
    #[error("ошибка ввода-вывода: {0}")]
    Io(#[from] io::Error),
    #[error("не удалось преобразовать содержимое файла в UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("некорректный идентификатор фильма '{value}': {source}")]
    InvalidId {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("путь {0:?} не содержит имени файла")]
    MissingFileName(PathBuf),
}

impl From<StateError> for io::Error {
    fn from(err: StateError) -> Self {
        io::Error::other(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::fs;
    use tempfile::tempdir;

    #[derive(Default, Clone)]
    struct MemoryStore {
        downloaded: Option<Vec<u8>>,
        uploads: RefCell<Vec<(String, String, Vec<u8>)>>,
    }

    impl ArtifactStore for MemoryStore {
        fn download_artifact(
            &self,
            _artifact_name: &str,
        ) -> Result<Option<Vec<u8>>, ArtifactError> {
            Ok(self.downloaded.clone())
        }

        fn upload_artifact(
            &self,
            artifact_name: &str,
            file_name: &str,
            content: &[u8],
        ) -> Result<(), ArtifactError> {
            self.uploads.borrow_mut().push((
                artifact_name.to_string(),
                file_name.to_string(),
                content.to_vec(),
            ));
            Ok(())
        }
    }

    #[test]
    fn append_and_contains_ignore_duplicates() {
        let mut history = SentHistory::with_store(
            "state/sent_movie_ids.txt",
            "artifact",
            MemoryStore::default(),
        );

        assert!(!history.contains(10));
        assert_eq!(history.append(&[10, 11, 10]), 2);
        assert!(history.contains(10));
        assert!(history.contains(11));
    }

    #[test]
    fn restore_prefers_artifact_over_disk() {
        let dir = tempdir().expect("временная директория должна создаваться");
        let file_path = dir.path().join("history.txt");
        fs::write(&file_path, b"1\n2\n").expect("локальный файл должен записываться");

        let store = MemoryStore {
            downloaded: Some(b"3\n4\n".to_vec()),
            uploads: RefCell::new(Vec::new()),
        };

        let mut history = SentHistory::with_store(&file_path, "artifact", store);
        history.restore().expect("восстановление должно пройти");

        let restored = fs::read_to_string(&file_path).expect("файл должен существовать");
        assert_eq!(restored.trim(), "3\n4");
        assert!(history.contains(3));
        assert!(history.contains(4));
        assert!(!history.contains(1));
    }

    #[test]
    fn persist_writes_and_uploads_artifact() {
        let dir = tempdir().expect("временная директория должна создаваться");
        let file_path = dir.path().join("history.txt");

        let store = MemoryStore::default();
        let mut history = SentHistory::with_store(&file_path, "artifact", store.clone());
        history.append(&[7, 5]);

        history.persist().expect("состояние должно сохраняться");

        let content = fs::read_to_string(&file_path).expect("файл должен записываться");
        assert_eq!(content.trim(), "5\n7");

        let uploads = history.artifact_store.uploads.borrow();
        assert_eq!(uploads.len(), 1);
        let (artifact_name, file_name, payload) =
            uploads.first().expect("должна быть одна загрузка");
        assert_eq!(artifact_name, "artifact");
        assert_eq!(file_name, "history.txt");
        assert_eq!(String::from_utf8_lossy(payload).trim(), "5\n7");
    }
}
