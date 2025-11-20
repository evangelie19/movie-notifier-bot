#![allow(dead_code)]

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde::Deserialize;
use std::io::{Cursor, Read, Write};
use std::time::Duration;
use thiserror::Error;
use urlencoding::encode;
use zip::write::FileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

/// Креденшелы для доступа к GitHub API.
#[derive(Debug, Clone)]
pub struct GitHubCredentials {
    pub owner: String,
    pub repo: String,
    pub token: String,
}

impl GitHubCredentials {
    pub fn new(
        owner: impl Into<String>,
        repo: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            owner: owner.into(),
            repo: repo.into(),
            token: token.into(),
        }
    }
}

/// Ошибки, возникающие при обращении к GitHub Artifacts API.
#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("HTTP-запрос к GitHub завершился ошибкой: {0}")]
    Http(#[from] reqwest::Error),
    #[error("ошибка работы с zip-архивом: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("ошибка ввода-вывода: {0}")]
    Io(#[from] std::io::Error),
}

/// Абстракция над операциями чтения/записи артефактов.
pub trait ArtifactStore {
    fn download_artifact(&self, artifact_name: &str) -> Result<Option<Vec<u8>>, ArtifactError>;
    fn upload_artifact(
        &self,
        artifact_name: &str,
        file_name: &str,
        content: &[u8],
    ) -> Result<(), ArtifactError>;
}

/// Клиент GitHub Artifacts API, работающий через `reqwest::blocking`.
pub struct GitHubArtifactsClient {
    client: Client,
    creds: GitHubCredentials,
}

impl GitHubArtifactsClient {
    pub fn new(creds: GitHubCredentials) -> Result<Self, ArtifactError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("movie-notifier-bot-state")
            .build()?;
        Ok(Self { client, creds })
    }

    #[allow(dead_code)]
    fn list_artifacts(&self) -> Result<Vec<ArtifactDescriptor>, ArtifactError> {
        let url = format!(
            "https://api.github.com/repos/{}/{}/actions/artifacts?per_page=100",
            self.creds.owner, self.creds.repo
        );
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.creds.token)
            .header(ACCEPT, "application/vnd.github+json")
            .send()?;
        let list: ArtifactList = response.error_for_status()?.json()?;
        Ok(list.artifacts)
    }

    #[allow(dead_code)]
    fn download_archive(&self, url: &str) -> Result<Vec<u8>, ArtifactError> {
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.creds.token)
            .header(ACCEPT, "application/vnd.github+json")
            .send()?;
        let bytes = response.error_for_status()?.bytes()?;
        Ok(bytes.to_vec())
    }

    fn zip_payload(&self, file_name: &str, content: &[u8]) -> Result<Vec<u8>, ArtifactError> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = ZipWriter::new(&mut cursor);
            writer.start_file(
                file_name,
                FileOptions::default().compression_method(CompressionMethod::Deflated),
            )?;
            writer.write_all(content)?;
            writer.finish()?;
        }
        Ok(cursor.into_inner())
    }

    #[allow(dead_code)]
    fn upload_archive(&self, name: &str, zip_bytes: Vec<u8>) -> Result<(), ArtifactError> {
        let url = format!(
            "https://uploads.github.com/repos/{}/{}/actions/artifacts?name={}&size={}",
            self.creds.owner,
            self.creds.repo,
            encode(name),
            zip_bytes.len()
        );
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.creds.token)
            .header(ACCEPT, "application/vnd.github+json")
            .header(CONTENT_TYPE, "application/zip")
            .body(zip_bytes)
            .send()?;
        response.error_for_status()?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct ArtifactList {
    artifacts: Vec<ArtifactDescriptor>,
}

#[derive(Debug, Deserialize)]
struct ArtifactDescriptor {
    #[allow(dead_code)]
    id: u64,
    name: String,
    archive_download_url: String,
    expired: bool,
}

impl ArtifactStore for GitHubArtifactsClient {
    fn download_artifact(&self, artifact_name: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
        let Some(artifact) = self
            .list_artifacts()?
            .into_iter()
            .find(|a| !a.expired && a.name == artifact_name)
        else {
            return Ok(None);
        };
        let archive_bytes = self.download_archive(&artifact.archive_download_url)?;
        let mut archive = ZipArchive::new(Cursor::new(archive_bytes))?;
        if archive.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut file = archive.by_index(0)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(Some(buffer))
    }

    fn upload_artifact(
        &self,
        artifact_name: &str,
        file_name: &str,
        content: &[u8],
    ) -> Result<(), ArtifactError> {
        let payload = self.zip_payload(file_name, content)?;
        self.upload_archive(artifact_name, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zip_payload_roundtrip() {
        let client = GitHubArtifactsClient::new(GitHubCredentials::new("owner", "repo", "token"))
            .expect("клиент GitHub должен успешно создаваться без сети");

        let payload = client
            .zip_payload("sample.txt", b"content")
            .expect("zip должен собираться");

        let mut archive = ZipArchive::new(Cursor::new(payload)).expect("архив должен читаться");
        assert_eq!(archive.len(), 1);

        let mut file = archive
            .by_name("sample.txt")
            .expect("имя файла должно совпадать");
        let mut buffer = String::new();
        file.read_to_string(&mut buffer)
            .expect("содержимое должно быть UTF-8");

        assert_eq!(buffer, "content");
    }
}
