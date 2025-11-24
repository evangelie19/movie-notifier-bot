#![allow(dead_code)]

mod config;
mod formatter;
mod orchestrator;

use std::env;

use chrono::Utc;
use thiserror::Error;

mod github;
mod state;
mod telegram;
mod tmdb;

use crate::config::{ChatConfig, TelegramConfig};
use crate::github::artifacts::{GitHubArtifactsClient, GitHubCredentials};
use crate::orchestrator::{Orchestrator, OrchestratorError};
use crate::state::SentHistory;
use crate::telegram::TelegramDispatcher;
use crate::tmdb::TmdbClient;
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
    Orchestrator(#[from] OrchestratorError),
    Tmdb(#[from] tmdb::TmdbError),
    #[error(transparent)]
    Telegram(#[from] telegram::TelegramError),
    #[error(transparent)]
    TelegramConfig(#[from] telegram::ConfigError),
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let config = AppConfig::from_env()?;
    let mut orchestrator = config.build_orchestrator()?;

    let now = Utc::now();
    let summary = orchestrator.run(now).await?;

    println!("{}", summary.render_markdown());

    Ok(())
}

fn required_env(name: &str) -> Result<String, AppError> {
    env::var(name).map_err(|_| AppError::MissingEnv(name.to_owned()))
}

#[derive(Debug, Clone)]
struct AppConfig {
    tmdb_api_key: String,
    telegram_token: String,
    telegram_chats: Vec<i64>,
    github_repo: String,
    github_token: String,
}

impl AppConfig {
    fn from_env() -> Result<Self, AppError> {
        let tmdb_api_key = required_env("TMDB_API_KEY")?;
        let telegram_token = required_env("TELEGRAM_BOT_TOKEN")?;
        let telegram_chats = parse_chat_ids(&required_env("TELEGRAM_CHAT_ID")?)?;
        let github_repo = required_env("GITHUB_REPOSITORY")?;
        let github_token = required_env("GITHUB_TOKEN")?;

        Ok(Self {
            tmdb_api_key,
            telegram_token,
            telegram_chats,
            github_repo,
            github_token,
        })
    }

    fn build_orchestrator(
        self,
    ) -> Result<Orchestrator<GitHubArtifactsClient, TmdbClient, TelegramDispatcher>, AppError> {
        let creds = github_credentials_from_env(&self.github_repo, &self.github_token)?;
        let history = SentHistory::new(HISTORY_FILE_PATH, HISTORY_ARTIFACT_NAME, creds)?;

        let telegram_config = TelegramConfig {
            chats: self
                .telegram_chats
                .iter()
                .copied()
                .map(|chat_id| ChatConfig {
                    chat_id,
                    locales: Vec::new(),
                })
                .collect(),
        };

        let tmdb_client = TmdbClient::new(self.tmdb_api_key, std::iter::empty());
        let dispatcher = TelegramDispatcher::new(self.telegram_token, self.telegram_chats.clone());

        Ok(Orchestrator::new(
            history,
            tmdb_client,
            dispatcher,
            telegram_config,
        ))
    }
}

fn github_credentials_from_env(repo: &str, token: &str) -> Result<GitHubCredentials, AppError> {
    let (owner, name) = repo
        .split_once('/')
        .ok_or_else(|| AppError::InvalidRepositoryFormat(repo.to_owned()))?;

    Ok(GitHubCredentials::new(owner, name, token))
}

fn parse_chat_ids(raw: &str) -> Result<Vec<i64>, AppError> {
    let mut ids = Vec::new();
    for value in raw.split(',') {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }

        let id: i64 = trimmed
            .parse()
            .map_err(|_| AppError::InvalidChatId(trimmed.to_owned()))?;
        ids.push(id);
    }

    if ids.is_empty() {
        return Err(AppError::InvalidChatId(raw.to_owned()));
    }

    Ok(ids)
}
