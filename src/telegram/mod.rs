use std::{collections::HashSet, env, time::Duration};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::sleep;
use tracing::warn;

const TELEGRAM_BASE_URL: &str = "https://api.telegram.org";
const DEFAULT_MAX_RETRIES: usize = 3;
const DEFAULT_RETRY_DELAYS: &[u64] = &[5, 15, 30];

#[derive(Debug, Clone)]
pub struct TelegramDispatcher {
    client: Client,
    chat_ids: HashSet<i64>,
    token: String,
    api_host: String,
    retry_delays: Vec<Duration>,
    max_retries: usize,
}

impl TelegramDispatcher {
    pub fn new(token: impl Into<String>, chat_ids: Vec<i64>) -> Self {
        Self::builder(token, chat_ids).build()
    }

    pub fn builder(token: impl Into<String>, chat_ids: Vec<i64>) -> TelegramDispatcherBuilder {
        TelegramDispatcherBuilder::new(token.into(), chat_ids)
    }

    pub async fn send_batch<S, I>(&self, chat_id: i64, messages: I) -> Result<(), TelegramError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        if !self.chat_ids.contains(&chat_id) {
            return Err(TelegramError::UnknownChat(chat_id));
        }

        for message in messages {
            let text = message.into();
            let text = text.trim().to_owned();
            if text.is_empty() {
                continue;
            }
            self.send_single(chat_id, text).await?;
        }

        Ok(())
    }

    fn endpoint(&self, method: &str) -> String {
        format!("{}/bot{}/{}", self.api_host, self.token, method)
    }

    async fn send_single(&self, chat_id: i64, text: String) -> Result<(), TelegramError> {
        let payload = SendMessageRequest { chat_id, text };
        let url = self.endpoint("sendMessage");
        let mut retries = 0usize;

        loop {
            let response = self.client.post(url.clone()).json(&payload).send().await?;

            if response.status().is_success() {
                return Ok(());
            }

            let status = response.status();
            let body = response.text().await.unwrap_or_else(|err| {
                warn!(?err, "Не удалось прочитать тело ответа Telegram API");
                String::new()
            });

            warn!(
                target: "telegram_dispatcher",
                chat_id,
                status = %status,
                body = %body,
                "Неуспешный ответ Telegram API"
            );

            if status == StatusCode::TOO_MANY_REQUESTS && retries < self.max_retries {
                retries += 1;
                let delay = parse_retry_after(&body).unwrap_or_else(|| Duration::from_secs(1));
                sleep(delay).await;
                continue;
            }

            if status.is_server_error() && retries < self.max_retries {
                let delay = self.retry_delay_for(retries);
                retries += 1;
                sleep(delay).await;
                continue;
            }

            return Err(TelegramError::Api { status, body });
        }
    }

    fn retry_delay_for(&self, attempt: usize) -> Duration {
        self.retry_delays
            .get(attempt)
            .copied()
            .or_else(|| self.retry_delays.last().copied())
            .unwrap_or_else(|| Duration::from_secs(1))
    }
}

#[allow(dead_code)]
pub struct TelegramDispatcherBuilder {
    token: String,
    chat_ids: Vec<i64>,
    base_url: String,
    client: Client,
    retry_delays: Vec<Duration>,
    max_retries: usize,
}

#[allow(dead_code)]
impl TelegramDispatcherBuilder {
    fn new(token: String, chat_ids: Vec<i64>) -> Self {
        Self {
            token,
            chat_ids,
            base_url: TELEGRAM_BASE_URL.to_owned(),
            client: Client::new(),
            retry_delays: DEFAULT_RETRY_DELAYS
                .iter()
                .copied()
                .map(Duration::from_secs)
                .collect(),
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }

    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    pub fn retry_delays(mut self, delays: Vec<Duration>) -> Self {
        if delays.is_empty() {
            self.retry_delays = vec![Duration::from_secs(1)];
        } else {
            self.retry_delays = delays;
        }
        self
    }

    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn build(self) -> TelegramDispatcher {
        let sanitized_base = self.base_url.trim_end_matches('/').to_owned();
        TelegramDispatcher {
            client: self.client,
            chat_ids: self.chat_ids.into_iter().collect(),
            token: self.token,
            api_host: sanitized_base,
            retry_delays: self.retry_delays,
            max_retries: self.max_retries,
        }
    }
}

#[derive(Debug, Error)]
pub enum TelegramError {
    #[error("неизвестный чат {0}")]
    UnknownChat(i64),
    #[error("ошибка HTTP клиента: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("ошибка Telegram API: {status} — {body}")]
    Api { status: StatusCode, body: String },
}

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("не задан токен Telegram в переменной {var}")]
    MissingToken { var: String },
    #[error("не задана переменная TELEGRAM_CHAT_ID")]
    MissingChatIds,
    #[error("некорректное значение TELEGRAM_CHAT_ID: {value}")]
    InvalidChatId { value: String },
    #[error("список TELEGRAM_CHAT_ID пустой")]
    EmptyChatIds,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotEnvironment {
    Dev,
    Prod,
}

impl BotEnvironment {
    pub fn from_env() -> Self {
        let raw = env::var("BOT_ENV").unwrap_or_else(|_| "dev".to_owned());
        match raw.to_lowercase().as_str() {
            "prod" => Self::Prod,
            _ => Self::Dev,
        }
    }

    pub fn token_var(&self) -> &'static str {
        match self {
            Self::Dev => "TELEGRAM_BOT_TOKEN_DEV",
            Self::Prod => "TELEGRAM_BOT_TOKEN_PROD",
        }
    }
}

#[allow(dead_code)]
pub fn dispatcher_from_env() -> Result<TelegramDispatcher, ConfigError> {
    let token = read_token_from_env()?;
    let chat_ids = read_chat_ids_from_env()?;

    Ok(TelegramDispatcher::new(token, chat_ids))
}

pub fn read_token_from_env() -> Result<String, ConfigError> {
    let environment = BotEnvironment::from_env();
    let token_var = environment.token_var();
    env::var(token_var).map_err(|_| ConfigError::MissingToken {
        var: token_var.to_owned(),
    })
}

pub fn read_chat_ids_from_env() -> Result<Vec<i64>, ConfigError> {
    let raw = env::var("TELEGRAM_CHAT_ID").map_err(|_| ConfigError::MissingChatIds)?;
    parse_chat_ids(&raw)
}

pub fn parse_chat_ids(raw: &str) -> Result<Vec<i64>, ConfigError> {
    let mut ids = Vec::new();

    for value in raw.split(',') {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }

        let id: i64 = trimmed.parse().map_err(|_| ConfigError::InvalidChatId {
            value: trimmed.to_owned(),
        })?;
        ids.push(id);
    }

    if ids.is_empty() {
        return Err(ConfigError::EmptyChatIds);
    }

    Ok(ids)
}

#[derive(Debug, Deserialize)]
struct TelegramErrorResponse {
    parameters: Option<TelegramErrorParameters>,
}

#[derive(Debug, Deserialize)]
struct TelegramErrorParameters {
    #[serde(rename = "retry_after")]
    retry_after: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SendMessageRequest {
    chat_id: i64,
    text: String,
}

fn parse_retry_after(body: &str) -> Option<Duration> {
    serde_json::from_str::<TelegramErrorResponse>(body)
        .ok()
        .and_then(|resp| resp.parameters?.retry_after)
        .map(Duration::from_secs)
}
