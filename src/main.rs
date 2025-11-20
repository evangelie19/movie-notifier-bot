use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::future::Future;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::pin::Pin;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ReleaseId(pub u64);

#[derive(Debug, Clone)]
pub struct Release {
    pub id: ReleaseId,
    pub title: String,
    pub release_date: String,
    pub chat_id: String,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub chat_id: String,
    pub text: String,
    pub release_ids: Vec<ReleaseId>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RunSummary {
    pub restored_entries: usize,
    pub fetched_releases: usize,
    pub duplicates_filtered: usize,
    pub sent_releases: usize,
    pub sent_messages: usize,
}

impl fmt::Display for RunSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Итоги запуска:")?;
        writeln!(
            f,
            "- восстановлено идентификаторов: {}",
            self.restored_entries
        )?;
        writeln!(f, "- получено релизов TMDB: {}", self.fetched_releases)?;
        writeln!(
            f,
            "- отфильтровано как дубликаты: {}",
            self.duplicates_filtered
        )?;
        writeln!(f, "- отправлено уникальных релизов: {}", self.sent_releases)?;
        writeln!(f, "- сообщений Telegram: {}", self.sent_messages)
    }
}

#[derive(Debug)]
pub enum BotError {
    Stage {
        stage: &'static str,
        message: String,
    },
}

impl BotError {
    fn stage(stage: &'static str, message: impl Into<String>) -> Self {
        Self::Stage {
            stage,
            message: message.into(),
        }
    }
}

impl fmt::Display for BotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BotError::Stage { stage, message } => {
                write!(f, "Ошибка на этапе '{stage}': {message}")
            }
        }
    }
}

impl std::error::Error for BotError {}

type BotResult<T> = Result<T, BotError>;
type BotFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait SentHistory: Send + Sync {
    fn load<'a>(&'a self) -> BotFuture<'a, BotResult<HashSet<ReleaseId>>>;
    fn save<'a>(&'a self, snapshot: &'a HashSet<ReleaseId>) -> BotFuture<'a, BotResult<()>>;
}

pub trait TmdbClient: Send + Sync {
    fn fetch_releases<'a>(&'a self) -> BotFuture<'a, BotResult<Vec<Release>>>;
}

pub trait Formatter: Send + Sync {
    fn prepare_messages(&self, releases: &[Release]) -> Vec<Message>;
}

pub trait TelegramDispatcher: Send + Sync {
    fn send<'a>(&'a self, message: &'a Message) -> BotFuture<'a, BotResult<()>>;
}

pub struct App<H, C, F, D>
where
    H: SentHistory,
    C: TmdbClient,
    F: Formatter,
    D: TelegramDispatcher,
{
    history: H,
    tmdb: C,
    formatter: F,
    dispatcher: D,
}

impl<H, C, F, D> App<H, C, F, D>
where
    H: SentHistory,
    C: TmdbClient,
    F: Formatter,
    D: TelegramDispatcher,
{
    pub fn new(history: H, tmdb: C, formatter: F, dispatcher: D) -> Self {
        Self {
            history,
            tmdb,
            formatter,
            dispatcher,
        }
    }

    pub async fn run(&self) -> BotResult<RunSummary> {
        eprintln!("▶ Этап: восстановление истории");
        let mut history = self.history.load().await?;
        let restored_entries = history.len();

        eprintln!("▶ Этап: получение релизов TMDB");
        let releases = self.tmdb.fetch_releases().await?;
        let fetched_releases = releases.len();

        eprintln!("▶ Этап: фильтрация дубликатов");
        let fresh_releases: Vec<Release> = releases
            .into_iter()
            .filter(|release| !history.contains(&release.id))
            .collect();
        let duplicates_filtered = fetched_releases.saturating_sub(fresh_releases.len());

        eprintln!("▶ Этап: подготовка сообщений");
        let messages = self.formatter.prepare_messages(&fresh_releases);
        let sent_releases = messages.iter().map(|m| m.release_ids.len()).sum();

        eprintln!("▶ Этап: отправка сообщений");
        for message in &messages {
            self.dispatcher.send(message).await?;
        }

        eprintln!("▶ Этап: сохранение истории");
        history.extend(fresh_releases.iter().map(|release| release.id));
        self.history.save(&history).await?;

        let summary = RunSummary {
            restored_entries,
            fetched_releases,
            duplicates_filtered,
            sent_releases,
            sent_messages: messages.len(),
        };

        eprintln!("▶ Этап: вывод summary");
        println!("{summary}");

        Ok(summary)
    }
}

struct FileSentHistory {
    path: PathBuf,
}

impl FileSentHistory {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl SentHistory for FileSentHistory {
    fn load<'a>(&'a self) -> BotFuture<'a, BotResult<HashSet<ReleaseId>>> {
        Box::pin(async move {
            match tokio::fs::read_to_string(&self.path).await {
                Ok(content) => Ok(parse_history(&content)),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(HashSet::new()),
                Err(err) => Err(BotError::stage(
                    "restore_history",
                    format!("не удалось прочитать файл {}: {err}", self.path.display()),
                )),
            }
        })
    }

    fn save<'a>(&'a self, snapshot: &'a HashSet<ReleaseId>) -> BotFuture<'a, BotResult<()>> {
        Box::pin(async move {
            if let Some(parent) = self.path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|err| BotError::stage("persist_history", format!("{err}")))?;
            }
            let mut ids: Vec<_> = snapshot.iter().map(|id| id.0).collect();
            ids.sort_unstable();
            let payload = ids
                .into_iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join("\n");
            tokio::fs::write(&self.path, payload)
                .await
                .map_err(|err| BotError::stage("persist_history", format!("{err}")))
        })
    }
}

fn parse_history(content: &str) -> HashSet<ReleaseId> {
    content
        .lines()
        .filter_map(|line| line.trim().parse::<u64>().ok())
        .map(ReleaseId)
        .collect()
}

struct StubTmdbClient;

impl TmdbClient for StubTmdbClient {
    fn fetch_releases<'a>(&'a self) -> BotFuture<'a, BotResult<Vec<Release>>> {
        Box::pin(async move {
            Ok(vec![
                Release {
                    id: ReleaseId(101),
                    title: "The Sample Awakens".into(),
                    release_date: "2024-05-01".into(),
                    chat_id: "@cinema_daily".into(),
                },
                Release {
                    id: ReleaseId(102),
                    title: "Rustacean Saga".into(),
                    release_date: "2024-05-01".into(),
                    chat_id: "@cinema_daily".into(),
                },
                Release {
                    id: ReleaseId(103),
                    title: "DevOps Forever".into(),
                    release_date: "2024-05-02".into(),
                    chat_id: "@premiere_club".into(),
                },
            ])
        })
    }
}

struct MarkdownFormatter;

impl Formatter for MarkdownFormatter {
    fn prepare_messages(&self, releases: &[Release]) -> Vec<Message> {
        if releases.is_empty() {
            return Vec::new();
        }
        let mut grouped: BTreeMap<String, Vec<&Release>> = BTreeMap::new();
        for release in releases {
            grouped
                .entry(release.chat_id.clone())
                .or_default()
                .push(release);
        }
        grouped
            .into_iter()
            .map(|(chat_id, items)| {
                let mut lines = Vec::with_capacity(items.len());
                let mut release_ids = Vec::with_capacity(items.len());
                for item in items {
                    lines.push(format!("• {} — {}", item.title, item.release_date));
                    release_ids.push(item.id);
                }
                Message {
                    chat_id,
                    text: format!("Новые цифровые релизы:\n{}", lines.join("\n")),
                    release_ids,
                }
            })
            .collect()
    }
}

struct ConsoleTelegramDispatcher;

impl TelegramDispatcher for ConsoleTelegramDispatcher {
    fn send<'a>(&'a self, message: &'a Message) -> BotFuture<'a, BotResult<()>> {
        Box::pin(async move {
            println!(
                "Отправка в {} ({} релизов):\n{}\n",
                message.chat_id,
                message.release_ids.len(),
                message.text
            );
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() {
    let history = FileSentHistory::new("state/sent_movie_ids.txt");
    let tmdb = StubTmdbClient;
    let formatter = MarkdownFormatter;
    let dispatcher = ConsoleTelegramDispatcher;
    let app = App::new(history, tmdb, formatter, dispatcher);

    if let Err(err) = app.run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
