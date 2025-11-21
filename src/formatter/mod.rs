use std::time::{Duration, SystemTime};

use crate::{config::TelegramConfig, state::MovieId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigitalRelease {
    pub id: MovieId,
    pub title: String,
    pub release_time: SystemTime,
    pub display_date: String,
    pub locale: String,
    pub platforms: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatRelease {
    pub title: String,
    pub release_time: SystemTime,
    pub display_date: String,
    pub platforms: Vec<String>,
    pub priority: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatPayload {
    pub chat_id: i64,
    pub releases: Vec<ChatRelease>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelegramMessage {
    pub chat_id: i64,
    pub text: String,
    pub parse_mode: &'static str,
    pub disable_web_page_preview: bool,
}

impl TelegramMessage {
    const PARSE_MODE: &'static str = "MarkdownV2";

    pub fn new(chat_id: i64, text: String) -> Self {
        Self {
            chat_id,
            text,
            parse_mode: Self::PARSE_MODE,
            disable_web_page_preview: true,
        }
    }
}

pub fn group_releases_by_chat(
    releases: &[DigitalRelease],
    config: &TelegramConfig,
    now: SystemTime,
) -> Vec<ChatPayload> {
    let mut payloads = Vec::new();

    for chat in &config.chats {
        let mut chat_releases: Vec<ChatRelease> = releases
            .iter()
            .filter(|release| chat.matches_locale(&release.locale))
            .map(|release| ChatRelease {
                title: release.title.clone(),
                release_time: release.release_time,
                display_date: release.display_date.clone(),
                platforms: release.platforms.clone(),
                priority: is_priority(release.release_time, now),
            })
            .collect();

        if chat_releases.is_empty() {
            continue;
        }

        chat_releases.sort_by(|a, b| match b.priority.cmp(&a.priority) {
            std::cmp::Ordering::Equal => match a.release_time.cmp(&b.release_time) {
                std::cmp::Ordering::Equal => a.title.cmp(&b.title),
                other => other,
            },
            other => other,
        });

        payloads.push(ChatPayload {
            chat_id: chat.chat_id,
            releases: chat_releases,
        });
    }

    payloads
}

fn is_priority(release_time: SystemTime, now: SystemTime) -> bool {
    match now.duration_since(release_time) {
        Ok(elapsed) => elapsed < Duration::from_secs(24 * 60 * 60),
        Err(_) => false,
    }
}

pub fn build_messages(
    releases: &[DigitalRelease],
    config: &TelegramConfig,
    now: SystemTime,
) -> Vec<TelegramMessage> {
    group_releases_by_chat(releases, config, now)
        .into_iter()
        .map(|payload| {
            let mut lines = vec!["*Новые цифровые релизы*".to_string()];
            for release in payload.releases {
                let marker = if release.priority { "🔥" } else { "•" };
                let title = escape_markdown_v2(&release.title);
                let date = release.display_date.clone();
                let platforms = if release.platforms.is_empty() {
                    "—".to_string()
                } else {
                    release
                        .platforms
                        .iter()
                        .map(|platform| escape_markdown_v2(platform))
                        .collect::<Vec<_>>()
                        .join(", ")
                };

                lines.push(format!("{marker} *{title}* — `{date}` \\({platforms}\\)"));
            }

            TelegramMessage::new(payload.chat_id, lines.join("\n"))
        })
        .collect()
}

pub fn escape_markdown_v2(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '_' | '*' | '[' | ']' | '(' | ')' | '~' | '`' | '>' | '#' | '+' | '-' | '=' | '|'
            | '{' | '}' | '.' | '!' | '\\' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChatConfig;

    fn test_config() -> TelegramConfig {
        TelegramConfig {
            chats: vec![ChatConfig {
                chat_id: 1,
                locales: vec!["ru".to_string()],
            }],
        }
    }

    fn sample_release(
        now: SystemTime,
        offset_hours: u64,
        title: &str,
        id: MovieId,
    ) -> DigitalRelease {
        DigitalRelease {
            id,
            title: title.to_string(),
            release_time: now - Duration::from_secs(offset_hours * 3600),
            display_date: "01.01.2024 10:00".to_string(),
            locale: "ru".to_string(),
            platforms: vec!["Kinopoisk".to_string()],
        }
    }

    #[test]
    fn empty_releases_produce_no_messages() {
        let config = test_config();
        let now = SystemTime::now();
        let messages = build_messages(&[], &config, now);
        assert!(messages.is_empty());
    }

    #[test]
    fn priority_releases_sorted_and_marked() {
        let config = test_config();
        let now = SystemTime::now();
        let releases = vec![
            sample_release(now, 30, "Далекий релиз", 1),
            sample_release(now, 2, "Свежий релиз", 2),
        ];

        let messages = build_messages(&releases, &config, now);
        assert_eq!(messages.len(), 1);
        let text = &messages[0].text;
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines[1].starts_with("🔥"));
        assert!(lines[2].starts_with("•"));
    }

    #[test]
    fn markdown_escape_is_correct() {
        let source = "Спецсимволы _[]()<>#-=";
        let escaped = escape_markdown_v2(source);
        assert_eq!(
            escaped, "Спецсимволы \\_\\[\\]\\(\\)<\\>\\#\\-\\=",
            "Must escape Markdown V2 characters"
        );
    }
}
