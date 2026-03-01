#![allow(dead_code)]

use std::cmp::Ordering;

use chrono::{Datelike, NaiveDate};

use crate::config::TelegramConfig;

const TELEGRAM_MESSAGE_LIMIT: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReleaseKind {
    Movie,
    TvPremiere,
    TvSeason { season_number: u32 },
}

#[derive(Clone, Debug, PartialEq)]
pub struct DigitalRelease {
    pub id: u64,
    pub title: String,
    pub event_date: NaiveDate,
    pub locale: String,
    pub kind: ReleaseKind,
    pub vote_average: Option<f64>,
    pub vote_count: Option<u32>,
    pub event_key: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChatRelease {
    pub title: String,
    pub event_date: NaiveDate,
    pub kind: ReleaseKind,
    pub vote_average: Option<f64>,
    pub vote_count: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChatPayload {
    pub chat_id: i64,
    pub releases: Vec<ChatRelease>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TelegramMessage {
    pub chat_id: i64,
    pub text: String,
    pub disable_web_page_preview: bool,
}

impl TelegramMessage {
    pub fn new(chat_id: i64, text: String) -> Self {
        Self {
            chat_id,
            text,
            disable_web_page_preview: true,
        }
    }
}

pub fn sort_releases_by_priority(releases: &mut [DigitalRelease]) {
    releases.sort_by(compare_release_priority);
}

fn compare_release_priority(a: &DigitalRelease, b: &DigitalRelease) -> Ordering {
    b.event_date
        .cmp(&a.event_date)
        .then_with(|| compare_optional_f64(b.vote_average, a.vote_average))
        .then_with(|| compare_optional_u32(b.vote_count, a.vote_count))
        .then_with(|| a.title.cmp(&b.title))
}

fn compare_chat_release_priority(a: &ChatRelease, b: &ChatRelease) -> Ordering {
    b.event_date
        .cmp(&a.event_date)
        .then_with(|| compare_optional_f64(b.vote_average, a.vote_average))
        .then_with(|| compare_optional_u32(b.vote_count, a.vote_count))
        .then_with(|| a.title.cmp(&b.title))
}

fn compare_optional_f64(left: Option<f64>, right: Option<f64>) -> Ordering {
    match (left, right) {
        (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn compare_optional_u32(left: Option<u32>, right: Option<u32>) -> Ordering {
    match (left, right) {
        (Some(a), Some(b)) => a.cmp(&b),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

pub fn group_releases_by_chat(
    releases: &[DigitalRelease],
    config: &TelegramConfig,
) -> Vec<ChatPayload> {
    let mut payloads = Vec::new();

    for chat in &config.chats {
        let mut chat_releases: Vec<ChatRelease> = releases
            .iter()
            .filter(|release| chat.matches_locale(&release.locale))
            .map(|release| ChatRelease {
                title: release.title.clone(),
                event_date: release.event_date,
                kind: release.kind.clone(),
                vote_average: release.vote_average,
                vote_count: release.vote_count,
            })
            .collect();

        if chat_releases.is_empty() {
            continue;
        }

        chat_releases.sort_by(compare_chat_release_priority);

        payloads.push(ChatPayload {
            chat_id: chat.chat_id,
            releases: chat_releases,
        });
    }

    payloads
}

pub fn build_messages(
    releases: &[DigitalRelease],
    config: &TelegramConfig,
) -> Vec<TelegramMessage> {
    group_releases_by_chat(releases, config)
        .into_iter()
        .flat_map(|payload| {
            let header = "";
            let mut lines = Vec::new();
            for release in payload.releases {
                let line = match release.kind {
                    ReleaseKind::Movie => {
                        let title = release.title;
                        let date = release.event_date.format("%Y-%m-%d").to_string();
                        format!("🔥 {title} — {date}")
                    }
                    ReleaseKind::TvPremiere => {
                        let title = release.title;
                        let year = release.event_date.year();
                        format!("📺 Премьера сериала: {title} ({year})")
                    }
                    ReleaseKind::TvSeason { season_number } => {
                        let title = release.title;
                        format!("📺 Новый сезон: {title} — сезон {season_number}")
                    }
                };
                lines.push(line);
            }

            chunk_lines(payload.chat_id, header, &lines, TELEGRAM_MESSAGE_LIMIT)
        })
        .collect()
}

pub fn build_empty_messages(config: &TelegramConfig) -> Vec<TelegramMessage> {
    let text = "Новых цифровых релизов нет.".to_string();
    config
        .chats
        .iter()
        .map(|chat| TelegramMessage::new(chat.chat_id, text.clone()))
        .collect()
}

fn chunk_lines(
    chat_id: i64,
    header: &str,
    lines: &[String],
    max_len: usize,
) -> Vec<TelegramMessage> {
    if lines.is_empty() {
        return Vec::new();
    }

    let mut messages = Vec::new();
    let mut current = header.to_string();
    for line in lines {
        let additional = if current.is_empty() {
            line.len()
        } else {
            1 + line.len()
        };
        if current.len() + additional > max_len {
            messages.push(TelegramMessage::new(chat_id, current));
            current = header.to_string();
        }

        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
    }

    if !current.is_empty() {
        messages.push(TelegramMessage::new(chat_id, current));
    }

    messages
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

    fn sample_release(now: NaiveDate, title: &str, id: u64) -> DigitalRelease {
        DigitalRelease {
            id,
            title: title.to_string(),
            event_date: now,
            locale: "ru".to_string(),
            kind: ReleaseKind::Movie,
            vote_average: Some(7.5),
            vote_count: Some(100),
            event_key: format!("movie:{id}"),
        }
    }

    #[test]
    fn empty_releases_produce_no_messages() {
        let config = test_config();
        let messages = build_messages(&[], &config);
        assert!(messages.is_empty());
    }

    #[test]
    fn releases_sorted_by_time_and_marked() {
        let config = test_config();
        let now = NaiveDate::from_ymd_opt(2024, 1, 5).expect("валидная дата");
        let releases = vec![
            sample_release(
                NaiveDate::from_ymd_opt(2024, 1, 1).expect("валидная дата"),
                "Далекий релиз",
                1,
            ),
            sample_release(now, "Свежий релиз", 2),
        ];

        let messages = build_messages(&releases, &config);
        assert_eq!(messages.len(), 1);
        let text = &messages[0].text;
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines[0].starts_with("🔥"));
        assert!(lines[1].starts_with("🔥"));
        assert!(lines[0].contains("Свежий релиз"));
    }

    #[test]
    fn tv_release_format_is_correct() {
        let config = test_config();
        let date = NaiveDate::from_ymd_opt(2024, 6, 1).expect("валидная дата");
        let release = DigitalRelease {
            id: 10,
            title: "Сериал".to_string(),
            event_date: date,
            locale: "ru".to_string(),
            kind: ReleaseKind::TvSeason { season_number: 2 },
            vote_average: None,
            vote_count: None,
            event_key: "tv:10:season:2".to_string(),
        };

        let messages = build_messages(&[release], &config);
        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .text
                .contains("📺 Новый сезон: Сериал — сезон 2")
        );
    }

    #[test]
    fn empty_message_is_plain_text_without_escaping() {
        let config = test_config();
        let messages = build_empty_messages(&config);

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].text, "Новых цифровых релизов нет.");
    }

    #[test]
    fn messages_are_chunked_by_limit() {
        let lines = vec![
            "1234567".to_string(),
            "7654321".to_string(),
            "abcdefg".to_string(),
        ];
        let messages = chunk_lines(1, "Header", &lines, 20);
        assert!(messages.len() > 1, "Сообщения должны быть разбиты");
        for message in messages {
            assert!(
                message.text.len() <= 20,
                "Сообщение не должно превышать лимит"
            );
        }
    }
}
