mod config;
mod formatter;
mod github;
mod state;
mod telegram;
mod tmdb;

use std::{env, error::Error, time::SystemTime};

use chrono::{Duration, Utc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let tmdb_api_key =
        env::var("TMDB_API_KEY").map_err(|_| "Не задан ключ TMDB_API_KEY в окружении")?;
    let telegram_bot_token = env::var("TELEGRAM_BOT_TOKEN")
        .map_err(|_| "Не задан токен TELEGRAM_BOT_TOKEN в окружении")?;
    let telegram_chat_id: i64 = env::var("TELEGRAM_CHAT_ID")
        .map_err(|_| "Не задан TELEGRAM_CHAT_ID в окружении")?
        .parse()
        .map_err(|_| "TELEGRAM_CHAT_ID должен быть числом")?;

    let tmdb_client = tmdb::TmdbClient::new(tmdb_api_key, Vec::<u64>::new());
    let telegram_dispatcher =
        telegram::TelegramDispatcher::new(telegram_bot_token, vec![telegram_chat_id]);
    let telegram_config = config::TelegramConfig::single_global_chat(telegram_chat_id);

    let now = Utc::now();
    let window = tmdb::ReleaseWindow {
        start: now - Duration::hours(24) - Duration::minutes(5),
        end: now,
    };

    let releases = tmdb_client.fetch_digital_releases(window).await?;
    let formatted_releases = releases
        .into_iter()
        .map(|release| {
            let release_dt = release
                .release_date
                .and_hms_opt(0, 0, 0)
                .expect("Дата релиза должна содержать время по умолчанию");
            let release_time: SystemTime = release_dt.and_utc().into();
            let display_date = release_dt.format("%d.%m.%Y %H:%M").to_string();

            Ok(formatter::DigitalRelease {
                title: release.title,
                release_time,
                display_date,
                locale: release.original_language,
                platforms: release.watch_providers,
            })
        })
        .collect::<Result<Vec<_>, std::time::SystemTimeError>>()?;

    let messages =
        formatter::build_messages(&formatted_releases, &telegram_config, SystemTime::now());
    for message in messages {
        telegram_dispatcher
            .send_batch(message.chat_id, [message.text])
            .await?;
    }

    Ok(())
}
