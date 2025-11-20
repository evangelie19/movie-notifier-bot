mod config;
mod formatter;

use std::time::SystemTime;
mod tmdb;
mod github;
mod state;

fn main() {
    let config = config::TelegramConfig::default();
    let _ = formatter::build_messages(&[], &config, SystemTime::now());
}
