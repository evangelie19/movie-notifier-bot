mod config;
mod formatter;

use std::time::SystemTime;
mod github;
mod state;
mod tmdb;

fn main() {
    let config = config::TelegramConfig::default();
    let _ = formatter::build_messages(&[], &config, SystemTime::now());
}
