mod config;
mod formatter;

use std::time::SystemTime;

fn main() {
    let config = config::TelegramConfig::default();
    let _ = formatter::build_messages(&[], &config, SystemTime::now());
}
