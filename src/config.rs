#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatConfig {
    pub chat_id: i64,
    pub locales: Vec<String>,
}

impl ChatConfig {
    pub fn matches_locale(&self, locale: &str) -> bool {
        self.locales.is_empty() || self.locales.iter().any(|l| l == locale)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelegramConfig {
    pub chats: Vec<ChatConfig>,
}

impl TelegramConfig {
    pub fn single_global_chat(chat_id: i64) -> Self {
        Self {
            chats: vec![ChatConfig {
                chat_id,
                locales: Vec::new(),
            }],
        }
    }
}

impl Default for TelegramConfig {
    fn default() -> Self {
        Self::single_global_chat(-1_000_000_000_000)
    }
}
