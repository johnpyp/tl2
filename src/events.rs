use std::fmt;

use chrono::{DateTime, Utc};
use colored::Colorize;
use derive_more::From;
use voca_rs::*;

use crate::scrapers::{dgg::DggEvent, twitch::events::TwitchEvent};

#[derive(Clone, Debug, From)]
pub enum AllEvents {
    Dgg(DggEvent),
    Twitch(TwitchEvent),
}

#[derive(Clone, Debug)]
pub struct SimpleMessage {
    pub id: Option<String>,
    pub channel: String,
    pub timestamp: DateTime<Utc>,
    pub username: Usernames,
    pub text: String,
}

impl SimpleMessage {
    pub fn normalize(&self) -> Self {
        let mut username = self.username.clone();
        if let Usernames::Normal(s) = username {
            username = Usernames::Normal(s.to_string().trim().to_lowercase());
        }

        Self {
            channel: case::capitalize(self.channel.trim(), true),
            username,
            text: self.text.trim().to_string(),
            ..self.clone()
        }
    }
}

#[derive(Clone, Debug)]
pub struct SimpleMessageGroup(pub Vec<SimpleMessage>);

impl From<SimpleMessage> for SimpleMessageGroup {
    fn from(msg: SimpleMessage) -> Self {
        return SimpleMessageGroup(vec![msg]);
    }
}

impl From<Option<SimpleMessage>> for SimpleMessageGroup {
    fn from(opt: Option<SimpleMessage>) -> Self {
        return SimpleMessageGroup(match opt {
            Some(msg) => vec![msg],
            None => Vec::new(),
        });
    }
}

impl From<AllEvents> for SimpleMessageGroup {
    fn from(event: AllEvents) -> Self {
        use AllEvents::*;
        match event {
            Dgg(e) => e.into(),
            Twitch(e) => e.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Usernames {
    Normal(String),
    System,
    Bits,
    Subscriber,
    GiftSub,
    Raid,
    Host,
    Moderation,
}

impl fmt::Display for Usernames {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let res = match self {
            Usernames::Normal(u) => u.trim(),
            Usernames::System => "@system",
            Usernames::Bits => "@bits",
            Usernames::Subscriber => "@subscriber",
            Usernames::GiftSub => "@giftsub",
            Usernames::Raid => "@raid",
            Usernames::Host => "@host",
            Usernames::Moderation => "@moderation",
        };
        write!(f, "{}", res)?;
        Ok(())
    }
}

impl From<Usernames> for colored::ColoredString {
    fn from(username: Usernames) -> colored::ColoredString {
        let s = username.clone().to_string();
        return match username {
            Usernames::Normal(u) => u.trim().bright_blue(),
            Usernames::System => s.bright_yellow(),
            Usernames::Bits => s.bold(),
            Usernames::Subscriber => s.bright_green(),
            Usernames::GiftSub => s.bright_magenta(),
            Usernames::Raid => s.bright_cyan(),
            Usernames::Host => s.bright_cyan(),
            Usernames::Moderation => s.bright_red(),
        };
    }
}
