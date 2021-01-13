use chrono::{serde::ts_milliseconds, DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::DggAsSimpleMessageGroup;
use crate::events::{SimpleMessage, Usernames};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawModeration {
    /// The person acted on (muted, banned, etc.)
    pub data: String,
    /// The person muting (a moderator, Bot, etc.)
    pub nick: String,
    #[serde(with = "ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

pub struct WithEventType<T> {
    pub value: T,
    pub event_type: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct Moderation {
    /// The type of moderation event
    pub moderation_type: ModerationTypes,
    /// The target of the moderation event
    pub target: String,
    /// The sender of the moderation event
    pub sender: String,
    /// Milliseconds event timestamp
    #[serde(with = "ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

impl From<WithEventType<RawModeration>> for Moderation {
    fn from(raw: WithEventType<RawModeration>) -> Self {
        let moderation_type = match raw.event_type.as_str() {
            "BAN" => ModerationTypes::Ban,
            "UNBAN" => ModerationTypes::Unban,
            "MUTE" => ModerationTypes::Mute,
            "UNMUTE" => ModerationTypes::Unmute,
            _ => ModerationTypes::Unknown,
        };
        Moderation {
            moderation_type,
            target: raw.value.data,
            sender: raw.value.nick,
            timestamp: raw.value.timestamp,
        }
    }
}

impl DggAsSimpleMessageGroup for Moderation {
    fn as_group(&self, channel: String) -> crate::events::SimpleMessageGroup {
        if let ModerationTypes::Unknown = self.moderation_type {
            return None.into();
        }

        SimpleMessage {
            id: None,
            channel,
            username: Usernames::Moderation,
            timestamp: self.timestamp,
            text: format!(
                "{} {} {}",
                self.sender,
                self.moderation_type.action_verbs(),
                self.target
            ),
        }
        .into()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ModerationTypes {
    #[serde(rename = "ban")]
    Ban,
    #[serde(rename = "unban")]
    Unban,
    #[serde(rename = "mute")]
    Mute,
    #[serde(rename = "unmute")]
    Unmute,
    #[serde(rename = "unknown", other)]
    Unknown,
}

impl ModerationTypes {
    fn action_verbs(&self) -> String {
        use ModerationTypes::*;
        match self {
            Ban => "banned",
            Unban => "unbanned",
            Mute => "muted",
            Unmute => "unmuted",
            Unknown => "did something to",
        }
        .to_string()
    }
}
