use chrono::{serde::ts_milliseconds, DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::DggAsSimpleMessageGroup;
use crate::events::{SimpleMessage, Usernames};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Broadcast {
    #[serde(rename(deserialize = "data"))]
    pub description: String,
    #[serde(with = "ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

impl DggAsSimpleMessageGroup for Broadcast {
    fn as_group(&self, channel: String) -> crate::events::SimpleMessageGroup {
        SimpleMessage {
            id: None,
            channel,
            username: Usernames::System,
            timestamp: self.timestamp,
            text: self.description.to_string(),
        }
        .into()
    }
}
