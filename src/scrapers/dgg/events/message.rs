use chrono::{serde::ts_milliseconds, DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{shared::user::User, DggAsSimpleMessageGroup};
use crate::events::{SimpleMessage, SimpleMessageGroup, Usernames};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// The user sending the message
    #[serde(flatten)]
    pub user: User,

    /// Text of the message
    #[serde(rename(deserialize = "data"))]
    pub text: String,

    /// Milliseconds event timestamp
    #[serde(with = "ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

impl DggAsSimpleMessageGroup for Message {
    fn as_group(&self, channel: String) -> SimpleMessageGroup {
        SimpleMessage {
            id: None,
            channel,
            username: Usernames::Normal(self.user.username.clone()),
            timestamp: self.timestamp,
            text: self.text.clone(),
        }
        .into()
    }
}
