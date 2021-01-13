use chrono::{serde::ts_milliseconds, DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{shared::user::User, DggAsSimpleMessageGroup};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Join {
    #[serde(flatten)]
    pub user: User,
    #[serde(with = "ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

impl DggAsSimpleMessageGroup for Join {
    fn as_group(&self, _channel: String) -> crate::events::SimpleMessageGroup {
        None.into()
    }
}
