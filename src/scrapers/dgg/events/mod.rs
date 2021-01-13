mod broadcast;
mod join;
mod message;
mod moderation;
mod names;
mod quit;
mod shared;

pub use broadcast::*;
use enum_dispatch::enum_dispatch;
pub use join::*;
pub use message::*;
pub use moderation::*;
pub use names::*;
pub use quit::*;
pub use shared::*;

use crate::events::SimpleMessageGroup;

#[enum_dispatch]
#[derive(Clone, Debug)]
enum Events {
    Broadcast(Broadcast),
    Join(Join),
    Message(Message),
    Moderation(Moderation),
    Names(Names),
    Quit(Quit),
}

#[derive(Clone, Debug)]
pub struct DggEvent {
    channel: String,
    event: Events,
}

#[enum_dispatch(Events)]
trait DggAsSimpleMessageGroup {
    fn as_group(&self, channel: String) -> SimpleMessageGroup;
}

impl From<DggEvent> for SimpleMessageGroup {
    fn from(dgg_event: DggEvent) -> Self {
        dgg_event.event.as_group(dgg_event.channel)
    }
}

impl DggEvent {
    pub fn from_ws(raw: String, channel: String) -> serde_json::Result<Option<DggEvent>> {
        let split: Vec<&str> = raw.splitn(2, ' ').collect();
        if split.len() < 2 {
            return Ok(None);
        }
        let (event_type, body) = (split[0], split[1]);
        let event = match event_type {
            "BROADCAST" => Events::Broadcast(serde_json::from_str(body)?),
            "MSG" => Events::Message(serde_json::from_str(body)?),
            "MUTE" | "UNMUTE" | "BAN" | "UNBAN" => {
                let raw: RawModeration = serde_json::from_str(body)?;
                Events::Moderation(
                    WithEventType {
                        value: raw,
                        event_type: event_type.to_string(),
                    }
                    .into(),
                )
            }
            "NAMES" => Events::Names(serde_json::from_str(body)?),
            "JOIN" => Events::Join(serde_json::from_str(body)?),
            "QUIT" => Events::Quit(serde_json::from_str(body)?),
            _ => return Ok(None),
        };

        Ok(Some(DggEvent { event, channel }))
    }
}
