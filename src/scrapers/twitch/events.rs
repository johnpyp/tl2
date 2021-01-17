use chrono::Utc;
use humantime::format_duration;
use twitch_irc::message::{
    ClearChatAction, ClearChatMessage, HostTargetAction, HostTargetMessage, PrivmsgMessage,
    UserNoticeEvent, UserNoticeMessage,
};

use crate::events::{SimpleMessage, SimpleMessageGroup, Usernames};

#[derive(Clone, Debug)]
pub enum TwitchEvent {
    HostTarget(HostTargetMessage),
    Privmsg(PrivmsgMessage),
    UserNotice(UserNoticeMessage),
    ClearChat(ClearChatMessage),
}

impl From<TwitchEvent> for SimpleMessageGroup {
    fn from(events: TwitchEvent) -> Self {
        use TwitchEvent::*;
        match events {
            HostTarget(m) => m.into(),
            Privmsg(m) => m.into(),
            UserNotice(m) => m.into(),
            ClearChat(m) => m.into(),
        }
    }
}

impl From<HostTargetMessage> for SimpleMessageGroup {
    fn from(msg: HostTargetMessage) -> Self {
        match msg.action {
            HostTargetAction::HostModeOn {
                viewer_count,
                hosted_channel_login,
            } => Some(SimpleMessage {
                id: None,
                channel: hosted_channel_login,
                timestamp: Utc::now(),
                username: Usernames::Host,
                text: format!(
                    "{} just raided the channel with {} viewers!",
                    msg.channel_login,
                    viewer_count.unwrap_or(0)
                ),
            }),

            _ => None,
        }
        .into()
    }
}
impl From<PrivmsgMessage> for SimpleMessageGroup {
    fn from(msg: PrivmsgMessage) -> Self {
        let mut messages: Vec<SimpleMessage> = Vec::new();

        messages.push(SimpleMessage {
            id: Some(msg.message_id),
            timestamp: msg.server_timestamp,
            channel: msg.channel_login.clone(),
            username: Usernames::Normal(msg.sender.login.clone()),
            text: msg.message_text,
        });

        if let Some(bits) = msg.bits {
            messages.push(SimpleMessage {
                id: None,
                timestamp: msg.server_timestamp,
                channel: msg.channel_login.clone(),
                username: Usernames::Bits,
                text: format!(
                    "{} donated {} bits to the channel!",
                    &msg.sender.login, bits
                ),
            })
        }
        SimpleMessageGroup(messages)
    }
}

impl From<ClearChatMessage> for SimpleMessageGroup {
    fn from(msg: ClearChatMessage) -> Self {
        let text = match msg.action {
            ClearChatAction::UserBanned { user_login, .. } => {
                format!("{} permanently banned", user_login)
            }
            ClearChatAction::UserTimedOut {
                user_login,
                timeout_length,
                ..
            } => format!(
                "{} timed out for {}",
                user_login,
                format_duration(timeout_length).to_string()
            ),
            _ => return None.into(),
        };
        SimpleMessage {
            id: None,
            channel: msg.channel_login.clone(),
            timestamp: msg.server_timestamp,
            username: Usernames::Moderation,
            text,
        }
        .into()
    }
}

impl From<UserNoticeMessage> for SimpleMessageGroup {
    fn from(msg: UserNoticeMessage) -> Self {
        let mut messages: Vec<SimpleMessage> = Vec::new();

        let easy_transform =
            |msg: &UserNoticeMessage, username: Usernames, text: String| SimpleMessage {
                id: Some(msg.message_id.clone()),
                channel: msg.channel_login.clone(),
                timestamp: msg.server_timestamp,
                username,
                text,
            };

        let tiers_format = |sub_plan| match sub_plan {
            "Prime" => "Prime",
            "1000" => "Tier 1",
            "2000" => "Tier 2",
            "3000" => "Tier 3",
            _ => sub_plan,
        };
        match msg.event.clone() {
            UserNoticeEvent::SubOrResub {
                sub_plan,
                cumulative_months,
                is_resub,
                ..
            } => {
                let sub_verb = match is_resub {
                    true => "resubscribed",
                    false => "subscribed",
                };

                messages.push(easy_transform(
                    &msg,
                    Usernames::Subscriber,
                    format!(
                        "{} just {} {} {} for {} months!{}",
                        msg.sender.login,
                        sub_verb,
                        if sub_plan == "Prime" { "with" } else { "at" },
                        tiers_format(&sub_plan),
                        cumulative_months,
                        if let Some(text) = msg.message_text.clone() {
                            format!(" Message: {}", text)
                        } else {
                            "".to_string()
                        }
                    ),
                ))
            }
            UserNoticeEvent::SubGift {
                recipient,
                sub_plan,
                ..
            } => messages.push(easy_transform(
                &msg,
                Usernames::Subscriber,
                format!(
                    "{} gifted a {} sub to {}!",
                    msg.sender.login,
                    tiers_format(&sub_plan),
                    recipient.login
                ),
            )),
            UserNoticeEvent::AnonSubMysteryGift {
                mass_gift_count,
                sub_plan,
            }
            | UserNoticeEvent::SubMysteryGift {
                mass_gift_count,
                sub_plan,
                ..
            } => messages.push(easy_transform(
                &msg,
                Usernames::GiftSub,
                format!(
                    "{} gifted {} {} subs to the community!",
                    msg.sender.login,
                    mass_gift_count,
                    tiers_format(&sub_plan)
                ),
            )),
            UserNoticeEvent::Raid { viewer_count, .. } => messages.push(easy_transform(
                &msg,
                Usernames::Raid,
                format!(
                    "{} just raided the channel with {} viewers!",
                    msg.sender.login, viewer_count
                ),
            )),
            UserNoticeEvent::GiftPaidUpgrade { gifter_name, .. } => messages.push(easy_transform(
                &msg,
                Usernames::Subscriber,
                format!(
                    "{} is continuing their gifted sub from {}",
                    msg.sender.login, gifter_name
                ),
            )),
            UserNoticeEvent::AnonGiftPaidUpgrade { .. } => messages.push(easy_transform(
                &msg,
                Usernames::Subscriber,
                format!(
                    "{} is continuing their anonymous gifted sub!",
                    msg.sender.login,
                ),
            )),
            UserNoticeEvent::Ritual { ritual_name: _ } => {}
            UserNoticeEvent::BitsBadgeTier { threshold: _ } => {}
            _ => {}
        };
        messages.push(SimpleMessage {
            id: None,
            channel: msg.channel_login,
            timestamp: msg.server_timestamp,
            username: Usernames::System,
            text: msg.system_message,
        });
        SimpleMessageGroup(messages)
    }
}
