use std::{str::FromStr, string::ToString};

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawUser {
    pub nick: String,
    pub features: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(from = "RawUser")]
pub struct User {
    /// The username of the message sender
    pub username: String,
    /// Present if the user is subscribed, and denotes the tier.
    pub subscription: Option<Subscriptions>,
    /// Whether or not the user is subscribed
    pub is_subscriber: bool,
    /// Whether or not the sending user is registered as a bot
    pub is_bot: bool,
    /// Whether or not the sending user is "protected"
    pub is_protected: bool,
    /// Whether or not the sending user is a VIP
    pub is_vip: bool,
    /// Whether or not the sending user is a recognized broadcaster
    pub is_broadcaster: bool,
    /// Whether or not the sending user is an admin
    pub is_admin: bool,
    /// Whether or not the sending user is a moderator
    pub is_moderator: bool,
    /// Other unhandled or unknown flairs
    pub flairs: Vec<Flairs>,
}

impl From<RawUser> for User {
    fn from(raw: RawUser) -> Self {
        let flairs: Vec<Flairs> = raw
            .features
            .iter()
            .map(|s| Flairs::from_str(s).unwrap())
            .collect();
        let subscription: Option<Subscriptions> = flairs.iter().find_map(|flair| {
            Some(match flair {
                Flairs::SubTier1 => Subscriptions::Tier1,
                Flairs::SubTier2 => Subscriptions::Tier2,
                Flairs::SubTier3 => Subscriptions::Tier3,
                Flairs::SubTier4 => Subscriptions::Tier4,
                Flairs::SubTwitch => Subscriptions::Twitch,
                _ => return None,
            })
        });

        let is_bot = flairs.contains(&Flairs::Bot) || flairs.contains(&Flairs::Bot2);
        let is_vip = flairs.contains(&Flairs::Vip);
        let is_protected = flairs.contains(&Flairs::Broadcaster);
        let is_moderator = flairs.contains(&Flairs::Moderator);
        let is_admin = flairs.contains(&Flairs::Admin);
        let is_broadcaster = flairs.contains(&Flairs::Broadcaster);

        User {
            username: raw.nick,
            is_subscriber: subscription.is_some(),
            is_bot,
            is_vip,
            is_protected,
            is_moderator,
            is_admin,
            is_broadcaster,
            flairs,
            subscription,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Display, EnumString, Serialize)]
#[serde(into = "String")]
pub enum Flairs {
    #[strum(serialize = "moderator")]
    Moderator,
    #[strum(serialize = "protected")]
    Protected,
    #[strum(serialize = "subscriber")]
    Subscriber,
    #[strum(serialize = "admin")]
    Admin,
    #[strum(serialize = "flair12")]
    Broadcaster,
    #[strum(serialize = "vip")]
    Vip,
    #[strum(serialize = "bot")]
    Bot,
    #[strum(serialize = "flair11")]
    Bot2,
    #[strum(serialize = "flair13")]
    SubTier1,
    #[strum(serialize = "flair1")]
    SubTier2,
    #[strum(serialize = "flair3")]
    SubTier3,
    #[strum(serialize = "flair8")]
    SubTier4,
    #[strum(serialize = "flair9")]
    SubTwitch,
    #[strum(serialize = "flair5")]
    Contributor,
    #[strum(serialize = "flair16")]
    EmoteContributor,
    #[strum(serialize = "flair2")]
    Notable,
    #[strum(serialize = "flair4")]
    Trusted,

    #[strum(serialize = "flair17")]
    Micro,
    #[strum(serialize = "flair7")]
    NFLAndy,
    #[strum(serialize = "flair20")]
    Verified,
    #[strum(serialize = "flair25")]
    YoutubeContributor,
    #[strum(serialize = "flair26")]
    DndKnightParty,
    #[strum(serialize = "flair24")]
    DndKnightScoria,
    #[strum(serialize = "flair23")]
    DndBaronBlue,
    #[strum(serialize = "flair22")]
    DndBaronGold,
    #[strum(serialize = "flair21")]
    YoutubeEditor,
    #[strum(serialize = "flair18")]
    EmoteMaster,
    #[strum(serialize = "flair19")]
    DggShirtDesigner,
    #[strum(serialize = "flair15")]
    DggBirthday,
    #[strum(serialize = "flair14")]
    MinecraftVIP,
    #[strum(serialize = "flair10")]
    Starcraft2,
    #[strum(serialize = "flair6")]
    CompositionWinner,

    #[strum(default)]
    Unknown(String),
}

impl From<Flairs> for String {
    fn from(flair: Flairs) -> Self {
        flair.to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Subscriptions {
    #[serde(rename = "twitch")]
    Twitch,
    #[serde(rename = "tier1")]
    Tier1,
    #[serde(rename = "tier2")]
    Tier2,
    #[serde(rename = "tier3")]
    Tier3,
    #[serde(rename = "tier4")]
    Tier4,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{RawUser, Subscriptions, User};

    #[test]
    fn test_deserialization_to_raw_user() {
        let string_user = json!({
            "nick": "nickname",
            "features": ["flair3", "moderator"]
        });

        let raw_user: RawUser = serde_json::from_value(string_user).unwrap();
        assert_eq!(raw_user.nick, "nickname");
        assert_eq!(raw_user.features, vec!["flair3", "moderator"])
    }

    #[test]
    fn test_raw_user_to_user() {
        let raw_user = RawUser {
            features: vec!["flair3".to_string(), "moderator".to_string()],
            nick: "nickname".to_string(),
        };

        let user: User = raw_user.into();

        assert!(!user.is_bot);
        assert!(user.is_moderator);
        assert!(user.is_subscriber);
        assert_eq!(user.subscription, Some(Subscriptions::Tier3));
    }
}
