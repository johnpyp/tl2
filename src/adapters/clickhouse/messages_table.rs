use std::convert::TryFrom;

use anyhow::Result;
use clickhouse::{Client, Row};
use log::debug;
use serde::Deserialize;
use serde::Serialize;
use twitch_irc::message::PrivmsgMessage;

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct ClickhouseMessage {
    pub ts: i64,
    pub channel: String,
    pub channel_id: u64,
    pub username: String,
    pub display_name: String,
    pub text: String,
    pub user_id: u64,
    pub badges: Vec<String>,
    pub subscribed: u16,
    pub bits: u64,
    pub color: String,
}

impl TryFrom<PrivmsgMessage> for ClickhouseMessage {
    type Error = anyhow::Error;
    fn try_from(msg: PrivmsgMessage) -> Result<Self> {
        let sub_badge = msg.badge_info.iter().find(|b| b.name == "subscriber");
        let subscribed = if let Some(badge) = sub_badge {
            match badge.version.parse::<u16>().ok() {
                Some(count) => count,
                None => 0,
            }
        } else {
            0
        };
        Ok(ClickhouseMessage {
            ts: msg.server_timestamp.timestamp_millis(),
            channel: msg.channel_login,
            channel_id: msg.channel_id.parse()?,
            username: msg.sender.login,
            display_name: msg.sender.name,
            text: msg.message_text,
            user_id: msg.sender.id.parse()?,
            badges: msg.badges.iter().map(|b| b.name.clone()).collect(),
            subscribed,
            bits: msg.bits.unwrap_or(0),
            color: msg
                .name_color
                .map(|rgb| format!("#{:02x}{:02x}{:02x}", rgb.r, rgb.g, rgb.b))
                .unwrap_or_else(|| String::new()),
        })
    }
}
pub async fn create_messages(client: &Client) -> Result<()> {
    client
        .query(
            "
          CREATE TABLE IF NOT EXISTS messages (
              ts DateTime64(3) CODEC(T64, ZSTD(12)),
              channel LowCardinality(String),
              channel_id UInt64 CODEC(T64, ZSTD(12)),
              username String CODEC(ZSTD(12)),
              display_name String CODEC(ZSTD(12)),
              text String CODEC(ZSTD(14)),
              user_id UInt64 CODEC(T64, ZSTD(12)),
              badges Array(LowCardinality(String)),
              subscribed UInt16 CODEC(Gorilla, ZSTD(1)),
              bits UInt64 CODEC(Gorilla, ZSTD(1)),
              color String CODEC(ZSTD(12))
          )
          ENGINE = ReplacingMergeTree
          PARTITION BY toYYYYMM(ts)
          ORDER BY (channel, username, ts, text);",
        )
        .execute()
        .await?;

    debug!("Created clickhouse messages table");

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct ClickhouseOrlMessage {
    pub ts: i64,
    pub channel: String,
    pub username: String,
    pub text: String,
}

pub async fn create_orl_messages(client: &Client) -> Result<()> {
    client
        .query(
            "
          CREATE TABLE IF NOT EXISTS orl_messages (
              ts DateTime64(3) CODEC(T64, ZSTD(12)),
              channel LowCardinality(String),
              username String CODEC(ZSTD(12)),
              text String CODEC(ZSTD(14))
          )
          ENGINE = ReplacingMergeTree
          PARTITION BY toYYYYMM(ts)
          ORDER BY (channel, username, ts, text);",
        )
        .execute()
        .await?;

    debug!("Created clickhouse orl_messages table");

    Ok(())
}
