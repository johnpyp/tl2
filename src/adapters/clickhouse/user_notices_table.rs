use std::convert::TryFrom;

use anyhow::Result;
use clickhouse::{Client, Row};
use log::debug;
use serde::Deserialize;
use serde::Serialize;
use twitch_irc::message::TwitchUserBasics;
use twitch_irc::message::UserNoticeEvent;
use twitch_irc::message::UserNoticeMessage;

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct ClickhouseUserNotice {
    pub ts: i64,
    pub message_type: String,
    pub channel: String,
    pub channel_id: u64,
    pub username: String,
    pub display_name: String,
    pub user_id: u64,
    pub badges: Vec<String>,
    pub system_message: String,
    pub text: String,
    pub color: String,
    pub sub_plan: String,
    pub sub_plan_name: String,
    pub cumulative_months: u16,
    pub streak_months: u16,
    pub viewer_count: u64,
    pub gift_months: u16,
    pub recipient_username: String,
    pub recipient_display_name: String,
    pub recipient_user_id: u64,
}

impl TryFrom<UserNoticeMessage> for ClickhouseUserNotice {
    type Error = anyhow::Error;
    fn try_from(msg: UserNoticeMessage) -> Result<Self> {
        let mut sub_plan: Option<String> = None;
        let mut sub_plan_name: Option<String> = None;
        let mut cumulative_months: Option<u16> = None;
        let mut streak_months: Option<u16> = None;

        let mut viewer_count: Option<u64> = None;

        let mut gift_months: Option<u16> = None;
        let mut recipient: Option<TwitchUserBasics> = None;

        match msg.event {
            UserNoticeEvent::SubOrResub {
                sub_plan: n_sub_plan,
                sub_plan_name: n_sub_plan_name,
                cumulative_months: n_cumulative_months,
                streak_months: n_streak_months,
                ..
            } => {
                sub_plan = Some(n_sub_plan);
                sub_plan_name = Some(n_sub_plan_name);
                cumulative_months = Some(n_cumulative_months as u16);
                streak_months = n_streak_months.map(|months| months as u16);
            }
            UserNoticeEvent::SubGift {
                sub_plan: n_sub_plan,
                sub_plan_name: n_sub_plan_name,
                cumulative_months: n_cumulative_months,
                num_gifted_months: n_num_gifted_months,
                recipient: n_recipient,
                ..
            } => {
                sub_plan = Some(n_sub_plan);
                sub_plan_name = Some(n_sub_plan_name);
                cumulative_months = Some(n_cumulative_months as u16);
                gift_months = Some(n_num_gifted_months as u16);
                recipient = Some(n_recipient);
            }
            UserNoticeEvent::Raid {
                viewer_count: n_viewer_count,
                ..
            } => viewer_count = Some(n_viewer_count),
            _ => {}
        }
        Ok(ClickhouseUserNotice {
            ts: msg.server_timestamp.timestamp_millis(),
            message_type: msg.event_id,
            channel: msg.channel_login,
            channel_id: msg.channel_id.parse()?,
            username: msg.sender.login,
            display_name: msg.sender.name,
            user_id: msg.sender.id.parse()?,
            badges: msg.badges.iter().map(|b| b.name.clone()).collect(),
            system_message: msg.system_message,
            text: msg.message_text.unwrap_or(String::new()),
            color: msg
                .name_color
                .map(|rgb| format!("#{:02x}{:02x}{:02x}", rgb.r, rgb.g, rgb.b))
                .unwrap_or(String::new()),

            sub_plan: sub_plan.unwrap_or(String::new()),
            sub_plan_name: sub_plan_name.unwrap_or(String::new()),
            cumulative_months: cumulative_months.unwrap_or(0),
            streak_months: streak_months.unwrap_or(0),
            viewer_count: viewer_count.unwrap_or(0),
            gift_months: gift_months.unwrap_or(0),
            recipient_username: recipient
                .as_ref()
                .map(|r| r.login.clone())
                .unwrap_or(String::new()),
            recipient_display_name: recipient
                .as_ref()
                .map(|r| r.name.clone())
                .unwrap_or(String::new()),
            recipient_user_id: recipient
                .and_then(|r| r.id.parse::<u64>().ok())
                .unwrap_or(0),
        })
    }
}

pub async fn create_user_notices(client: &Client) -> Result<()> {
    client
        .query(
            "
      CREATE TABLE IF NOT EXISTS usernotices (
          ts DateTime64(3) CODEC(T64, ZSTD(12)),
          message_type LowCardinality(String),
          channel LowCardinality(String),
          channel_id UInt64 CODEC(T64, ZSTD(12)),
          username String CODEC(ZSTD(12)),
          display_name String CODEC(ZSTD(12)),
          user_id UInt64 CODEC(T64, ZSTD(12)),
          badges Array(LowCardinality(String)),
          system_message String CODEC(ZSTD(14)),
          text String CODEC(ZSTD(14)),
          color String CODEC(ZSTD(12)),
          sub_plan LowCardinality(String),
          sub_plan_name LowCardinality(String),
          cumulative_months UInt16 CODEC(Gorilla, ZSTD(1)),
          streak_months UInt16 CODEC(Gorilla, ZSTD(1)),
          viewer_count UInt64 CODEC(T64, ZSTD(12)),
          gift_months UInt16 CODEC(Gorilla, ZSTD(1)),
          recipient_username String CODEC(ZSTD(12)),
          recipient_display_name String CODEC(ZSTD(12)),
          recipient_user_id UInt64 CODEC(T64, ZSTD(12))
      )
      ENGINE = ReplacingMergeTree
      PARTITION BY toYYYYMM(ts)
      ORDER BY (channel, username, ts, text);",
        )
        .execute()
        .await?;

    debug!("Created clickhouse user_notices table");

    Ok(())
}
