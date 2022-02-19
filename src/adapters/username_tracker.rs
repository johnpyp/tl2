use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use log::{debug, error};
use sqlx::SqlitePool;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use twitch_irc::message::UserNoticeEvent;

use super::Writer;
use crate::{
    events::AllEvents, scrapers::twitch::events::TwitchEvent, settings::UsernameTrackerSettings,
};

pub struct UsernameTracker {
    tx: UnboundedSender<TwitchEvent>,
    pub config: Arc<UsernameTrackerSettings>,
}

impl UsernameTracker {
    pub fn new(config: UsernameTrackerSettings, sqlite: SqlitePool) -> UsernameTracker {
        let config = Arc::new(config);
        let (tx, rx) = mpsc::unbounded_channel();
        UsernameWorker::spawn(config.clone(), rx, sqlite);
        UsernameTracker { tx, config }
    }
}

impl Writer for UsernameTracker {
    fn write(&self, event: AllEvents) -> Result<()> {
        if let AllEvents::Twitch(t) = event {
            self.tx.send(t)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct UsernameUpdateEvent {
    pub id: String,
    pub username: String,
    pub timestamp: DateTime<Utc>,
}

struct UsernameWorker {
    sqlite: SqlitePool,
    config: Arc<UsernameTrackerSettings>,
    rx: UnboundedReceiver<TwitchEvent>,
}
impl UsernameWorker {
    fn spawn(
        config: Arc<UsernameTrackerSettings>,
        rx: UnboundedReceiver<TwitchEvent>,
        sqlite: SqlitePool,
    ) {
        let worker = UsernameWorker { config, rx, sqlite };
        tokio::spawn(worker.run());
    }
    async fn run(mut self) {
        if let Err(error) = init_tables(&self.sqlite).await {
            error!(
                "Couldn't initialize sqlite table for usernames: {:?}",
                error
            );
            return;
        }
        let mut updates_queue: Vec<UsernameUpdateEvent> = Vec::new();
        while let Some(evt) = self.rx.recv().await {
            let mut new_update_events = self.get_username_updates(evt);
            updates_queue.append(&mut new_update_events);

            let batch_size = self.config.batch_size as usize;
            if updates_queue.len() >= batch_size {
                if let Err(error) = self.process(&updates_queue).await {
                    error!("Error writing usernames to disk: {:?}", error);
                } else {
                    updates_queue.clear();
                }
            }

            if updates_queue.len() >= batch_size * 10 {
                error!(
                    "Update queue backed up to {} items! Removing {} old items",
                    updates_queue.len(),
                    batch_size
                );
                updates_queue.drain(0..batch_size);
            }
        }
    }
    async fn process(&mut self, update_events: &[UsernameUpdateEvent]) -> Result<()> {
        debug!(
            "Writing {} username update events to db",
            update_events.len()
        );
        let trx = self.sqlite.begin().await?;
        for update_event in update_events {
            self.write_to_db(update_event).await?;
        }

        trx.commit().await?;
        Ok(())
    }

    async fn write_to_db(&mut self, update_event: &UsernameUpdateEvent) -> Result<()> {
        let timestamp_unix = update_event.timestamp.timestamp();
        sqlx::query(
            r#"
              INSERT OR REPLACE INTO name_changes(username, twitch_id, last_seen)
              VALUES (?, ?, ?);
            "#,
        )
        .bind(&update_event.username)
        .bind(&update_event.id)
        .bind(timestamp_unix)
        .execute(&self.sqlite)
        .await?;
        // let filename = date.format("%Y-%m-%d").to_string() + ".txt";
        // let path = Path::new(&self.config.path).join(&channel).join(&filename);
        // if !self.file_queues.contains_key(channel) {
        //     tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        // }
        // let queue = self
        //     .file_queues
        //     .entry(channel.to_string())
        //     .or_insert_with(|| {
        //         QueuedAppender::new(channel.to_string(), 50, Duration::from_secs(5))
        //     });
        // queue.write(path, line.to_string()).await?;

        Ok(())
    }

    fn get_username_updates(&self, evt: TwitchEvent) -> Vec<UsernameUpdateEvent> {
        let mut ues: Vec<UsernameUpdateEvent> = Vec::new();
        match evt {
            TwitchEvent::Privmsg(msg) => ues.push(UsernameUpdateEvent {
                id: msg.sender.id,
                username: msg.sender.login.trim().to_string(),
                timestamp: msg.server_timestamp,
            }),
            TwitchEvent::UserNotice(notice) => match notice.event {
                UserNoticeEvent::SubGift {
                    is_sender_anonymous,
                    recipient,
                    ..
                } => {
                    if !is_sender_anonymous {
                        ues.push(UsernameUpdateEvent {
                            id: notice.sender.id,
                            username: notice.sender.login.trim().to_string(),
                            timestamp: notice.server_timestamp,
                        })
                    }
                    ues.push(UsernameUpdateEvent {
                        id: recipient.id,
                        username: recipient.login,
                        timestamp: notice.server_timestamp,
                    })
                }
                UserNoticeEvent::Raid { .. }
                | UserNoticeEvent::Ritual { .. }
                | UserNoticeEvent::SubOrResub { .. }
                | UserNoticeEvent::BitsBadgeTier { .. }
                | UserNoticeEvent::GiftPaidUpgrade { .. } => ues.push(UsernameUpdateEvent {
                    id: notice.sender.id,
                    username: notice.sender.login.trim().to_string(),
                    timestamp: notice.server_timestamp,
                }),
                _ => (),
            },
            _ => (),
        };
        ues
    }
}

pub async fn init_tables(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
          CREATE TABLE IF NOT EXISTS name_changes (
            username TEXT NOT NULL,
            twitch_id TEXT NOT NULL,
            last_seen INTEGER NOT NULL,
            PRIMARY KEY(username, twitch_id)
          );
      "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}
