use std::{convert::TryInto, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use clickhouse::{inserter::Inserter, Client};
use log::{error, info};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use self::{messages_table::ClickhouseMessage, user_notices_table::ClickhouseUserNotice};
use super::Writer;
use crate::{
    alerts::DiscordAlerting, events::AllEvents, scrapers::twitch::events::TwitchEvent,
    settings::ClickhouseSettings,
};

pub mod messages_table;
pub mod user_notices_table;

pub struct ClickhouseWriter {
    tx: UnboundedSender<AllEvents>,
}

impl ClickhouseWriter {
    pub fn new(config: ClickhouseSettings, alerting: Arc<DiscordAlerting>) -> ClickhouseWriter {
        let (tx, rx) = mpsc::unbounded_channel();
        let alerting = alerting.clone();
        let mut worker = ClickhouseWorker {
            rx,
            alerting,
            config: config.clone(),
        };
        tokio::spawn(async move { worker.work().await });
        Self { tx }
    }
}
impl Writer for ClickhouseWriter {
    fn write(&self, msg: AllEvents) -> Result<()> {
        self.tx
            .send(msg)
            .with_context(|| "Sending message to Clickhouse worker failed, rx probably dropped")?;
        Ok(())
    }
}

pub struct ClickhouseWorker {
    pub config: ClickhouseSettings,
    pub rx: UnboundedReceiver<AllEvents>,
    pub alerting: Arc<DiscordAlerting>,
}

impl ClickhouseWorker {
    pub async fn work(&mut self) {
        info!("Pogchamp");
        let client = self.create_client();

        loop {
            if let Err(e) = self.run_writer(&client).await {
                error!("Clickhouse worker failed: {:?}", e);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    async fn run_writer(&mut self, client: &Client) -> Result<()> {
        ClickhouseWorker::init_tables(client).await?;
        info!("Starting Clickhouse ingestion loop");
        let mut message_inserter = client
            .inserter::<ClickhouseMessage>("messages")?
            .with_max_entries(100)
            .with_max_duration(Duration::from_secs(5));
        let mut user_notice_inserter = client
            .inserter::<ClickhouseUserNotice>("usernotices")?
            .with_max_entries(100)
            .with_max_duration(Duration::from_secs(5));
        while let Some(event) = self.rx.recv().await {
            ClickhouseWorker::write_message(&mut message_inserter, event.clone())
                .await
                .with_context(|| "Write message failed")?;
            ClickhouseWorker::write_user_notice(&mut user_notice_inserter, event.clone())
                .await
                .with_context(|| "Write user notice failed")?;
        }
        Ok(())
    }

    fn create_client(&self) -> Client {
        let mut client = Client::default().with_url(&self.config.url);
        if let Some(db_user) = &self.config.db_user {
            client = client.with_user(db_user);
        }
        if let Some(db_pass) = &self.config.db_pass {
            client = client.with_password(db_pass);
        }
        if let Some(db_name) = &self.config.db_name {
            client = client.with_database(db_name);
        }
        return client;
    }

    async fn init_tables(client: &Client) -> Result<()> {
        messages_table::create_messages(client).await?;
        user_notices_table::create_user_notices(client).await?;
        Ok(())
    }

    async fn write_message(
        inserter: &mut Inserter<ClickhouseMessage>,
        event: AllEvents,
    ) -> Result<()> {
        if let AllEvents::Twitch(TwitchEvent::Privmsg(msg)) = event {
            let ch_message: ClickhouseMessage = msg.try_into()?;
            inserter.write(&ch_message).await?;
            inserter.commit().await?;
        }
        Ok(())
    }

    async fn write_user_notice(
        inserter: &mut Inserter<ClickhouseUserNotice>,
        event: AllEvents,
    ) -> Result<()> {
        if let AllEvents::Twitch(TwitchEvent::UserNotice(msg)) = event {
            let ch_user_notice: ClickhouseUserNotice = msg.try_into()?;
            inserter.write(&ch_user_notice).await?;
            inserter.commit().await?;
        }
        Ok(())
    }
}
