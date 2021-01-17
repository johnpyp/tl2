use serde::Deserialize;
pub mod events;
use std::{collections::HashSet, iter::FromIterator, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use events::TwitchEvent;
use log::error;
use reqwest::Client;
use tokio::{
    fs,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        Semaphore,
    },
};
use twitch_irc::{
    login::StaticLoginCredentials, message::ServerMessage, ClientConfig, TwitchIRCClient,
    WSSTransport,
};

use crate::{
    events::AllEvents,
    settings::{ChannelsAdapter, TwitchSettings},
};

pub struct TwitchScraper {
    pub client: TwitchIRCClient<WSSTransport, StaticLoginCredentials>,
    config: TwitchSettings,
}

impl TwitchScraper {
    pub fn start(sender: UnboundedSender<AllEvents>, config: TwitchSettings) -> Arc<TwitchScraper> {
        let client_config = ClientConfig {
            login_credentials: StaticLoginCredentials::anonymous(),
            max_channels_per_connection: 90,

            max_waiting_messages_per_connection: 5,
            time_per_message: Duration::from_millis(150),

            // 1 connection every 2 seconds seems to work well
            connection_rate_limiter: Arc::new(Semaphore::new(1)),
            new_connection_every: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(20),
        };
        let (incoming_messages, client) =
            TwitchIRCClient::<WSSTransport, StaticLoginCredentials>::new(client_config);

        // first thing you should do: start consuming incoming messages,
        // otherwise they will back up.

        tokio::spawn({
            let sender = sender.clone();
            async move { TwitchScraper::run_forwarder(incoming_messages, &sender).await }
        });

        let scraper = Arc::new(TwitchScraper { client, config });
        tokio::spawn({
            let scraper = scraper.clone();
            async move { scraper.run_channel_syncer().await }
        });

        scraper
    }

    pub async fn hydrate_channels(&self) -> Result<Vec<String>> {
        match &self.config.channels {
            ChannelsAdapter::Json { path } => {
                let content = fs::read_to_string(&path)
                    .await
                    .with_context(|| "JSON channels file doesn't exist")?;
                let channels: Vec<String> = serde_json::from_str(&content)?;
                Ok(channels)
            }
            ChannelsAdapter::Sqlite { path: _, table: _ } => Ok(vec!["Xqcow".to_string()]),
            ChannelsAdapter::Http { url, bearer_token } => {
                let response = Client::new()
                    .get(url)
                    .header("Authorization", format!("Bearer {}", bearer_token))
                    .send()
                    .await?;
                let response: GenericHttpResponse<ChannelsResponse> = response
                    .json()
                    .await
                    .with_context(|| "Unexpected response json from node api")?;
                Ok(response.data.channels)
            }
        }
    }

    pub fn join_channels(&self, channels: Vec<String>) {
        self.client.set_wanted_channels(HashSet::from_iter(
            channels.iter().map(|c| c.trim().to_lowercase()),
        ));
    }

    async fn run_forwarder(
        mut rx: UnboundedReceiver<ServerMessage>,
        sender: &UnboundedSender<AllEvents>,
    ) {
        while let Some(raw) = rx.recv().await {
            if let Some(msg) = TwitchScraper::map_message(raw) {
                sender.send(msg.into()).unwrap();
            }
        }
    }

    pub async fn sync_channels(&self) {
        match self.hydrate_channels().await {
            Ok(channels) => self.join_channels(channels),
            Err(e) => error!("Error hydrating channels, keeping the same. {:?}", e),
        }
    }

    async fn run_channel_syncer(&self) {
        let mut check_interval =
            tokio::time::interval(Duration::from_secs(self.config.sync_channels_interval));
        loop {
            check_interval.tick().await;
            self.sync_channels().await;
        }
    }

    pub fn map_message(raw: ServerMessage) -> Option<TwitchEvent> {
        Some(match raw {
            ServerMessage::Privmsg(msg) => TwitchEvent::Privmsg(msg),
            ServerMessage::UserNotice(msg) => TwitchEvent::UserNotice(msg),
            ServerMessage::ClearChat(msg) => TwitchEvent::ClearChat(msg),
            // ServerMessage::HostTarget(msg) => TwitchEvent::HostTarget(msg),
            _ => {
                // println!("Some random thing: {}", raw.source().command);
                return None;
            }
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
struct GenericHttpResponse<T> {
    data: T,
}

#[derive(Clone, Debug, Deserialize)]
struct ChannelsResponse {
    channels: Vec<String>,
}
