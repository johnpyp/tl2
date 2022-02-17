use std::{sync::Arc, time::Duration};

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use log::{debug, error, info};
use reqwest::Client;
use serde::Deserialize;
use tokio::{
    sync::mpsc::UnboundedSender,
    time::{interval_at, Instant},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{http::Request, Error as WsError, Message},
};

use super::DggEvent;
use crate::{events::AllEvents, settings::DggSiteSettings};

pub struct DggScraper {
    pub config: DggSiteSettings,
}

impl DggScraper {
    pub fn start(
        tx: UnboundedSender<AllEvents>,
        config: DggSiteSettings,
        max_retry_seconds: u64,
    ) -> Arc<DggScraper> {
        let channel = config.name.clone();
        let endpoint = config.endpoint.clone();
        let origin = config.origin.clone();
        let mut worker = DggWorker {
            tx,
            channel,
            endpoint,
            origin,
            use_get_key: config.use_get_key,
            failing: false,
            backoff_min: 2,
            backoff_max: max_retry_seconds,
        };
        tokio::spawn(async move { worker.run().await });

        Arc::new(DggScraper { config })
    }
}

enum WorkerCommands {
    Reconnect,
    Stop,
}

pub struct DggWorker {
    tx: UnboundedSender<AllEvents>,
    channel: String,
    endpoint: String,
    origin: String,
    use_get_key: bool,
    failing: bool,
    backoff_min: u64,
    backoff_max: u64,
}

impl DggWorker {
    pub async fn run(&mut self) {
        info!("Starting work loop for '{}' dgg-like chat", &self.channel);

        let mut backoff = self.backoff_min;
        loop {
            if self.failing {
                info!("Reconnecting after {} seconds...", backoff);
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = self.backoff_max.min(backoff * 3);
            }

            match self.start_websocket().await {
                WorkerCommands::Reconnect => {
                    info!("Received WorkerCommands::Reconnect");
                    if !self.failing {
                        backoff = self.backoff_min;
                        self.failing = true;
                    }
                }
                WorkerCommands::Stop => {
                    info!("Received Stop command from websocket, terminating websocket connection");
                    return;
                }
            }
        }
    }

    async fn start_websocket(&mut self) -> WorkerCommands {
        let mut endpoint = self.endpoint.to_owned();
        if self.use_get_key {
            let get_key_url = self.origin.clone() + "/api/chat/getkey";
            let chat_key = match self.fetch_get_key(get_key_url.clone()).await {
                Ok(v) => v,
                Err(err) => {
                    error!("Error fetching key for '{}': {:?}", get_key_url, err);
                    return WorkerCommands::Reconnect;
                }
            };
            endpoint = endpoint.to_owned() + "/" + &chat_key;
        }
        let mut request = Request::builder().uri(endpoint);
        request = request.header("Origin", &self.origin);

        let body = request.body(()).unwrap();
        info!("Connecting with {:?}", body);
        let (ws_stream, _) = match connect_async(body).await {
            Ok(v) => v,
            Err(err) => {
                error!("Error connecting to the websocket: {:?}", err);
                return WorkerCommands::Reconnect;
            }
        };

        let (mut write, mut read) = ws_stream.split();

        info!("Starting request loop...");

        let ping_every = Duration::from_secs(30);
        let check_pong_every = Duration::from_secs(5);
        let mut ping_interval = interval_at(Instant::now() + ping_every, ping_every);
        let mut check_pong_interval =
            interval_at(Instant::now() + ping_every + check_pong_every, ping_every);

        let mut received_pong = false;
        loop {
            tokio::select! {
                Some(res) = read.next() => {
                    match res {
                        Ok(msg) => match msg {
                            Message::Text(text) => {
                                self.failing = false;
                                let event = DggEvent::from_ws(text, self.channel.clone());
                                match event {
                                    Ok(Some(event)) => {
                                        self.tx.send(event.into()).unwrap();
                                    }
                                    Err(err) => {
                                        error!("Serde parsing error from dgg messages: {:?}", err);
                                    }
                                    _ => {}
                                }
                            }
                            Message::Pong(_) => {
                                debug!("Received pong!");
                                received_pong = true;
                            }
                            Message::Close(_) => {
                                info!("Received Closed message from websocket, triggering reconnect");
                                return WorkerCommands::Reconnect;
                            }

                            _ => {}
                        },
                        Err(raw_error) => {
                            match raw_error {
                                WsError::ConnectionClosed | WsError::AlreadyClosed => {
                                    return WorkerCommands::Reconnect
                                }
                                WsError::Url(url) => {
                                    error!("Invalid url: {}", url);
                                    return WorkerCommands::Stop;
                                }
                                WsError::Io(err) => {
                                    error!("IO Error in websocket: {:?}", err);
                                }
                                WsError::Http(code) => {
                                    error!("HTTP error response code: {:?}", code);
                                }
                                WsError::HttpFormat(err) => {
                                    error!("HTTP formatting error: {:?}", err);
                                }

                                WsError::Protocol(err) => {
                                    error!("Websocket Protocol error (often reconnect after internet out): {:?}", err)
                                }

                                _ => {
                                    error!("Unhandled websocket error: {:?}", raw_error);
                                }
                            }
                            return WorkerCommands::Reconnect;
                        }
                    }
                },
                _ = ping_interval.tick() => {
                    received_pong = false;
                    debug!("Sending ping request");
                    if let Err(err) = write.send(Message::Ping(Vec::new())).await {
                        error!("Error sending ping command: {:?}", err);
                        return WorkerCommands::Reconnect
                    }
                }
                _ = check_pong_interval.tick() => {
                    if !received_pong {
                        error!("Didn't receive PONG back within 5 seconds of ping, triggering reconnect");
                        return WorkerCommands::Reconnect;
                    }
                }

            }
        }
    }

    async fn fetch_get_key(&self, get_key_url: String) -> Result<String> {
        let response: GetKeyResponse = Client::new().get(get_key_url).send().await?.json().await?;

        Ok(response.chat_key)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct GetKeyResponse {
    #[serde(rename = "chatKey")]
    chat_key: String,
}
