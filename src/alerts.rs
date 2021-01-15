use std::sync::Arc;

use log::error;
use reqwest::Client;
use serde_json::json;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::settings::DiscordAlertingSettings;

#[derive(Clone, Debug)]
pub enum AlertType {
    Info,
    Error,
}
#[derive(Clone, Debug)]
pub struct Alert {
    alert_type: AlertType,
    message: String,
}

impl Alert {
    pub fn new(alert_type: AlertType, message: String) -> Self {
        Self {
            alert_type,
            message,
        }
    }
}

pub struct DiscordAlerting {
    tx: UnboundedSender<Alert>,
    config: DiscordAlertingSettings,
}

impl DiscordAlerting {
    pub fn new(config: DiscordAlertingSettings) -> Arc<Self> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let alerting = Arc::new(DiscordAlerting { config, tx });
        tokio::spawn({
            let alerting = Arc::clone(&alerting);
            async move { alerting.run(rx).await }
        });
        alerting
    }

    pub fn error(&self, message: &str) {
        self.send_alert(Alert::new(AlertType::Error, message.into()));
    }

    pub fn info(&self, message: &str) {
        self.send_alert(Alert::new(AlertType::Info, message.into()));
    }

    fn send_alert(&self, alert: Alert) {
        if let Err(e) = self.tx.send(alert) {
            error!("Error sending alert to alert receiver: {:?}", e);
        }
    }

    async fn run(&self, mut rx: UnboundedReceiver<Alert>) {
        while let Some(alert) = rx.recv().await {
            if self.config.enabled {
                self.send(alert).await;
            }
        }
    }

    async fn send(&self, alert: Alert) {
        let mention = self
            .config
            .owner
            .as_ref()
            .map(|owner| format!("<@{}>", owner))
            .unwrap_or("".to_string());
        if let Some(url) = &self.config.webhook_url {
            let body = match alert.alert_type {
                AlertType::Error => {
                    json!({
                        "content": mention,
                        "embeds": [
                            {
                                "title": "ERROR",
                                "description": alert.message,
                                "color": 0xff0000
                            }
                        ]

                    })
                }
                AlertType::Info => {
                    json!({
                        "content": format!("**INFO** {}", alert.message),
                    })
                }
            };

            let response = Client::new().post(url).json(&body).send().await;
            if let Err(e) = response {
                error!("Error sending discord alert: {:?}", e);
            }
        }
    }
}
