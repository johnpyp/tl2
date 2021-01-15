use std::{env, path::PathBuf};

use anyhow::Result;
use config::{Config, Environment, File};
use log::info;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct DiscordAlertingSettings {
    pub enabled: bool,
    pub webhook_url: Option<String>,
    pub owner: Option<String>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct ConsoleMetricsSettings {
    pub enabled: bool,
}
#[derive(Clone, Debug, Deserialize)]
pub struct ConsoleSettings {
    pub enabled: bool,
}
#[derive(Clone, Debug, Deserialize)]
pub struct ElasticsearchSettings {
    pub enabled: bool,
    pub host: String,
    pub port: u32,
    pub index: String,
    pub pipeline: Option<String>,
    pub batch_size: u64,
    pub batch_period_seconds: u64,
}
#[derive(Clone, Debug, Deserialize)]
pub struct ClickhouseSettings {
    pub enabled: bool,
    pub host: String,
    pub port: u32,
}
#[derive(Clone, Debug, Deserialize)]
pub struct FileSettings {
    pub enabled: bool,
    pub path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WritersSettings {
    pub elasticsearch: ElasticsearchSettings,
    pub clickhouse: ClickhouseSettings,
    pub filesystem: FileSettings,
    pub console: ConsoleSettings,
    pub console_metrics: ConsoleMetricsSettings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "adapter")]
pub enum ChannelsAdapter {
    Json { path: String },
    Sqlite { path: String, table: String },
    Http { url: String, bearer_token: String },
}

#[derive(Clone, Debug, Deserialize)]
pub struct TwitchSettings {
    pub enabled: bool,
    pub sync_channels_interval: u64,
    pub use_websocket: bool,
    pub channels: ChannelsAdapter,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DggSettings {
    pub name: String,
    pub endpoint: String,
    pub origin: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    pub debug: String,
    pub discord_alerting: DiscordAlertingSettings,
    pub writers: WritersSettings,
    pub twitch: TwitchSettings,
    pub dgg_like: Vec<DggSettings>,
}

impl Settings {
    pub fn new() -> Result<Self> {
        let mut s = Config::default();

        let config_path =
            PathBuf::from(env::var("CONFIG_PATH").unwrap_or_else(|_| "config".into()));
        let env = env::var("RUST_ENV").unwrap_or_else(|_| "development".into());

        s.merge(File::from(config_path.join("default")))?
            .merge(File::from(config_path.join(&env)))?
            .merge(File::from(config_path.join(env + "_local")))?
            .merge(Environment::with_prefix("app"))?;

        let settings: Settings = s.try_into()?;
        info!("Loaded settings: {:#?}", settings);
        Ok(settings)
    }
}
