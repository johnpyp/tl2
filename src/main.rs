use adapters::{
    console::ConsoleWriter, console_metrics::ConsoleMetricsWriter,
    elasticsearch::ElasticsearchWriter, file::FileWriter, Writer, Writers,
};
use anyhow::Result;
use env_logger::Env;
use events::AllEvents;
use log::{error, info};
use scrapers::{dgg::DggScraper, twitch::TwitchScraper};
use settings::Settings;
use tokio::sync::mpsc;

pub mod adapters;
pub mod events;
pub mod scrapers;
pub mod settings;

async fn run() -> Result<(), anyhow::Error> {
    let settings = Settings::new()?;

    info!("Logger initialized!");

    let (sender, mut receiver) = mpsc::unbounded_channel::<AllEvents>();

    let mut writers: Vec<Writers> = Vec::new();
    if settings.writers.elasticsearch.enabled {
        writers.push(ElasticsearchWriter::new(settings.writers.elasticsearch)?.into());
    }
    if settings.writers.filesystem.enabled {
        writers.push(FileWriter::new(settings.writers.filesystem).into());
    }
    if settings.writers.console.enabled {
        writers.push(ConsoleWriter::new().into());
    }

    if settings.writers.console_metrics.enabled {
        writers.push(ConsoleMetricsWriter::new().into())
    }

    if settings.twitch.enabled {
        let scraper = TwitchScraper::start(sender.clone(), settings.twitch.clone());
        scraper.sync_channels().await;
    }

    for dgg in settings.dgg_like {
        DggScraper::start(sender.clone(), dgg);
    }

    while let Some(message) = receiver.recv().await {
        for writer in &writers {
            writer.write(message.clone())?;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    if let Err(e) = run().await {
        error!("{:?}", e);
    }
}
