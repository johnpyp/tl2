use adapters::{
    console::ConsoleWriter, console_metrics::ConsoleMetricsWriter,
    elasticsearch::ElasticsearchWriter, file::FileWriter, Writer, Writers,
};
use alerts::{Alert, AlertType, DiscordAlerting};
use anyhow::Result;
use env_logger::Env;
use events::AllEvents;
use log::{error, info, warn};
use scrapers::{dgg::DggScraper, twitch::TwitchScraper};
use settings::Settings;
use tokio::sync::mpsc;

pub mod adapters;
pub mod alerts;
pub mod events;
pub mod scrapers;
pub mod settings;

async fn run() -> Result<(), anyhow::Error> {
    let settings = Settings::new()?;

    info!("Logger initialized!");

    let alerting = DiscordAlerting::new(settings.discord_alerting);

    alerting.info(&"Starting TL2");
    let mut writers: Vec<Option<Writers>> = Vec::new();
    if settings.writers.elasticsearch.enabled {
        writers.push(Some(
            ElasticsearchWriter::new(settings.writers.elasticsearch)?.into(),
        ));
    }
    if settings.writers.filesystem.enabled {
        writers.push(Some(FileWriter::new(settings.writers.filesystem).into()));
    }
    if settings.writers.console.enabled {
        writers.push(Some(ConsoleWriter::new().into()));
    }

    if settings.writers.console_metrics.enabled {
        writers.push(Some(ConsoleMetricsWriter::new().into()))
    }

    let (event_sender, mut event_receiver) = mpsc::unbounded_channel::<AllEvents>();
    if settings.twitch.enabled {
        TwitchScraper::start(event_sender.clone(), settings.twitch.clone());
        // scraper.sync_channels().await;
    }

    for dgg in settings.dgg_like {
        DggScraper::start(event_sender.clone(), dgg);
    }

    while let Some(message) = event_receiver.recv().await {
        let mut to_remove = Vec::new();
        for (i, writer) in writers.iter().enumerate() {
            if let Some(writer) = writer {
                if let Err(e) = writer.write(message.clone()) {
                    error!("Error writing message for writer #{}: {:?}", i, e);
                    to_remove.push(i);
                }
            }
        }
        for i in to_remove {
            warn!("Removing failing writer #{} from queue", i);
            alerting.error("Removed a writer from the queue, probably means elasticsearch broke!");
            writers[i] = None;
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
