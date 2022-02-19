use log::{error, info, warn};
use tokio::sync::mpsc;

use crate::{
    adapters::{
        clickhouse::ClickhouseWriter, console::ConsoleWriter,
        console_metrics::ConsoleMetricsWriter, elasticsearch::ElasticsearchWriter,
        file::FileWriter, username_tracker::UsernameTracker, Writer, Writers,
    },
    alerts::DiscordAlerting,
    events::AllEvents,
    scrapers::{dgg::DggScraper, twitch::TwitchScraper},
    settings::Settings,
    sqlite_pool::create_sqlite,
};

pub async fn run_ingester() -> Result<(), anyhow::Error> {
    let settings = Settings::new()?;

    info!("Logger initialized!");

    let alerting = DiscordAlerting::new(settings.discord_alerting);

    alerting.info("Starting TL2");
    let mut writers: Vec<Option<Writers>> = Vec::new();
    if settings.writers.elasticsearch.enabled {
        writers.push(Some(
            ElasticsearchWriter::new(settings.writers.elasticsearch, alerting.clone())?.into(),
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

    if settings.writers.clickhouse.enabled {
        writers.push(Some(
            ClickhouseWriter::new(settings.writers.clickhouse, alerting.clone()).into(),
        ))
    }

    if settings.writers.username_tracker.enabled {
        let sqlite = create_sqlite(&settings.writers.username_tracker.sqlite_path).await?;
        writers.push(Some(
            UsernameTracker::new(settings.writers.username_tracker, sqlite).into(),
        ))
    }

    let (event_sender, mut event_receiver) = mpsc::unbounded_channel::<AllEvents>();
    if settings.twitch.enabled {
        TwitchScraper::start(event_sender.clone(), settings.twitch.clone());
        // scraper.sync_channels().await;
    }

    for site in settings.dgg_like.sites {
        DggScraper::start(
            event_sender.clone(),
            site,
            settings.dgg_like.max_retry_seconds,
        );
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
