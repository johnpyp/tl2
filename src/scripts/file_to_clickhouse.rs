use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use clickhouse::{inserter::Inserter, Client};
use itertools::Itertools;
use log::info;

use crate::{
    adapters::clickhouse::messages_table::{self, ClickhouseMessage},
    orl_file_parser::{parse_file_to_logs, read_orl_structured_dir, OrlLog},
};

async fn exec_files_to_clickhouse(
    message_inserter: &mut Inserter<ClickhouseMessage>,
    paths: Vec<PathBuf>,
    channel: &str,
) -> Result<()> {
    let mut count = 0;
    for path in paths {
        info!("Indexing file: {:?}", path);
        let logs = parse_file_to_logs(&path, &channel).await?;
        let messages = logs.into_iter().map(|log| orl_log_to_message(log));
        info!("Starting to index messages");
        for message_chunk in &messages.chunks(64000) {
            for message in message_chunk.into_iter() {
                message_inserter.write(&message).await?;
                count += 1;
            }
            let quantities = message_inserter.commit().await?;
            info!(
                "Committed {} messages to clickhouse. Total: {}",
                quantities.entries, count
            );
        }
    }
    info!(
        "Finished inserting, all results flushed. Total messages inserted: {}",
        count
    );
    Ok(())
}
pub async fn files_to_clickhouse(paths: Vec<PathBuf>, channel: &str, url: &str) -> Result<()> {
    let client = Client::default().with_url(url);

    init_tables(&client).await?;

    let mut message_inserter = client
        .inserter::<ClickhouseMessage>("messages")?
        .with_max_entries(16000)
        .with_max_duration(Duration::from_secs(10));

    exec_files_to_clickhouse(&mut message_inserter, paths, channel).await?;
    message_inserter.end().await?;
    Ok(())
}

pub async fn dir_to_clickhouse(dir_path: PathBuf, url: &str) -> Result<()> {
    let orl_files = read_orl_structured_dir(&dir_path).await?;
    let mut orl_files_grouped = Vec::new();
    for (key, group) in &orl_files.into_iter().group_by(|of| of.channel.clone()) {
        let paths: Vec<PathBuf> = group.map(|of| of.path).collect();
        orl_files_grouped.push((key, paths));
    }

    let client = Client::default().with_url(url);

    init_tables(&client).await?;

    let mut message_inserter = client
        .inserter::<ClickhouseMessage>("messages")?
        .with_max_entries(16000)
        .with_max_duration(Duration::from_secs(10));
    for (channel, paths) in orl_files_grouped {
        exec_files_to_clickhouse(&mut message_inserter, paths, &channel).await?;
    }
    message_inserter.end().await?;
    Ok(())
}
async fn init_tables(client: &Client) -> Result<()> {
    messages_table::create_messages(client).await?;
    Ok(())
}

fn orl_log_to_message(orl_log: OrlLog) -> ClickhouseMessage {
    ClickhouseMessage {
        ts: orl_log.ts.timestamp_millis(),
        text: orl_log.text,
        bits: 0,
        color: "".into(),
        badges: vec![],
        channel: orl_log.channel,
        user_id: 0,
        username: orl_log.username,
        channel_id: 0,
        subscribed: 0,
        display_name: "".into(),
    }
}

/* async fn write_message(inserter: &mut Inserter<ClickhouseMessage>, event: AllEvents) -> Result<()> {
    if let AllEvents::Twitch(TwitchEvent::Privmsg(msg)) = event {
        let ch_message: ClickhouseMessage = msg.try_into()?;
        inserter.write(&ch_message).await?;
        inserter.commit().await?;
    }
    Ok(())
} */
