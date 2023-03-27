use std::iter::zip;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::Result;
use async_stream::try_stream;
use clickhouse::inserter::Inserter;
use clickhouse::Client;
use futures::future::try_join_all;
use futures::Stream;
use futures::TryStreamExt;
use itertools::Itertools;
use log::debug;
use log::info;
use tokio::spawn;
use tokio::sync::Mutex;

use crate::adapters::clickhouse::messages_table::ClickhouseOrlMessage;
use crate::adapters::clickhouse::messages_table::{self};
use crate::formats::orl::CleanOrlLog;
use crate::sources::orl::orl_file_parser::parse_file_to_logs;
use crate::sources::orl::orl_file_parser::read_orl_structured_dir;
use crate::sources::orl::orl_file_parser::OrlDirFile;

type SyncInserter<T> = Arc<Mutex<Inserter<T>>>;
async fn insert_messages(
    inserter: SyncInserter<ClickhouseOrlMessage>,
    messages: Vec<ClickhouseOrlMessage>,
) -> Result<()> {
    let mut inserter = inserter.lock().await;
    for message in messages {
        inserter.write(&message).await?;
    }
    inserter.commit().await?;
    Ok(())
}
async fn split_insert(
    inserters: &[SyncInserter<ClickhouseOrlMessage>],
    messages: Vec<ClickhouseOrlMessage>,
) -> Result<()> {
    let message_count = messages.len();
    let chunk_size = (messages.len() / inserters.len()).max(1);
    let mut futures = vec![];
    let message_chunks = messages.chunks(chunk_size).collect_vec();
    for (inserter, chunk) in zip(inserters, message_chunks) {
        let inserter = inserter.clone();
        let handle = spawn(insert_messages(inserter, chunk.to_vec()));
        futures.push(handle);
    }
    try_join_all(futures).await?;
    debug!("Committed {} messages to clickhouse", message_count);
    Ok(())
}

fn create_message_stream(
    orl_files: Vec<OrlDirFile>,
) -> impl Stream<Item = Result<ClickhouseOrlMessage>> {
    try_stream! {
        for file in orl_files {
            debug!("Processing file: {:?}", file.path);
            let logs = parse_file_to_logs(&file.path, &file.channel).await?;
            let messages = logs.into_iter().map(|log| orl_log_to_message(log));
            for message in messages {
                yield message;
            }
        }
    }
}

async fn create_inserters(
    client: &Client,
    count: i32,
) -> Result<Vec<SyncInserter<ClickhouseOrlMessage>>> {
    let mut inserters = Vec::new();
    for _ in 0..count {
        let inserter = client
            .inserter::<ClickhouseOrlMessage>("orl_messages")?
            .with_max_entries(32_000)
            .with_period(Some(Duration::from_secs(10)));
        inserters.push(Arc::new(Mutex::new(inserter)));
    }
    Ok(inserters)
}

const STREAM_CHUNK_SIZE: usize = 1_000_000;
pub async fn files_to_clickhouse(paths: Vec<PathBuf>, channel: &str, url: &str) -> Result<()> {
    let client = Client::default().with_url(url);

    init_tables(&client).await?;

    let inserters = create_inserters(&client, 10).await?;

    let orl_files = paths
        .into_iter()
        .map(|path| OrlDirFile {
            channel: channel.to_string(),
            path,
        })
        .collect_vec();
    let mut message_stream =
        Box::pin(create_message_stream(orl_files)).try_chunks(STREAM_CHUNK_SIZE);

    while let Some(chunk) = message_stream.try_next().await? {
        split_insert(&inserters, chunk).await?;
    }
    // exec_files_to_clickhouse(&inserters, paths, channel).await?;
    for inserter in inserters {
        let inserter = Arc::try_unwrap(inserter)
            .map_err(|_| anyhow!("Failed to unwrap the inserter to close it"))?
            .into_inner();
        inserter.end().await?;
    }
    Ok(())
}

pub async fn dir_to_clickhouse(dir_path: PathBuf, url: &str) -> Result<()> {
    let start = Instant::now();
    let orl_files = read_orl_structured_dir(&dir_path).await?;

    let client = Client::default().with_url(url);

    init_tables(&client).await?;

    let inserters = create_inserters(&client, 10).await?;
    let mut message_stream =
        Box::pin(create_message_stream(orl_files)).try_chunks(STREAM_CHUNK_SIZE);

    let mut count = 0;
    while let Some(chunk) = message_stream.try_next().await? {
        count += chunk.len();
        split_insert(&inserters, chunk).await?;
        let elapsed = start.elapsed();
        info!(
            "Currently indexed {} messages after {} ms, {:.2} m/s",
            count,
            elapsed.as_millis(),
            (count as f64 / elapsed.as_millis() as f64) * 1000f64
        );
    }

    for inserter in inserters {
        let inserter = Arc::try_unwrap(inserter)
            .map_err(|_| anyhow!("Failed to unwrap the inserter to close it"))?
            .into_inner();
        inserter.end().await?;
    }
    let elapsed = start.elapsed();
    info!(
        "Finished indexing {} messages after {} ms, {:.2} m/s",
        count,
        elapsed.as_millis(),
        (count as f64 / elapsed.as_millis() as f64) * 1000f64
    );
    Ok(())
}
async fn init_tables(client: &Client) -> Result<()> {
    messages_table::create_orl_messages(client).await?;
    Ok(())
}

fn orl_log_to_message(orl_log: CleanOrlLog) -> ClickhouseOrlMessage {
    ClickhouseOrlMessage {
        ts: orl_log.ts.timestamp_millis(),
        text: orl_log.text,
        channel: orl_log.channel,
        username: orl_log.username,
    }
}
