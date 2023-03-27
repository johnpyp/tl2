use std::path::PathBuf;

use anyhow::Result;
use async_stream::try_stream;
use futures::Stream;
use futures::TryStreamExt;
use log::debug;
use log::info;
use tokio::time::Instant;

use crate::formats::orl::CleanOrlLog;
use crate::sinks::sqlite::messages::init_unified_messages_tables;
use crate::sinks::sqlite::messages::submit_orl_message_batch;
use crate::sources::orl::orl_file_parser::parse_file_to_logs;
use crate::sources::orl::orl_file_parser::read_orl_structured_dir;
use crate::sources::orl::orl_file_parser::OrlDirFile;
use crate::sqlite_pool::create_sqlite;

fn create_message_stream(orl_files: Vec<OrlDirFile>) -> impl Stream<Item = Result<CleanOrlLog>> {
    try_stream! {
        for file in orl_files {
            debug!("Processing file: {:?}", file.path);
            let logs = parse_file_to_logs(&file.path, &file.channel).await?;
            for log in logs {
                yield log;
            }
        }
    }
}

const STREAM_CHUNK_SIZE: usize = 100_000;

pub async fn dir_to_sqlite(dir_path: PathBuf) -> Result<()> {
    let start = Instant::now();
    let orl_files = read_orl_structured_dir(&dir_path).await?;

    let client = create_sqlite("./out.db").await?;

    init_unified_messages_tables(&client).await?;

    let mut message_stream =
        Box::pin(create_message_stream(orl_files)).try_chunks(STREAM_CHUNK_SIZE);

    let mut count = 0;
    while let Some(chunk) = message_stream.try_next().await? {
        count += chunk.len();
        submit_orl_message_batch(&client, chunk).await?;
        let elapsed = start.elapsed();
        info!(
            "Currently indexed {} messages after {} ms, {:.2} m/s",
            count,
            elapsed.as_millis(),
            (count as f64 / elapsed.as_millis() as f64) * 1000f64
        );
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
