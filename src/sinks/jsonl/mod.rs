use std::path::PathBuf;

use crate::{
    formats::orl::OrlLog,
    sinks::jsonl::messages::{submit_orl_message_batch, JsonInputBatch},
};
use anyhow::Result;
use async_trait::async_trait;
use futures::{Stream, TryStreamExt};
use log::info;
use tokio::{pin, time::Instant};

use self::messages::JsonLinesSinkContext;

use super::Sink;

pub mod messages;

pub struct JsonFileSink {
    ctx: JsonLinesSinkContext,
}

impl JsonFileSink {
    pub fn new(root_dir: PathBuf) -> Self {
        let ctx = JsonLinesSinkContext::new(root_dir);
        JsonFileSink { ctx }
    }
}

const STREAM_CHUNK_SIZE: usize = 1_000_000;

#[async_trait(?Send)]
impl Sink<Result<OrlLog>> for JsonFileSink {
    async fn run(mut self, stream: impl Stream<Item = Result<OrlLog>>) -> anyhow::Result<()> {
        pin!(stream);

        let mut chunked_stream = stream.try_chunks(STREAM_CHUNK_SIZE);

        // while let Some(chunk) = message_stream.try_next().await? {

        // }

        let start = Instant::now();

        let mut count = 0;
        let mut bytes_count = 0;
        while let Some(chunk) = chunked_stream.try_next().await? {
            count += chunk.len();

            let batch = JsonInputBatch::new(&self.ctx, chunk);
            let bytes = submit_orl_message_batch(batch).await?;
            bytes_count += bytes;

            let elapsed = start.elapsed();
            info!(
                "Currently indexed {} messages after {} ms, {:.2} m/s, {:.2} MB/s",
                count,
                elapsed.as_millis(),
                (count as f64 / elapsed.as_millis() as f64) * 1000f64,
                (bytes_count as f64 / elapsed.as_millis() as f64) / 1000f64
            );
        }

        Ok(())
    }
}
