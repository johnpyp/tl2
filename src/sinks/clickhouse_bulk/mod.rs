use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use clickhouse::inserter::Inserter;
use clickhouse::Client;
use clickhouse::Row;
use futures::future::join_all;
use futures::Stream;
use futures::TryStreamExt;
use log::error;
use log::info;
use tokio::pin;

use super::Sink;
use crate::adapters::clickhouse::messages_table;
use crate::adapters::clickhouse::messages_table::ClickhouseOrlMessage;
use crate::formats::unified::OrlLog1_0;

pub struct ClickhouseBulkSinkOpts {
    table_name: String,
    url: String,
}

pub struct ClickhouseBulkSink {
    client: Client,
    opts: ClickhouseBulkSinkOpts,
}

impl ClickhouseBulkSink {
    pub fn new(url: String) -> Result<ClickhouseBulkSink> {
        let client = Client::default().with_url(&url);

        let opts = ClickhouseBulkSinkOpts {
            url,
            table_name: "orl_messages".into(),
        };

        Ok(ClickhouseBulkSink { client, opts })
    }

    pub async fn init(&self) -> Result<()> {
        messages_table::create_orl_messages(&self.client)
            .await
            .with_context(|| "Error initializing clickhouse tables")?;

        Ok(())
    }

    fn get_workers(&self, count: usize) -> Result<Vec<ClickhouseWorker>> {
        let mut workers = vec![];
        for _ in 0..count {
            let inserter = create_inserter(&self.client, &self.opts.table_name)?;

            let worker = ClickhouseWorker { inserter };
            workers.push(worker);
        }

        Ok(workers)
    }
}

pub struct ClickhouseBulkBatch {
    logs: Vec<OrlLog1_0>,
}

fn create_inserter<T: Row>(client: &Client, table_name: &str) -> Result<Inserter<T>> {
    let inserter = client
        .inserter::<T>(table_name)?
        .with_max_entries(256_000)
        .with_period(Some(Duration::from_secs(10)));
    Ok(inserter)
}

struct ClickhouseWorker {
    pub inserter: Inserter<ClickhouseOrlMessage>,
}

impl ClickhouseWorker {
    async fn write_batch(&mut self, batch: ClickhouseBulkBatch) -> Result<()> {
        for message in batch.logs {
            let ch_message = self.map_log(message);
            self.inserter.write(&ch_message).await?;
        }
        self.inserter.commit().await?;
        Ok(())
    }

    fn map_log(&self, log: OrlLog1_0) -> ClickhouseOrlMessage {
        ClickhouseOrlMessage {
            ts: log.key.timestamp,
            channel: log.channel_name,
            username: log.username,
            text: log.text,
        }
    }
}

const WORKER_COUNT: usize = 10;
const QUEUED_LIMIT: usize = 4;
const STREAM_CHUNK_SIZE: usize = 32_000;

#[async_trait(?Send)]
impl Sink<Result<OrlLog1_0>> for ClickhouseBulkSink {
    async fn run(
        mut self,
        stream: impl Stream<Item = Result<OrlLog1_0>> + Send,
    ) -> anyhow::Result<()> {
        pin!(stream);

        let mut chunked_stream = stream.try_chunks(STREAM_CHUNK_SIZE);

        let start = Instant::now();
        let mut last_status = start;

        let count = Arc::new(AtomicUsize::new(0));

        let (sender, receiver) = async_channel::bounded::<Vec<OrlLog1_0>>(QUEUED_LIMIT);

        let mut worker_join = vec![];
        let workers = self.get_workers(WORKER_COUNT)?;

        for mut worker in workers.into_iter() {
            let receiver = receiver.clone();
            let count = count.clone();
            let task = tokio::spawn(async move {
                while let Ok(logs) = receiver.recv().await {
                    let logs_len = logs.len();
                    let batch = ClickhouseBulkBatch { logs };
                    if let Err(err) = worker.write_batch(batch).await {
                        error!("Worker failed to write to clickhouse {err:?}");
                        return Err(err);
                    }

                    count.fetch_add(logs_len, Ordering::Relaxed);
                }
                Ok(())
            });

            worker_join.push(task);
        }

        while let Some(chunk) = chunked_stream.try_next().await? {
            sender.send(chunk).await?;

            if Instant::now().duration_since(last_status) > Duration::from_secs(2) {
                last_status = Instant::now();
                let elapsed = start.elapsed();
                let count = count.load(Ordering::Relaxed);
                info!(
                    "Currently indexed {} messages after {} ms, {:.2} m/s",
                    count,
                    elapsed.as_millis(),
                    (count as f64 / elapsed.as_millis() as f64) * 1000f64,
                );
            }
        }

        sender.close();

        join_all(worker_join).await;

        let elapsed = start.elapsed();
        let count = count.load(Ordering::Relaxed);
        info!(
            "Total indexed {} messages after {} ms, {:.2} m/s",
            count,
            elapsed.as_millis(),
            (count as f64 / elapsed.as_secs_f64()),
        );

        Ok(())
    }
}
