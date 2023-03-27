use std::{sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use elasticsearch::{http::request::JsonBody, BulkParts, Elasticsearch};
use futures::prelude::*;
use futures::{channel::mpsc, future};
use log::{info, warn};
use par_stream::prelude::*;
use serde_json::{json, Value};
use tokio::{pin, time::Instant};

use crate::adapters::elasticsearch::initialize_template;
use crate::{
    adapters::elasticsearch::create_elasticsearch_client_from_url, formats::unified::OrlLog1_0,
};

use super::Sink;

pub struct ElasticsearchBulkSinkOpts {
    url: String,
    index_base_name: String,
    pipeline: Option<String>,
}

pub struct ElasticsearchBulkSink {
    client: Elasticsearch,
    opts: ElasticsearchBulkSinkOpts,
}

impl ElasticsearchBulkSink {
    pub fn new(
        url: String,
        index_base_name: String,
        pipeline: Option<String>,
    ) -> Result<ElasticsearchBulkSink> {
        let opts = ElasticsearchBulkSinkOpts {
            index_base_name,
            pipeline,
            url,
        };

        let client = create_elasticsearch_client_from_url(opts.url.clone())?;
        Ok(ElasticsearchBulkSink { client, opts })
    }

    pub async fn init_templates(&self) -> Result<()> {
        initialize_template(&self.client, &self.opts.index_base_name)
            .await
            .with_context(|| "Error initializing elasticsearch templates")
    }
}

pub struct ElasticsearchBulkBatch {
    logs: Vec<OrlLog1_0>,
}

impl ElasticsearchBulkSink {
    async fn write_batch(&self, batch: ElasticsearchBulkBatch) -> Result<()> {
        let client = &self.client;
        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(batch.logs.len() * 2);
        for log in batch.logs {
            self.add_log_to_payload(&mut body, log);
        }

        let mut req = client.bulk(BulkParts::Index("non-index-placeholder"));
        if let Some(pipeline) = &self.opts.pipeline {
            req = req.pipeline(pipeline)
        }

        let response = req.body(body).send().await?;

        let response_body = response.json::<Value>().await?;

        if let Some(request_level_error) = response_body.get("error") {
            let error_reason = request_level_error["reason"].as_str().unwrap();
            bail!(
                "Bulk request failed for request-level error: '{}'",
                error_reason
            );
        }
        let has_errors = response_body["errors"].as_bool().unwrap();
        if has_errors {
            let reason = response_body["items"][0]["index"]["error"]["reason"].as_str();
            if let Some(reason) = reason {
                bail!("Bulk request failed, first error reason: '{}'", reason);
            } else {
                bail!(
                    "Some of bulk request failed, first document seems to have succeeded though."
                );
            }
        }
        Ok(())
    }

    fn add_log_to_payload(&self, bulk_body: &mut Vec<JsonBody<Value>>, log: OrlLog1_0) {
        let date_time = Utc.timestamp_millis_opt(log.key.timestamp).unwrap();
        let timestamp_string = date_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        // 2020-12-12 - 10 characters for ymd of an ISO date
        let floor_ymd = timestamp_string[..7].to_string() + "-01";

        let index_with_date = self.opts.index_base_name.to_string() + "-" + &floor_ymd;

        let id = log.key.id;

        bulk_body.push(json!({ "index": { "_index": &index_with_date , "_id": id}}).into());
        bulk_body.push(
            json!({
                "channel": log.channel_name,
                "username": log.username,
                "text": log.text,
                "ts": timestamp_string
            })
            .into(),
        );
    }
}

const STREAM_CHUNK_SIZE: usize = 2_000;

#[async_trait(?Send)]
impl Sink<Result<OrlLog1_0>> for ElasticsearchBulkSink {
    async fn run(
        mut self,
        stream: impl Stream<Item = Result<OrlLog1_0>> + Send,
    ) -> anyhow::Result<()> {
        pin!(stream);

        let sink = Arc::new(self);

        let (mut sender, receiver) = mpsc::channel::<Vec<OrlLog1_0>>(500);

        // {

        // let orig_chunk_stream = {
        //     let mut sender = sender.clone();

        //     chunked_stream.for_each(|item| async move {
        //         if let Ok(item) = item {
        //             sender
        //                 .send(item)
        //                 .await
        //                 .expect("Sending to sender of channel shouldn't fail");
        //         }
        //     })
        // };
        let mut chunked_stream = stream.try_chunks(STREAM_CHUNK_SIZE).filter_map(|result| {
            if let Ok(result) = result {
                return future::ready(Some(Ok(result)));
            }
            warn!("Error from chunks: {:?}", result);
            future::ready(None)
        });
        let flush_to_stream_fut = sender.send_all(&mut chunked_stream);
        // let orig_chunk_stream = {
        //     let mut chunked_stream = stream
        //         .try_chunks(STREAM_CHUNK_SIZE)
        //         .err_into::<anyhow::Error>();
        //     let mut sender = sender.clone();
        //     tokio::spawn(async move {
        //         while let Some(item) = chunked_stream.next().await {
        //             if let Ok(item) = item {
        //                 sender.send(item);
        //             }
        //         }
        //     })
        // };

        let stream_handle = tokio::spawn({
            async move {
                let mut count = 0;
                let start = Instant::now();

                let mut last_status = start;

                receiver
                    .par_then_unordered(None, move |chunk| {
                        let sink = sink.clone();
                        async move {
                            let count = chunk.len();

                            let batch = ElasticsearchBulkBatch { logs: chunk };

                            // Can't use ? because of https://github.com/rust-lang/rust/issues/63502
                            match sink.write_batch(batch).await {
                                Ok(_) => Ok(count),
                                Err(e) => Err(e),
                            }
                        }
                    })
                    .for_each(|batch_count| {
                        let batch_count = match batch_count {
                            Ok(x) => x,
                            Err(e) => {
                                warn!("Error processing batch: {:?}", e);
                                return future::ready(());
                            }
                        };
                        count += batch_count;

                        if Instant::now().duration_since(last_status) > Duration::from_secs(2) {
                            last_status = Instant::now();

                            let elapsed = start.elapsed();
                            info!(
                                "Currently indexed {} messages after {} ms, {:.2} m/s",
                                count,
                                elapsed.as_millis(),
                                (count as f64 / elapsed.as_millis() as f64) * 1000f64,
                            );
                        }

                        future::ready(())
                    })
                    .await;
            }
        });

        flush_to_stream_fut.await?;

        sender.close_channel();

        stream_handle.await?;

        // let (flush_err, stream_err) = join!(flush_to_stream_fut, stream_handle);

        // if let Err(e) = flush_err {
        //     bail!(e)
        // }

        // if let Err(e) = stream_err {
        //     bail!(e)
        // }

        Ok(())

        // while let Some(chunk) = message_stream.try_next().await? {

        // }

        // let start = Instant::now();

        // let mut count = 0;
        // while let Some(chunk) = chunked_stream.try_next().await? {
        //     count += chunk.len();

        //     let batch = ElasticsearchBulkBatch { logs: chunk };

        //     self.write_batch(batch).await?;

        //     let elapsed = start.elapsed();
        //     info!(
        //         "Currently indexed {} messages after {} ms, {:.2} m/s",
        //         count,
        //         elapsed.as_millis(),
        //         (count as f64 / elapsed.as_millis() as f64) * 1000f64,
        //     );
        // }

        // Ok(())
    }
}
