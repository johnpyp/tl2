use std::env;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use async_channel::Receiver;
use async_stream::try_stream;
use elasticsearch::http::request::JsonBody;
use elasticsearch::BulkParts;
use elasticsearch::Elasticsearch;
use futures::future::join_all;
use futures::future::select;
use futures::Stream;
use futures::TryStreamExt;
use log::debug;
use log::info;
use log::warn;
use serde_json::json;
use serde_json::Value;
use tokio::time;

use crate::adapters::elasticsearch::create_elasticsearch_client_from_url;
use crate::adapters::elasticsearch::initialize_template;
use crate::formats::orl::CleanOrlLog;
use crate::formats::orl::OrlLog;
use crate::sources::orl::orl_file_parser::parse_file_to_logs;
use crate::sources::orl::orl_file_parser::read_orl_structured_dir;
use crate::sources::orl::orl_file_parser::OrlDirFile;

fn create_message_stream_recv(
    orl_files_receiver: Receiver<Vec<OrlDirFile>>,
) -> impl Stream<Item = Result<CleanOrlLog>> {
    try_stream! {
        while let Ok(orl_files) = orl_files_receiver.recv().await {
            for file in orl_files {
                debug!("Processing file: {:?}", file.path);
                let logs = parse_file_to_logs(&file.path, &file.channel).await?;
                for message in logs {
                    yield message;
                }
            }
        }
    }
}

// async fn create_inserters(
//     client: &Client,
//     count: i32,
// ) -> Result<Vec<SyncInserter<ClickhouseOrlMessage>>> {
//     let mut inserters = Vec::new();
//     for _ in 0..count {
//         let inserter = client
//             .inserter::<ClickhouseOrlMessage>("orl_messages")?
//             .with_max_entries(32_000)
//             .with_max_duration(Duration::from_secs(10));
//         inserters.push(Arc::new(Mutex::new(inserter)));
//     }
//     Ok(inserters)
// }
//
//
//
pub async fn write_elastic_chunk(
    client: &Elasticsearch,
    chunk: Vec<CleanOrlLog>,
    index: &str,
    pipeline: Option<&str>,
) -> Result<()> {
    let mut body: Vec<JsonBody<_>> = Vec::with_capacity(chunk.len() * 2);
    for msg in chunk {
        let username = msg.username.to_string();
        let ts = msg.ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        // 2020-12-12 - 10 characters for ymd of an ISO date
        let ymd_first_of_mo = ts[..7].to_string() + "-01";

        let index_with_date = index.to_string() + "-" + &ymd_first_of_mo;

        let id = msg.channel.clone() + "-" + &msg.username + "-" + &ts;

        body.push(json!({ "index": { "_index": &index_with_date , "_id": id}}).into());
        body.push(
            json!({
                "channel": msg.channel,
                "username": username,
                "text": msg.text,
                "ts": ts
            })
            .into(),
        );
    }
    let mut req = client.bulk(BulkParts::Index("non-index-placeholder"));
    if let Some(pipeline) = &pipeline {
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
            bail!("Some of bulk request failed, first document seems to have succeeded though.");
        }
    }
    Ok(())
}

// pub async fn files_to_clickhouse(paths: Vec<PathBuf>, channel: &str, url: &str) -> Result<()> {
//     let client = Client::default().with_url(url);

//     init_tables(&client).await?;

//     let inserters = create_inserters(&client, 10).await?;

//     let orl_files = paths
//         .into_iter()
//         .map(|path| OrlDirFile {
//             channel: channel.to_string(),
//             path,
//         })
//         .collect_vec();
//     let mut message_stream =
//         Box::pin(create_message_stream(orl_files)).try_chunks(ELASTIC_STREAM_CHUNK_SIZE);

//     while let Some(chunk) = message_stream.try_next().await? {
//         split_insert(&inserters, chunk).await?;
//     }
//     // exec_files_to_clickhouse(&inserters, paths, channel).await?;
//     for inserter in inserters {
//         let inserter = Arc::try_unwrap(inserter)
//             .map_err(|_| anyhow!("Failed to unwrap the inserter to close it"))?
//             .into_inner();
//         inserter.end().await?;
//     }
//     Ok(())
// }

// const WORKER_COUNT: usize = 24;
pub async fn dir_to_elasticsearch(dir_path: PathBuf, url: &str, index: &str) -> Result<()> {
    let worker_count: usize =
        env::var("TL2_WORKER_COUNT").map_or_else(|_| 16, |s| s.parse::<usize>().unwrap());
    let elastic_stream_chunk_size: usize = env::var("TL2_ELASTIC_STREAM_CHUNK_SIZE")
        .map_or_else(|_| 2_000, |s| s.parse::<usize>().unwrap());

    let start = Instant::now();
    let orl_files = read_orl_structured_dir(&dir_path).await?;

    let client = create_elasticsearch_client_from_url(url.to_string())?;

    initialize_template(&client, index)
        .await
        .with_context(|| "Error initializing elasticsearch templates")?;

    // let mut message_stream =
    //     Box::pin(create_message_stream(orl_files)).try_chunks(ELASTIC_STREAM_CHUNK_SIZE);

    let count = Arc::new(AtomicUsize::new(0));
    let mut futures = vec![];

    // let handle = tokio::spawn(async move {
    //     loop {
    //         let chunk = match message_stream.try_next().await {
    //             Ok(x) => match x {
    //                 Some(v) => v,
    //                 _ => return,
    //             },
    //             Err(e) => {
    //                 error!("Message stream broke: {:?}", e);
    //                 return;
    //             }
    //         };
    //         sender.send(chunk).await.unwrap();
    //         // info!(
    //         //     "Currently indexed {} messages after {} ms, {:.2} msg/s",
    //         //     count,
    //         //     elapsed.as_millis(),
    //         //     (count as f64 / elapsed.as_millis() as f64) * 1000f64
    //         // );
    //     }
    // });
    // futures.push(handle);

    let (sender, receiver) = async_channel::unbounded::<Vec<OrlDirFile>>();

    let file_chunks = orl_files.chunks(10).collect::<Vec<_>>();
    for file_chunk in file_chunks {
        sender.send(file_chunk.to_vec()).await.unwrap();
    }
    sender.close();

    for i in 0..worker_count {
        let index = index.to_owned();
        let receiver = receiver.clone();
        let client = client.clone();
        info!("Spawning index worker [{}]", i);
        let count = count.clone();

        // let orl_files = file_chunks
        //     .get(i)
        //     .expect("Index should be in len of file chunks")
        //     .to_vec();
        let handle = tokio::spawn(async move {
            let mut message_stream = Box::pin(create_message_stream_recv(receiver))
                .try_chunks(elastic_stream_chunk_size);
            while let Ok(chunk) = message_stream.try_next().await {
                let chunk = match chunk {
                    Some(x) => x,
                    _ => break,
                };
                let start_time = Instant::now();
                let chunk_len = chunk.len();

                let write_result = write_elastic_chunk(&client, chunk, &index, None).await;

                match write_result {
                    Ok(_) => {
                        let elapsed = start_time.elapsed();
                        count.fetch_add(chunk_len, std::sync::atomic::Ordering::Relaxed);
                        debug!(
                            "[worker {}] Finished indexing {} messages in {:.2}ms",
                            i,
                            chunk_len,
                            elapsed.as_millis()
                        );
                    }
                    Err(e) => {
                        warn!(
                            "[worker {}] Index failed for {} messages. Error: {:?}",
                            i, chunk_len, e
                        );
                    }
                }
            }
        });
        futures.push(handle);
    }

    {
        let count = count.clone();

        let timer_handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let elapsed = start.elapsed();
                let count = count.load(Ordering::Relaxed);
                info!(
                    "Currently indexed {} messages after {} ms, {:.2} msg/s",
                    count,
                    elapsed.as_millis(),
                    (count as f64 / elapsed.as_millis() as f64) * 1000f64
                );
            }
        });

        select(join_all(futures), timer_handle).await;
    }

    // while let Some(chunk) = message_stream.try_next().await? {
    //     count += chunk.len();
    //     let handle = write_elastic_chunk(&client, chunk, index, None);
    //     let elapsed = start.elapsed();
    //     info!(
    //         "Currently indexed {} messages after {} ms, {:.2} msg/s",
    //         count,
    //         elapsed.as_millis(),
    //         (count as f64 / elapsed.as_millis() as f64) * 1000f64
    //     );
    // }

    let final_count = count.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    info!(
        "Finished indexing {} messages after {} ms, {:.2} m/s",
        final_count,
        elapsed.as_millis(),
        (final_count as f64 / elapsed.as_millis() as f64) * 1000f64
    );
    Ok(())
}
