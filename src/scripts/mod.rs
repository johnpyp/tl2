use anyhow::Result;
use futures::{future, StreamExt};
use std::{path::PathBuf, time::Instant};

use crate::{
    sinks::{elasticsearch_bulk::ElasticsearchBulkSink, jsonl::JsonFileSink},
    sources::{jsonl::JsonFileSource, orl::OrlFileSource, Source},
};

pub mod file_to_clickhouse;
pub mod file_to_elasticsearch;
pub mod file_to_sqlite;

pub async fn dir_to_jsonl(orl_input_directory: PathBuf, output_directory: PathBuf) -> Result<()> {
    let mut orl_source = OrlFileSource::new(orl_input_directory);
    let jsonl_sink = JsonFileSink::new(output_directory);

    orl_source.pipe(jsonl_sink).await
}

pub async fn jsonl_to_elasticsearch(
    input_directory: PathBuf,
    elastic_url: String,
    elastic_index: String,
) -> Result<()> {
    let mut json_file_source = JsonFileSource::new(input_directory);

    let elasticsearch_bulk_sink = ElasticsearchBulkSink::new(elastic_url, elastic_index, None)?;

    json_file_source.pipe(elasticsearch_bulk_sink).await
}

pub async fn jsonl_to_console(input_directory: PathBuf) -> Result<()> {
    let json_file_source = JsonFileSource::new(input_directory);

    let s = json_file_source.create_orl_log_stream().await?;

    let mut count = 0;
    let start = Instant::now();
    let fut = s.for_each(|log_result| {
        count += 1;
        let _log = match log_result {
            Ok(x) => x,
            Err(e) => {
                println!("Errored log: {:?}", e);
                return future::ready(());
            }
        };

        // let s = serde_json::to_string(&log).expect("Shouldn't fail");

        // byte_count += s.len();

        if count % 1_000_000 == 0 {
            let elapsed = start.elapsed();
            println!(
                "Total {:?}, time: {:.2} m/s",
                count,
                (count as f64 / elapsed.as_millis() as f64) * 1000f64,
                // (byte_count as f64 / elapsed.as_secs_f64()) / 1000f64
            );
            // println!("{:?}", log);
        }

        future::ready(())
    });

    fut.await;

    Ok(())
}
