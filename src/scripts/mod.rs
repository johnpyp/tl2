use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use bytesize::ByteSize;
use futures::future;
use futures::StreamExt;

use crate::sinks::clickhouse_bulk::ClickhouseBulkSink;
use crate::sinks::elasticsearch_bulk::ElasticsearchBulkSink;
use crate::sinks::jsonl::JsonFileSink;
use crate::sources::jsonl::JsonFileSource;
use crate::sources::jsonl::KnownSize;
use crate::sources::orl::OrlFileSource;
use crate::sources::Source;

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

    elasticsearch_bulk_sink.init_templates().await?;

    json_file_source.pipe(elasticsearch_bulk_sink).await
}

pub async fn jsonl_to_clickhouse(input_directory: PathBuf, clickhouse_url: String) -> Result<()> {
    let mut json_file_source = JsonFileSource::new(input_directory);

    let clickhouse_bulk_sink = ClickhouseBulkSink::new(clickhouse_url)
        .with_context(|| "Failed to create clickhouse client")?;

    clickhouse_bulk_sink
        .init()
        .await
        .with_context(|| "Failed to init clickhouse")?;

    json_file_source.pipe(clickhouse_bulk_sink).await?;

    Ok(())
}

pub async fn jsonl_to_console(input_directory: PathBuf) -> Result<()> {
    let json_file_source = JsonFileSource::new(input_directory);

    let stream = json_file_source.create_orl_log_stream().await?;

    let mut count = 0;
    let mut byte_count = 0;
    let start = Instant::now();
    let fut = stream.for_each(|log_result| {
        count += 1;
        let sized_log = match log_result {
            Ok(x) => x,
            Err(e) => {
                println!("Errored log: {:?}", e);
                return future::ready(());
            }
        };

        let KnownSize { v: log, size } = sized_log;

        byte_count += size;

        if count % 1_000_000 == 0 {
            let elapsed = start.elapsed();

            let bytes_per_second = (byte_count as f64 / elapsed.as_secs_f64()).floor() as u64;
            let byte_size = ByteSize::b(bytes_per_second);
            println!(
                "Total {:?}, time: {:.2} msg/s, {}/s",
                count,
                (count as f64 / elapsed.as_secs_f64()),
                byte_size,
            );
            // println!("{:?}", log);
        }

        future::ready(())
    });

    fut.await;

    Ok(())
}
