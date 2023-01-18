use rayon::prelude::*;
use std::{collections::HashMap, path::PathBuf};

use anyhow::Result;
use chrono::NaiveDate;

use tokio::{fs::OpenOptions, io::AsyncWriteExt};

use crate::formats::{orl::OrlLog, unified::UnifiedMessageLog};

pub struct JsonLinesSinkContext {
    root_dir: PathBuf,
}

impl JsonLinesSinkContext {
    pub fn new(root_dir: PathBuf) -> Self {
        JsonLinesSinkContext { root_dir }
    }
}

pub struct JsonInputBatch<'a> {
    ctx: &'a JsonLinesSinkContext,
    logs: Vec<OrlLog>,
}

impl JsonInputBatch<'_> {
    pub fn new(ctx: &'_ JsonLinesSinkContext, logs: Vec<OrlLog>) -> JsonInputBatch<'_> {
        JsonInputBatch { ctx, logs }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct JsonFileTarget {
    channel_name: String,
    day: NaiveDate,
}

struct JsonFileWriteBatch<'a> {
    ctx: &'a JsonLinesSinkContext,
    target: JsonFileTarget,
    // These logs should all have the same date as `day` when rounded
    logs: Vec<OrlLog>,
}

impl JsonFileWriteBatch<'_> {
    async fn write_to_file(&self) -> Result<usize> {
        let path = self.get_path();

        tokio::fs::create_dir_all(path.parent().unwrap()).await?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        let json_lines: Vec<String> = self
            .logs
            .par_iter()
            .map(|x| x.clone().into())
            .map(UnifiedMessageLog::OrlLog1_0)
            .map(|x| serde_json::to_string(&x).expect("Expect no failure"))
            .collect();

        let write_content = json_lines.join("\n") + "\n";

        let bytes_content = write_content.as_bytes();

        let byte_len = bytes_content.len();
        file.write_all(bytes_content).await?;

        Ok(byte_len)
    }

    fn get_path(&self) -> PathBuf {
        let filename = self.target.day.format("%Y-%m-%d").to_string() + ".jsonl";
        self.ctx
            .root_dir
            .join(&self.target.channel_name)
            .join(filename)
    }
}

fn group_logs(batch: JsonInputBatch) -> Vec<JsonFileWriteBatch> {
    let mut day_map: HashMap<JsonFileTarget, Vec<OrlLog>> = HashMap::new();
    for log in batch.logs {
        let day = log.ts.date_naive();
        let target = JsonFileTarget {
            day,
            channel_name: log.channel.clone(),
        };

        let log_list = day_map.entry(target).or_insert_with(Vec::new);
        log_list.push(log);
    }

    let mut batch_results = Vec::new();
    for (target, logs) in day_map.into_iter() {
        let write_batch = JsonFileWriteBatch {
            ctx: batch.ctx,
            target,
            logs,
        };
        batch_results.push(write_batch);
    }

    batch_results
}

pub async fn submit_orl_message_batch(batch: JsonInputBatch<'_>) -> Result<usize> {
    let write_batches = group_logs(batch);

    let mut bytes_total = 0;
    for write_batch in write_batches {
        let bytes = write_batch.write_to_file().await?;
        bytes_total += bytes;
    }

    Ok(bytes_total)
}
