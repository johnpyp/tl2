use std::{
    convert::TryInto,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use elasticsearch::{
    http::{request::JsonBody, transport::Transport},
    indices::IndicesPutTemplateParts,
    ingest::IngestPutPipelineParts,
    BulkParts, Elasticsearch,
};
use log::{error, info};
use serde_json::{json, Value};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_compat_02::FutureExt;

use super::Writer;
use crate::{
    events::{AllEvents, SimpleMessage, SimpleMessageGroup},
    settings::ElasticsearchSettings,
};

pub struct ElasticsearchWriter {
    tx: UnboundedSender<SimpleMessage>,
    pub config: ElasticsearchSettings,
}

impl ElasticsearchWriter {
    pub fn new(config: ElasticsearchSettings) -> Result<ElasticsearchWriter> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut worker = ElasticsearchWorker {
            client: create_elasticsearch_client(&config.host, config.port)?,
            rx,
            index: config.index.clone(),
            pipeline: config.pipeline.clone(),
            batch_size: config.batch_size,
            period: Duration::from_secs(config.batch_period_seconds),
            retries: 0,
        };

        tokio::spawn(async move { worker.work().compat().await });
        Ok(ElasticsearchWriter { config, tx })
    }
}
impl Writer for ElasticsearchWriter {
    fn write(&self, msg: AllEvents) -> Result<()> {
        let msgs: SimpleMessageGroup = msg.into();
        for msg in msgs.0 {
            self.tx.send(msg).with_context(|| {
                "Sending message to Elasticsearch worker failed, rx probably dropped"
            })?;
        }
        Ok(())
    }
}

struct ElasticsearchWorker {
    pub client: Elasticsearch,
    pub rx: UnboundedReceiver<SimpleMessage>,
    pub index: String,
    pub pipeline: Option<String>,
    pub batch_size: u64,
    pub period: Duration,
    pub retries: i32,
}

impl ElasticsearchWorker {
    async fn work(&mut self) {
        loop {
            if let Err(e) = self.run().await {
                error!("Elasticsearch adapter failed: {:?}", e);
                self.retries += 1;
            }
            if self.retries > 5 {
                error!("Reached 5 failing retries on elasticsearch without a successful batch, shutting down writer...");
                return;
            }
            info!("Reinitializing elasticsearch writer in 5 seconds...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    async fn run(&mut self) -> Result<()> {
        self.inititalize().await?;

        let mut batch = Vec::new();
        let mut last_time = Instant::now();

        info!("Starting ES ingestion loop");
        while let Some(msg) = self.rx.recv().await {
            batch.push(msg);
            if batch.len() >= self.batch_size.try_into().unwrap()
                || Instant::now().duration_since(last_time) > self.period
            {
                self.process(&batch)
                    .await
                    .with_context(|| "Processing batch of messages failed")?;
                self.retries = 0;
                batch.clear();
                last_time = Instant::now();
            }
        }
        Ok(())
    }

    async fn inititalize(&mut self) -> Result<()> {
        info!("Initializing ES templates");
        initialize_template(&self.client, &self.index)
            .await
            .with_context(|| "Error initializing elasticsearch templates")?;
        info!("Initializing ES pipelines");
        if let Some(pipeline) = &self.pipeline {
            initialize_pipeline(&self.client, &self.index, &pipeline)
                .await
                .with_context(|| "Error initializing elasticsearch pipelines")?;
        }
        Ok(())
    }

    async fn process(&mut self, msgs: &Vec<SimpleMessage>) -> Result<()> {
        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(msgs.len() * 2);
        for msg in msgs {
            let msg = msg.normalize();
            let username = msg.username.to_string();
            let ts = msg
                .timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            body.push(json!({ "index": { "_index": self.index }}).into());
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

        let mut req = self.client.bulk(BulkParts::Index(&self.index));
        if let Some(pipeline) = &self.pipeline {
            req = req.pipeline(pipeline)
        }
        let response = req.body(body).send().await?.error_for_status_code()?;

        let response_body = response.json::<Value>().await?;

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
}

fn create_elasticsearch_client(host: &str, port: u32) -> Result<Elasticsearch> {
    let url = format!("{}:{}", host, port);

    let transport =
        Transport::single_node(&url).with_context(|| "Building elasticsearch url failed")?;
    let client = Elasticsearch::new(transport);
    Ok(client)
}

async fn initialize_template(client: &Elasticsearch, index: &str) -> Result<()> {
    let exception = client
        .indices()
        .put_template(IndicesPutTemplateParts::Name(&format!(
            "{}-template",
            index
        )))
        .body(json!({
          "index_patterns": index.to_string() + "-*",
          "mappings": {
            "properties": {
              "channel": { "type": "keyword" },
              "text": { "type": "text" },
              "ts": { "type": "date" },
              "username": { "type": "keyword" },
            },
          },
          "settings": {
            "number_of_replicas": 0,
            "number_of_shards": 1,
            "refresh_interval": "1s",
            "sort.field": ["ts", "ts"],
            "sort.order": ["desc", "asc"],
            "codec": "best_compression",
          },
        }))
        .send()
        .await?
        .exception()
        .await?;
    if let Some(exception) = exception {
        bail!(
            "Initializing templates failed with Exception: {:?}",
            exception
        );
    }

    Ok(())
}

async fn initialize_pipeline(client: &Elasticsearch, index: &str, pipeline: &str) -> Result<()> {
    let exception = client
        .ingest()
        .put_pipeline(IngestPutPipelineParts::Id(pipeline))
        .body(json!({
          "description": "monthly date-time index naming",
          "processors": [
            {
              "date_index_name": {
                "date_rounding": "M",
                "field": "ts",
                "index_name_prefix": index.to_string() + "-",
              },
            },
            {
              "set": {
                "field": "_id",
                "value": "{{channel}}-{{username}}-{{ts}}",
              },
            },
          ],
        }))
        .send()
        .await?
        .exception()
        .await?;
    if let Some(exception) = exception {
        bail!(
            "Initializing pipeline failed with Exception: {:?}",
            exception
        );
    }

    Ok(())
}
