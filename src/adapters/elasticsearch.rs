use std::{
    convert::TryInto,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use elasticsearch::{
    http::{
        request::JsonBody,
        transport::{SingleNodeConnectionPool, Transport, TransportBuilder},
    },
    indices::IndicesPutTemplateParts,
    ingest::IngestPutPipelineParts,
    BulkParts, Elasticsearch,
};
use log::{error, info};
use reqwest::Url;
use serde_json::{json, Value};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

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
        };

        tokio::spawn(async move { worker.run().await });
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
}

impl ElasticsearchWorker {
    async fn run(&mut self) {
        info!("Initializing ES templates");
        if let Err(e) = initialize_template(&self.client, &self.index).await {
            error!("Error initializing elasticsearch templates: {:?}", e);
            return;
        }
        info!("Initializing ES pipelines");
        if let Some(pipeline) = &self.pipeline {
            if let Err(e) = initialize_pipeline(&self.client, &self.index, &pipeline).await {
                error!("Error initializing elasticsearch pipelines: {:?}", e);
                return;
            }
        }
        let mut batch = Vec::new();
        let mut last_time = Instant::now();
        info!("Starting ES ingestion loop");
        while let Some(msg) = self.rx.recv().await {
            batch.push(msg);
            if batch.len() >= self.batch_size.try_into().unwrap()
                || Instant::now().duration_since(last_time) > self.period
            {
                if let Err(error) = self.process(&batch).await {
                    error!("Error writing messages to elasticsearch: {:?}", error);
                }
                batch.clear();
                last_time = Instant::now();
            }
        }
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
    let response = client
        .indices()
        .put_template(IndicesPutTemplateParts::Name(&format!(
            "{}-template",
            index
        )))
        .body(json!({
          "index_patterns": index.to_string() + "*",
          "template": {
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
          },
        }))
        .send()
        .await?;
    let response_body = response.json::<Value>().await?;
    dbg!(response_body);

    Ok(())
}

async fn initialize_pipeline(client: &Elasticsearch, index: &str, pipeline: &str) -> Result<()> {
    let response = client
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
        .await?;
    let response_body = response.json::<Value>().await?;
    dbg!(response_body);

    Ok(())
}
