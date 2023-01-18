use async_trait::async_trait;
use futures::stream::Stream;
use anyhow::Result;

pub mod avro;
pub mod clickhouse_bulk;
pub mod elasticsearch_bulk;
pub mod sqlite;
pub mod jsonl;

#[async_trait(?Send)]
pub trait Sink<SourceItem> : Sized {
    async fn run(mut self, stream: impl Stream<Item = SourceItem> + Send) -> Result<()>;
}
