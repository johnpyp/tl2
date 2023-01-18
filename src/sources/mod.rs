use async_trait::async_trait;
use anyhow::Result;

use crate::sinks::Sink;

pub mod orl;
pub mod jsonl;

#[async_trait(?Send)]
pub trait Source<SourceItem> : Sized {
    async fn pipe(&mut self, sink: impl Sink<SourceItem>) -> Result<()>; 
}
