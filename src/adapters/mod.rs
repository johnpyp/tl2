use anyhow::Result;
use enum_dispatch::enum_dispatch;

use self::{
    clickhouse::ClickhouseWriter, console::ConsoleWriter, console_metrics::ConsoleMetricsWriter,
    elasticsearch::ElasticsearchWriter, file::FileWriter,
};
use crate::events::AllEvents;

pub mod clickhouse;
pub mod console;
pub mod console_metrics;
pub mod elasticsearch;
pub mod file;

#[enum_dispatch]
pub enum Writers {
    File(FileWriter),
    Elasticsearch(ElasticsearchWriter),
    Console(ConsoleWriter),
    ConsoleMetrics(ConsoleMetricsWriter),
    Clickhouse(ClickhouseWriter),
}

#[enum_dispatch(Writers)]
pub trait Writer {
    fn write(&self, event: AllEvents) -> Result<()>;
}
