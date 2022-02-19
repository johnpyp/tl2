use anyhow::Result;
use enum_dispatch::enum_dispatch;

use self::{
    clickhouse::ClickhouseWriter, console::ConsoleWriter, console_metrics::ConsoleMetricsWriter,
    elasticsearch::ElasticsearchWriter, file::FileWriter, username_tracker::UsernameTracker,
};
use crate::events::AllEvents;

pub mod clickhouse;
pub mod console;
pub mod console_metrics;
pub mod elasticsearch;
pub mod file;
pub mod username_tracker;

#[enum_dispatch]
pub enum Writers {
    File(FileWriter),
    Elasticsearch(ElasticsearchWriter),
    Console(ConsoleWriter),
    ConsoleMetrics(ConsoleMetricsWriter),
    Clickhouse(ClickhouseWriter),
    UsernameTracker(UsernameTracker),
}

#[enum_dispatch(Writers)]
pub trait Writer {
    fn write(&self, event: AllEvents) -> Result<()>;
}
