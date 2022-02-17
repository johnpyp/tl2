use std::time::{Duration, Instant};

use anyhow::Result;
use log::info;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use super::Writer;
use crate::events::{AllEvents, SimpleMessage, SimpleMessageGroup};

pub struct ConsoleMetricsWriter {
    tx: UnboundedSender<SimpleMessage>,
}

impl ConsoleMetricsWriter {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut worker = ConsoleMetricsWorker::new(rx);
        tokio::spawn(async move { worker.run().await });
        Self { tx }
    }
}

impl Writer for ConsoleMetricsWriter {
    fn write(&self, event: AllEvents) -> Result<()> {
        let msgs = SimpleMessageGroup::from(event);

        for msg in msgs.0 {
            self.tx.send(msg)?;
        }
        Ok(())
    }
}

struct ConsoleMetricsWorker {
    rx: UnboundedReceiver<SimpleMessage>,
    count: u64,
    last_time: Instant,
}
impl ConsoleMetricsWorker {
    pub fn new(rx: UnboundedReceiver<SimpleMessage>) -> Self {
        Self {
            rx,
            count: 0,
            last_time: Instant::now(),
        }
    }

    pub async fn run(&mut self) {
        while self.rx.recv().await.is_some() {
            self.count += 1;
            let seconds = 30;
            if Instant::now().duration_since(self.last_time) > Duration::from_secs(seconds) {
                info!(
                    "{} messages/min, {:.2} messages/s",
                    (self.count * 60) / seconds,
                    self.count as f64 / seconds as f64,
                );
                self.last_time = Instant::now();
                self.count = 0;
            }
        }
    }
}
