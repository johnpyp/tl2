use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use log::error;
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};

use super::Writer;
use crate::{
    events::{AllEvents, SimpleMessageGroup},
    settings::FileSettings,
};

pub struct FileWriter {
    tx: UnboundedSender<SimpleMessageGroup>,
    pub config: Arc<FileSettings>,
}

impl FileWriter {
    pub fn new(config: FileSettings) -> FileWriter {
        let config = Arc::new(config);
        let (tx, rx) = mpsc::unbounded_channel();
        FileWorker::spawn(config.clone(), rx);
        FileWriter { tx, config }
    }
}

impl Writer for FileWriter {
    fn write(&self, event: AllEvents) -> Result<()> {
        let smg = SimpleMessageGroup::from(event);
        self.tx.send(smg)?;
        Ok(())
    }
}

struct FileWorker {
    config: Arc<FileSettings>,
    rx: UnboundedReceiver<SimpleMessageGroup>,
    file_queues: HashMap<String, QueuedAppender>,
}
impl FileWorker {
    fn spawn(config: Arc<FileSettings>, rx: UnboundedReceiver<SimpleMessageGroup>) {
        let worker = FileWorker {
            config,
            rx,
            file_queues: HashMap::new(),
        };
        tokio::spawn(worker.run());
    }
    async fn run(mut self) {
        while let Some(msgs) = self.rx.recv().await {
            if let Err(error) = self.process(msgs).await {
                error!("[FileWriter] Error writing messages to disk: {:?}", error);
            }
        }
    }
    async fn process(&mut self, msgs: SimpleMessageGroup) -> Result<()> {
        for msg in msgs.0 {
            let msg = msg.normalize();
            let d = msg.timestamp.format("%Y-%m-%d %H:%M:%S%.3f %Z");
            let line = format!("[{}] {}: {}", d, msg.username, msg.text);
            self.write_to_file(&msg.timestamp, &msg.channel, &line)
                .await?;
        }
        Ok(())
    }

    async fn write_to_file(
        &mut self,
        date: &DateTime<Utc>,
        channel: &str,
        line: &str,
    ) -> Result<()> {
        let filename = date.format("%Y-%m-%d").to_string() + ".txt";
        let path = Path::new(&self.config.path).join(&channel).join(&filename);
        if !self.file_queues.contains_key(channel) {
            tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        }
        let queue = self
            .file_queues
            .entry(channel.to_string())
            .or_insert_with(|| QueuedAppender::new(path, 100, Duration::from_secs(30)));
        queue.write(line.to_string()).await?;

        Ok(())
    }
}

struct QueuedAppender {
    path: PathBuf,
    period: Duration,
    capacity: usize,
    queue: Vec<String>,
    last_time: Instant,
}

impl QueuedAppender {
    fn new(path: PathBuf, capacity: usize, period: Duration) -> Self {
        QueuedAppender {
            path,
            period,
            capacity,
            queue: Vec::new(),
            last_time: Instant::now(),
        }
    }

    async fn write(&mut self, line: String) -> std::io::Result<()> {
        self.queue.push(line);
        if self.queue.len() >= self.capacity || self.time_ready() {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        if self.queue.len() > 0 {
            let to_write = self.queue.join("\n") + "\n";
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .await?;

            file.write_all(to_write.as_bytes()).await?;

            self.queue.clear();
            self.last_time = Instant::now();
        }
        Ok(())
    }

    fn time_ready(&self) -> bool {
        return self.period >= Instant::now().duration_since(self.last_time);
    }
}
