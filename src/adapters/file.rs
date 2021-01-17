use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use log::{error, trace};
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
            .or_insert_with(|| {
                QueuedAppender::new(channel.to_string(), 50, Duration::from_secs(5))
            });
        queue.write(path, line.to_string()).await?;

        Ok(())
    }
}

struct QueuedAppender {
    channel: String,
    period: Duration,
    capacity: usize,
    queue: HashMap<PathBuf, Vec<String>>,
    last_time: Instant,
}

impl QueuedAppender {
    fn new(channel: String, capacity: usize, period: Duration) -> Self {
        QueuedAppender {
            channel,
            period,
            capacity,
            queue: HashMap::new(),
            last_time: Instant::now(),
        }
    }

    fn queue_len(&self) -> usize {
        self.queue.iter().fold(0usize, |sum, (_, v)| sum + v.len())
    }
    async fn write(&mut self, path: PathBuf, line: String) -> std::io::Result<()> {
        let list = self.queue.entry(path).or_insert_with(|| Vec::new());
        list.push(line);
        let queue_len = self.queue_len();
        let time_ready = self.time_ready();
        if queue_len >= self.capacity || time_ready {
            trace!(
                "Flushing with channel: {}, queue_len: {}, capacity: {}, time_ready: {}",
                self.channel,
                queue_len,
                self.capacity,
                time_ready
            );
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        for (path, list) in &mut self.queue {
            if list.len() > 0 {
                let to_write = list.join("\n") + "\n";
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .await?;

                file.write_all(to_write.as_bytes()).await?;
            }
        }
        self.last_time = Instant::now();
        self.queue.clear();
        Ok(())
    }

    fn time_ready(&self) -> bool {
        return self.period <= Instant::now().duration_since(self.last_time);
    }
}
