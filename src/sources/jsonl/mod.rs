use anyhow::{Context, Result};
use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZstdDecoder};
use async_trait::async_trait;
use futures::{future, stream, Stream, StreamExt, TryStreamExt};
use log::warn;
use par_stream::TryParStreamExt;
use rayon::prelude::*;
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, File},
    io::{AsyncBufRead, AsyncRead, AsyncReadExt, BufReader},
};
use tokio_stream::wrappers::ReadDirStream;

use crate::{formats::unified::OrlLog1_0, sinks::Sink};

use super::Source;

struct JsonFileTarget {
    path: PathBuf,
}

pub struct JsonFileSourceContext {
    root_dir: PathBuf,
}

pub struct JsonFileSource {
    ctx: JsonFileSourceContext,
}

impl JsonFileSource {
    pub fn new(root_dir: PathBuf) -> JsonFileSource {
        JsonFileSource {
            ctx: JsonFileSourceContext { root_dir },
        }
    }

    pub async fn create_orl_log_stream(&self) -> Result<impl Stream<Item = Result<OrlLog1_0>>> {
        let target_stream = self.create_json_target_stream().await?;

        let log_stream = target_stream
            .try_par_then_unordered(None, |target| async move {
                let contents = JsonFileSource::read_target_contents(&target.path).await?;
                Ok(contents)
            })
            .try_chunks(30)
            .try_map_blocking(None, move |contents| {
                let logs = JsonFileSource::parse_contents_to_logs(contents.join(""));
                Ok(stream::iter(logs).map(Ok))
            })
            .try_flatten();

        Ok(log_stream)
    }

    // This is blocking, but it doesn't seem like putting this in a spawn_blocking loop helps with
    // performance much.
    fn parse_contents_to_logs(contents: String) -> Vec<OrlLog1_0> {
        return contents
            .par_lines()
            .filter_map(JsonFileSource::parse_jsonl_line)
            // .filter_map(|uml| {
            //     if let UnifiedMessageLog::OrlLog1_0(log) = uml {
            //         return Some(log);
            //     }
            //     None
            // })
            .collect();
    }

    fn parse_jsonl_line(line: &str) -> Option<OrlLog1_0> {
        // let mut vec_bytes = line.as_bytes().to_vec();
        // Some(simd_json::serde::from_slice(&mut vec_bytes).unwrap_or_else(|_| panic!("Shouldn't fail parsing {:?}", line)))

        // Some(OrlLog1_0 {
        //     channel_name: "Destiny".to_string(),
        //     key: CommonKey {
        //         id: "1628035200095-b0c0ade6-d41a6bfa-3a94d2dd".to_string(),
        //         timestamp: 1628035200064
        //     },
        //     text: "PepegaAim".to_string(),
        //     username: "lucrezia_002".to_string()
        // })
        Some(
            serde_json::from_str(line)
                .unwrap_or_else(|_| panic!("Shouldn't fail parsing {:?}", line)),
        )
    }

    async fn read_target_contents(path: &Path) -> Result<String> {
        let file = File::open(path).await?;
        let mut buf_reader = BufReader::new(file);

        let decoded = JsonFileSource::read_to_vec(path.extension(), &mut buf_reader).await?;
        let contents = String::from_utf8(decoded)?;

        Ok(contents)
    }

    async fn read_to_vec(ext: Option<&OsStr>, buf_reader: &mut BufReader<File>) -> Result<Vec<u8>> {
        let mut decoded: Vec<u8> = vec![];

        let ext = ext.unwrap_or_else(|| OsStr::new("txt"));

        if ext == OsStr::new("gz") {
            let mut reader = GzipDecoder::new(buf_reader);
            reader.read_to_end(&mut decoded).await?;
            return Ok(decoded);
        }

        if ext == OsStr::new("zst") {
            let mut reader = ZstdDecoder::new(buf_reader);
            reader.read_to_end(&mut decoded).await?;
            return Ok(decoded);
        }

        if ext == OsStr::new("br") {
            let mut reader = BrotliDecoder::new(buf_reader);
            reader.read_to_end(&mut decoded).await?;
            return Ok(decoded);
        }

        buf_reader.read_to_end(&mut decoded).await?;
        Ok(decoded)
    }

    async fn create_json_target_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<JsonFileTarget>>> {
        let dir: fs::ReadDir = fs::read_dir(&self.ctx.root_dir).await?;

        let root_dir_stream = ReadDirStream::new(dir);

        Ok(root_dir_stream
            .inspect_err(|err| warn!("Error processing json files list, {:?}", err))
            .err_into::<anyhow::Error>()
            .and_then(|entry| async move {
                let sub_path = entry.path();
                let file_type = entry
                    .file_type()
                    .await
                    .context("Fetching file type failed")?;

                if !file_type.is_dir() {
                    return Ok(None);
                }
                Ok(Some(sub_path))
            })
            .try_filter_map(|x| async move { Ok(x) })
            .and_then(|sub_path| async move {
                let sub_dir = fs::read_dir(sub_path).await?;

                let sub_dir_stream = ReadDirStream::new(sub_dir);

                Ok(sub_dir_stream
                    .filter_map(|x| async move {
                        if let Ok(x) = x {
                            return Some(x);
                        }
                        None
                    })
                    .map(|entry| entry.path())
                    .filter(|p| {
                        future::ready({
                            let s = p.to_str();
                            if let Some(s) = s {
                                return future::ready(
                                    s.ends_with(".jsonl")
                                        || s.ends_with(".jsonl.gz")
                                        || s.ends_with(".jsonl.br"),
                                );
                            }
                            false
                        })
                    })
                    .map(move |p| Ok(JsonFileTarget { path: p })))
            })
            .try_flatten())
    }
}

#[async_trait(?Send)]
impl Source<Result<OrlLog1_0>> for JsonFileSource {
    async fn pipe(&mut self, sink: impl Sink<Result<OrlLog1_0>>) -> anyhow::Result<()> {
        let stream = self.create_orl_log_stream().await?;
        sink.run(stream).await?;

        Ok(())
    }
}
