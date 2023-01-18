use std::path::PathBuf;

use anyhow::Result;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use log::debug;

use crate::{formats::orl::OrlLog, sources::orl::orl_file_parser::parse_file_to_logs};

use self::orl_file_parser::{OrlDirFile, read_orl_structured_dir};

use super::Source;

pub mod orl_file_parser;
pub mod orl_line_parser;

pub struct OrlFileSource {
    orl_dir: PathBuf
}

impl OrlFileSource {
    pub fn new(orl_dir: PathBuf) -> Self {
        OrlFileSource { orl_dir }
    }

    fn create_message_stream(
        &self,
        orl_files: Vec<OrlDirFile>,
    ) -> impl Stream<Item = Result<OrlLog>> {
        try_stream! {
            for file in orl_files {
                debug!("Processing file: {:?}", file.path);
                let logs = parse_file_to_logs(&file.path, &file.channel).await?;
                for message in logs {
                    yield message;
                }
            }
        }
    }
    pub async fn get_stream(&self) -> Result<impl Stream<Item = Result<OrlLog>>> {
        let orl_files = read_orl_structured_dir(&self.orl_dir).await?;
        Ok(self.create_message_stream(orl_files))
    }
}

#[async_trait(?Send)]
impl Source<Result<OrlLog>> for OrlFileSource {
    async fn pipe(&mut self, sink: impl super::Sink<Result<OrlLog>>) -> anyhow::Result<()> {
        let stream = self.get_stream().await?;
        sink.run(stream).await?;

        Ok(())
    }
}
