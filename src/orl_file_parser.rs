use std::ffi::OsStr;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use async_stream::try_stream;
use chrono::DateTime;
use chrono::Utc;
use futures::Stream;
use futures::TryStreamExt;
use log::debug;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::fs::DirEntry;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio_stream::wrappers::ReadDirStream;

use crate::orl_line_parser::{parse_orl_line, RawOrlLog};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OrlLog {
    pub ts: DateTime<Utc>,
    pub username: String,
    pub text: String,
    pub channel: String,
}

pub struct OrlDirFile {
    pub path: PathBuf,
    pub channel: String,
}

impl OrlLog {
    pub fn from_raw(raw: RawOrlLog, channel: &str) -> OrlLog {
        OrlLog {
            channel: channel.to_string(),
            ts: raw.ts,
            username: raw.username,
            text: raw.text,
        }
    }
}

async fn read_file_to_string(path: &Path) -> Result<String> {
    let file = File::open(path).await?;
    let mut buf_reader = BufReader::new(file);

    let gz_ext = OsStr::new("gz");
    if path.extension() == Some(gz_ext) {
        let mut reader = GzipDecoder::new(buf_reader);
        let mut decoded: Vec<u8> = vec![];
        reader.read_to_end(&mut decoded).await?;

        let contents = String::from_utf8(decoded)?;

        return Ok(contents);
    }

    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).await?;
    Ok(contents)
}

fn parse_contents_to_logs(contents: String, channel: &str) -> Vec<OrlLog> {
    return contents
        .lines()
        .into_iter()
        .map(|line| line.trim())
        .flat_map(|line| parse_orl_line(channel, line))
        .collect();
}

pub async fn read_orl_structured_dir(dir_path: &Path) -> Result<Vec<OrlDirFile>> {
    let mut res: Vec<OrlDirFile> = Vec::new();
    let mut dir = fs::read_dir(dir_path).await?;
    while let Some(entry) = dir.next_entry().await? {
        let sub_path = entry.path();
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            let channel_name = entry
                .file_name()
                .to_str()
                .context("File name couldn't be converted to str")?
                .to_string();
            let sub_dir = ReadDirStream::new(fs::read_dir(sub_path).await?);
            let entries: Vec<DirEntry> = sub_dir.try_collect::<Vec<DirEntry>>().await?;
            let orl_dir_files: Vec<OrlDirFile> = entries
                .iter()
                .map(|entry| entry.path())
                .filter(|p| {
                    let s = p.to_str();
                    if let Some(s) = s {
                        return s.ends_with(".txt") || s.ends_with(".txt.gz");
                    }
                    false
                })
                .map(|p| OrlDirFile {
                    path: p,
                    channel: channel_name.clone(),
                })
                .collect();
            debug!(
                "Found {} valid files for channel {}, out of {} entries",
                orl_dir_files.len(),
                channel_name,
                entries.len()
            );
            res.extend(orl_dir_files)
        }
    }
    Ok(res)
}
pub async fn parse_orl_dir_to_logs(
    dir_path: &Path,
) -> Result<impl Stream<Item = Result<Vec<OrlLog>>>> {
    let orl_dir_files = read_orl_structured_dir(dir_path).await?;
    Ok(try_stream! {

        for orl_file in orl_dir_files {
            let channel = orl_file.channel;
            let path = orl_file.path;

            let logs = parse_file_to_logs(&path, &channel).await?;
            yield logs;
        }
    })
}
pub async fn parse_file_to_logs(path: &Path, channel: &str) -> Result<Vec<OrlLog>> {
    let contents = read_file_to_string(path).await?;
    debug!("Contents length: {:?}", contents.len());

    Ok(parse_contents_to_logs(contents, channel))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::orl_line_parser::parse_orl_date;

    #[test]
    fn test_parse_contents_to_logs() {
        let channel = "A_seagull";
        let contents = r#"
            [2021-08-04 00:44:12.616 UTC] megablade136: !commands
            [2021-08-04 07:01:21.350 UTC] @subscriber: zakwern just subscribed with Prime for 1 months!
        "#;
        let expected = vec![
            OrlLog {
                channel: "A_seagull".into(),
                ts: parse_orl_date("2021-08-04 00:44:12.616 UTC").unwrap(),
                text: "!commands".into(),
                username: "megablade136".into(),
            },
            OrlLog {
                channel: "A_seagull".into(),
                ts: parse_orl_date("2021-08-04 07:01:21.350 UTC").unwrap(),
                text: "zakwern just subscribed with Prime for 1 months!".into(),
                username: "@subscriber".into(),
            },
        ];

        let parsed_logs = parse_contents_to_logs(contents.to_string(), channel);
        assert_eq!(parsed_logs, expected);
    }
}
