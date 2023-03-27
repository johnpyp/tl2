use std::ffi::OsStr;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use async_stream::try_stream;
use futures::Stream;
use futures::TryStreamExt;
use log::debug;
use rayon::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use tokio::fs;
use tokio::fs::DirEntry;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::pin;
use tokio_stream::wrappers::ReadDirStream;

use super::line_parser::parse_orl_log;
use crate::formats::orl::CleanOrlLog;
use crate::formats::orl::OrlLog;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OrlDirFile {
    pub path: PathBuf,
    pub channel: String,
}

pub async fn read_orl_file_to_string(path: &Path) -> Result<String> {
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

fn parse_contents_to_logs(contents: String, channel: &str) -> Vec<CleanOrlLog> {
    return contents
        .lines()
        .into_iter()
        .map(|line| line.trim())
        .flat_map(|line| parse_orl_log(channel.to_string(), line).ok())
        .collect();
}

pub struct MinimalOrlLine {
    channel: String,
    line: String,
}

pub fn parse_minimal_lines(contents: String, channel: &str) -> Vec<MinimalOrlLine> {
    return contents
        .lines()
        .into_iter()
        .map(|x| MinimalOrlLine {
            line: x.to_string(),
            channel: channel.to_string(),
        })
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
) -> Result<impl Stream<Item = Result<Vec<CleanOrlLog>>>> {
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

fn create_orl_lines_stream(
    orl_files: Vec<OrlDirFile>,
) -> impl Stream<Item = Result<MinimalOrlLine>> {
    try_stream! {
        for file in orl_files {
            debug!("Processing file: {:?}", file.path);
            let contents = read_orl_file_to_string(&file.path).await?;
            let minimal_lines = parse_minimal_lines(contents, &file.channel);
            debug!("Got lines: {:?}", minimal_lines.len());
            for line in minimal_lines {
                yield line;
            }
        }
    }
}

pub fn create_orl_messages_stream(
    orl_files: Vec<OrlDirFile>,
) -> impl Stream<Item = Result<CleanOrlLog>> {
    // let lines_stream = create_orl_lines_stream(orl_files);

    // return lines_stream
    //     .try_chunks(100_000)
    //     .map_ok(|chunk| -> Vec<_> {
    //         chunk
    //             .par_iter()
    //             .flat_map(|x| parse_orl_line(x.line.trim(), &x.channel))
    //             .collect()
    //     })
    //     .map_ok(|messages| stream::iter(messages))
    //     .flatten();

    try_stream! {
        let lines_stream = create_orl_lines_stream(orl_files);
        pin!(lines_stream);
        let mut stream = lines_stream.try_chunks(1_000_000);

        while let Some(chunk) = stream.try_next().await? {
            // debug!("Got one chunk of len: {:?}", chunk.len());
            let messages: Vec<_> = chunk
                .par_iter()
                .flat_map(|x| parse_orl_log(x.channel.to_string(), x.line.trim()).ok())
                .collect();


            for message in messages {
                yield message;
            }
        }
    }
}

pub async fn parse_file_to_logs(path: &Path, channel: &str) -> Result<Vec<CleanOrlLog>> {
    let contents = read_orl_file_to_string(path).await?;
    debug!("Contents length: {:?}", contents.len());

    Ok(parse_contents_to_logs(contents, channel))
}

#[cfg(test)]
mod tests {

    use std::marker::PhantomData;

    use super::*;
    use crate::formats::orl::Clean;
    use crate::sources::orl::orl_line_parser::parse_orl_date;

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

                _s: PhantomData::<Clean>,
            },
            OrlLog {
                channel: "A_seagull".into(),
                ts: parse_orl_date("2021-08-04 07:01:21.350 UTC").unwrap(),
                text: "zakwern just subscribed with Prime for 1 months!".into(),
                username: "@subscriber".into(),

                _s: PhantomData::<Clean>,
            },
        ];

        let parsed_logs = parse_contents_to_logs(contents.to_string(), channel);
        assert_eq!(parsed_logs, expected);
    }
}
