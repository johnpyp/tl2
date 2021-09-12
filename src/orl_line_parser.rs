use std::fmt::Debug;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, ParseError, TimeZone, Utc};
use nom::{
    bytes::complete::{tag, take, take_until1},
    character::complete::space1,
    combinator::rest,
    error::VerboseError,
    sequence::tuple,
    IResult,
};

use crate::orl_file_parser::OrlLog;

#[derive(Debug, PartialEq, Eq)]
pub struct RawOrlLog {
    pub ts: DateTime<Utc>,
    pub username: String,
    pub text: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct OrlDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub ms: u32,
}

type Res<T, U> = IResult<T, U, VerboseError<T>>;

pub fn parse_orl_date(input: &str) -> Result<DateTime<Utc>, ParseError> {
    Ok(DateTime::from_utc(
        NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S%.3f %Z")?,
        Utc,
    ))
}

fn orl_date_string_parser(input: &str) -> Res<&str, OrlDate> {
    let (rest, (yyyy, _, mm, _, dd, _, hh, _, minute, _, ss, _, ms, _)) = tuple((
        take(4usize),
        tag("-"),
        take(2usize),
        tag("-"),
        take(2usize),
        tag(" "),
        take(2usize),
        tag(":"),
        take(2usize),
        tag(":"),
        take(2usize),
        tag("."),
        take(3usize),
        tag(" UTC"),
    ))(input)?;
    Ok((
        rest,
        OrlDate {
            year: yyyy.parse().unwrap(),
            month: mm.parse().unwrap(),
            day: dd.parse().unwrap(),
            hour: hh.parse().unwrap(),
            minute: minute.parse().unwrap(),
            second: ss.parse().unwrap(),
            ms: ms.parse().unwrap(),
        },
    ))
}
fn raw_orl_log_parser(input: &str) -> Res<&str, (OrlDate, &str, &str)> {
    let (_, (_, orl_date, _, _, username, _, _, text)) = tuple((
        tag("["),
        orl_date_string_parser,
        tag("]"),
        space1,
        take_until1(":"),
        tag(":"),
        space1,
        rest,
    ))(input)?;

    return Ok(("", (orl_date, username, text)));
}
pub fn parse_orl_line(channel: &str, input: &str) -> Option<OrlLog> {
    let (_, (od, username, text)) = raw_orl_log_parser(input).ok()?;

    let timestamp = Utc
        .ymd(od.year, od.month, od.day)
        .and_hms_milli(od.hour, od.minute, od.second, od.ms);
    Some(OrlLog {
        ts: timestamp,
        channel: channel.to_string(),
        username: username.into(),
        text: text.into(),
    })
}

pub fn parse_orl_line_simple(channel: &str, line: &str) -> Result<OrlLog> {
    let date_string = line[1..=27].to_string();
    let after_date = &line[30..];
    let first_colon = after_date.find(':').context("no colon in orl line")?;
    let username = after_date[..first_colon].to_string();
    let text = after_date[first_colon + 2..].to_string();
    Ok(OrlLog {
        ts: parse_orl_date(&date_string)?,
        text,
        username,
        channel: channel.to_string(),
    })
}

#[cfg(test)]
mod tests {

    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_parse_orl_date() {
        let datetime = parse_orl_date("2021-08-04 00:44:12.616 UTC");
        assert_eq!(
            datetime,
            Ok(Utc.ymd(2021, 8, 4).and_hms_milli(0, 44, 12, 616))
        );
    }
    #[test]
    fn test_parse_orl_line() {
        let datetime = Utc.ymd(2021, 8, 4).and_hms_milli(0, 44, 12, 616);
        let expected_log = OrlLog {
            ts: datetime,
            channel: "Xqcow".to_string(),
            text: "!commands".to_string(),
            username: "megablade136".to_string(),
        };
        assert_eq!(
            parse_orl_line(
                "Xqcow",
                "[2021-08-04 00:44:12.616 UTC] megablade136: !commands"
            ),
            Some(expected_log.clone())
        );
        assert_eq!(
            parse_orl_line_simple(
                "Xqcow",
                "[2021-08-04 00:44:12.616 UTC] megablade136: !commands"
            )
            .unwrap(),
            expected_log
        );
    }
}
