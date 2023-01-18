use std::fmt::Debug;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, ParseError, TimeZone, Utc};
use nom::{
    bytes::complete::{tag, take, take_until1},
    character::complete::space1,
    combinator::rest,
    error::VerboseError,
    sequence::tuple,
    IResult,
};

use crate::formats::orl::OrlLog;

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

    Ok(("", (orl_date, username, text)))
}
pub fn parse_orl_line(channel: &str, input: &str) -> Option<OrlLog> {
    let (_, (od, username, text)) = raw_orl_log_parser(input).ok()?;

    let timestamp = NaiveDate::from_ymd_opt(od.year, od.month, od.day)
        .and_then(|d| d.and_hms_milli_opt(od.hour, od.minute, od.second, od.ms))
        .map(|dt| Utc.from_utc_datetime(&dt))?;
    Some(OrlLog {
        ts: timestamp,
        channel: channel.to_string(),
        username: username.into(),
        text: text.into(),

        is_normal: false,
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

        is_normal: false,
    })
}

#[cfg(test)]
mod tests {

    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_parse_orl_date() {
        let datetime = parse_orl_date("2021-08-04 00:44:12.616 UTC");

        let expected_date = NaiveDate::from_ymd_opt(2021, 8, 4)
            .and_then(|d| d.and_hms_milli_opt(0, 44, 12, 616))
            .map(|dt| Utc.from_utc_datetime(&dt))
            .unwrap();
        assert_eq!(
            datetime,
            Ok(expected_date)
        );
    }
    #[test]
    fn test_parse_orl_line() {
        let expected_date = NaiveDate::from_ymd_opt(2021, 8, 4)
            .and_then(|d| d.and_hms_milli_opt(0, 44, 12, 616))
            .map(|dt| Utc.from_utc_datetime(&dt))
            .unwrap();
        let expected_log = OrlLog {
            ts: expected_date,
            channel: "Xqcow".to_string(),
            text: "!commands".to_string(),
            username: "megablade136".to_string(),

            is_normal: false,
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
