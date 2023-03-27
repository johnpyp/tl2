use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::TimeZone;
use chrono::Utc;

enum State {
    Start,
    Year,
    Month,
    Day,
    Time,
    Hour,
    Minute,
    Second,
    Millisecond,
}

#[inline(always)]
pub fn parse_timestamp(timestamp_str: &str) -> Result<DateTime<Utc>> {
    let mut year: i32 = 0;
    let mut month: u32 = 0;
    let mut day: u32 = 0;
    let mut hour: u32 = 0;
    let mut minute: u32 = 0;
    let mut second: u32 = 0;
    let mut millisecond: u32 = 0;
    let mut state = State::Start;

    for c in timestamp_str.chars() {
        match state {
            State::Start => {
                if c.is_ascii_digit() {
                    year = c.to_digit(10).unwrap() as i32;
                    state = State::Year;
                } else if !c.is_whitespace() {
                    bail!("Invalid timestamp: expected digit or whitespace at start");
                }
            }
            State::Year => {
                if c.is_ascii_digit() {
                    year = year * 10 + c.to_digit(10).unwrap() as i32;
                } else if c == '-' {
                    state = State::Month;
                } else {
                    bail!("Invalid timestamp: expected '-' after year");
                }
            }
            State::Month => {
                if c.is_ascii_digit() {
                    month = month * 10 + c.to_digit(10).unwrap();
                } else if c == '-' {
                    state = State::Day;
                } else {
                    bail!("Invalid timestamp: expected '-' after month");
                }
            }
            State::Day => {
                if c.is_ascii_digit() {
                    day = day * 10 + c.to_digit(10).unwrap();
                } else if c == ' ' {
                    state = State::Time;
                } else {
                    bail!("Invalid timestamp: expected digit or whitespace after day");
                }
            }
            State::Time => {
                if c.is_ascii_digit() {
                    hour = hour * 10 + c.to_digit(10).unwrap();
                    state = State::Hour;
                } else if !c.is_whitespace() {
                    bail!("Invalid timestamp: expected digit or whitespace after date");
                }
            }
            State::Hour => {
                if c.is_ascii_digit() {
                    hour = hour * 10 + c.to_digit(10).unwrap();
                } else if c == ':' {
                    state = State::Minute;
                } else {
                    bail!("Invalid timestamp: expected ':' after hour");
                }
            }
            State::Minute => {
                if c.is_ascii_digit() {
                    minute = minute * 10 + c.to_digit(10).unwrap();
                } else if c == ':' {
                    state = State::Second;
                } else {
                    bail!("Invalid timestamp: expected ':' after minute");
                }
            }
            State::Second => {
                if c.is_ascii_digit() {
                    second = second * 10 + c.to_digit(10).unwrap();
                } else if c == '.' {
                    state = State::Millisecond;
                } else if c.is_whitespace() {
                    break;
                } else {
                    bail!("Invalid timestamp: expected '.' or whitespace after second");
                }
            }
            State::Millisecond => {
                if c.is_ascii_digit() {
                    millisecond = millisecond * 10 + c.to_digit(10).unwrap();
                } else if c.is_whitespace() {
                    break;
                } else {
                    bail!("Invalid timestamp: expected digit or whitespace after millisecond")
                }
            }
        };
    }

    match state {
        State::Second => {}
        State::Millisecond => {}
        _ => {
            bail!("Invalid timestamp: ended too early")
        }
    }

    let naive_date =
        NaiveDate::from_ymd_opt(year, month, day).context("Invalid timestamp: invalid date")?;
    let naive_time = NaiveTime::from_hms_milli_opt(hour, minute, second, millisecond)
        .context("Invalid timestamp: invalid time")?;
    let naive_dt = NaiveDateTime::new(naive_date, naive_time);
    Ok(Utc.from_local_datetime(&naive_dt).single().unwrap())
}

pub fn parse_timestamp_slow(timestamp_str: &str) -> Result<DateTime<Utc>> {
    let naive_dt = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f %Z"))
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S %Z"))
        .context("Invalid timestamp")?;

    Ok(Utc.from_local_datetime(&naive_dt).single().unwrap())
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use pretty_assertions::assert_matches;
    use test_case::test_case;

    use super::*;

    #[test_case("2021-08-03 17:40:27.313 UTC", "2021-08-03T17:40:27.313+00:00" ; "with_milliseconds_and_utc")]
    #[test_case("2021-08-03 17:40:27.313", "2021-08-03T17:40:27.313+00:00" ; "with_milliseconds_no_utc")]
    #[test_case("2021-08-03 17:40:27", "2021-08-03T17:40:27+00:00" ; "no_milliseconds_no_utc")]
    #[test_case("2021-08-03 17:40:27 UTC", "2021-08-03T17:40:27+00:00" ; "no_milliseconds_and_utc")]
    #[test_case("2021-08-03 02:00:00 UTC", "2021-08-03T02:00:00+00:00" ; "low_no_milliseconds_and_utc")]
    #[test_case("2021-08-03 00:01:27.010 UTC", "2021-08-03T00:01:27.010+00:00" ; "low_with_milliseconds_and_utc")]
    // Edge cases
    #[test_case("1970-01-01 00:00:00.000 UTC", "1970-01-01T00:00:00+00:00" ; "earliest_possible_date")]
    #[test_case("9999-12-31 23:59:59.999 UTC", "9999-12-31T23:59:59.999+00:00" ; "latest_possible_date")]
    #[test_case("2020-02-29 12:34:56 UTC", "2020-02-29T12:34:56+00:00" ; "leap_year_date")]
    #[test_case("2021-08-03 17:40:27.999 UTC", "2021-08-03T17:40:27.999+00:00" ; "max_milliseconds")]
    #[test_case("2021-08-03 17:40:27.001 UTC", "2021-08-03T17:40:27.001+00:00" ; "min_milliseconds")]
    fn test_parse_timestamp(input_ts: &str, expected_ts: &str) -> Result<()> {
        let parsed_timestamp = parse_timestamp(input_ts)?;
        assert_eq!(parsed_timestamp.to_rfc3339(), expected_ts);

        let parsed_timestamp_slow = parse_timestamp_slow(input_ts)?;
        assert_eq!(parsed_timestamp_slow.to_rfc3339(), expected_ts);

        Ok(())
    }

    // Failing test cases
    #[test_case(""; "empty_string")]
    #[test_case(" "; "space_string")]
    #[test_case("2021-08-03"; "missing_time")]
    #[test_case("17:40:27 UTC"; "missing_date")]
    #[test_case("2021-08-03T17:40:27.313 UTC"; "invalid_separator")]
    #[test_case("2021-08-32 17:40:27 UTC"; "invalid_day")]
    #[test_case("2021-13-03 17:40:27 UTC"; "invalid_month")]
    #[test_case("2021-08-03 24:40:27 UTC"; "invalid_hour")]
    #[test_case("2021-08-03 17:60:27 UTC"; "invalid_minute")]
    #[test_case("2021-08-03 17:40:60 UTC" => ignore; "invalid_second")]
    #[test_case("2021-08-03 17:40:68 UTC"; "invalid_second_2")]
    #[test_case("2022-02-29 12:34:56 UTC"; "non_leap_year_date")]
    fn test_parse_timestamp_fail(input_ts: &str) {
        let result = parse_timestamp(input_ts);
        assert_matches!(result, Err(_));

        let result_slow = parse_timestamp_slow(input_ts);
        assert_matches!(result_slow, Err(_));
    }
}
