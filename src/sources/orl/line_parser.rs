use std::marker::PhantomData;

use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;

use super::parse_timestamp::parse_timestamp;
use crate::formats::orl::Clean;
use crate::formats::orl::OrlLog;

#[derive(Debug)]
pub struct OrlLineMessage {
    pub ts: DateTime<Utc>,
    pub username: String,
    pub text: String,
}

pub fn parse_message_line(input_line: &str) -> Result<OrlLineMessage> {
    let mut parts = input_line.splitn(2, ']');
    let timestamp_str = parts
        .next()
        .context("Invalid input line: timestamp")?
        .trim_start_matches('[');

    let mut username_and_message = parts.next().context("Invalid input line")?.splitn(2, ':');

    let username = username_and_message
        .next()
        .context("Invalid input line: username")?
        .trim()
        .to_lowercase();

    let message = username_and_message
        .next()
        .context("Invalid input line: message")?
        .trim()
        .replace('\n', " ");

    let timestamp = parse_timestamp(timestamp_str.trim())?;

    Ok(OrlLineMessage {
        ts: timestamp,
        username,
        text: message,
    })
}

pub fn parse_orl_log(channel: String, input_line: &str) -> Result<OrlLog<Clean>> {
    let message = parse_message_line(input_line)?;
    Ok(OrlLog {
        username: message.username,
        text: message.text,
        ts: message.ts,
        channel,
        _s: PhantomData::<Clean>,
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use test_case::test_case;

    use super::*;

    #[test_case("[2021-08-03 17:40:27.313 UTC] jOhNpYp: Example message", "2021-08-03T17:40:27.313+00:00", "johnpyp", "Example message" ; "with_milliseconds_and_utc")]
    #[test_case("[2021-08-03 17:40:27.313] jOhNpYp: Example message", "2021-08-03T17:40:27.313+00:00", "johnpyp", "Example message" ; "with_milliseconds_no_utc")]
    #[test_case("[2021-08-03 17:40:27] jOhNpYp: Example message", "2021-08-03T17:40:27+00:00", "johnpyp", "Example message" ; "no_milliseconds_no_utc")]
    #[test_case("[2021-08-03 17:40:27 UTC] jOhNpYp: Example message", "2021-08-03T17:40:27+00:00", "johnpyp", "Example message" ; "no_milliseconds_and_utc")]
    #[test_case("[2021-08-03 17:40:27 UTC]   test cat: Example message", "2021-08-03T17:40:27+00:00", "test cat", "Example message" ; "spaced_out_username")]
    #[test_case("[2021-08-03 17:40:27 UTC]   tEst Cat  :   Example message   ", "2021-08-03T17:40:27+00:00", "test cat", "Example message" ; "spaced_out_username_and_message")]
    #[test_case("[  2021-08-03 17:40:27 UTC  ]   TeSt Cat  :   Example message   ", "2021-08-03T17:40:27+00:00", "test cat", "Example message" ; "spaced_out_username_and_message_and_timestamp")]
    #[test_case("[2021-08-03 17:40:27 UTC] test cat: Example message\nfollowing message", "2021-08-03T17:40:27+00:00", "test cat", "Example message following message" ; "new_line_in_message")]
    #[test_case("[0001-01-01 00:00:00 UTC] user: message", "0001-01-01T00:00:00+00:00", "user", "message" ; "min_date")]
    #[test_case("[9999-12-31 23:59:59 UTC] user: message", "9999-12-31T23:59:59+00:00", "user", "message" ; "max_date")]
    #[test_case("[2021-08-03 17:40:27 UTC] user: a", "2021-08-03T17:40:27+00:00", "user", "a" ; "single_char_message")]
    #[test_case("[2021-08-03 17:40:27 UTC] user:", "2021-08-03T17:40:27+00:00", "user", "" ; "empty_message")]
    #[test_case("[2021-08-03 17:40:27 UTC]user: message", "2021-08-03T17:40:27+00:00", "user", "message"; "username_no_space")]
    fn test_parse_message_line(
        input_line: &str,
        expected_ts: &str,
        expected_user: &str,
        expected_msg: &str,
    ) -> Result<()> {
        let parsed_message = parse_message_line(input_line)?;
        assert_eq!(parsed_message.ts.to_rfc3339(), expected_ts);
        assert_eq!(parsed_message.username, expected_user);
        assert_eq!(parsed_message.text, expected_msg);

        Ok(())
    }

    // Expected failure test cases
    #[test_case("[2021-08-03 17:40:27 UTC] user"; "missing_message")]
    #[test_case("2021-08-03 17:40:27 UTC user: message"; "missing_timestamp_closing_brace")]
    #[test_case("[2021-08-03 17:40:27] user message"; "missing colon separator")]
    fn test_parse_message_line_failure(input_line: &str) {
        let parsed_message = parse_message_line(input_line);
        assert!(
            parsed_message.is_err(),
            "parse_message = {parsed_message:?}"
        );
    }
}
