use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;

use chrono::DateTime;
use chrono::Utc;
use fasthash::xx::Hasher32;
use serde::Deserialize;
use serde::Serialize;
use voca_rs::case;

use super::unified::CommonKey;
use super::unified::OrlLog1_0;

#[derive(Debug, PartialEq, Eq)]
pub struct RawOrlLog {
    pub ts: DateTime<Utc>,
    pub username: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Raw;
#[derive(Debug, Clone, PartialEq)]
pub struct Clean;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OrlLog<S = Raw> {
    pub ts: DateTime<Utc>,
    pub username: String,
    pub text: String,
    pub channel: String,

    #[serde(skip)]
    pub _s: PhantomData<S>,
}

pub type CleanOrlLog = OrlLog<Clean>;

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s: Hasher32 = Default::default();
    t.hash(&mut s);
    s.finish()
}

fn squash_hash(s: &str, max_characters: u8) -> String {
    // Our hash is 32 bits, or 8 hex characters, so anything longer than that is purely padding.
    assert!(max_characters <= 8);

    let hash_result = hash(&s); // Actually a u32 because of XXHash32

    // 8 max characters means that we can only have up to 8 * 4 (32) bits of precision.
    // 32 - 8 * 4 = 0, so no change is needed.
    // 5 max characters means that we can only have up to 5 * 4 (20) bits of precision.
    // 32 - 5 * 4 = 12, so we need to remove 12 bits of precision to make sure we can fit into 5
    //    characters.
    let less_precision = 32 - (max_characters * 4);

    assert!(less_precision < 32); // We can't remove ALL the precision...

    // We bitshift right the resulting 32bit number
    let reduced_hash = hash_result >> less_precision;

    let precision_bits = 32 - less_precision as u32;

    // This is the maximum length of a hex string for the given precision bits. Essentially
    // ceil(precision / 4)
    // Div ceil is unstable, so use the underlying implementation.
    let max_len_hex = (precision_bits + 4 - 1) / 4;

    let hex_str: String = format!("{:x}", reduced_hash);

    format!("{:0>width$}", hex_str, width = max_len_hex as usize)
}

impl OrlLog<Raw> {
    pub fn normalize(&self) -> OrlLog<Clean> {
        return OrlLog {
            ts: self.ts,
            username: self.username.to_lowercase(),
            text: self.text.trim().replace('\n', " "),
            channel: case::capitalize(self.channel.trim(), true),

            _s: PhantomData::<Clean>,
        };
    }
}

impl OrlLog<Clean> {
    pub fn get_id(&self) -> String {
        let ts_str = self.ts.timestamp_millis().to_string();
        let hash_channel = squash_hash(&self.channel, 8);
        let hash_username = squash_hash(&self.username, 8);
        let hash_text = squash_hash(&self.text, 8);

        format!(
            "{}-{}-{}-{}",
            ts_str, hash_channel, hash_username, hash_text
        )
    }
}

impl<S> OrlLog<S> {
    pub fn from_raw(raw: RawOrlLog, channel: &str) -> OrlLog<Raw> {
        OrlLog {
            channel: channel.to_string(),
            ts: raw.ts,
            username: raw.username,
            text: raw.text,

            _s: PhantomData::<Raw>,
        }
    }

    pub fn get_unix_millis(&self) -> i64 {
        self.ts.timestamp_millis()
    }
}

impl From<OrlLog<Clean>> for OrlLog1_0 {
    fn from(item: OrlLog<Clean>) -> Self {
        return OrlLog1_0 {
            key: CommonKey {
                id: item.get_id(),
                timestamp: item.get_unix_millis(),
            },
            username: item.username,
            channel_name: item.channel,
            text: item.text,
        };
    }
}
