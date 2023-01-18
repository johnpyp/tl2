use std::hash::{Hash, Hasher};
use chrono::{DateTime, Utc};
use fasthash::xx::Hasher32;
use serde::{Serialize, Deserialize};
use voca_rs::case;

use super::unified::{OrlLog1_0, CommonKey};

#[derive(Debug, PartialEq, Eq)]
pub struct RawOrlLog {
    pub ts: DateTime<Utc>,
    pub username: String,
    pub text: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OrlLog {
    pub ts: DateTime<Utc>,
    pub username: String,
    pub text: String,
    pub channel: String,

    pub is_normal: bool
}

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

impl OrlLog {
    pub fn from_raw(raw: RawOrlLog, channel: &str) -> OrlLog {
        OrlLog {
            channel: channel.to_string(),
            ts: raw.ts,
            username: raw.username,
            text: raw.text,
            is_normal: false,
        }
    }

    pub fn normalize(&self) -> OrlLog {
        if self.is_normal { return self.clone() }
        return OrlLog {
            ts: self.ts,
            username: self.username.to_lowercase(),
            text: self.text.trim().replace('\n', " "),
            channel: case::capitalize(self.channel.trim(), true),
            is_normal: true
        }
    }

    pub fn get_id(&self) -> String {

        let normal = self.normalize();

        let ts_str = normal.ts.timestamp_millis().to_string();
        let hash_channel = squash_hash(&normal.channel, 8);
        let hash_username = squash_hash(&normal.username, 8);
        let hash_text = squash_hash(&normal.text, 8);
            
        format!("{}-{}-{}-{}", ts_str, hash_channel, hash_username, hash_text)

    }

    pub fn get_unix_millis(&self) -> i64 {
        self.ts.timestamp_millis()
    }
}

impl From<OrlLog> for OrlLog1_0 {
    fn from(item: OrlLog) -> Self {
        let item = item.normalize();

        return OrlLog1_0 {
            key: CommonKey {
                id: item.get_id(),
                timestamp: item.get_unix_millis(),
            },
            username: item.username,
            channel_name: item.channel,
            text: item.text,
        }
        
    }
}
