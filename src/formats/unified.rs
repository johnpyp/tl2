use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonKey {
    pub id: String,
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrlLog1_0 {
    #[serde(flatten)]
    pub key: CommonKey,

    pub username: String,
    pub channel_name: String,
    pub text: String
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChannelType {
    #[serde(rename = "dgg")]
    Dgg,
    #[serde(rename = "twitch")]
    Twitch,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SimpleLog1_0 {
    #[serde(flatten)]
    pub key: CommonKey,

    pub channel_type: ChannelType,
    pub message_id: String,
    pub user_id: Option<String>,
    pub username: String,
    pub display_name: Option<String>,
    pub channel_name: String,
    pub text: String,
    pub source: Option<String>
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum UnifiedMessageLog {
    #[serde(rename = "orl-log/1.0")]
    OrlLog1_0(OrlLog1_0),

    #[serde(rename = "simple-log/1.0")]
    SimpleLog1_0(SimpleLog1_0)
}


// #[cfg(test)]
// mod tests {

//     use super::*;

//     #[test]
//     fn test_format_orl_log_1_0() {
//         let expected_json = r#"
//             [
//               {
//                  "kind": "orl-log/1.0",
//                  "id": "asdf",
//                  "timestamp": 1234,
//                  "type": "Foo",
//                  "fooSpecificA": 3,
//                  "fooSpecificB": 4
//               },
//               {
//                  "commonA": 5,
//                  "commonB": 6,
//                  "type": "Bar",
//                  "barSpecificA": 7,
//                  "barSpecificB": 8
//               }
//             ]"#;
//         let datetime = parse_orl_date("2021-08-04 00:44:12.616 UTC");
//         assert_eq!(
//             datetime,
//             Ok(Utc.ymd(2021, 8, 4).and_hms_milli(0, 44, 12, 616))
//         );
//     }
