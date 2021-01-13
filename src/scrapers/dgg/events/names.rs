use serde::{Deserialize, Serialize};

use super::{shared::user::User, DggAsSimpleMessageGroup};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Names {
    #[serde(rename(deserialize = "connectioncount"))]
    pub connection_count: u32,
    pub users: Vec<User>,
}

impl DggAsSimpleMessageGroup for Names {
    fn as_group(&self, _channel: String) -> crate::events::SimpleMessageGroup {
        None.into()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_parse_to_names() {
        let names_str = json!({
            "connectioncount": 30,
            "users": [
                { "nick": "pogchamp", "features": ["flair3", "moderator"]},
                { "nick": "pogchamp2", "features": ["flair1", "admin"]}
            ]
        });

        let names: Names = serde_json::from_value(names_str).unwrap();
        dbg!(&names);

        assert_eq!(names.connection_count, 30);
        assert_eq!(names.users.len(), 2);
        assert_eq!(names.users.get(0).unwrap().is_subscriber, true);
        assert_eq!(names.users.get(0).unwrap().username, "pogchamp");
        assert_eq!(names.users.get(1).unwrap().is_subscriber, true);
        assert_eq!(names.users.get(1).unwrap().is_admin, true);
        assert_eq!(names.users.get(1).unwrap().is_moderator, false);
    }
}
