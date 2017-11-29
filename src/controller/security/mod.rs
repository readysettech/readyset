use serde_json;
use serde_json::Value;
use nom_sql::parser as sql_parser;
use nom_sql::SqlQuery;

pub mod policy;
pub mod group;

use controller::security::policy::Policy;
use controller::security::group::Group;

#[derive(Clone, Debug)]
pub struct SecurityConfig {
    groups: Vec<Group>,
    policies: Vec<Policy>,
}

impl SecurityConfig {
    pub fn parse(policy_text: &str) -> SecurityConfig {
        let config: serde_json::Map<String, Value> = match serde_json::from_str(policy_text) {
            Ok(v) => v,
            Err(e) => panic!(e.to_string()),
        };

        let groups = match config.get("groups") {
            Some(groups) => Group::parse(&format!("{}", groups)),
            None => Vec::new()
        };

        let policies = Policy::parse(&format!("{}", config["policies"]));

        SecurityConfig {
            groups: groups,
            policies: policies,
        }
    }

    pub fn policies(&self) -> &[Policy] {
        self.policies.as_slice()
    }
}


mod tests {
    #[test]
    fn it_parses() {
        use super::*;

        let config_txt = r#"
        {
            "groups": [
                {
                    "name": "ta",
                    "membership": "select uid, cid FROM tas;",
                    "policies": [
                        { "table": "post", "predicate": "WHERE post.type = ?" }
                    ]
                }
            ],
            "policies": [
                            { "table": "post", "predicate": "WHERE post.type = ?" },
                            { "table": "post", "predicate": "WHERE post.author = ?" }
                        ]
        }"#;

        let config = SecurityConfig::parse(config_txt);

        assert_eq!(config.policies.len(), 2);
        assert_eq!(config.groups.len(), 1);
    }
}

