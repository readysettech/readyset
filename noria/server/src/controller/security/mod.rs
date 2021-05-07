use serde_json;
use serde_json::Value;
use std::collections::HashMap;

pub mod group;
pub mod policy;

use crate::controller::security::group::Group;
use crate::controller::security::policy::Policy;

#[derive(Clone, Debug)]
pub struct SecurityConfig {
    pub groups: HashMap<String, Group>,
    policies: Vec<Policy>,
}

impl SecurityConfig {
    pub fn parse(policy_text: &str) -> SecurityConfig {
        let config: serde_json::Map<String, Value> = match serde_json::from_str(policy_text) {
            Ok(v) => v,
            Err(e) => panic!("{}", e),
        };

        let groups = match config.get("groups") {
            Some(groups) => Group::parse(&format!("{}", groups)),
            None => Vec::new(),
        };

        let groups_map = groups.iter().map(|g| (g.name(), g.clone())).collect();

        let policies = Policy::parse(&format!("{}", config["policies"]));

        SecurityConfig {
            groups: groups_map,
            policies,
        }
    }

    pub fn policies(&self) -> &[Policy] {
        self.policies.as_slice()
    }

    pub fn get_group_policies(&self, group_name: String) -> &[Policy] {
        self.groups[&group_name].policies()
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
