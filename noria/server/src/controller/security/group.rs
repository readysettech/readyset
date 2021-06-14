use crate::controller::security::policy::Policy;
use nom_sql::parser as sql_parser;
use nom_sql::SqlQuery;
use serde_json::Value;

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct Group {
    name: String,
    membership: SqlQuery,
    policies: Vec<Policy>,
}

impl Group {
    pub fn parse(grou_txt: &str) -> Vec<Group> {
        let groups: Vec<Value> = match serde_json::from_str(grou_txt) {
            Ok(v) => v,
            Err(e) => panic!("{}", e),
        };

        groups
            .iter()
            .map(|g| {
                let name = g["name"].as_str().unwrap();
                let membership = g["membership"].as_str().unwrap();
                let policies = format!("{}", g["policies"]);

                Group {
                    name: name.to_string(),
                    membership: sql_parser::parse_query(membership).unwrap(),
                    policies: Policy::parse(&policies),
                }
            })
            .collect()
    }

    pub fn membership(&self) -> SqlQuery {
        self.membership.clone()
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn policies(&self) -> &[Policy] {
        self.policies.as_slice()
    }
}

mod tests {
    #[test]
    fn it_parses() {
        use super::*;

        let membership = "select uid, cid FROM tas";
        let group_text = r#"
            [
                {
                    "name": "ta",
                    "membership": "select uid, cid FROM tas;",
                    "policies": [
                        { "table": "post", "predicate": "WHERE post.type = ?" }
                    ]
                }
            ]"#;

        let groups = Group::parse(group_text);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, "ta");
        assert_eq!(
            groups[0].membership,
            sql_parser::parse_query(membership).unwrap()
        );
    }
}
