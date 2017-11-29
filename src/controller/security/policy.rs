use serde_json;
use serde_json::Value;
use nom_sql::parser as sql_parser;
use nom_sql::SqlQuery;

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
enum Action {
    Allow,
    Deny
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub name: String,
    pub table: String,
    pub predicate: SqlQuery,
    action: Action
}

impl Policy {
    pub fn parse(policy_text: &str) -> Vec<Policy> {
        let config: Vec<Value> = match serde_json::from_str(policy_text) {
                Ok(v) => v,
                Err(e) => panic!(e.to_string()),
            };

        config
            .iter()
            .map(|p| {
                let name = match p.get("name") {
                    Some(n) => n.as_str().unwrap(),
                    None => "",
                };
                let table = p["table"].as_str().unwrap();
                let pred = p["predicate"].as_str().unwrap();

                let sq =
                    sql_parser::parse_query(&format!("select * from {} {};", table, pred)).unwrap();

                Policy {
                    name: name.to_string(),
                    table: table.to_string(),
                    predicate: sq,
                    action: Action::Allow,
                }
            })
            .collect()
    }
}

mod tests {
    #[test]
    fn it_parses() {
        use super::*;
        let p0 = "select * from post WHERE post.type = ?";
        let p1 = "select * from post WHERE post.author = ?";

        let policy_text = r#"[{ "table": "post", "predicate": "WHERE post.type = ?" },
                              { "table": "post", "predicate": "WHERE post.author = ?" }]"#;

        let policies = Policy::parse(policy_text);

        assert_eq!(policies.len(), 2);
        assert_eq!(policies[0].predicate, sql_parser::parse_query(p0).unwrap());
        assert_eq!(policies[1].predicate, sql_parser::parse_query(p1).unwrap());
    }
}
