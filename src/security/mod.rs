use serde_json;
use serde_json::Value;
use nom_sql::parser as sql_parser;
use nom_sql::SqlQuery;

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub table: String,
    pub predicate: SqlQuery,
}

impl Policy {
    pub fn parse(policy_text: &str) -> Vec<Policy> {
        let policies: Vec<Value> = match serde_json::from_str(policy_text) {
            Ok(v) => v,
            Err(e) => panic!(e.to_string()),
        };

        policies
            .iter()
            .map(|p| {
                let table = p["table"].as_str().unwrap();
                let pred = p["predicate"].as_str().unwrap();

                let sq =
                    sql_parser::parse_query(&format!("select * from {} {};", table, pred)).unwrap();

                Policy {
                    table: table.to_string(),
                    predicate: sq,
                }
            })
            .collect()
    }
}

mod tests {
    #[test]
    fn it_parses() {
        use super::*;

        let policy_text = r#"[{ "table": "post", "predicate": "WHERE post.type = ?" },
							  { "table": "post", "predicate": "WHERE post.author = ?" }]"#;

        let policies = Policy::parse(policy_text);

        assert_eq!(policies.len(), 2);
    }
}
