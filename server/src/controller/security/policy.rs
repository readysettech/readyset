use nom_sql::parser as sql_parser;
use nom_sql::SqlQuery;
use serde_json;
use serde_json::Value;

enum Action {
    Allow,
    Deny,
    #[allow(dead_code)]
    Rewrite,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum Policy {
    Rewrite(RewritePolicy),
    Allow(RowPolicy),
    Deny(RowPolicy),
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct RowPolicy {
    pub name: String,
    pub table: String,
    pub predicate: SqlQuery,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct RewritePolicy {
    pub name: String,
    pub table: String,
    pub value: String,
    pub column: String,
    pub key: String,
    pub rewrite_view: SqlQuery,
}

impl Policy {
    pub fn name(&self) -> String {
        match *self {
            Policy::Rewrite(ref p) => p.name.clone(),
            Policy::Allow(ref p) => p.name.clone(),
            Policy::Deny(ref p) => p.name.clone(),
        }
    }

    pub fn table(&self) -> String {
        match *self {
            Policy::Rewrite(ref p) => p.table.clone(),
            Policy::Allow(ref p) => p.table.clone(),
            Policy::Deny(ref p) => p.table.clone(),
        }
    }

    pub fn is_row_policy(&self) -> bool {
        match *self {
            Policy::Rewrite(_) => false,
            Policy::Allow(_) => true,
            Policy::Deny(_) => true,
        }
    }

    pub fn predicate(&self) -> SqlQuery {
        match *self {
            Policy::Rewrite(ref p) => p.rewrite_view.clone(),
            Policy::Allow(ref p) => p.predicate.clone(),
            Policy::Deny(ref p) => p.predicate.clone(),
        }
    }

    pub fn value(&self) -> String {
        match *self {
            Policy::Rewrite(ref p) => p.value.clone(),
            Policy::Allow(_) => panic!("Row policy doesn't have value field"),
            Policy::Deny(_) => panic!("Row policy doesn't have value field"),
        }
    }

    pub fn column(&self) -> String {
        match *self {
            Policy::Rewrite(ref p) => p.column.clone(),
            Policy::Allow(_) => panic!("Row policy doesn't have column field"),
            Policy::Deny(_) => panic!("Row policy doesn't have column field"),
        }
    }

    pub fn key(&self) -> String {
        match *self {
            Policy::Rewrite(ref p) => p.key.clone(),
            Policy::Allow(_) => panic!("Row policy doesn't have key field"),
            Policy::Deny(_) => panic!("Row policy doesn't have key field"),
        }
    }

    pub fn parse(policy_text: &str) -> Vec<Policy> {
        let config: Vec<Value> = match serde_json::from_str(policy_text) {
            Ok(v) => v,
            Err(e) => panic!(e.to_string()),
        };

        config
            .iter()
            .map(|p| match p.get("action") {
                Some(action) => match action.as_str() {
                    Some("rewrite") => Policy::parse_rewrite_policy(p),
                    Some("allow") => Policy::parse_row_policy(p, Action::Allow),
                    Some("deny") => Policy::parse_row_policy(p, Action::Deny),
                    _ => panic!("Unsupported policy action {}", action),
                },
                None => Policy::parse_row_policy(p, Action::Allow),
            })
            .collect()
    }

    fn parse_row_policy(p: &Value, action: Action) -> Policy {
        let name = match p.get("name") {
            Some(n) => n.as_str().unwrap(),
            None => "",
        };
        let table = p["table"].as_str().unwrap();
        let pred = p["predicate"].as_str().unwrap();

        let sq = sql_parser::parse_query(&format!("select * from {} {};", table, pred)).unwrap();

        let rp = RowPolicy {
            name: name.to_string(),
            table: table.to_string(),
            predicate: sq,
        };

        match action {
            Action::Allow => Policy::Allow(rp),
            Action::Deny => Policy::Deny(rp),
            Action::Rewrite => unreachable!(),
        }
    }

    fn parse_rewrite_policy(p: &Value) -> Policy {
        let name = match p.get("name") {
            Some(n) => n.as_str().unwrap(),
            None => "",
        };

        let table = p["table"].as_str().unwrap();
        let rewrite = p["rewrite"].as_str().unwrap();
        let value = p["value"].as_str().unwrap();
        let column = p["column"].as_str().unwrap();
        let key = p["key"].as_str().unwrap();

        let sq = sql_parser::parse_query(rewrite).unwrap();

        Policy::Rewrite(RewritePolicy {
            name: name.to_string(),
            table: table.to_string(),
            value: value.to_string(),
            column: column.to_string(),
            key: key.to_string(),
            rewrite_view: sq,
        })
    }
}

mod tests {
    #[test]
    fn it_parses_row_policies() {
        use super::*;
        let p0 = "select * from post WHERE post.type = ?";
        let p1 = "select * from post WHERE post.author = ?";

        let policy_text = r#"[{ "table": "post", "predicate": "WHERE post.type = ?" },
                              { "table": "post", "predicate": "WHERE post.author = ?" }]"#;

        let policies = Policy::parse(policy_text);

        assert_eq!(policies.len(), 2);
        assert_eq!(
            policies[0].predicate(),
            sql_parser::parse_query(p0).unwrap()
        );
        assert_eq!(
            policies[1].predicate(),
            sql_parser::parse_query(p1).unwrap()
        );
    }
}
