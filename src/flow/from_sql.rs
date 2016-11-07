use nom_sql::parser as sql_parser;
use nom_sql::parser::{ConditionBase, ConditionExpression, SqlQuery};
use nom_sql::{InsertStatement, SelectStatement};
use FlowGraph;
use ops;
use ops::Node;
use ops::base::Base;
use ops::identity::Identity;
use query::{DataType, Query};
use shortcut;

use petgraph;
use std::iter::FromIterator;
use std::str;
use std::sync::Arc;
use std::vec::Vec;

/// Converts a condition tree stored in the `ConditionExpr` returned by the SQL parser into a
/// vector of conditions that `shortcut` understands.
fn to_conditions(ce: &ConditionExpression) -> Vec<shortcut::Condition<DataType>> {
    match *ce {
        ConditionExpression::Base(_) => vec![],
        ConditionExpression::ComparisonOp(ref ct) => {
            // TODO(malte): fix this once nom-sql has better operator representations
            assert_eq!(ct.operator, "=");
            // TODO(malte): we only support one level of condition nesting at this point :(
            let l = match ct.left.as_ref().unwrap().as_ref() {
                &ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
                _ => unimplemented!(),
            };
            let r = match ct.right.as_ref().unwrap().as_ref() {
                &ConditionExpression::Base(ConditionBase::Placeholder) => String::from("?"),
                &ConditionExpression::Base(ConditionBase::Literal(ref l)) => l.clone(),
                _ => unimplemented!(),
            };
            vec![shortcut::Condition {
                     // column: field_to_columnid(l),
                     column: 1,
                     cmp: shortcut::Comparison::Equal(shortcut::Value::Const(DataType::Text(Arc::new(r)))),
                 }]
        }
        _ => unimplemented!(),
    }
}

trait FromSql {
    fn incorporate_query(&mut self, &str) -> Result<Vec<petgraph::graph::NodeIndex>, String>;
    fn lookup_node(&self, &str) -> petgraph::graph::NodeIndex;
    fn make_base_node(&self, &InsertStatement) -> Node;
    fn make_filter_node(&self, &SelectStatement) -> Node;
    fn nodes_for_query(&self, SqlQuery) -> Vec<Node>;
}

impl FromSql for FlowGraph<Query, ops::Update, Vec<DataType>> {
    fn incorporate_query(&mut self, q: &str) -> Result<Vec<petgraph::graph::NodeIndex>, String> {
        // try parsing the incoming SQL
        let parsed_query = sql_parser::parse_query(q);

        // if ok, manufacture a node for the query structure we got
        let nodes = match parsed_query {
            Ok(q) => self.nodes_for_query(q),
            Err(e) => return Err(String::from(e)),
        };

        // finally, let's hook in the node
        Ok(nodes.into_iter()
            .map(|n| self.incorporate(n))
            .collect())
    }

    fn lookup_node(&self, vn: &str) -> petgraph::graph::NodeIndex {
        self.named[vn]
    }

    fn make_base_node(&self, st: &InsertStatement) -> Node {
        ops::new(st.table.clone(),
                 Vec::from_iter(st.fields.iter().map(String::as_str)).as_slice(),
                 true,
                 Base {})
    }

    fn make_filter_node(&self, st: &SelectStatement) -> Node {
        // XXX(malte): we need a custom name/identifier scheme for filter nodes, since the table
        // name is already used for the base node. Maybe this is where identifiers based on query
        // prefixes come in.
        let mut n = ops::new(st.table.clone(),
                             Vec::from_iter(st.fields.iter().map(String::as_str)).as_slice(),
                             true,
                             Identity::new(self.lookup_node(&st.table)));
        match st.where_clause {
            None => (),
            // convert ConditionTree to shortcut-style condition vector
            Some(ref cond) => {
                n = n.having(to_conditions(cond));
            }
        }
        n
    }

    fn nodes_for_query(&self, q: SqlQuery) -> Vec<Node> {
        println!("{:#?}", q);

        match q {
            SqlQuery::Insert(iq) => vec![self.make_base_node(&iq)],
            SqlQuery::Select(sq) => vec![self.make_filter_node(&sq)],
        }
    }
}

#[cfg(test)]
mod tests {
    use FlowGraph;
    use flow;
    use ops;
    use ops::new;
    use ops::base::Base;
    use query::{DataType, Query};
    use super::FromSql;

    type Update = ops::Update;
    type Data = Vec<DataType>;

    // Helper to grab a reference to a named view.
    // TODO(malte): maybe this should be available in FlowGraph?
    fn get_view<'a>(g: &'a FlowGraph<Query, ops::Update, Vec<DataType>>,
                    vn: &str)
                    -> &'a flow::View<Query, Update = ops::Update, Data = Vec<DataType>> {
        &**(g.graph[g.named[vn]].as_ref().unwrap())
    }

    #[test]
    fn it_parses() {
        // set up graph
        let mut g = FlowGraph::new();

        // Must have a base node for type inference to work, so make one manually
        let _ = g.incorporate(new("users", &["id", "username"], true, Base {}));
        // Should have two nodes: source and "users" base table
        assert_eq!(g.graph.node_count(), 2);
        assert_eq!(get_view(&g, "users").name(), "users");

        assert!(g.incorporate_query("SELECT * from users;").is_ok());
        // Should now have source, "users" and the new selection
        assert_eq!(g.graph.node_count(), 3);

        // Invalid query should fail parsing and add no nodes
        assert!(g.incorporate_query("foo bar from whatever;").is_err());
        // Should still only have source, "users" and the new selection
        assert_eq!(g.graph.node_count(), 3);
    }

    #[test]
    fn it_incorporates_simple_selection() {
        // set up graph
        let mut g = FlowGraph::new();

        // Establish a base write type
        assert!(g.incorporate_query("INSERT INTO users VALUES (?, ?);").is_ok());
        // Should have source and "users" base table node
        assert_eq!(g.graph.node_count(), 2);
        assert_eq!(get_view(&g, "users").name(), "users");

        // Try a simple query
        assert!(g.incorporate_query("SELECT * from users;").is_ok());
    }
}
