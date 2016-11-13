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

fn lookup_node(vn: &str,
               g: &mut FlowGraph<Query, ops::Update, Vec<DataType>>)
               -> petgraph::graph::NodeIndex {
    g.named[vn]
}

fn make_base_node(st: &InsertStatement) -> Node {
    ops::new(st.table.clone(),
             Vec::from_iter(st.fields.iter().map(String::as_str)).as_slice(),
             true,
             Base {})
}

fn make_filter_node(st: &SelectStatement,
                    g: &mut FlowGraph<Query, ops::Update, Vec<DataType>>)
                    -> Node {
    // XXX(malte): we need a custom name/identifier scheme for filter nodes, since the table
    // name is already used for the base node. Maybe this is where identifiers based on query
    // prefixes come in.
    let mut n = ops::new(st.table.clone(),
                         Vec::from_iter(st.fields.iter().map(String::as_str)).as_slice(),
                         true,
                         Identity::new(lookup_node(&st.table, g)));
    match st.where_clause {
        None => (),
        // convert ConditionTree to shortcut-style condition vector
        Some(ref cond) => {
            n = n.having(to_conditions(cond));
        }
    }
    n
}

fn nodes_for_query(q: SqlQuery, g: &mut FlowGraph<Query, ops::Update, Vec<DataType>>) -> Vec<Node> {
    println!("{:#?}", q);

    match q {
        SqlQuery::Insert(iq) => vec![make_base_node(&iq)],
        SqlQuery::Select(sq) => vec![make_filter_node(&sq, g)],
    }
}

trait ToFlowParts {
    fn to_flow_parts<'a>(&self,
                         &mut FlowGraph<Query, ops::Update, Vec<DataType>>)
                         -> Result<Vec<petgraph::graph::NodeIndex>, String>;
}

impl<'a> ToFlowParts for &'a str {
    fn to_flow_parts(&self,
                     g: &mut FlowGraph<Query, ops::Update, Vec<DataType>>)
                     -> Result<Vec<petgraph::graph::NodeIndex>, String> {
        // try parsing the incoming SQL
        let parsed_query = sql_parser::parse_query(self);

        // if ok, manufacture a node for the query structure we got
        let nodes = match parsed_query {
            Ok(q) => nodes_for_query(q, g),
            Err(e) => return Err(String::from(e)),
        };

        // finally, let's hook in the node
        Ok(nodes.into_iter()
            .map(|n| g.incorporate(n))
            .collect())
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
    use super::ToFlowParts;

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

        assert!("SELECT * from users;".to_flow_parts(&mut g).is_ok());
        // Should now have source, "users" and the new selection
        assert_eq!(g.graph.node_count(), 3);

        // Invalid query should fail parsing and add no nodes
        assert!("foo bar from whatever;".to_flow_parts(&mut g).is_err());
        // Should still only have source, "users" and the new selection
        assert_eq!(g.graph.node_count(), 3);
    }

    #[test]
    fn it_incorporates_simple_selection() {
        // set up graph
        let mut g = FlowGraph::new();

        // Establish a base write type
        assert!("INSERT INTO users VALUES (?, ?);".to_flow_parts(&mut g).is_ok());
        // Should have source and "users" base table node
        assert_eq!(g.graph.node_count(), 2);
        assert_eq!(get_view(&g, "users").name(), "users");

        // Try a simple query
        assert!("SELECT * from users;".to_flow_parts(&mut g).is_ok());
        println!("{}", g);
    }
}
