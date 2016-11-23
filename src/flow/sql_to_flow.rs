use nom_sql::parser as sql_parser;
use nom_sql::parser::{Column, ConditionBase, ConditionExpression, FieldExpression, Operator,
                      SqlQuery};
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

type FG = FlowGraph<Query, ops::Update, Vec<DataType>>;

/// Converts a condition tree stored in the `ConditionExpr` returned by the SQL parser into a
/// vector of conditions that `shortcut` understands.
fn to_conditions(ce: &ConditionExpression) -> Vec<shortcut::Condition<DataType>> {
    match *ce {
        ConditionExpression::Base(_) => vec![],
        ConditionExpression::ComparisonOp(ref ct) => {
            // TODO(malte): fix this once nom-sql has better operator representations
            assert_eq!(ct.operator, Operator::Equal);
            // TODO(malte): we only support one level of condition nesting at this point :(
            let l = match *ct.left.as_ref().unwrap().as_ref() {
                ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
                _ => unimplemented!(),
            };
            let r = match *ct.right.as_ref().unwrap().as_ref() {
                ConditionExpression::Base(ConditionBase::Placeholder) => String::from("?"),
                ConditionExpression::Base(ConditionBase::Literal(ref l)) => l.clone(),
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

fn lookup_node(vn: &str, g: &mut FG) -> petgraph::graph::NodeIndex {
    g.named[vn]
}

fn make_base_node(st: &InsertStatement) -> Node {
    let (cols, vals): (Vec<Column>, Vec<String>) = st.fields.iter().cloned().unzip();
    ops::new(st.table.clone(),
             Vec::from_iter(cols.iter().map(|c| c.name.as_str())).as_slice(),
             true,
             Base {})
}

fn make_filter_nodes(st: &SelectStatement, g: &mut FG) -> Vec<Node> {
    // XXX(malte): we need a custom name/identifier scheme for filter nodes, since the table
    // name is already used for the base node. Maybe this is where identifiers based on query
    // prefixes come in.
    let cols = match st.fields {
        FieldExpression::All => unimplemented!(),
        FieldExpression::Seq(ref s) => s.clone(),
    };
    let mut nodes = Vec::new();
    for t in st.tables.iter() {
        // XXX(malte): This isn't correct yet, because it completely ignores joins. This basically
        // creates an edge node that projects the right base columns and applies any filter
        // conditions from the query.
        let mut n = ops::new(t.clone(),
                             Vec::from_iter(cols.iter()
                                     .filter(|c| c.table.as_ref().unwrap() == t)
                                     .map(|c| c.name.as_str()))
                                 .as_slice(),
                             true,
                             Identity::new(lookup_node(t, g)));
        match st.where_clause {
            None => (),
            // convert ConditionTree to shortcut-style condition vector
            Some(ref cond) => {
                n = n.having(to_conditions(cond));
            }
        }
        nodes.push(n);
    }
    nodes
}

fn nodes_for_query(q: SqlQuery, g: &mut FG) -> Vec<Node> {
    println!("{:#?}", q);

    match q {
        SqlQuery::Insert(iq) => vec![make_base_node(&iq)],
        SqlQuery::Select(sq) => make_filter_nodes(&sq, g),
    }
}

trait ToFlowParts {
    fn to_flow_parts<'a>(&self, &mut FG) -> Result<Vec<petgraph::graph::NodeIndex>, String>;
}

impl<'a> ToFlowParts for &'a str {
    fn to_flow_parts(&self, g: &mut FG) -> Result<Vec<petgraph::graph::NodeIndex>, String> {
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
    use super::{FG, ToFlowParts};

    type Update = ops::Update;
    type Data = Vec<DataType>;

    // Helper to grab a reference to a named view.
    // TODO(malte): maybe this should be available in FlowGraph?
    fn get_view<'a>(g: &'a FG,
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

        assert!("SELECT users.id from users;".to_flow_parts(&mut g).is_ok());
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
        assert!("SELECT users.id, users.name FROM users WHERE id = 42;"
            .to_flow_parts(&mut g)
            .is_ok());
        println!("{}", g);
    }
}
