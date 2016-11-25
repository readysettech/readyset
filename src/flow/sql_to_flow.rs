use nom_sql::parser as sql_parser;
use nom_sql::parser::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator,
                      SqlQuery};
use nom_sql::{InsertStatement, SelectStatement};
use flow;
use FlowGraph;
use ops;
use ops::Node;
use ops::base::Base;
use ops::identity::Identity;
use query::{DataType, Query};
use super::query_graph::{QueryGraphNode, to_query_graph};
use shortcut;

use petgraph;
use std::iter::FromIterator;
use std::str;
use std::sync::Arc;
use std::vec::Vec;

type FG = FlowGraph<Query, ops::Update, Vec<DataType>>;
type V = flow::View<Query, Update = ops::Update, Data = Vec<DataType>>;

fn field_to_columnid(v: &flow::View<Query, Update = ops::Update, Data = Vec<DataType>>,
                     f: String)
                     -> Result<usize, String> {
    let i = 0;
    for field in v.args().iter() {
        if *field == f {
            return Ok(i);
        }
    }
    Err(format!("field {} not found", f))
}

/// Converts a condition tree stored in the `ConditionExpr` returned by the SQL parser into a
/// vector of conditions that `shortcut` understands.
fn to_conditions(ct: &ConditionTree, v: &V) -> Vec<shortcut::Condition<DataType>> {
    // TODO(malte): fix this once nom-sql has better operator representations
    if ct.operator != Operator::Equal {
        println!("Conditionals with {:?} are not supported in shortcut yet, so ignoring {:?}",
                 ct.operator,
                 ct);
        vec![]
    } else {
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
                     column: field_to_columnid(v, l.name).unwrap(),
                     cmp: shortcut::Comparison::Equal(shortcut::Value::Const(DataType::Text(Arc::new(r)))),
                 }]
    }
}

fn lookup_node(vn: &str, g: &mut FG) -> petgraph::graph::NodeIndex {
    g.named[vn]
}

fn make_base_node(st: &InsertStatement) -> Node {
    let (cols, _): (Vec<Column>, Vec<String>) = st.fields.iter().cloned().unzip();
    ops::new(st.table.name.clone(),
             Vec::from_iter(cols.iter().map(|c| c.name.as_str())).as_slice(),
             true,
             Base {})
}

fn make_filter_node(name: &str, qgn: &QueryGraphNode, g: &mut FG) -> Node {
    // XXX(malte): we need a custom name/identifier scheme for filter nodes, since the table
    // name is already used for the base node. Maybe this is where identifiers based on query
    // prefixes come in.
    let mut n = ops::new(String::from(name),
                         Vec::from_iter(qgn.columns.iter().map(String::as_str)).as_slice(),
                         true,
                         Identity::new(lookup_node(&qgn.rel_name, g)));
    for cond in qgn.predicates.iter() {
        // convert ConditionTree to shortcut-style condition vector
        let filter = to_conditions(cond, &n);
        n = n.having(filter);
    }
    n
}

fn make_nodes_for_selection(st: &SelectStatement, g: &mut FG) -> Vec<Node> {
    let qg = to_query_graph(st);
    println!("Query graph: {:#?}", qg);

    let mut new_nodes = Vec::new();
    let mut i = 0;
    for (_, qgn) in qg.relations.iter() {
        // add a basic filter/permute node for each query graph node
        let n = make_filter_node(&format!("query-{}", i), qgn, g);
        new_nodes.push(n);
        i += 1;
    }
    for _edge in qg.edges.iter() {
        // query graph edges are joins, so add a join node for each
        // TODO(malte): implement!
    }
    new_nodes
}

fn nodes_for_query(q: SqlQuery, g: &mut FG) -> Vec<Node> {
    println!("{:#?}", q);

    match q {
        SqlQuery::Insert(iq) => vec![make_base_node(&iq)],
        SqlQuery::Select(sq) => make_nodes_for_selection(&sq, g),
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
    use ops;
    use ops::new;
    use ops::base::Base;
    use query::DataType;
    use super::{FG, ToFlowParts, V};

    type Update = ops::Update;
    type Data = Vec<DataType>;

    /// Helper to grab a reference to a named view.
    /// TODO(malte): maybe this should be available in FlowGraph?
    fn get_view<'a>(g: &'a FG, vn: &str) -> &'a V {
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
        assert!("SELECT users.id, users.name FROM users WHERE users.id = 42;"
            .to_flow_parts(&mut g)
            .is_ok());
        println!("{}", g);
    }
}
