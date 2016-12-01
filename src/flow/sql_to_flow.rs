use nom_sql::parser as sql_parser;
use flow;
use flow::sql::query_graph::{QueryGraphEdge, QueryGraphNode, to_query_graph};
use FlowGraph;
use nom_sql::parser::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator,
                      SqlQuery};
use nom_sql::{InsertStatement, SelectStatement};
use ops;
use ops::Node;
use ops::base::Base;
use ops::join::Builder as JoinBuilder;
use ops::permute::Permute;
use query::{DataType, Query};
use shortcut;

use petgraph;
use petgraph::graph::NodeIndex;
use std::iter::FromIterator;
use std::str;
use std::sync::Arc;
use std::vec::Vec;

type FG = FlowGraph<Query, ops::Update, Vec<DataType>>;
type V = flow::View<Query, Update = ops::Update, Data = Vec<DataType>>;

fn field_to_columnid(v: &flow::View<Query, Update = ops::Update, Data = Vec<DataType>>,
                     f: &str)
                     -> Result<usize, String> {
    for (i, field) in v.args().iter().enumerate() {
        if field == f {
            return Ok(i);
        }
    }
    Err(format!("field {} not found in view {}", f, v.name()))
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
                     column: field_to_columnid(v, &l.name).unwrap(),
                     cmp: shortcut::Comparison::Equal(shortcut::Value::Const(DataType::Text(Arc::new(r)))),
                 }]
    }
}

fn lookup_nodeindex(vn: &str, g: &FG) -> NodeIndex {
    g.named[vn]
}

fn lookup_view_by_nodeindex<'a>(ni: NodeIndex, g: &'a FG) -> &'a V {
    // TODO(malte): this is a bit of a monster. Maybe we can use something less ugly?
    g.graph().0.node_weight(ni).unwrap().as_ref().unwrap().as_ref()
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
    let parent_ni = lookup_nodeindex(&qgn.rel_name, g);
    let parent_view = lookup_view_by_nodeindex(parent_ni, g);
    let projected_columns: Vec<usize> = qgn.columns
        .iter()
        .map(|c| field_to_columnid(parent_view, &c).unwrap())
        .collect();
    let mut n = ops::new(String::from(name),
                         Vec::from_iter(qgn.columns.iter().map(String::as_str)).as_slice(),
                         true,
                         Permute::new(parent_ni, projected_columns.as_slice()));
    for cond in qgn.predicates.iter() {
        // convert ConditionTree to shortcut-style condition vector.
        let filter = to_conditions(cond, parent_view);
        n = n.having(filter);
    }
    n
}

fn make_join_node(name: &str,
                  qge: &QueryGraphEdge,
                  left_ni: NodeIndex,
                  right_ni: NodeIndex,
                  g: &mut FG)
                  -> Node {
    let left_node = lookup_view_by_nodeindex(left_ni, g);
    let right_node = lookup_view_by_nodeindex(right_ni, g);
    let projected_cols_left = left_node.args();
    let projected_cols_right = right_node.args();

    let tuples_for_cols = |ni: NodeIndex, cols: &[String]| -> Vec<(NodeIndex, usize)> {
        let view = lookup_view_by_nodeindex(ni, g);
        cols.iter().map(|c| (ni, field_to_columnid(view, &c).unwrap())).collect()
    };

    // non-join columns projected are the union of the ancestor's projected columns
    // TODO(malte): this will need revisiting when we do smart reuse
    let mut join_proj_config = tuples_for_cols(left_ni, projected_cols_left);
    join_proj_config.extend(tuples_for_cols(right_ni, projected_cols_right));
    // join columns need us to generate join group configs for the operator
    let mut left_join_group = vec![0; left_node.args().len()];
    let mut right_join_group = vec![0; right_node.args().len()];
    for (i, p) in qge.join_predicates.iter().enumerate() {
        // equi-join only
        assert_eq!(p.operator, Operator::Equal);
        let l_col = match **p.left.as_ref().unwrap() {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
            _ => unimplemented!(),
        };
        // assert_eq!(l_col.table.unwrap(), left_node.name());
        let r_col = match **p.right.as_ref().unwrap() {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
            _ => unimplemented!(),
        };
        // assert_eq!(r_col.table.unwrap(), right_node.name());
        left_join_group[field_to_columnid(left_node, &l_col.name).unwrap()] = i + 1;
        right_join_group[field_to_columnid(right_node, &r_col.name).unwrap()] = i + 1;
    }
    let j = JoinBuilder::new(join_proj_config)
        .from(left_ni, left_join_group)
        .join(right_ni, right_join_group);
    let n = ops::new(String::from(name),
                     projected_cols_left.into_iter()
                         .chain(projected_cols_right.into_iter())
                         .map(String::as_str)
                         .collect::<Vec<&str>>()
                         .as_slice(),
                     true,
                     j);
    n
}

fn make_nodes_for_selection(st: &SelectStatement, g: &mut FG) -> Vec<NodeIndex> {
    use std::collections::HashMap;

    let qg = to_query_graph(st);

    // TODO(malte): the following ugliness is required since we need to iterate over the HashMap in
    // a *sorted* order, as views are otherwise numbered randomly and unit tests fail. Clearly, we
    // need a better identifier scheme...
    let mut i = g.graph().0.node_count();
    let mut rels = qg.relations.keys().collect::<Vec<&String>>();
    rels.sort();

    // 1. Generate a filter node for each relation node in the query graph.
    let mut filter_nodes = HashMap::new();
    for rel in rels.iter() {
        let qgn = qg.relations.get(*rel).unwrap();
        // the following conditional is required to avoid "empty" nodes (without any projected
        // columns) that are required as inputs to joins
        if qgn.columns.len() > 0 || qgn.predicates.len() > 0 {
            // add a basic filter/permute node for each query graph node if it either has:
            // 1. projected columns, or
            // 2. a filter condition
            let n = make_filter_node(&format!("query-{}", i), qgn, g);
            let ni = g.incorporate(n);
            filter_nodes.insert(rel.clone(), ni);
        } else {
            // otherwise, just record the node index of the base node for the relation that is
            // being selected from
            filter_nodes.insert(rel.clone(), lookup_nodeindex(rel, g));
        }
        i += 1;
    }

    // 2. Direct joins between filter nodes on base tables: this generates one join per join
    //    edge in the query graph.
    let mut join_nodes = Vec::new();
    for (&(ref src, ref dst), edge) in qg.edges.iter() {
        // query graph edges are joins, so add a join node for each
        let left_ni = filter_nodes.get(src).unwrap().clone();
        let right_ni = filter_nodes.get(dst).unwrap().clone();
        let n = make_join_node(&format!("query-{}", i), edge, left_ni, right_ni, g);
        let ni = g.incorporate(n);
        join_nodes.push(ni);
        i += 1;
    }
    // 3. Nested joins: if a query contains > 1 join condition, we need to chain several joins
    //    nodes together.
    // TODO(malte): implement this

    // finally, we output all the nodes we generated
    filter_nodes.into_iter().map(|(_, n)| n).chain(join_nodes.into_iter()).collect()
}

fn nodes_for_query(q: SqlQuery, g: &mut FG) -> Vec<NodeIndex> {
    use flow::sql::passes::Pass;
    use flow::sql::passes::alias_removal::AliasRemoval;

    // first run some standard rewrite passes on the query. This makes the later work easier, as we
    // no longer have to consider complications like aliases.
    let mut ar = AliasRemoval::new();
    let q = ar.apply(q);

    match q {
        SqlQuery::Insert(iq) => {
            let n = make_base_node(&iq);
            vec![g.incorporate(n)]
        }
        SqlQuery::Select(sq) => make_nodes_for_selection(&sq, g),
    }
}

trait ToFlowParts {
    fn to_flow_parts<'a>(&self, &mut FG) -> Result<Vec<NodeIndex>, String>;
}

impl<'a> ToFlowParts for &'a String {
    fn to_flow_parts(&self, g: &mut FG) -> Result<Vec<NodeIndex>, String> {
        self.as_str().to_flow_parts(g)
    }
}

impl<'a> ToFlowParts for &'a str {
    fn to_flow_parts(&self, g: &mut FG) -> Result<Vec<NodeIndex>, String> {
        // try parsing the incoming SQL
        let parsed_query = sql_parser::parse_query(self);

        // if ok, manufacture a node for the query structure we got
        let nodes = match parsed_query {
            Ok(q) => Ok(nodes_for_query(q, g)),
            Err(e) => Err(String::from(e)),
        };
        nodes
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
    use std::io::Read;
    use std::fs::File;

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
    fn it_incorporates_simple_join() {
        use ops::NodeOp;

        // set up graph
        let mut g = FlowGraph::new();

        // Establish a base write type for "users"
        assert!("INSERT INTO users (id, name) VALUES (?, ?);".to_flow_parts(&mut g).is_ok());
        // Should have source and "users" base table node
        assert_eq!(g.graph.node_count(), 2);
        assert_eq!(get_view(&g, "users").name(), "users");
        assert_eq!(get_view(&g, "users").args(), &["id", "name"]);
        assert_eq!(get_view(&g, "users").node().unwrap().operator().description(),
                   "B");

        // Establish a base write type for "articles"
        assert!("INSERT INTO articles (id, author, title) VALUES (?, ?, ?);"
            .to_flow_parts(&mut g)
            .is_ok());
        // Should have source and "users" base table node
        assert_eq!(g.graph.node_count(), 3);
        assert_eq!(get_view(&g, "articles").name(), "articles");
        assert_eq!(get_view(&g, "articles").args(), &["id", "author", "title"]);
        assert_eq!(get_view(&g, "articles").node().unwrap().operator().description(),
                   "B");

        // Try a simple equi-JOIN query
        assert!("SELECT users.name, articles.title FROM articles, users WHERE users.id = \
                 articles.author;"
            .to_flow_parts(&mut g)
            .is_ok());
        println!("{}", g);
        // permute node 1 (for articles)
        let new_view1 = get_view(&g, "query-3");
        assert_eq!(new_view1.args(), &["title", "author"]);
        assert_eq!(new_view1.node().unwrap().operator().description(),
                   format!("π[2, 1]"));
        // permute node 2 (for users)
        let new_view2 = get_view(&g, "query-4");
        assert_eq!(new_view2.args(), &["name", "id"]);
        assert_eq!(new_view2.node().unwrap().operator().description(),
                   format!("π[1, 0]"));
        // join node
        let new_view3 = get_view(&g, "query-5");
        assert_eq!(new_view3.args(), &["name", "id", "title", "author"]);
    }

    #[test]
    fn it_incorporates_simple_selection() {
        use ops::NodeOp;

        // set up graph
        let mut g = FlowGraph::new();

        // Establish a base write type
        assert!("INSERT INTO users (id, name) VALUES (?, ?);".to_flow_parts(&mut g).is_ok());
        // Should have source and "users" base table node
        assert_eq!(g.graph.node_count(), 2);
        assert_eq!(get_view(&g, "users").name(), "users");
        assert_eq!(get_view(&g, "users").args(), &["id", "name"]);
        assert_eq!(get_view(&g, "users").node().unwrap().operator().description(),
                   "B");

        // Try a simple query
        assert!("SELECT users.name FROM users WHERE users.id = 42;"
            .to_flow_parts(&mut g)
            .is_ok());
        let new_view = get_view(&g, "query-2");
        assert_eq!(new_view.args(), &["name"]);
        assert_eq!(new_view.node().unwrap().operator().description(),
                   format!("π[1]"));
    }


    #[test]
    fn it_incorporates_finkelstein1982_naively() {
        // set up graph
        let mut g = FlowGraph::new();

        let mut f = File::open("tests/finkelstein82.txt").unwrap();
        let mut s = String::new();

        // Load queries
        f.read_to_string(&mut s).unwrap();
        let lines: Vec<String> = s.lines()
            .filter(|l| !l.is_empty() && !l.starts_with("#"))
            .map(|l| {
                if !(l.ends_with("\n") || l.ends_with(";")) {
                    String::from(l) + "\n"
                } else {
                    String::from(l)
                }
            })
            .collect();

        // Add them one by one
        for q in lines.iter() {
            q.to_flow_parts(&mut g);
        }

        println!("{}", g);
    }
}
