use nom_sql::parser as sql_parser;
use flow;
use flow::sql::query_graph::{QueryGraph, QueryGraphEdge, QueryGraphNode, to_query_graph};
use FlowGraph;
use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator, SqlQuery};
use nom_sql::{InsertStatement, SelectStatement};
use ops;
use ops::Node;
use ops::base::Base;
use ops::join::Builder as JoinBuilder;
use ops::permute::Permute;
use query::{DataType, Query};
use shortcut;

use petgraph::graph::NodeIndex;
use std::collections::{HashMap, HashSet};
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
    if !g.named.contains_key(vn) {
        panic!("Failed to resolve view named \"{}\" in graph!", vn);
    }
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
    let parent_ni = lookup_nodeindex(&qgn.rel_name, g);
    let parent_view = lookup_view_by_nodeindex(parent_ni, g);
    let projected_columns: Vec<usize> = qgn.columns
        .iter()
        .map(|c| field_to_columnid(parent_view, &c.name).unwrap())
        .collect();
    let mut n = ops::new(String::from(name),
                         Vec::from_iter(qgn.columns.iter().map(|c| c.name.as_str())).as_slice(),
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
                  jps: &Vec<ConditionTree>,
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
    for (i, p) in jps.iter().enumerate() {
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

fn make_nodes_for_selection(st: &SelectStatement,
                            name: &str,
                            g: &mut FG)
                            -> (QueryGraph, Vec<NodeIndex>) {
    use std::collections::HashMap;

    let qg = match to_query_graph(st) {
        Ok(qg) => qg,
        Err(e) => panic!(e),
    };

    let nodes_added;

    let mut i = 0;
    {
        // 1. Generate a filter node for each relation node in the query graph.
        let mut filter_nodes = HashMap::<String, NodeIndex>::new();
        // Need to iterate over relations in a deterministic order, as otherwise nodes will be added in
        // a different order every time, which will yield different node identifiers and make it
        // difficult for applications to check what's going on.
        let mut sorted_rels: Vec<&String> = qg.relations.keys().collect();
        sorted_rels.sort();
        for rel in sorted_rels.iter() {
            let qgn = qg.relations.get(*rel).unwrap();
            // the following conditional is required to avoid "empty" nodes (without any projected
            // columns) that are required as inputs to joins
            if qgn.columns.len() > 0 || qgn.predicates.len() > 0 {
                // add a basic filter/permute node for each query graph node if it either has:
                // 1. projected columns, or
                // 2. a filter condition
                let n = make_filter_node(&format!("q_{:x}_n{}", qg.signature().hash, i), qgn, g);
                let ni = g.incorporate(n);
                filter_nodes.insert((*rel).clone(), ni);
            } else {
                // otherwise, just record the node index of the base node for the relation that is
                // being selected from
                filter_nodes.insert((*rel).clone(), lookup_nodeindex(rel, g));
            }
            i += 1;
        }

        // 2. Generate join nodes for the query. This starts out by joining two of the filter nodes
        //    corresponding to relations in the first join predicate, and then continues to join the
        //    result against previously unseen tables from the remaining predicates.
        //    Note that no (src, dst) pair ever occurs twice, since we've already previously moved all
        //    predicates pertaining to src/dst joins onto a single edge.
        let mut join_nodes = Vec::new();
        let mut joined_tables = HashSet::new();
        let mut sorted_edges: Vec<(&(String, String), &QueryGraphEdge)> = qg.edges.iter().collect();
        sorted_edges.sort_by_key(|ref k| &(k.0).0);
        let mut edge_iter = sorted_edges.iter();
        let mut prev_ni = None;
        while let Some(&(&(ref src, ref dst), edge)) = edge_iter.next() {
            let left_ni = match prev_ni {
                None => {
                    joined_tables.insert(src);
                    filter_nodes.get(src).unwrap().clone()
                }
                Some(ni) => ni,
            };
            let right_ni = if joined_tables.contains(src) {
                joined_tables.insert(dst);
                filter_nodes.get(dst).unwrap().clone()
            } else if joined_tables.contains(dst) {
                joined_tables.insert(src);
                filter_nodes.get(src).unwrap().clone()
            } else {
                // We have already handled *both* tables that are part of the join. This should never
                // occur, because their join predicates must be associated with the same query graph
                // edge.
                unreachable!();
            };
            let jps = match *edge {
                QueryGraphEdge::Join(ref jps) => jps,
                _ => unimplemented!(),
            };
            let n = make_join_node(&format!("q_{:x}_n{}", qg.signature().hash, i),
                                   jps,
                                   left_ni,
                                   right_ni,
                                   g);
            let ni = g.incorporate(n);
            join_nodes.push(ni);
            i += 1;
            prev_ni = Some(ni);
        }

        // 3. Generate leaf views that expose the query result
        let n;
        {
            let final_join_ni = if !join_nodes.is_empty() {
                join_nodes.last().unwrap()
            } else {
                assert!(filter_nodes.len() == 1);
                filter_nodes.iter().next().as_ref().unwrap().1
            };
            let final_join_view = lookup_view_by_nodeindex(*final_join_ni, g);
            let projected_columns: Vec<Column> = qg.relations
                .iter()
                .fold(Vec::new(), |mut v, (_, qgn)| {
                    v.extend(qgn.columns.clone().into_iter());
                    v
                });
            let projected_column_ids: Vec<usize> = projected_columns.iter()
                .map(|c| field_to_columnid(final_join_view, &c.name).unwrap())
                .collect();
            n = ops::new(String::from(name),
                         projected_columns.iter()
                             .map(|c| c.name.as_str())
                             .collect::<Vec<&str>>()
                             .as_slice(),
                         true,
                         Permute::new(*final_join_ni, projected_column_ids.as_slice()));
        }
        let ni = g.incorporate(n);
        filter_nodes.insert(String::from(name), ni);

        // finally, we output all the nodes we generated
        nodes_added =
            filter_nodes.into_iter().map(|(_, n)| n).chain(join_nodes.into_iter()).collect();
    }
    (qg, nodes_added)
}

/// Long-lived struct that holds information about the SQL queries that have been incorporated into
/// the Soup graph `grap`.
/// The incorporator shares the lifetime of the flow graph it is associated with.
pub struct SqlIncorporator<'a> {
    write_schemas: HashMap<String, Vec<String>>,
    query_graphs: Vec<QueryGraph>,
    graph: &'a mut FG,
    num_queries: usize,
}

impl<'a> SqlIncorporator<'a> {
    /// Creates a new `SqlIncorporator` for the flow graph `g` (of which it takes and stores a
    /// mutable borrow). Use this to add SQL queries to a Soup graph, but note that once you have
    /// started using a `SqlIncorporator`, you can't manually add nodes any more (deliberately so).
    pub fn new(g: &'a mut FG) -> SqlIncorporator {
        SqlIncorporator {
            write_schemas: HashMap::new(),
            query_graphs: Vec::new(),
            graph: g,
            num_queries: 0,
        }
    }

    fn nodes_for_query(&mut self, q: SqlQuery) -> (String, Vec<NodeIndex>) {
        let name = match q {
            SqlQuery::Insert(ref iq) => iq.table.name.clone(),
            SqlQuery::Select(_) => format!("q_{}", self.num_queries),
        };
        self.num_queries += 1;
        self.nodes_for_named_query(q, name)
    }

    fn nodes_for_named_query(&mut self,
                             q: SqlQuery,
                             query_name: String)
                             -> (String, Vec<NodeIndex>) {
        use flow::sql::passes::alias_removal::AliasRemoval;
        use flow::sql::passes::implied_tables::ImpliedTableExpansion;
        use flow::sql::passes::star_expansion::StarExpansion;

        // first run some standard rewrite passes on the query. This makes the later work easier, as we
        // no longer have to consider complications like aliases.
        let q = q.expand_table_aliases()
            .expand_stars(&self.write_schemas)
            .expand_implied_tables(&self.write_schemas);

        match q {
            SqlQuery::Insert(iq) => {
                if self.write_schemas.contains_key(&iq.table.name) {
                    println!("WARNING: base table for write typye {} already exists: ignoring \
                              query.",
                             iq.table.name);
                    (iq.table.name.clone(), vec![])
                } else {
                    assert_eq!(query_name, iq.table.name);
                    let n = make_base_node(&iq);
                    self.write_schemas.insert(iq.table.name.clone(),
                                              iq.fields
                                                  .iter()
                                                  .map(|&(ref c, _)| c.name.clone())
                                                  .collect());
                    (query_name, vec![self.graph.incorporate(n)])
                }
            }
            SqlQuery::Select(sq) => {
                let (qg, nodes) = make_nodes_for_selection(&sq, &query_name, self.graph);
                // Store the query graph for later reference
                self.query_graphs.push(qg);
                // Return new nodes
                (query_name, nodes)
            }
        }
    }
}

/// Enables incorporation of a textual SQL query into a Soup graph.
pub trait ToFlowParts {
    /// Turn a SQL query into a set of nodes inserted into the Soup graph managed by
    /// the `SqlIncorporator` in the second argument. The query can optionally be named by the
    /// string in the `Option<String>` in the third argument.
    fn to_flow_parts<'a>(&self,
                         &mut SqlIncorporator,
                         Option<String>)
                         -> Result<(String, Vec<NodeIndex>), String>;
}

impl<'a> ToFlowParts for &'a String {
    fn to_flow_parts(&self,
                     inc: &mut SqlIncorporator,
                     name: Option<String>)
                     -> Result<(String, Vec<NodeIndex>), String> {
        self.as_str().to_flow_parts(inc, name)
    }
}

impl<'a> ToFlowParts for &'a str {
    fn to_flow_parts(&self,
                     inc: &mut SqlIncorporator,
                     name: Option<String>)
                     -> Result<(String, Vec<NodeIndex>), String> {
        // try parsing the incoming SQL
        let parsed_query = sql_parser::parse_query(self);

        // if ok, manufacture a node for the query structure we got
        let nodes = match parsed_query {
            Ok(q) => {
                match name {
                    Some(name) => Ok(inc.nodes_for_named_query(q, name)),
                    None => Ok(inc.nodes_for_query(q)),
                }
            }
            Err(e) => Err(String::from(e)),
        };
        nodes
    }
}

#[cfg(test)]
mod tests {
    use FlowGraph;
    use nom_sql::Column;
    use ops;
    use query::DataType;
    use super::{FG, SqlIncorporator, ToFlowParts, V};
    use std::io::Read;
    use std::fs::File;

    type Update = ops::Update;
    type Data = Vec<DataType>;

    /// Helper to grab a reference to a named view.
    /// TODO(malte): maybe this should be available in FlowGraph?
    fn get_view<'a>(g: &'a FG, vn: &str) -> &'a V {
        &**(g.graph[g.named[vn]].as_ref().unwrap())
    }

    /// Helper to compute a query ID hash via the same method as in `QueryGraph::signature()`.
    /// Note that the argument slices must be ordered in the same way as &str and &Column are
    /// ordered by `Ord`.
    fn query_id_hash(tables: &[&str], columns: &[&Column]) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        for t in tables.iter() {
            t.hash(&mut hasher);
        }
        for c in columns.iter() {
            c.hash(&mut hasher);
        }
        hasher.finish()
    }

    #[test]
    fn it_parses() {
        // set up graph
        let mut g = FlowGraph::new();
        let mut inc = SqlIncorporator::new(&mut g);

        // Must have a base node for type inference to work, so make one manually
        assert!("INSERT INTO users (id, name) VALUES (?, ?);"
            .to_flow_parts(&mut inc, None)
            .is_ok());

        // Should have two nodes: source and "users" base table
        assert_eq!(inc.graph.graph.node_count(), 2);
        assert_eq!(get_view(&inc.graph, "users").name(), "users");


        assert!("SELECT users.id from users;".to_flow_parts(&mut inc, None).is_ok());
        // Should now have source, "users", and two nodes for the new selection: one filter node
        // and one edge view node
        assert_eq!(inc.graph.graph.node_count(), 4);

        // Invalid query should fail parsing and add no nodes
        assert!("foo bar from whatever;".to_flow_parts(&mut inc, None).is_err());
        // Should still only have source, "users" and the two nodes for the above selection
        assert_eq!(inc.graph.graph.node_count(), 4);
    }

    #[test]
    fn it_incorporates_simple_join() {
        use ops::NodeOp;

        // set up graph
        let mut g = FlowGraph::new();
        let mut inc = SqlIncorporator::new(&mut g);

        // Establish a base write type for "users"
        assert!("INSERT INTO users (id, name) VALUES (?, ?);"
            .to_flow_parts(&mut inc, None)
            .is_ok());
        // Should have source and "users" base table node
        assert_eq!(inc.graph.graph.node_count(), 2);
        assert_eq!(get_view(&inc.graph, "users").name(), "users");
        assert_eq!(get_view(&inc.graph, "users").args(), &["id", "name"]);
        assert_eq!(get_view(&inc.graph, "users").node().unwrap().operator().description(),
                   "B");

        // Establish a base write type for "articles"
        assert!("INSERT INTO articles (id, author, title) VALUES (?, ?, ?);"
            .to_flow_parts(&mut inc, None)
            .is_ok());
        // Should have source and "users" base table node
        assert_eq!(inc.graph.graph.node_count(), 3);
        assert_eq!(get_view(&inc.graph, "articles").name(), "articles");
        assert_eq!(get_view(&inc.graph, "articles").args(),
                   &["id", "author", "title"]);
        assert_eq!(get_view(&inc.graph, "articles").node().unwrap().operator().description(),
                   "B");

        // Try a simple equi-JOIN query
        assert!("SELECT users.name, articles.title FROM articles, users WHERE users.id = \
                 articles.author;"
            .to_flow_parts(&mut inc, None)
            .is_ok());
        println!("{}", inc.graph);
        let qid = query_id_hash(&["articles", "users"],
                                &[&Column::from("articles.author"), &Column::from("users.id")]);
        // permute node 1 (for articles)
        let new_view1 = get_view(&inc.graph, &format!("q_{:x}_n0", qid));
        assert_eq!(new_view1.args(), &["title", "author"]);
        assert_eq!(new_view1.node().unwrap().operator().description(),
                   format!("π[2, 1]"));
        // permute node 2 (for users)
        let new_view2 = get_view(&inc.graph, &format!("q_{:x}_n1", qid));
        assert_eq!(new_view2.args(), &["name", "id"]);
        assert_eq!(new_view2.node().unwrap().operator().description(),
                   format!("π[1, 0]"));
        // join node
        let new_view3 = get_view(&inc.graph, &format!("q_{:x}_n2", qid));
        assert_eq!(new_view3.args(), &["title", "author", "name", "id"]);
    }

    #[test]
    fn it_incorporates_simple_selection() {
        use ops::NodeOp;

        // set up graph
        let mut g = FlowGraph::new();
        let mut inc = SqlIncorporator::new(&mut g);

        // Establish a base write type
        assert!("INSERT INTO users (id, name) VALUES (?, ?);"
            .to_flow_parts(&mut inc, None)
            .is_ok());
        // Should have source and "users" base table node
        assert_eq!(inc.graph.graph.node_count(), 2);
        assert_eq!(get_view(&inc.graph, "users").name(), "users");
        assert_eq!(get_view(&inc.graph, "users").args(), &["id", "name"]);
        assert_eq!(get_view(&inc.graph, "users").node().unwrap().operator().description(),
                   "B");

        // Try a simple query
        let res = "SELECT users.name FROM users WHERE users.id = 42;".to_flow_parts(&mut inc, None);
        assert!(res.is_ok());
        let qid = query_id_hash(&["users"], &[&Column::from("users.id")]);
        let new_view = get_view(&inc.graph, &format!("q_{:x}_n0", qid));
        assert_eq!(new_view.args(), &["name"]);
        assert_eq!(new_view.node().unwrap().operator().description(),
                   format!("π[1]"));
    }


    #[test]
    fn it_incorporates_finkelstein1982_naively() {
        // set up graph
        let mut g = FlowGraph::new();
        let mut inc = SqlIncorporator::new(&mut g);

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
            println!("{:?}", q.to_flow_parts(&mut inc, None));
        }

        println!("{}", inc.graph);
    }
}
