use nom_sql::parser as sql_parser;
use flow::Migration;
use flow::core::{NodeAddress, DataType};
use mir::{MirNode, MirNodeType, MirQuery};
use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator, TableKey,
              SqlQuery};
use nom_sql::{SelectStatement, LimitClause, OrderType, OrderClause};
use ops;
use ops::base::Base;
use ops::identity::Identity;
use ops::join::Builder as JoinBuilder;
use ops::permute::Permute;
use sql::query_graph::{QueryGraph, QueryGraphEdge, QueryGraphNode, to_query_graph};

use slog;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::str;
use std::vec::Vec;
use std::cmp::Ordering;
use std::sync::Arc;

pub mod passes;
pub mod query_graph;
pub mod query_signature;

/// Represents the result of a query incorporation, specifying query name (auto-generated or
/// reflecting a pre-specified name), new nodes added for the query, reused nodes that are part of
/// the query, and the leaf node that represents the query result (and off whom we've hung a
/// `Reader` node),
#[derive(Clone, Debug, PartialEq)]
pub struct QueryFlowParts {
    pub name: String,
    pub new_nodes: Vec<NodeAddress>,
    pub reused_nodes: Vec<NodeAddress>,
    pub query_leaf: NodeAddress,
}

/// Helper enum to avoid having separate `make_aggregation_node` and `make_extremum_node` functions
enum GroupedNodeType {
    Aggregation(ops::grouped::aggregate::Aggregation),
    Extremum(ops::grouped::extremum::Extremum),
    GroupConcat(String),
}

fn target_columns_from_computed_column(computed_col: &Column) -> &Vec<Column> {
    use nom_sql::FunctionExpression::*;
    use nom_sql::FieldExpression::*;

    match *computed_col.function.as_ref().unwrap() {
        Avg(Seq(ref cols)) |
        Count(Seq(ref cols)) |
        GroupConcat(Seq(ref cols), _) |
        Max(Seq(ref cols)) |
        Min(Seq(ref cols)) |
        Sum(Seq(ref cols)) => cols,
        Count(All) => {
            // see comment re COUNT(*) rewriting in make_aggregation_node
            panic!("COUNT(*) should have been rewritten earlier!")
        }
        _ => panic!("invalid aggregation function"),
    }
}

/// Long-lived struct that holds information about the SQL queries that have been incorporated into
/// the Soup graph `grap`.
/// The incorporator shares the lifetime of the flow graph it is associated with.
#[derive(Clone, Debug)]
pub struct SqlIncorporator {
    log: slog::Logger,
    write_schemas: HashMap<String, Vec<String>>,
    node_addresses: HashMap<String, NodeAddress>,
    node_fields: HashMap<NodeAddress, Vec<String>>,
    query_graphs: Vec<(QueryGraph, NodeAddress)>,
    num_queries: usize,
}

impl Default for SqlIncorporator {
    fn default() -> Self {
        SqlIncorporator {
            log: slog::Logger::root(slog::Discard, None),
            write_schemas: HashMap::default(),
            node_addresses: HashMap::default(),
            node_fields: HashMap::default(),
            query_graphs: Vec::new(),
            num_queries: 0,
        }
    }
}

impl SqlIncorporator {
    /// Creates a new `SqlIncorporator` for an empty flow graph.
    pub fn new(log: slog::Logger) -> Self {
        let mut inc = SqlIncorporator::default();
        inc.log = log;
        inc
    }

    /// TODO(malte): modify once `SqlIntegrator` has a better intermediate graph representation.
    fn fields_for(&self, na: NodeAddress) -> &[String] {
        self.node_fields[&na].as_slice()
    }

    /// TODO(malte): modify once `SqlIntegrator` has a better intermediate graph representation.
    fn field_to_columnid(&self, na: NodeAddress, f: &str) -> Result<usize, String> {
        match self.fields_for(na).iter().position(|s| *s == f) {
            None => {
                Err(format!("field {} not found in view {} (which has: {:?})",
                            f,
                            na,
                            self.fields_for(na)))
            }
            Some(i) => Ok(i),
        }
    }

    /// TODO(malte): modify once `SqlIntegrator` has a better intermediate graph representation.
    pub fn address_for(&self, name: &str) -> NodeAddress {
        match self.node_addresses.get(name) {
            None => panic!("node {} unknown!", name),
            Some(na) => *na,
        }
    }

    /// Converts a condition tree stored in the `ConditionExpr` returned by the SQL parser into a
    /// vector of conditions that `shortcut` understands.
    fn to_conditions(&self,
                     ct: &ConditionTree,
                     na: &NodeAddress)
                     -> Vec<Option<(Operator, DataType)>> {
        // TODO(malte): we only support one level of condition nesting at this point :(
        let l = match *ct.left
                   .as_ref()
                   .unwrap()
                   .as_ref() {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
            _ => unimplemented!(),
        };
        let r = match *ct.right
                   .as_ref()
                   .unwrap()
                   .as_ref() {
            ConditionExpression::Base(ConditionBase::Literal(ref l)) => l.clone(),
            _ => unimplemented!(),
        };
        let num_columns = self.fields_for(*na).len();
        let mut filter = vec![None; num_columns];
        filter[self.field_to_columnid(*na, &l.name).unwrap()] = Some((ct.operator.clone(),
                                                                      DataType::from(r)));
        filter
    }

    /// Incorporates a single query into via the flow graph migration in `mig`. The `query` argument is a
    /// string that holds a parameterized SQL query, and the `name` argument supplies an optional
    /// name for the query. If no `name` is specified, the table name is used in the case of INSERT
    /// queries, and a deterministic, unique name is generated and returned otherwise.
    ///
    /// The return value is a tuple containing the query name (specified or computing) and a `Vec`
    /// of `NodeAddress`es representing the nodes added to support the query.
    pub fn add_query(&mut self,
                     query: &str,
                     name: Option<String>,
                     mut mig: &mut Migration)
                     -> Result<QueryFlowParts, String> {
        query.to_flow_parts(self, name, &mut mig)
    }

    /// Incorporates a single query into via the flow graph migration in `mig`. The `query` argument is a
    /// `SqlQuery` structure, and the `name` argument supplies an optional name for the query. If no
    /// `name` is specified, the table name is used in the case of INSERT queries, and a deterministic,
    /// unique name is generated and returned otherwise.
    ///
    /// The return value is a tuple containing the query name (specified or computing) and a `Vec`
    /// of `NodeAddress`es representing the nodes added to support the query.
    pub fn add_parsed_query(&mut self,
                            query: SqlQuery,
                            name: Option<String>,
                            mut mig: &mut Migration)
                            -> Result<QueryFlowParts, String> {
        let res = match name {
            None => self.nodes_for_query(query, mig),
            Some(n) => self.nodes_for_named_query(query, n, mig),
        };
        // TODO(malte): this currently always succeeds because `nodes_for_query` and
        // `nodes_for_named_query` can't fail
        Ok(res)
    }

    fn nodes_for_query(&mut self, q: SqlQuery, mig: &mut Migration) -> QueryFlowParts {
        let name = match q {
            SqlQuery::CreateTable(ref ctq) => ctq.table.name.clone(),
            SqlQuery::Insert(ref iq) => iq.table.name.clone(),
            SqlQuery::Select(_) => format!("q_{}", self.num_queries),
        };
        self.nodes_for_named_query(q, name, mig)
    }

    fn nodes_for_named_query(&mut self,
                             q: SqlQuery,
                             query_name: String,
                             mut mig: &mut Migration)
                             -> QueryFlowParts {
        use sql::passes::alias_removal::AliasRemoval;
        use sql::passes::count_star_rewrite::CountStarRewrite;
        use sql::passes::implied_tables::ImpliedTableExpansion;
        use sql::passes::star_expansion::StarExpansion;

        info!(self.log, "Processing query \"{}\"", query_name);

        // first run some standard rewrite passes on the query. This makes the later work easier,
        // as we no longer have to consider complications like aliases.
        let q = q.expand_table_aliases()
            .expand_stars(&self.write_schemas)
            .expand_implied_tables(&self.write_schemas)
            .rewrite_count_star(&self.write_schemas);

        // compute MIR representation of the SQL query
        let mut mir = self.mir_for_named_query(&query_name, q);

        // run MIR-level optimizations
        mir = mir.optimize();

        self.num_queries += 1;
        // push it into the flow graph using the migration in `mig`
        mir.into_flow_parts(&mut mig)
    }

    fn mir_for_named_query(&self, name: &str, query: SqlQuery) -> MirQuery {
        match query {
            SqlQuery::CreateTable(ctq) => {
                assert_eq!(name, ctq.table.name);
                let n = self.make_base_node(&name, &ctq.fields, ctq.keys.as_ref());

                MirQuery {
                    name: String::from(name),
                    roots: vec![Box::new(n)],
                    leaf: None,
                }
            }
            SqlQuery::Insert(iq) => {
                assert_eq!(name, iq.table.name);
                let (cols, _): (Vec<Column>, Vec<String>) = iq.fields
                    .iter()
                    .cloned()
                    .unzip();
                let n = self.make_base_node(&name, &cols, None);

                MirQuery {
                    name: String::from(name),
                    roots: vec![Box::new(n)],
                    leaf: None,
                }
            }
            _ => unimplemented!(),
            /*SqlQuery::Select(sq) => {
                let nodes = self.make_nodes_for_selection(&sq, &name);
                let roots = nodes.iter()
                    .filter(|mn| mn.ancestors().len() == 0)
                    .map(|mn| Box::new(mn))
                    .collect();
                let leaves = nodes.iter()
                    .filter(|mn| mn.children().len() == 0)
                    .map(|mn| Box::new(mn))
                    .collect();
                assert_eq!(leaves.len(), 1);

                MirQuery {
                    name: String::from(name),
                    roots: roots,
                    leaf: leaves.into_iter().next().unwrap(),
                }
            }*/
        }
    }

    /// Return is (`node`, `is_new`)
    fn make_base_node(&self,
                      name: &str,
                      cols: &Vec<Column>,
                      keys: Option<&Vec<TableKey>>)
                      -> MirNode {
        /*if self.write_schemas.contains_key(name) {
            let ref existing_schema = self.write_schemas[name];

            // TODO(malte): check the keys too
            if *existing_schema == cols.iter().map(|c| c.name.as_str()).collect::<Vec<&str>>() {
                info!(self.log,
                      "base table for {} already exists with identical schema; ignoring it.",
                      name);
            } else {
                error!(self.log,
                       "base table for write type {} already exists, but has a different schema!",
                       name);
            }
            return (self.node_addresses[name], false);
        }*/

        assert!(cols.iter().all(|c| c.table == Some(String::from(name))));

        let primary_keys = match keys {
            None => vec![],
            Some(keys) => {
                keys.iter()
                    .filter_map(|k| match *k {
                                    ref k @ TableKey::PrimaryKey(..) => Some(k),
                                    _ => None,
                                })
                    .collect()
            }
        };
        assert!(primary_keys.len() <= 1);

        if !primary_keys.is_empty() {
            match **primary_keys.iter().next().unwrap() {
                TableKey::PrimaryKey(ref key_cols) => {
                    debug!(self.log,
                           "Assigning primary key {:?} for base {}",
                           key_cols,
                           name);
                    MirNode {
                        name: String::from(name),
                        from_version: 0,
                        columns: cols.clone(),
                        inner: MirNodeType::Base(cols.clone(), key_cols.clone()),
                    }
                }
                _ => unreachable!(),
            }
        } else {
            MirNode {
                name: String::from(name),
                from_version: 0,
                columns: cols.clone(),
                inner: MirNodeType::Base(cols.clone(), vec![]),
            }
        }
    }

    /// Return is (`new_nodes`, `leaf_node`).
    fn make_nodes_for_selection(&mut self, st: &SelectStatement, name: &str) -> Vec<MirNode> {
        vec![]
    }
}

/// Enables incorporation of a textual SQL query into a Soup graph.
pub trait ToFlowParts {
    /// Turn a SQL query into a set of nodes inserted into the Soup graph managed by
    /// the `SqlIncorporator` in the second argument. The query can optionally be named by the
    /// string in the `Option<String>` in the third argument.
    fn to_flow_parts(&self,
                     &mut SqlIncorporator,
                     Option<String>,
                     &mut Migration)
                     -> Result<QueryFlowParts, String>;
}

impl<'a> ToFlowParts for &'a String {
    fn to_flow_parts(&self,
                     inc: &mut SqlIncorporator,
                     name: Option<String>,
                     mig: &mut Migration)
                     -> Result<QueryFlowParts, String> {
        self.as_str().to_flow_parts(inc, name, mig)
    }
}

impl<'a> ToFlowParts for &'a str {
    fn to_flow_parts(&self,
                     inc: &mut SqlIncorporator,
                     name: Option<String>,
                     mig: &mut Migration)
                     -> Result<QueryFlowParts, String> {
        // try parsing the incoming SQL
        let parsed_query = sql_parser::parse_query(self);

        // if ok, manufacture a node for the query structure we got
        match parsed_query {
            Ok(q) => {
                match name {
                    Some(name) => Ok(inc.nodes_for_named_query(q, name, mig)),
                    None => Ok(inc.nodes_for_query(q, mig)),
                }
            }
            Err(e) => Err(String::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::Column;
    use flow::node::Node;
    use flow::Migration;
    use Blender;
    use super::{SqlIncorporator, ToFlowParts};
    use nom_sql::{FieldExpression, FunctionExpression};

    /// Helper to grab a reference to a named view.
    fn get_node<'a>(inc: &SqlIncorporator, mig: &'a Migration, name: &str) -> &'a Node {
        let na = inc.address_for(name);
        mig.graph().node_weight(na.as_global().clone()).unwrap()
    }

    /// Helper to compute a query ID hash via the same method as in `QueryGraph::signature()`.
    /// Note that the argument slices must be ordered in the same way as &str and &Column are
    /// ordered by `Ord`.
    fn query_id_hash(relations: &[&str], attrs: &[&Column], columns: &[&Column]) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        for r in relations.iter() {
            r.hash(&mut hasher);
        }
        for a in attrs.iter() {
            a.hash(&mut hasher);
        }
        for c in columns.iter() {
            c.hash(&mut hasher);
        }
        hasher.finish()
    }

    #[test]
    fn it_parses() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Must have a base node for type inference to work, so make one manually
        assert!("INSERT INTO users (id, name) VALUES (?, ?);"
                    .to_flow_parts(&mut inc, None, &mut mig)
                    .is_ok());

        // Should have two nodes: source and "users" base table
        let ncount = mig.graph().node_count();
        assert_eq!(ncount, 2);
        assert_eq!(get_node(&inc, &mig, "users").name(), "users");

        assert!("SELECT users.id from users;".to_flow_parts(&mut inc, None, &mut mig).is_ok());
        // Should now have source, "users", a leaf projection node for the new selection, and
        // a reader node
        assert_eq!(mig.graph().node_count(), ncount + 2);

        // Invalid query should fail parsing and add no nodes
        assert!("foo bar from whatever;".to_flow_parts(&mut inc, None, &mut mig).is_err());
        // Should still only have source, "users" and the two nodes for the above selection
        assert_eq!(mig.graph().node_count(), ncount + 2);
    }

    #[test]
    fn it_incorporates_simple_join() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Establish a base write type for "users"
        assert!(inc.add_query("INSERT INTO users (id, name) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        // Should have source and "users" base table node
        assert_eq!(mig.graph().node_count(), 2);
        assert_eq!(get_node(&inc, &mig, "users").name(), "users");
        assert_eq!(get_node(&inc, &mig, "users").fields(), &["id", "name"]);
        assert_eq!(get_node(&inc, &mig, "users").description(), "B");

        // Establish a base write type for "articles"
        assert!(inc.add_query("INSERT INTO articles (id, author, title) VALUES (?, ?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        // Should have source and "users" base table node
        assert_eq!(mig.graph().node_count(), 3);
        assert_eq!(get_node(&inc, &mig, "articles").name(), "articles");
        assert_eq!(get_node(&inc, &mig, "articles").fields(),
                   &["id", "author", "title"]);
        assert_eq!(get_node(&inc, &mig, "articles").description(), "B");

        // Try a simple equi-JOIN query
        let q = "SELECT users.name, articles.title \
                 FROM articles, users \
                 WHERE users.id = articles.author;";
        let q = inc.add_query(q, None, &mut mig);
        assert!(q.is_ok());
        let qid = query_id_hash(&["articles", "users"],
                                &[&Column::from("articles.author"), &Column::from("users.id")],
                                &[&Column::from("articles.author"),
                                  &Column::from("articles.title"),
                                  &Column::from("users.id"),
                                  &Column::from("users.name")]);
        // join node
        let new_join_view = get_node(&inc, &mig, &format!("q_{:x}_n0", qid));
        assert_eq!(new_join_view.fields(),
                   &["id", "author", "title", "id", "name"]);
        // leaf node
        let new_leaf_view = get_node(&inc, &mig, &q.unwrap().name);
        // XXX(malte): leaf overprojection needs fixing
        assert_eq!(new_leaf_view.fields(), &["title", "author", "name", "id"]);
        assert_eq!(new_leaf_view.description(), format!("π[2, 1, 4, 0]"));
    }

    #[test]
    fn it_incorporates_simple_selection() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Establish a base write type
        assert!(inc.add_query("INSERT INTO users (id, name) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        // Should have source and "users" base table node
        assert_eq!(mig.graph().node_count(), 2);
        assert_eq!(get_node(&inc, &mig, "users").name(), "users");
        assert_eq!(get_node(&inc, &mig, "users").fields(), &["id", "name"]);
        assert_eq!(get_node(&inc, &mig, "users").description(), "B");

        // Try a simple query
        let res = inc.add_query("SELECT users.name FROM users WHERE users.id = 42;",
                                None,
                                &mut mig);
        assert!(res.is_ok());

        let qid = query_id_hash(&["users"],
                                &[&Column::from("users.id")],
                                &[&Column::from("users.name")]);
        // filter node
        let filter = get_node(&inc, &mig, &format!("q_{:x}_n0_f0", qid));
        assert_eq!(filter.fields(), &["id", "name"]);
        assert_eq!(filter.description(), format!("σ[f0 = \"42\"]"));
        // leaf view node
        let edge = get_node(&inc, &mig, &res.unwrap().name);
        assert_eq!(edge.fields(), &["name"]);
        assert_eq!(edge.description(), format!("π[1]"));
    }

    #[test]
    fn it_incorporates_aggregation() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Establish a base write types
        assert!(inc.add_query("INSERT INTO votes (aid, userid) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        // Should have source and "users" base table node
        assert_eq!(mig.graph().node_count(), 2);
        assert_eq!(get_node(&inc, &mig, "votes").name(), "votes");
        assert_eq!(get_node(&inc, &mig, "votes").fields(), &["aid", "userid"]);
        assert_eq!(get_node(&inc, &mig, "votes").description(), "B");

        // Try a simple COUNT function
        let res = inc.add_query("SELECT COUNT(votes.userid) AS votes FROM votes GROUP BY votes.aid;",
                                None,
                                &mut mig);
        assert!(res.is_ok());
        println!("{:?}", res);
        // added the aggregation and the edge view, and a reader
        assert_eq!(mig.graph().node_count(), 5);
        // check aggregation view
        let qid = query_id_hash(&["computed_columns", "votes"],
                                &[&Column::from("votes.aid")],
                                &[&Column {
                                    name: String::from("votes"),
                                    alias: Some(String::from("votes")),
                                    table: None,
                                    function: Some(FunctionExpression::Count(
                                            FieldExpression::Seq(
                                                vec![Column::from("votes.userid")]))),
                                }]);
        let agg_view = get_node(&inc, &mig, &format!("q_{:x}_n0", qid));
        assert_eq!(agg_view.fields(), &["aid", "votes"]);
        assert_eq!(agg_view.description(), format!("|*| γ[0]"));
        // check edge view
        let edge_view = get_node(&inc, &mig, &res.unwrap().name);
        assert_eq!(edge_view.fields(), &["votes"]);
        assert_eq!(edge_view.description(), format!("π[1]"));
    }

    #[test]
    fn it_reuses_identical_query() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Establish a base write type
        assert!(inc.add_query("INSERT INTO users (id, name) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        // Should have source and "users" base table node
        assert_eq!(mig.graph().node_count(), 2);
        assert_eq!(get_node(&inc, &mig, "users").name(), "users");
        assert_eq!(get_node(&inc, &mig, "users").fields(), &["id", "name"]);
        assert_eq!(get_node(&inc, &mig, "users").description(), "B");

        // Add a new query
        let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 42;",
                                None,
                                &mut mig);
        assert!(res.is_ok());
        let leaf = res.unwrap().query_leaf;

        // Add the same query again
        let ncount = mig.graph().node_count();
        let res = inc.add_query("SELECT name, id FROM users WHERE users.id = 42;",
                                None,
                                &mut mig);
        assert!(res.is_ok());
        // should have added no more nodes
        let qfp = res.unwrap();
        assert_eq!(qfp.new_nodes, vec![]);
        assert_eq!(mig.graph().node_count(), ncount);
        // should have ended up with the same leaf node
        assert_eq!(qfp.query_leaf, leaf);
    }

    #[test]
    fn it_reuses_with_different_parameter() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Establish a base write type
        assert!(inc.add_query("INSERT INTO users (id, name) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        // Should have source and "users" base table node
        assert_eq!(mig.graph().node_count(), 2);
        assert_eq!(get_node(&inc, &mig, "users").name(), "users");
        assert_eq!(get_node(&inc, &mig, "users").fields(), &["id", "name"]);
        assert_eq!(get_node(&inc, &mig, "users").description(), "B");

        // Add a new query
        let res = inc.add_query("SELECT id, name FROM users WHERE users.id = ?;",
                                None,
                                &mut mig);
        assert!(res.is_ok());

        // Add the same query again, but with a parameter on a different column
        let ncount = mig.graph().node_count();
        let res = inc.add_query("SELECT id, name FROM users WHERE users.name = ?;",
                                None,
                                &mut mig);
        assert!(res.is_ok());
        // should have added two more nodes: one identity node and one reader node
        let qfp = res.unwrap();
        assert_eq!(mig.graph().node_count(), ncount + 2);
        // only the identity node is returned in the vector of new nodes
        assert_eq!(qfp.new_nodes.len(), 1);
        assert_eq!(get_node(&inc, &mig, &qfp.name).description(), "≡");
        // we should be based off the identity as our leaf
        let id_node = qfp.new_nodes
            .iter()
            .next()
            .unwrap();
        assert_eq!(qfp.query_leaf, *id_node);
    }

    #[test]
    fn it_incorporates_aggregation_no_group_by() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Establish a base write type
        assert!(inc.add_query("INSERT INTO votes (aid, userid) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        // Should have source and "users" base table node
        assert_eq!(mig.graph().node_count(), 2);
        assert_eq!(get_node(&inc, &mig, "votes").name(), "votes");
        assert_eq!(get_node(&inc, &mig, "votes").fields(), &["aid", "userid"]);
        assert_eq!(get_node(&inc, &mig, "votes").description(), "B");
        // Try a simple COUNT function without a GROUP BY clause
        let res = inc.add_query("SELECT COUNT(votes.userid) AS count FROM votes;",
                                None,
                                &mut mig);
        assert!(res.is_ok());
        // added the aggregation, a project helper, the edge view, and reader
        assert_eq!(mig.graph().node_count(), 6);
        // check project helper node
        let qid = query_id_hash(&["computed_columns", "votes"], &[],
                                &[&Column {
                                    name: String::from("count"),
                                    alias: Some(String::from("count")),
                                    table: None,
                                    function: Some(FunctionExpression::Count(
                                            FieldExpression::Seq(
                                                vec![Column::from("votes.userid")]))),
                                }]);
        let proj_helper_view = get_node(&inc, &mig, &format!("q_{:x}_n0_prj_hlpr", qid));
        assert_eq!(proj_helper_view.fields(), &["userid", "grp"]);
        assert_eq!(proj_helper_view.description(), format!("π[1, lit: 0]"));
        // check aggregation view
        let agg_view = get_node(&inc, &mig, &format!("q_{:x}_n0", qid));
        assert_eq!(agg_view.fields(), &["grp", "count"]);
        assert_eq!(agg_view.description(), format!("|*| γ[1]"));
        // check edge view -- note that it's not actually currently possible to read from
        // this for a lack of key (the value would be the key)
        let edge_view = get_node(&inc, &mig, &res.unwrap().name);
        assert_eq!(edge_view.fields(), &["count"]);
        assert_eq!(edge_view.description(), format!("π[1]"));
    }

    #[test]
    fn it_incorporates_aggregation_count_star() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Establish a base write type
        assert!(inc.add_query("INSERT INTO votes (userid, aid) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        // Should have source and "users" base table node
        assert_eq!(mig.graph().node_count(), 2);
        assert_eq!(get_node(&inc, &mig, "votes").name(), "votes");
        assert_eq!(get_node(&inc, &mig, "votes").fields(), &["userid", "aid"]);
        assert_eq!(get_node(&inc, &mig, "votes").description(), "B");
        // Try a simple COUNT function without a GROUP BY clause
        let res = inc.add_query("SELECT COUNT(*) AS count FROM votes GROUP BY votes.userid;",
                                None,
                                &mut mig);
        assert!(res.is_ok());
        // added the aggregation, a project helper, the edge view, and reader
        assert_eq!(mig.graph().node_count(), 5);
        // check aggregation view
        let qid = query_id_hash(&["computed_columns", "votes"],
                                &[&Column::from("votes.userid")],
                                &[&Column {
                                    name: String::from("count"),
                                    alias: Some(String::from("count")),
                                    table: None,
                                    function: Some(FunctionExpression::Count(
                                            FieldExpression::Seq(
                                                vec![Column::from("votes.aid")]))),
                                }]);
        let agg_view = get_node(&inc, &mig, &format!("q_{:x}_n0", qid));
        assert_eq!(agg_view.fields(), &["userid", "count"]);
        assert_eq!(agg_view.description(), format!("|*| γ[0]"));
        // check edge view -- note that it's not actually currently possible to read from
        // this for a lack of key (the value would be the key)
        let edge_view = get_node(&inc, &mig, &res.unwrap().name);
        assert_eq!(edge_view.fields(), &["count"]);
        assert_eq!(edge_view.description(), format!("π[1]"));
    }

    #[test]
    fn it_incorporates_explicit_multi_join() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Establish base write types for "users" and "articles" and "votes"
        assert!(inc.add_query("INSERT INTO users (id, name) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        assert!(inc.add_query("INSERT INTO votes (aid, uid) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        assert!(inc.add_query("INSERT INTO articles (aid, title, author) VALUES (?, ?, ?);",
                              None,
                              &mut mig)
                    .is_ok());

        // Try an explicit multi-way-join
        let q = "SELECT users.name, articles.title, votes.uid \
                 FROM articles
                 JOIN users ON (users.id = articles.author) \
                 JOIN votes ON (votes.aid = articles.aid);";
        let q = inc.add_query(q, None, &mut mig);
        assert!(q.is_ok());
        let qid = query_id_hash(&["articles", "users", "votes"],
                                &[&Column::from("articles.aid"),
                                  &Column::from("articles.author"),
                                  &Column::from("users.id"),
                                  &Column::from("votes.aid")],
                                &[&Column::from("articles.title"),
                                  &Column::from("users.name"),
                                  &Column::from("votes.uid")]);
        // XXX(malte): non-deterministic join ordering make it difficult to assert on the join
        // views
        // leaf view
        let leaf_view = get_node(&inc, &mig, "q_3");
        assert_eq!(leaf_view.fields(), &["title", "name", "uid"]);
    }

    #[test]
    fn it_incorporates_implicit_multi_join() {
        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Establish base write types for "users" and "articles" and "votes"
        assert!(inc.add_query("INSERT INTO users (id, name) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        assert!(inc.add_query("INSERT INTO votes (aid, uid) VALUES (?, ?);",
                              None,
                              &mut mig)
                    .is_ok());
        assert!(inc.add_query("INSERT INTO articles (aid, title, author) VALUES (?, ?, ?);",
                              None,
                              &mut mig)
                    .is_ok());

        // Try an implicit multi-way-join
        let q = "SELECT users.name, articles.title, votes.uid \
                 FROM articles, users, votes
                 WHERE users.id = articles.author \
                 AND votes.aid = articles.aid;";
        let q = inc.add_query(q, None, &mut mig);
        assert!(q.is_ok());
        // XXX(malte): below over-projects into the final leaf, and is thus inconsistent
        // with the explicit JOIN case!
        let qid = query_id_hash(&["articles", "users", "votes"],
                                &[&Column::from("articles.aid"),
                                  &Column::from("articles.author"),
                                  &Column::from("users.id"),
                                  &Column::from("votes.aid")],
                                &[&Column::from("articles.aid"),
                                  &Column::from("articles.author"),
                                  &Column::from("articles.title"),
                                  &Column::from("users.id"),
                                  &Column::from("users.name"),
                                  &Column::from("votes.aid"),
                                  &Column::from("votes.uid")]);
        // XXX(malte): non-deterministic join ordering below
        let _join1_view = get_node(&inc, &mig, &format!("q_{:x}_n0", qid));
        // articles join votes
        //assert_eq!(join1_view.fields(),
        //           &["aid", "title", "author", "id", "name"]);
        let _join2_view = get_node(&inc, &mig, &format!("q_{:x}_n1", qid));
        // join1_view join users
        //assert_eq!(join2_view.fields(),
        //           &["aid", "title", "author", "aid", "uid", "id", "name"]);
        // leaf view
        let leaf_view = get_node(&inc, &mig, "q_3");
        assert_eq!(leaf_view.fields(),
                   &["title", "author", "aid", "name", "id", "uid", "aid"]);
    }

    #[test]
    #[ignore]
    fn it_incorporates_finkelstein1982_naively() {
        use std::io::Read;
        use std::fs::File;

        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        let mut f = File::open("tests/finkelstein82.txt").unwrap();
        let mut s = String::new();

        // Load queries
        f.read_to_string(&mut s).unwrap();
        let lines: Vec<String> = s.lines()
            .filter(|l| !l.is_empty() && !l.starts_with("#"))
            .map(|l| if !(l.ends_with("\n") || l.ends_with(";")) {
                     String::from(l) + "\n"
                 } else {
                     String::from(l)
                 })
            .collect();

        // Add them one by one
        for (i, q) in lines.iter().enumerate() {
            if let Ok(qfp) = inc.add_query(q, None, &mut mig) {
                println!("{}: {} -- {}\n", qfp.name, i, q);
            } else {
                println!("Failed to parse: {}\n", q);
            };
            // println!("{}", inc.graph);
        }
    }
}
