mod mir;
mod passes;
mod query_graph;
mod query_signature;
mod reuse;

use nom_sql::parser as sql_parser;
use flow::Migration;
use flow::core::NodeAddress;
use nom_sql::{Column, SqlQuery};
use nom_sql::SelectStatement;
use self::mir::{MirNodeRef, MirQuery, SqlToMirConverter};
use self::reuse::ReuseType;
use sql::query_graph::{QueryGraph, to_query_graph};

use slog;
use std::collections::HashMap;
use std::str;
use std::vec::Vec;

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

#[derive(Clone, Debug)]
enum QueryGraphReuse {
    ExactMatch(MirNodeRef),
    ExtendExisting(MirQuery),
    ReaderOntoExisting(MirNodeRef, Vec<Column>),
    None,
}

/// Long-lived struct that holds information about the SQL queries that have been incorporated into
/// the Soup graph `grap`.
/// The incorporator shares the lifetime of the flow graph it is associated with.
#[derive(Clone, Debug)]
pub struct SqlIncorporator {
    log: slog::Logger,
    mir_converter: SqlToMirConverter,
    leaf_addresses: HashMap<String, NodeAddress>,
    num_queries: usize,
    query_graphs: HashMap<u64, (QueryGraph, MirQuery)>,
    schema_version: usize,
    view_schemas: HashMap<String, Vec<String>>,
    transactional: bool,
}

impl Default for SqlIncorporator {
    fn default() -> Self {
        SqlIncorporator {
            log: slog::Logger::root(slog::Discard, o!()),
            mir_converter: SqlToMirConverter::default(),
            leaf_addresses: HashMap::default(),
            num_queries: 0,
            query_graphs: HashMap::default(),
            schema_version: 0,
            view_schemas: HashMap::default(),
            transactional: false,
        }
    }
}

impl SqlIncorporator {
    /// Creates a new `SqlIncorporator` for an empty flow graph.
    pub fn new(log: slog::Logger) -> Self {
        let lc = log.clone();
        SqlIncorporator {
            log: log,
            mir_converter: SqlToMirConverter::with_logger(lc),
            ..Default::default()
        }
    }

    /// Make any future base nodes added be transactional.
    pub fn set_transactional(&mut self, transactional: bool) {
        self.transactional = transactional;
    }

    /// Incorporates a single query into via the flow graph migration in `mig`. The `query`
    /// argument is a string that holds a parameterized SQL query, and the `name` argument supplies
    /// an optional name for the query. If no `name` is specified, the table name is used in the
    /// case of INSERT queries, and a deterministic, unique name is generated and returned
    /// otherwise.
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

    /// Incorporates a single query into via the flow graph migration in `mig`. The `query`
    /// argument is a `SqlQuery` structure, and the `name` argument supplies an optional name for
    /// the query. If no `name` is specified, the table name is used in the case of INSERT queries,
    /// and a deterministic, unique name is generated and returned otherwise.
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

    #[cfg(test)]
    fn get_flow_node_address(&self, name: &str, v: usize) -> Option<NodeAddress> {
        self.mir_converter.get_flow_node_address(name, v)
    }

    /// Retrieves the flow node associated with a given query's leaf view.
    pub fn get_query_address(&self, name: &str) -> Option<NodeAddress> {
        self.mir_converter.get_leaf(name)
    }

    fn consider_query_graph(&mut self,
                            query_name: &str,
                            st: &SelectStatement)
                            -> (QueryGraph, QueryGraphReuse) {
        debug!(self.log, "Making QG for \"{}\"", query_name);
        trace!(self.log, "Query \"{}\": {:#?}", query_name, st);

        let qg = match to_query_graph(st) {
            Ok(qg) => qg,
            Err(e) => panic!(e),
        };

        trace!(self.log, "QG for \"{}\": {:#?}", query_name, qg);

        // Do we already have this exact query or a subset of it?
        // TODO(malte): make this an O(1) lookup by QG signature
        let qg_hash = qg.signature().hash;
        match self.query_graphs.get(&qg_hash) {
            None => (),
            Some(&(ref existing_qg, ref mir_query)) => {
                // note that this also checks the *order* in which parameters are specified; a
                // different order means that we cannot simply reuse the existing reader.
                if existing_qg.signature() == qg.signature() &&
                   existing_qg.parameters() == qg.parameters() {
                    // we already have this exact query, down to the exact same reader key columns
                    // in exactly the same order
                    info!(self.log,
                          "An exact match for query \"{}\" already exists, reusing it",
                          query_name);
                    return (qg, QueryGraphReuse::ExactMatch(mir_query.leaf.clone()));
                } else if existing_qg.signature() == qg.signature() {
                    // QGs are identical, except for parameters (or their order)
                    info!(self.log,
                          "Query '{}' has an exact match modulo parameters, \
                          so making a new reader",
                          query_name);

                    let params = qg.parameters().into_iter().cloned().collect();
                    return (qg,
                            QueryGraphReuse::ReaderOntoExisting(mir_query.leaf.clone(), params));
                }
            }
        }

        let mut reuse_candidates = Vec::new();
        for &(ref existing_qg, _) in self.query_graphs.values() {
            // queries are different, but one might be a generalization of the other
            if existing_qg
                   .signature()
                   .is_generalization_of(&qg.signature()) {
                trace!(self.log,
                       "Checking reuse compatibility of {:#?}",
                       existing_qg);
                match reuse::check_compatibility(&qg, existing_qg) {
                    Some(reuse) => {
                        // QGs are compatible, we can reuse `existing_qg` as part of `qg`!
                        reuse_candidates.push((reuse, existing_qg));
                    }
                    None => (),
                }
            }
        }
        if reuse_candidates.len() > 0 {
            info!(self.log,
                  "Identified {} candidate QGs for reuse",
                  reuse_candidates.len());
            trace!(self.log,
                   "This QG: {:#?}\nReuse candidates:\n{:#?}",
                   qg,
                   reuse_candidates);

            // TODO(malte): score reuse candidates
            let choice = reuse::choose_best_option(reuse_candidates);

            match choice.0 {
                ReuseType::DirectExtension => {
                    let ref mir_query = self.query_graphs[&choice.1.signature().hash].1;
                    info!(self.log,
                          "Can reuse by directly extending existing query {}",
                          mir_query.name);
                    return (qg, QueryGraphReuse::ExtendExisting(mir_query.clone()));
                }
                ReuseType::BackjoinRequired(_) => {
                    error!(self.log, "Choose unsupported reuse via backjoin!");
                }
            }
        } else {
            info!(self.log, "No reuse opportunity, adding fresh query");
        }

        (qg, QueryGraphReuse::None)
    }

    fn add_leaf_to_existing_query(&mut self,
                                  query_name: &str,
                                  params: &Vec<Column>,
                                  leaf: MirNodeRef,
                                  mut mig: &mut Migration)
                                  -> QueryFlowParts {
        // We want to hang the new leaf off the last non-leaf node of the query, so backtrack one
        // step here.
        let final_node_of_query = leaf.borrow().ancestors().iter().next().unwrap().clone();

        let mut mir = self.mir_converter
            .add_leaf_below(final_node_of_query, query_name, params);

        trace!(self.log, "Reused leaf node MIR: {:#?}", mir);

        // push it into the flow graph using the migration in `mig`, and obtain `QueryFlowParts`.
        // Note that we don't need to optimize the MIR here, because the query is trivial.
        let qfp = mir.into_flow_parts(&mut mig);

        // TODO(malte): we currently need to remember these for local state, but should figure out
        // a better plan (see below)
        let fields = mir.leaf
            .borrow()
            .columns()
            .into_iter()
            .map(|c| String::from(c.name.as_str()))
            .collect::<Vec<_>>();

        // TODO(malte): get rid of duplication and figure out where to track this state
        self.view_schemas.insert(String::from(query_name), fields);

        // We made a new query, so store the query graph and the corresponding leaf MIR query
        //self.query_graphs.insert(qg.signature().hash, (qg, mir));

        qfp
    }

    fn add_base_via_mir(&mut self,
                        query_name: &str,
                        query: &SqlQuery,
                        mut mig: &mut Migration)
                        -> QueryFlowParts {
        // first, compute the MIR representation of the SQL query
        let mut mir = self.mir_converter
            .named_base_to_mir(query_name, query, self.transactional);

        trace!(self.log, "Base node MIR: {:#?}", mir);

        // no optimization, because standalone base nodes can't be optimized

        // TODO(malte): we currently need to remember these for local state, but should figure out
        // a better plan (see below)
        let fields = mir.leaf
            .borrow()
            .columns()
            .into_iter()
            .map(|c| String::from(c.name.as_str()))
            .collect::<Vec<_>>();

        // push it into the flow graph using the migration in `mig`, and obtain `QueryFlowParts`
        let qfp = mir.into_flow_parts(&mut mig);

        // TODO(malte): get rid of duplication and figure out where to track this state
        self.view_schemas.insert(String::from(query_name), fields);

        qfp
    }

    fn add_query_via_mir(&mut self,
                         query_name: &str,
                         query: &SelectStatement,
                         qg: QueryGraph,
                         mut mig: &mut Migration)
                         -> QueryFlowParts {
        // no QG-level reuse possible, so we'll build a new query.
        // first, compute the MIR representation of the SQL query
        let mut mir = self.mir_converter
            .named_query_to_mir(query_name, query, &qg);

        trace!(self.log, "Unoptimized MIR: {}", mir);

        // run MIR-level optimizations
        mir = mir.optimize();

        trace!(self.log, "Optimized MIR: {}", mir);

        // TODO(malte): we currently need to remember these for local state, but should figure out
        // a better plan (see below)
        let fields = mir.leaf
            .borrow()
            .columns()
            .into_iter()
            .map(|c| String::from(c.name.as_str()))
            .collect::<Vec<_>>();

        // push it into the flow graph using the migration in `mig`, and obtain `QueryFlowParts`
        let qfp = mir.into_flow_parts(&mut mig);

        // TODO(malte): get rid of duplication and figure out where to track this state
        self.view_schemas.insert(String::from(query_name), fields);

        // We made a new query, so store the query graph and the corresponding leaf MIR node
        self.query_graphs.insert(qg.signature().hash, (qg, mir));

        qfp
    }

    fn extend_existing_query(&mut self,
                             query_name: &str,
                             query: &SelectStatement,
                             qg: QueryGraph,
                             extend_mir: MirQuery,
                             mut mig: &mut Migration)
                             -> QueryFlowParts {
        use super::mir::reuse::merge_mir_for_queries;

        // no QG-level reuse possible, so we'll build a new query.
        // first, compute the MIR representation of the SQL query
        let new_query_mir = self.mir_converter
            .named_query_to_mir(query_name, query, &qg);
        // TODO(malte): should we run the MIR-level optimizations here?
        let new_opt_mir = new_query_mir.optimize();

        // compare to existing query MIR and reuse prefix
        let (reused_mir, num_reused_nodes) =
            merge_mir_for_queries(&self.log, &new_opt_mir, &extend_mir);

        let mut post_reuse_opt_mir = reused_mir.optimize_post_reuse();

        let qfp = post_reuse_opt_mir.into_flow_parts(&mut mig);

        info!(self.log,
              "Reused {} nodes for {}",
              num_reused_nodes,
              query_name);

        // We made a new query, so store the query graph and the corresponding leaf MIR node
        self.query_graphs
            .insert(qg.signature().hash, (qg, post_reuse_opt_mir));

        qfp
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
        use sql::passes::negation_removal::NegationRemoval;

        info!(self.log, "Processing query \"{}\"", query_name);

        // first run some standard rewrite passes on the query. This makes the later work easier,
        // as we no longer have to consider complications like aliases.
        let q = q.expand_table_aliases()
            .remove_negation()
            .expand_stars(&self.view_schemas)
            .expand_implied_tables(&self.view_schemas)
            .rewrite_count_star(&self.view_schemas);

        // if this is a selection, we compute its `QueryGraph` and consider the existing ones we
        // hold for reuse or extension
        let qfp = match q {
            SqlQuery::Select(ref sq) => {
                let (qg, reuse) = self.consider_query_graph(&query_name, sq);
                match reuse {
                    QueryGraphReuse::ExactMatch(mn) => {
                        let flow_node = mn.borrow().flow_node.as_ref().unwrap().address();
                        QueryFlowParts {
                            name: query_name.clone(),
                            new_nodes: vec![],
                            reused_nodes: vec![flow_node],
                            query_leaf: flow_node,
                        }
                    }
                    QueryGraphReuse::ExtendExisting(mq) => {
                        self.extend_existing_query(&query_name, sq, qg, mq, mig)
                    }
                    QueryGraphReuse::ReaderOntoExisting(mn, params) => {
                        self.add_leaf_to_existing_query(&query_name, &params, mn, mig)
                    }
                    QueryGraphReuse::None => self.add_query_via_mir(&query_name, sq, qg, mig),
                }
            }
            ref q @ SqlQuery::CreateTable { .. } |
            ref q @ SqlQuery::Insert { .. } => self.add_base_via_mir(&query_name, q, mig),
        };

        // record info about query
        self.num_queries += 1;
        self.leaf_addresses
            .insert(String::from(query_name.as_str()), qfp.query_leaf);

        qfp
    }

    /// Upgrades the schema version that any nodes created for queries will be tagged with.
    /// `new_version` must be strictly greater than the current version in `self.schema_version`.
    pub fn upgrade_schema(&mut self, new_version: usize) {
        assert!(new_version > self.schema_version);
        info!(self.log,
              "Schema version advanced from {} to {}",
              self.schema_version,
              new_version);
        self.schema_version = new_version;
        self.mir_converter.upgrade_schema(new_version);
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
    use nom_sql::FunctionExpression;

    /// Helper to grab a reference to a named view.
    fn get_node<'a>(inc: &SqlIncorporator, mig: &'a Migration, name: &str) -> &'a Node {
        let na = inc.get_flow_node_address(name, 0)
            .expect(&format!("No node named \"{}\" at v0", name));
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

        assert!("SELECT users.id from users;"
                    .to_flow_parts(&mut inc, None, &mut mig)
                    .is_ok());
        // Should now have source, "users", a leaf projection node for the new selection, and
        // a reader node
        assert_eq!(mig.graph().node_count(), ncount + 2);

        // Invalid query should fail parsing and add no nodes
        assert!("foo bar from whatever;"
                    .to_flow_parts(&mut inc, None, &mut mig)
                    .is_err());
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
        assert_eq!(new_leaf_view.description(), format!("π[2, 1, 4, 3]"));
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
        assert_eq!(filter.description(), format!("σ[f0 = 42]"));
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
        let res = inc.add_query("SELECT COUNT(votes.userid) AS votes \
                                FROM votes GROUP BY votes.aid;",
                                None,
                                &mut mig);
        assert!(res.is_ok());
        println!("{:?}", res);
        // added the aggregation and the edge view, and a reader
        assert_eq!(mig.graph().node_count(), 5);
        // check aggregation view
        let f = Box::new(FunctionExpression::Count(Column::from("votes.userid"), false));
        let qid = query_id_hash(&["computed_columns", "votes"],
                                &[&Column::from("votes.aid")],
                                &[&Column {
                                     name: String::from("votes"),
                                     alias: Some(String::from("votes")),
                                     table: None,
                                     function: Some(f),
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
        let id_node = qfp.new_nodes.iter().next().unwrap();
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
        let f = Box::new(FunctionExpression::Count(Column::from("votes.userid"), false));
        let qid = query_id_hash(&["computed_columns", "votes"],
                                &[],
                                &[&Column {
                                     name: String::from("count"),
                                     alias: Some(String::from("count")),
                                     table: None,
                                     function: Some(f),
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
        let f = Box::new(FunctionExpression::Count(Column::from("votes.aid"), false));
        let qid = query_id_hash(&["computed_columns", "votes"],
                                &[&Column::from("votes.userid")],
                                &[&Column {
                                     name: String::from("count"),
                                     alias: Some(String::from("count")),
                                     table: None,
                                     function: Some(f),
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
        let _qid = query_id_hash(&["articles", "users", "votes"],
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
    fn it_incorporates_finkelstein1982_naively() {
        use std::io::Read;
        use std::fs::File;

        // set up graph
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        {
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
            for q in lines.iter() {
                assert!(inc.add_query(q, None, &mut mig).is_ok());
            }
            mig.commit();
        }

        println!("{}", g);
    }
}
