use nom_sql::parser as sql_parser;
use flow::{NodeAddress, Migration};
use flow::sql::query_graph::{QueryGraph, QueryGraphEdge, QueryGraphNode, to_query_graph};
use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, FieldExpression,
              FunctionExpression, Operator, SqlQuery};
use nom_sql::{InsertStatement, SelectStatement};
use ops;
use ops::base::Base;
use ops::join::Builder as JoinBuilder;
use ops::permute::Permute;
use query::DataType;

use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::str;
use std::sync::Arc;
use std::vec::Vec;

/// Long-lived struct that holds information about the SQL queries that have been incorporated into
/// the Soup graph `grap`.
/// The incorporator shares the lifetime of the flow graph it is associated with.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlIncorporator {
    write_schemas: HashMap<String, Vec<String>>,
    node_addresses: HashMap<String, NodeAddress>,
    node_fields: HashMap<NodeAddress, Vec<String>>,
    query_graphs: Vec<QueryGraph>,
    num_queries: usize,
}

impl Default for SqlIncorporator {
    /// Creates a new `SqlIncorporator` for an empty flow graph.
    fn default() -> Self {
        SqlIncorporator {
            write_schemas: HashMap::default(),
            node_addresses: HashMap::default(),
            node_fields: HashMap::default(),
            query_graphs: Vec::new(),
            num_queries: 0,
        }
    }
}

impl SqlIncorporator {
    /// TODO(malte): modify once `SqlIntegrator` has a better intermediate graph representation.
    fn fields_for(&self, na: NodeAddress) -> &[String] {
        self.node_fields[&na].as_slice()
    }

    /// TODO(malte): modify once `SqlIntegrator` has a better intermediate graph representation.
    fn field_to_columnid(&self, na: NodeAddress, f: &str) -> Result<usize, String> {
        match self.fields_for(na).iter().position(|s| *s == f) {
            None => Err(format!("field {} not found in view {}", f, na)),
            Some(i) => Ok(i),
        }
    }

    /// TODO(malte): modify once `SqlIntegrator` has a better intermediate graph representation.
    fn address_for(&self, name: &str) -> NodeAddress {
        match self.node_addresses.get(name) {
            None => panic!("node {} unknown!", name),
            Some(na) => *na,
        }
    }

    /// Converts a condition tree stored in the `ConditionExpr` returned by the SQL parser into a
    /// vector of conditions that `shortcut` understands.
    fn to_conditions(&self, ct: &ConditionTree, na: &NodeAddress) -> Vec<Option<DataType>> {
        // TODO(malte): support other types of operators
        if ct.operator != Operator::Equal {
            println!("Conditionals with {:?} are not supported yet, so ignoring {:?}",
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
                ConditionExpression::Base(ConditionBase::Literal(ref l)) => l.clone(),
                _ => unimplemented!(),
            };
            let num_columns = self.fields_for(*na).len();
            let mut filter = vec![None; num_columns];
            filter[self.field_to_columnid(*na, &l.name).unwrap()] =
                Some(DataType::Text(Arc::new(r)));
            filter
        }
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
                     -> Result<(String, Vec<NodeAddress>), String> {
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
                            -> Result<(String, Vec<NodeAddress>), String> {
        let res = match name {
            None => self.nodes_for_query(query, mig),
            Some(n) => self.nodes_for_named_query(query, n, mig),
        };
        // TODO(malte): this currently always succeeds because `nodes_for_query` and
        // `nodes_for_named_query` can't fail
        Ok(res)
    }

    fn nodes_for_query(&mut self, q: SqlQuery, mig: &mut Migration) -> (String, Vec<NodeAddress>) {
        let name = match q {
            SqlQuery::Insert(ref iq) => iq.table.name.clone(),
            SqlQuery::Select(_) => format!("q_{}", self.num_queries),
        };
        self.nodes_for_named_query(q, name, mig)
    }

    fn nodes_for_named_query(&mut self,
                             q: SqlQuery,
                             query_name: String,
                             mut mig: &mut Migration)
                             -> (String, Vec<NodeAddress>) {
        use flow::sql::passes::alias_removal::AliasRemoval;
        use flow::sql::passes::implied_tables::ImpliedTableExpansion;
        use flow::sql::passes::star_expansion::StarExpansion;

        // first run some standard rewrite passes on the query. This makes the later work easier,
        // as we no longer have to consider complications like aliases.
        let q = q.expand_table_aliases()
            .expand_stars(&self.write_schemas)
            .expand_implied_tables(&self.write_schemas);

        let (name, new_nodes) = match q {
            SqlQuery::Insert(iq) => {
                if self.write_schemas.contains_key(&iq.table.name) {
                    println!("WARNING: base table for write type {} already exists: ignoring \
                              query.",
                             iq.table.name);
                    (iq.table.name.clone(), vec![])
                } else {
                    assert_eq!(query_name, iq.table.name);
                    let ni = self.make_base_node(&iq, &mut mig);
                    self.write_schemas.insert(iq.table.name.clone(),
                                              iq.fields
                                                  .iter()
                                                  .map(|&(ref c, _)| c.name.clone())
                                                  .collect());
                    (query_name, vec![ni])
                }
            }
            SqlQuery::Select(sq) => {
                let (qg, nodes) = self.make_nodes_for_selection(&sq, &query_name, &mut mig);
                // Store the query graph for later reference
                self.query_graphs.push(qg);
                // Return new nodes
                (query_name, nodes)
            }
        };

        self.num_queries += 1;

        (name, new_nodes)
    }

    fn make_base_node(&mut self, st: &InsertStatement, mig: &mut Migration) -> NodeAddress {
        let name = st.table.name.clone();
        let (cols, _): (Vec<Column>, Vec<String>) = st.fields.iter().cloned().unzip();
        let fields = Vec::from_iter(cols.iter().map(|c| c.name.clone()));

        // make the new base node and record its information
        let na = mig.add_ingredient(st.table.name.clone(), fields.as_slice(), Base {});
        self.node_addresses.insert(name, na);
        self.node_fields.insert(na, fields);
        na
    }

    fn make_aggregation_node(&mut self,
                             name: &str,
                             computed_col_name: &str,
                             over: &Column,
                             group_by: &[Column],
                             agg: ops::grouped::aggregate::Aggregation,
                             mig: &mut Migration)
                             -> NodeAddress {
        let parent_ni = self.address_for(over.table.as_ref().unwrap());

        // Resolve column IDs in parent
        let over_col_indx = self.field_to_columnid(parent_ni, &over.name).unwrap();
        let group_col_indx = group_by.iter()
            .map(|c| self.field_to_columnid(parent_ni, &c.name).unwrap())
            .collect::<Vec<_>>();

        // The function node's set of output columns is the group columns plus the function
        // column
        let mut combined_columns = Vec::from_iter(group_by.iter().map(|c| c.name.clone()));
        combined_columns.push(String::from(computed_col_name));

        // make the new operator and record its metadata
        let na = mig.add_ingredient(String::from(name),
                                    combined_columns.as_slice(),
                                    agg.over(parent_ni, over_col_indx, group_col_indx.as_slice()));
        self.node_addresses.insert(String::from(name), na);
        self.node_fields.insert(na, combined_columns);
        na
    }

    fn make_function_node(&mut self,
                          name: &str,
                          func_col: &Column,
                          group_cols: &[Column],
                          mig: &mut Migration)
                          -> NodeAddress {
        use ops::grouped::aggregate::Aggregation;

        let func = func_col.function.as_ref().unwrap();
        match *func {
            FunctionExpression::Sum(FieldExpression::Seq(ref cols)) => {
                // No support for multi-columns counts
                assert_eq!(cols.len(), 1);

                // println!("SUM over {:?}", over);
                let over = cols.iter().next().unwrap();
                self.make_aggregation_node(name,
                                           &func_col.name,
                                           over,
                                           group_cols,
                                           Aggregation::SUM,
                                           mig)
            }
            FunctionExpression::Count(FieldExpression::Seq(ref cols)) => {
                // No support for multi-columns counts
                assert_eq!(cols.len(), 1);

                // println!("COUNT over {:?}", over);
                let over = cols.iter().next().unwrap();
                self.make_aggregation_node(name,
                                           &func_col.name,
                                           over,
                                           group_cols,
                                           Aggregation::COUNT,
                                           mig)
            }
            FunctionExpression::Count(FieldExpression::All) => {
                // XXX(malte): we will need a special operator here, since COUNT(*) refers to all
                // rows in the group (if we have a GROUP BY) or table/view (if we don't). As such,
                // there is no "over" column, but our aggregation operators' API requires one to be
                // specified.
                unimplemented!()
            }
            _ => unimplemented!(),
        }
    }

    fn make_filter_and_project_nodes(&mut self,
                                     name: &str,
                                     qgn: &QueryGraphNode,
                                     mig: &mut Migration)
                                     -> Vec<NodeAddress> {
        use ops::filter::Filter;

        let mut parent_ni = self.address_for(&qgn.rel_name);
        let mut new_nodes = vec![];
        // chain all the filters associated with this QGN
        for (i, cond) in qgn.predicates.iter().enumerate() {
            // convert ConditionTree to a chain of Filter operators.
            let filter = self.to_conditions(cond, &parent_ni);
            let parent_fields = Vec::from(self.fields_for(parent_ni));
            let f_name = String::from(format!("{}_f{}", name, i));
            let n = mig.add_ingredient(f_name.clone(),
                                       parent_fields.as_slice(),
                                       Filter::new(parent_ni, filter.as_slice()));
            self.node_addresses.insert(f_name, n);
            self.node_fields.insert(n, parent_fields);
            parent_ni = n;
            new_nodes.push(n);
        }
        // finally, project only the columns we need
        let projected_columns = Vec::from_iter(qgn.columns.iter().map(|c| c.name.clone()));
        let projected_column_ids: Vec<usize> = qgn.columns
            .iter()
            .map(|c| self.field_to_columnid(parent_ni, &c.name).unwrap())
            .collect();
        let n = mig.add_ingredient(String::from(name),
                                   projected_columns.as_slice(),
                                   Permute::new(parent_ni, projected_column_ids.as_slice()));
        self.node_addresses.insert(String::from(name), n);
        self.node_fields.insert(n, projected_columns);
        new_nodes.push(n);
        new_nodes
    }

    fn make_join_node(&mut self,
                      name: &str,
                      jps: &[ConditionTree],
                      left_ni: NodeAddress,
                      right_ni: NodeAddress,
                      mig: &mut Migration)
                      -> NodeAddress {
        let j;
        let fields;
        {
            let projected_cols_left = self.fields_for(left_ni);
            let projected_cols_right = self.fields_for(right_ni);

            let tuples_for_cols =
                |ni: NodeAddress, cols: &[String]| -> Vec<(NodeAddress, usize)> {
                    cols.iter()
                        .map(|c| (ni, self.field_to_columnid(ni, c).unwrap()))
                        .collect()
                };

            // non-join columns projected are the union of the ancestor's projected columns
            // TODO(malte): this will need revisiting when we do smart reuse
            let mut join_proj_config = tuples_for_cols(left_ni, projected_cols_left);
            join_proj_config.extend(tuples_for_cols(right_ni, projected_cols_right));
            // join columns need us to generate join group configs for the operator
            let mut left_join_group = vec![0; projected_cols_left.len()];
            let mut right_join_group = vec![0; projected_cols_right.len()];
            for (i, p) in jps.iter().enumerate() {
                // equi-join only
                assert_eq!(p.operator, Operator::Equal);
                let l_col = match **p.left.as_ref().unwrap() {
                    ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
                    _ => unimplemented!(),
                };
                let r_col = match **p.right.as_ref().unwrap() {
                    ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
                    _ => unimplemented!(),
                };
                left_join_group[self.field_to_columnid(left_ni, &l_col.name).unwrap()] = i + 1;
                right_join_group[self.field_to_columnid(right_ni, &r_col.name).unwrap()] = i + 1;
            }
            j = JoinBuilder::new(join_proj_config)
                .from(left_ni, left_join_group)
                .join(right_ni, right_join_group);
            fields = projected_cols_left.into_iter()
                .chain(projected_cols_right.into_iter())
                .cloned()
                .collect::<Vec<String>>();
        }
        let n = mig.add_ingredient(String::from(name), fields.as_slice(), j);
        // println!("added join on {:?} and {:?}: {}", left_ni, right_ni, name);
        self.node_addresses.insert(String::from(name), n);
        self.node_fields.insert(n, fields);
        n
    }

    fn make_nodes_for_selection(&mut self,
                                st: &SelectStatement,
                                name: &str,
                                mig: &mut Migration)
                                -> (QueryGraph, Vec<NodeAddress>) {
        use std::collections::HashMap;

        let qg = match to_query_graph(st) {
            Ok(qg) => qg,
            Err(e) => panic!(e),
        };

        let nodes_added;

        let mut i = 0;
        {
            // 1. Generate the necessary filter node for each relation node in the query graph.
            let mut filter_nodes = HashMap::<String, Vec<NodeAddress>>::new();
            // Need to iterate over relations in a deterministic order, as otherwise nodes will be
            // added in a different order every time, which will yield different node identifiers
            // and make it difficult for applications to check what's going on.
            let mut sorted_rels: Vec<&String> = qg.relations.keys().collect();
            sorted_rels.sort();
            for rel in &sorted_rels {
                let qgn = &qg.relations[*rel];
                // we'll handle computed columns later
                if *rel != "computed_columns" {
                    // the following conditional is required to avoid "empty" nodes (without any
                    // projected columns) that are required as inputs to joins
                    if !qgn.columns.is_empty() || !qgn.predicates.is_empty() {
                        // add a basic filter/permute node for each query graph node if it either
                        // has: 1) projected columns; or 2) a filter condition
                        let fns = self.make_filter_and_project_nodes(&format!("q_{:x}_n{}",
                                                                              qg.signature().hash,
                                                                              i),
                                                                     qgn,
                                                                     mig);
                        filter_nodes.insert((*rel).clone(), fns);
                    } else {
                        // otherwise, just record the node index of the base node for the relation
                        // that is being selected from
                        filter_nodes.insert((*rel).clone(), vec![self.address_for(rel)]);
                    }
                }
                i += 1;
            }

            // 2. Generate join nodes for the query. This starts out by joining two of the filter
            //    nodes corresponding to relations in the first join predicate, and then continues
            //    to join the result against previously unseen tables from the remaining
            //    predicates. Note that no (src, dst) pair ever occurs twice, since we've already
            //    previously moved all predicates pertaining to src/dst joins onto a single edge.
            let mut join_nodes = Vec::new();
            let mut joined_tables = HashSet::new();
            let mut sorted_edges: Vec<(&(String, String), &QueryGraphEdge)> =
                qg.edges.iter().collect();
            sorted_edges.sort_by_key(|k| &(k.0).0);
            let mut prev_ni = None;
            for &(&(ref src, ref dst), edge) in &sorted_edges {
                match *edge {
                    // Edge represents a JOIN
                    QueryGraphEdge::Join(ref jps) => {
                        let left_ni = match prev_ni {
                            None => {
                                joined_tables.insert(src);
                                let filters = &filter_nodes[src];
                                assert_ne!(filters.len(), 0);
                                *filters.last().unwrap()
                            }
                            Some(ni) => ni,
                        };
                        let right_ni = if joined_tables.contains(src) {
                            joined_tables.insert(dst);
                            *filter_nodes[dst].last().unwrap()
                        } else if joined_tables.contains(dst) {
                            joined_tables.insert(src);
                            *filter_nodes[src].last().unwrap()
                        } else {
                            // We have already handled *both* tables that are part of the join.
                            // This should never occur, because their join predicates must be
                            // associated with the same query graph edge.
                            unreachable!();
                        };
                        let ni = self.make_join_node(&format!("q_{:x}_n{}", qg.signature().hash, i),
                                            jps,
                                            left_ni,
                                            right_ni,
                                            mig);
                        join_nodes.push(ni);
                        i += 1;
                        prev_ni = Some(ni);
                    }
                    // Edge represents a GROUP BY, which we handle later
                    QueryGraphEdge::GroupBy(_) => (),
                }
            }
            let mut func_nodes = Vec::new();
            match qg.relations.get("computed_columns") {
                None => (),
                Some(computed_cols_cgn) => {
                    // Function columns with GROUP BY clause
                    for e in qg.edges.values() {
                        match *e {
                            QueryGraphEdge::Join(_) => (),
                            QueryGraphEdge::GroupBy(ref gb_cols) => {
                                // Generate the right function nodes for all relevant columns in
                                // the "computed_columns" node
                                // TODO(malte): I think we don't need to record the group columns
                                // with the function since there can only be one GROUP BY in each
                                // query, but I should verify this.
                                // TODO(malte): what about computed columns without a GROUP BY?
                                // XXX(malte): ensure that the GROUP BY columns are all on the same
                                // (and correct) table assert!(computed_cols_cgn.columns.all());
                                for fn_col in &computed_cols_cgn.columns {
                                    let ni = self.make_function_node(&format!("q_{:x}_n{}",
                                                                              qg.signature().hash,
                                                                              i),
                                                                     fn_col,
                                                                     gb_cols,
                                                                     mig);
                                    func_nodes.push(ni);
                                    i += 1;
                                }
                            }
                        }
                    }
                    // TODO: Function columns without GROUP BY
                    for fn_col in computed_cols_cgn.columns
                        .iter()
                        .filter(|c| match c.function {
                            Some(FunctionExpression::Count(FieldExpression::All)) => true,
                            _ => false,
                        })
                        .collect::<Vec<_>>() {
                        let ni =
                            self.make_function_node(&format!("q_{:x}_n{}", qg.signature().hash, i),
                                                    fn_col,
                                                    &[],
                                                    mig);
                        func_nodes.push(ni);
                        i += 1;
                    }

                }
            }

            // 3. Generate leaf views that expose the query result
            let ni;
            {
                let final_ni = if !join_nodes.is_empty() {
                    join_nodes.last().unwrap()
                } else if !func_nodes.is_empty() {
                    // XXX(malte): This won't work if (a) there are multiple function nodes in the
                    // query, or (b) computed columns are used within JOIN clauses
                    func_nodes.last().unwrap()
                } else {
                    assert!(filter_nodes.len() == 1);
                    let filter = filter_nodes.iter().next().as_ref().unwrap().1;
                    assert_ne!(filter.len(), 0);
                    filter.last().unwrap()
                };
                let projected_columns: Vec<Column> = sorted_rels.iter()
                    .fold(Vec::new(), |mut v, s| {
                        v.extend(qg.relations[*s].columns.clone().into_iter());
                        v
                    });
                let projected_column_ids: Vec<usize> = projected_columns.iter()
                    .map(|c| self.field_to_columnid(*final_ni, &c.name).unwrap())
                    .collect();
                let fields = projected_columns.iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<String>>();
                ni = mig.add_ingredient(String::from(name),
                                        fields.as_slice(),
                                        Permute::new(*final_ni, projected_column_ids.as_slice()));
                // We always materializes leaves of queries (at least currently)
                // XXX(malte): this hard-codes the primary key to be the first column, since
                // queries do not currently carry this information
                mig.maintain(ni, 0);
                self.node_addresses.insert(String::from(name), ni);
                self.node_fields.insert(ni, fields);
            }
            filter_nodes.insert(String::from(name), vec![ni]);

            // finally, we output all the nodes we generated
            nodes_added = filter_nodes.into_iter()
                .flat_map(|(_, n)| n.into_iter())
                .chain(join_nodes.into_iter())
                .chain(func_nodes.into_iter())
                .collect();
        }
        (qg, nodes_added)
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
                     -> Result<(String, Vec<NodeAddress>), String>;
}

impl<'a> ToFlowParts for &'a String {
    fn to_flow_parts(&self,
                     inc: &mut SqlIncorporator,
                     name: Option<String>,
                     mig: &mut Migration)
                     -> Result<(String, Vec<NodeAddress>), String> {
        self.as_str().to_flow_parts(inc, name, mig)
    }
}

impl<'a> ToFlowParts for &'a str {
    fn to_flow_parts(&self,
                     inc: &mut SqlIncorporator,
                     name: Option<String>,
                     mig: &mut Migration)
                     -> Result<(String, Vec<NodeAddress>), String> {
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

    /// Helper to grab a reference to a named view.
    fn get_node<'a>(inc: &SqlIncorporator, mig: &'a Migration, name: &str) -> &'a Node {
        let na = inc.address_for(name);
        mig.graph().node_weight(na.as_global().clone()).unwrap()
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
        let mut g = Blender::new();
        let mut inc = SqlIncorporator::default();
        let mut mig = g.start_migration();

        // Must have a base node for type inference to work, so make one manually
        assert!("INSERT INTO users (id, name) VALUES (?, ?);"
            .to_flow_parts(&mut inc, None, &mut mig)
            .is_ok());

        // Should have two nodes: source and "users" base table
        assert_eq!(mig.graph().node_count(), 2);
        assert_eq!(get_node(&inc, &mig, "users").name(), "users");

        assert!("SELECT users.id from users;".to_flow_parts(&mut inc, None, &mut mig).is_ok());
        // Should now have source, "users", two nodes for the new selection: one filter node
        // and one edge view node, and a reader node.
        assert_eq!(mig.graph().node_count(), 5);

        // Invalid query should fail parsing and add no nodes
        assert!("foo bar from whatever;".to_flow_parts(&mut inc, None, &mut mig).is_err());
        // Should still only have source, "users" and the two nodes for the above selection
        assert_eq!(mig.graph().node_count(), 5);
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
                                &[&Column::from("articles.author"), &Column::from("users.id")]);
        // permute node 1 (for articles)
        let new_view1 = get_node(&inc, &mig, &format!("q_{:x}_n0", qid));
        assert_eq!(new_view1.fields(), &["title", "author"]);
        assert_eq!(new_view1.description(), format!("π[2, 1]"));
        // permute node 2 (for users)
        let new_view2 = get_node(&inc, &mig, &format!("q_{:x}_n1", qid));
        assert_eq!(new_view2.fields(), &["name", "id"]);
        assert_eq!(new_view2.description(), format!("π[1, 0]"));
        // join node
        let new_view3 = get_node(&inc, &mig, &format!("q_{:x}_n2", qid));
        assert_eq!(new_view3.fields(), &["title", "author", "name", "id"]);
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

        let qid = query_id_hash(&["users"], &[&Column::from("users.id")]);
        // filter node
        let filter = get_node(&inc, &mig, &format!("q_{:x}_n0_f0", qid));
        assert_eq!(filter.fields(), &["id", "name"]);
        assert_eq!(filter.description(), format!("σ[0=\"42\"]"));
        // projection node
        let project = get_node(&inc, &mig, &format!("q_{:x}_n0", qid));
        assert_eq!(project.fields(), &["name"]);
        assert_eq!(project.description(), format!("π[1]"));
        // edge node
        let edge = get_node(&inc, &mig, &res.unwrap().0);
        assert_eq!(edge.fields(), &["name"]);
        assert_eq!(edge.description(), format!("π[0]"));
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
        let res =
            inc.add_query("SELECT COUNT(votes.userid) AS votes FROM votes GROUP BY votes.aid;",
                          None,
                          &mut mig);
        assert!(res.is_ok());
        // added the aggregation and the edge view, and a reader
        assert_eq!(mig.graph().node_count(), 5);
        // check aggregation view
        let qid = query_id_hash(&["computed_columns", "votes"],
                                &[&Column::from("votes.aid")]);
        let agg_view = get_node(&inc, &mig, &format!("q_{:x}_n2", qid));
        assert_eq!(agg_view.fields(), &["aid", "votes"]);
        assert_eq!(agg_view.description(), format!("|*| γ[0]"));
        // check edge view
        let edge_view = get_node(&inc, &mig, &res.unwrap().0);
        assert_eq!(edge_view.fields(), &["votes"]);
        assert_eq!(edge_view.description(), format!("π[1]"));
    }

    #[test]
    // Activate when we have support for COUNT(*)
    #[ignore]
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
        let res = inc.add_query("SELECT COUNT(*) AS count FROM votes;", None, &mut mig);
        assert!(res.is_ok());
        // added the aggregation and the edge view, and reader
        assert_eq!(mig.graph().node_count(), 5);
        // check aggregation view
        let qid = query_id_hash(&["computed_columns", "votes"], &[]);
        let agg_view = get_node(&inc, &mig, &format!("q_{:x}_n2", qid));
        assert_eq!(agg_view.fields(), &["count"]);
        assert_eq!(agg_view.description(), format!("|*| γ[1]"));
        // check edge view
        let edge_view = get_node(&inc, &mig, &res.unwrap().0);
        assert_eq!(edge_view.fields(), &["count"]);
        assert_eq!(edge_view.description(), format!("π[0]"));
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
            if let Ok((name, _)) = inc.add_query(q, None, &mut mig) {
                println!("{}: {} -- {}\n", name, i, q);
            } else {
                println!("Failed to parse: {}\n", q);
            };
            // println!("{}", inc.graph);
        }
    }
}
