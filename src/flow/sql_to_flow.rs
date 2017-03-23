use nom_sql::parser as sql_parser;
use flow::{NodeAddress, Migration};
use flow::sql::query_graph::{QueryGraph, QueryGraphEdge, QueryGraphNode, to_query_graph};
use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator, TableKey,
              SqlQuery};
use nom_sql::SelectStatement;
use ops;
use ops::base::Base;
use ops::identity::Identity;
use ops::join::Builder as JoinBuilder;
use ops::permute::Permute;
use flow::data::DataType;

use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
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
#[derive(Clone, Debug, PartialEq)]
pub struct SqlIncorporator {
    write_schemas: HashMap<String, Vec<String>>,
    node_addresses: HashMap<String, NodeAddress>,
    node_fields: HashMap<NodeAddress, Vec<String>>,
    query_graphs: Vec<(QueryGraph, NodeAddress)>,
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
        // TODO(malte): support other types of operators
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
        use flow::sql::passes::alias_removal::AliasRemoval;
        use flow::sql::passes::count_star_rewrite::CountStarRewrite;
        use flow::sql::passes::implied_tables::ImpliedTableExpansion;
        use flow::sql::passes::star_expansion::StarExpansion;

        // first run some standard rewrite passes on the query. This makes the later work easier,
        // as we no longer have to consider complications like aliases.
        let q = q.expand_table_aliases()
            .expand_stars(&self.write_schemas)
            .expand_implied_tables(&self.write_schemas)
            .rewrite_count_star(&self.write_schemas);

        let (name, new_nodes, leaf) = match q {
            SqlQuery::CreateTable(ctq) => {
                //assert_eq!(query_name, ctq.table.name);
                let (na, new) =
                    self.make_base_node(&query_name, &ctq.fields, ctq.keys.as_ref(), &mut mig);
                if new {
                    (query_name, vec![na], na)
                } else {
                    (query_name, vec![], na)
                }
            }
            SqlQuery::Insert(iq) => {
                //assert_eq!(query_name, iq.table.name);
                let (cols, _): (Vec<Column>, Vec<String>) = iq.fields.iter().cloned().unzip();
                let (na, new) = self.make_base_node(&query_name, &cols, None, &mut mig);
                if new {
                    (query_name, vec![na], na)
                } else {
                    (query_name, vec![], na)
                }
            }
            SqlQuery::Select(sq) => {
                let (nodes, leaf) = self.make_nodes_for_selection(&sq, &query_name, &mut mig);
                // Return new nodes
                (query_name, nodes, leaf)
            }
        };

        self.num_queries += 1;

        QueryFlowParts {
            name: name,
            new_nodes: new_nodes,
            reused_nodes: vec![],
            query_leaf: leaf,
        }
    }

    /// Return is (`node`, `is_new`)
    fn make_base_node(&mut self,
                      name: &str,
                      cols: &Vec<Column>,
                      keys: Option<&Vec<TableKey>>,
                      mig: &mut Migration)
                      -> (NodeAddress, bool) {
        if self.write_schemas.contains_key(name) {
            println!("WARNING: base table for write type {} already exists: ignoring query.",
                     name);
            return (self.node_addresses[name], false);
        }

        let fields = Vec::from_iter(cols.iter().map(|c| c.name.clone()));

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

        // make the new base node and record its information
        let na = if !primary_keys.is_empty() {
            match **primary_keys.iter().next().unwrap() {
                TableKey::PrimaryKey(ref key_cols) => {
                    debug!(mig.log,
                           "Assigning primary key {:?} for base {}",
                           key_cols,
                           name);
                    let pkey_column_ids = key_cols.iter()
                        .map(|pkc| {
                            //assert_eq!(pkc.table.as_ref().unwrap(), name);
                            cols.iter().position(|c| c == pkc).unwrap()
                        })
                        .collect();
                    mig.add_ingredient(name, fields.as_slice(), Base::new(pkey_column_ids))
                }
                _ => unreachable!(),
            }
        } else {
            mig.add_ingredient(name, fields.as_slice(), Base::default())
        };
        self.node_addresses.insert(String::from(name), na);
        // TODO(malte): get rid of annoying duplication
        self.node_fields.insert(na, fields.clone());
        self.write_schemas.insert(String::from(name), fields);

        (na, true)
    }

    fn make_grouped_node(&mut self,
                         name: &str,
                         computed_col_name: &str,
                         over: (NodeAddress, usize), // address, column ID
                         group_by: &[Column],
                         node_type: GroupedNodeType,
                         mig: &mut Migration)
                         -> NodeAddress {
        let parent_ni = over.0;

        // Resolve column IDs in parent
        let over_col_indx = over.1;
        let group_col_indx = group_by.iter()
            .map(|c| self.field_to_columnid(parent_ni, &c.name).unwrap())
            .collect::<Vec<_>>();

        // The function node's set of output columns is the group columns plus the function
        // column
        let mut combined_columns = Vec::from_iter(group_by.iter().map(|c| c.name.clone()));
        combined_columns.push(String::from(computed_col_name));

        // make the new operator and record its metadata
        let na = match node_type {
            GroupedNodeType::Aggregation(agg) => {
                mig.add_ingredient(String::from(name),
                                   combined_columns.as_slice(),
                                   agg.over(parent_ni, over_col_indx, group_col_indx.as_slice()))
            }
            GroupedNodeType::Extremum(extr) => {
                mig.add_ingredient(String::from(name),
                                   combined_columns.as_slice(),
                                   extr.over(parent_ni, over_col_indx, group_col_indx.as_slice()))
            }
            GroupedNodeType::GroupConcat(sep) => {
                use ops::grouped::concat::{GroupConcat, TextComponent};

                let gc =
                    GroupConcat::new(parent_ni, vec![TextComponent::Column(over_col_indx)], sep);
                mig.add_ingredient(String::from(name), combined_columns.as_slice(), gc)
            }
        };
        self.node_addresses.insert(String::from(name), na);
        self.node_fields.insert(na, combined_columns);
        na
    }

    fn make_function_node(&mut self,
                          name: &str,
                          func_col: &Column,
                          group_cols: &[Column],
                          parent: Option<NodeAddress>, // XXX(malte): nasty hack for non-grouped funcs
                          mig: &mut Migration)
                          -> NodeAddress {
        use ops::grouped::aggregate::Aggregation;
        use ops::grouped::extremum::Extremum;
        use nom_sql::FunctionExpression::*;
        use nom_sql::FieldExpression::*;

        let mut mknode = |cols: &Vec<Column>, t: GroupedNodeType| {
            // No support for multi-columns functions at this point
            assert_eq!(cols.len(), 1);

            let over = cols.iter().next().unwrap();
            let parent_ni = match parent {
                // If no explicit parent node is specified, we extract the base node from the
                // "over" column's specification
                None => self.address_for(over.table.as_ref().unwrap()),
                // We have an explicit parent node (likely a projection helper), so use that
                Some(ni) => ni,
            };
            let over_col_indx = self.field_to_columnid(parent_ni, &over.name).unwrap();

            let computed_col_name = &func_col.name;
            self.make_grouped_node(name,
                                   computed_col_name,
                                   (parent_ni, over_col_indx),
                                   group_cols,
                                   t,
                                   mig)
        };

        let func = func_col.function.as_ref().unwrap();
        match *func {
            Sum(Seq(ref cols)) => mknode(cols, GroupedNodeType::Aggregation(Aggregation::SUM)),
            Count(Seq(ref cols)) => mknode(cols, GroupedNodeType::Aggregation(Aggregation::COUNT)),
            Count(All) => {
                // XXX(malte): there is no "over" column, but our aggregation operators' API
                // requires one to be specified, so we earlier rewrote it to use the last parent
                // column (see passes/count_star_rewrite.rs). However, this isn't *entirely*
                // faithful to COUNT(*) semantics, because COUNT(*) is supposed to count all
                // rows including those with NULL values, and we don't have a mechanism to do that
                // (but we also don't have a NULL value, so maybe we're okay).
                panic!("COUNT(*) should have been rewritten earlier!")
            }
            Max(Seq(ref cols)) => mknode(cols, GroupedNodeType::Extremum(Extremum::MAX)),
            Min(Seq(ref cols)) => mknode(cols, GroupedNodeType::Extremum(Extremum::MIN)),
            GroupConcat(Seq(ref cols), ref separator) => {
                mknode(cols, GroupedNodeType::GroupConcat(separator.clone()))
            }
            _ => unimplemented!(),
        }
    }

    fn make_projection_helper(&mut self,
                              name: &str,
                              computed_col: &Column,
                              mig: &mut Migration)
                              -> NodeAddress {
        let target_cols = target_columns_from_computed_column(computed_col);
        // TODO(malte): we only support a single column argument at this point
        assert_eq!(target_cols.len(), 1);
        let fn_col = target_cols.last().unwrap();

        self.make_project_node(name,
                               fn_col.table.as_ref().unwrap(),
                               vec![fn_col],
                               vec![("grp", DataType::from(0 as i32))],
                               mig)
    }

    fn make_project_node(&mut self,
                         name: &str,
                         parent_name: &str,
                         proj_cols: Vec<&Column>,
                         literals: Vec<(&str, DataType)>,
                         mig: &mut Migration)
                         -> NodeAddress {
        use ops::project::Project;

        let parent_ni = self.address_for(&parent_name);
        //assert!(proj_cols.iter().all(|c| c.table == parent_name));
        let proj_col_ids: Vec<usize> = proj_cols.iter()
            .map(|c| self.field_to_columnid(parent_ni, &c.name).unwrap())
            .collect();

        let mut col_names: Vec<String> = proj_cols.iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        let (literal_names, literal_values): (Vec<_>, Vec<_>) = literals.iter().cloned().unzip();
        col_names.extend(literal_names.into_iter().map(String::from));

        let n = mig.add_ingredient(String::from(name),
                                   col_names.as_slice(),
                                   Project::new(parent_ni,
                                                proj_col_ids.as_slice(),
                                                Some(literal_values)));
        self.node_addresses.insert(String::from(name), n);
        self.node_fields.insert(n, col_names);
        n
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

            // non-join columns projected are the union of the ancestors' projected columns
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

    /// Return is (`new_nodes`, `leaf_node`).
    fn make_nodes_for_selection(&mut self,
                                st: &SelectStatement,
                                name: &str,
                                mig: &mut Migration)
                                -> (Vec<NodeAddress>, NodeAddress) {
        use std::collections::HashMap;

        debug!(mig.log,
               format!("Making nodes for query named \"{}\"", name));
        trace!(mig.log, format!("Query \"{}\": {:#?}", name, st));

        let qg = match to_query_graph(st) {
            Ok(qg) => qg,
            Err(e) => panic!(e),
        };

        trace!(mig.log, format!("QG for \"{}\": {:#?}", name, qg));

        // Do we already have this exact query or a subset of it?
        // TODO(malte): make this an O(1) lookup by QG signature
        for &(ref existing_qg, leaf) in self.query_graphs.iter() {
            // note that this also checks the *order* in which parameters are specified; a
            // different order means that we cannot simply reuse the existing reader.
            if existing_qg.signature() == qg.signature() &&
               existing_qg.parameters() == qg.parameters() {
                // we already have this exact query, down to the exact same reader key columns
                // in exactly the same order
                info!(mig.log,
                      "An exact match for query \"{}\" already exists, reusing it",
                      name);
                return (vec![], leaf);
            } else if existing_qg.signature() == qg.signature() {
                // QGs are identical, except for parameters (or their order)
                info!(mig.log,
                      "Query \"{}\" has an exact match modulo parameters, so making a new reader",
                      name);

                // we must add a new reader for this query. This also requires adding an
                // identity node (at least currently), since a node can only have a single
                // associated reader.
                // TODO(malte): consider the case when the projected columns need reordering
                let id_fields = Vec::from(self.fields_for(leaf));
                let id_na = mig.add_ingredient(String::from(name),
                                               id_fields.as_slice(),
                                               Identity::new(leaf));
                self.node_addresses.insert(String::from(name), id_na);
                self.node_fields.insert(id_na, id_fields);
                // TODO(malte): this does not yet cover the case when there are multiple query
                // parameters, which compound key support on Reader nodes.
                let query_params = qg.parameters();
                if !query_params.is_empty() {
                    //assert_eq!(query_params.len(), 1);
                    let key_column = query_params.iter().next().unwrap();
                    mig.maintain(id_na,
                                 self.field_to_columnid(id_na, &key_column.name).unwrap());
                } else {
                    // no query parameters, so we index on the first (and often only) column
                    mig.maintain(id_na, 0);
                }
                return (vec![id_na], id_na);
            }
            // queries are different, but one might be a generalization of the other
            if existing_qg.signature().is_generalization_of(&qg.signature()) {
                info!(mig.log,
                      "this QG: {:#?}\ncandidate query graph for reuse: {:#?}",
                      qg,
                      existing_qg);
            }
        }

        let nodes_added;
        let leaf_na;

        let mut i = 0;
        {
            // 1. Generate the necessary filter node for each relation node in the query graph.
            let mut filter_nodes = HashMap::<String, Vec<NodeAddress>>::new();
            let mut new_filter_nodes = Vec::new();
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
                    if !qgn.predicates.is_empty() {
                        // add a basic filter/permute node for each query graph node if it either
                        // has: 1) projected columns; or 2) a filter condition
                        let fns = self.make_filter_and_project_nodes(&format!("q_{:x}_n{}",
                                                                              qg.signature().hash,
                                                                              i),
                                                                     qgn,
                                                                     mig);
                        filter_nodes.insert((*rel).clone(), fns.clone());
                        new_filter_nodes.extend(fns);
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
                    // Edge represents a LEFT JOIN
                    QueryGraphEdge::LeftJoin(ref jps) => unimplemented!(),
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
                    let mut grouped_fn_columns = HashSet::new();
                    for e in qg.edges.values() {
                        match *e {
                            QueryGraphEdge::Join(_) |
                            QueryGraphEdge::LeftJoin(_) => (),
                            QueryGraphEdge::GroupBy(ref gb_cols) => {
                                // Generate the right function nodes for all relevant columns in
                                // the "computed_columns" node
                                // TODO(malte): there can only be one GROUP BY in each query, but
                                // the columns can come from different tables. In that case, we
                                // would need to generate an Agg-Join-Agg sequence for each pair of
                                // tables involved.
                                for fn_col in &computed_cols_cgn.columns {
                                    // we must also push parameter columns through the group by
                                    let over_cols = target_columns_from_computed_column(fn_col);
                                    // TODO(malte): we only support a single `over` column here
                                    assert_eq!(over_cols.len(), 1);
                                    let over_table = over_cols.iter()
                                        .next()
                                        .unwrap()
                                        .table
                                        .as_ref()
                                        .unwrap()
                                        .as_str();
                                    // get any parameter columns that aren't also in the group-by
                                    // column set
                                    let param_cols: Vec<_> = qg.relations
                                        .get(over_table)
                                        .as_ref()
                                        .unwrap()
                                        .parameters
                                        .iter()
                                        .filter(|ref c| !gb_cols.contains(c))
                                        .collect();
                                    // combine
                                    let gb_and_param_cols: Vec<_> = gb_cols.iter()
                                        .chain(param_cols.into_iter())
                                        .cloned()
                                        .collect();
                                    let ni = self.make_function_node(&format!("q_{:x}_n{}",
                                                                              qg.signature().hash,
                                                                              i),
                                                                     fn_col,
                                                                     gb_and_param_cols.as_slice(),
                                                                     None,
                                                                     mig);
                                    func_nodes.push(ni);
                                    grouped_fn_columns.insert(fn_col);
                                    i += 1;
                                }
                            }
                        }
                    }
                    // Function columns without GROUP BY
                    for computed_col in computed_cols_cgn.columns
                        .iter()
                        .filter(|c| !grouped_fn_columns.contains(c))
                        .collect::<Vec<_>>() {

                        let agg_node_name = &format!("q_{:x}_n{}", qg.signature().hash, i);

                        let over_cols = target_columns_from_computed_column(computed_col);
                        let ref proj_cols_from_target_table = qg.relations
                            .get(over_cols.iter().next().as_ref().unwrap().table.as_ref().unwrap())
                            .as_ref()
                            .unwrap()
                            .columns;
                        let (group_cols, parent_ni) = if proj_cols_from_target_table.is_empty() {
                            // slightly messy hack: if there are no group columns and the table on
                            // which we compute has no projected columns in the output, we make one
                            // up a group column by adding an extra projection node
                            let proj_name = format!("{}_prj_hlpr", agg_node_name);
                            let proj = self.make_projection_helper(&proj_name, computed_col, mig);
                            func_nodes.push(proj);

                            let bogo_group_col = Column::from(format!("{}.grp", proj_name)
                                .as_str());
                            (vec![bogo_group_col], Some(proj))
                        } else {
                            (proj_cols_from_target_table.clone(), None)
                        };
                        let ni = self.make_function_node(agg_node_name,
                                                         computed_col,
                                                         group_cols.as_slice(),
                                                         parent_ni,
                                                         mig);
                        func_nodes.push(ni);
                        i += 1;
                    }
                }
            }

            // 3. Generate leaf views that expose the query result
            {
                let final_na = if !func_nodes.is_empty() {
                    // XXX(malte): This won't work if (a) there are multiple function nodes in the
                    // query, or (b) computed columns are used within JOIN clauses
                    assert!(func_nodes.len() <= 2);
                    *func_nodes.last().unwrap()
                } else if !join_nodes.is_empty() {
                    *join_nodes.last().unwrap()
                } else if !filter_nodes.is_empty() {
                    assert_eq!(filter_nodes.len(), 1);
                    let filter = filter_nodes.iter().next().as_ref().unwrap().1;
                    assert_ne!(filter.len(), 0);
                    *filter.last().unwrap()
                } else {
                    // no join, filter, or function node --> base node is parent
                    assert_eq!(sorted_rels.len(), 1);
                    self.address_for(&sorted_rels.last().unwrap())
                };
                let projected_columns: Vec<Column> = sorted_rels.iter()
                    .fold(Vec::new(), |mut v, s| {
                        v.extend(qg.relations[*s].columns.clone().into_iter());
                        v
                    });
                let projected_column_ids: Vec<usize> = projected_columns.iter()
                    .map(|c| self.field_to_columnid(final_na, &c.name).unwrap())
                    .collect();
                let fields = projected_columns.iter()
                    .map(|c| match c.alias {
                        Some(ref a) => a.clone(),
                        None => c.name.clone(),
                    })
                    .collect::<Vec<String>>();
                leaf_na = mig.add_ingredient(String::from(name),
                                             fields.as_slice(),
                                             Permute::new(final_na,
                                                          projected_column_ids.as_slice()));
                self.node_addresses.insert(String::from(name), leaf_na);
                self.node_fields.insert(leaf_na, fields);

                // We always materialize leaves of queries (at least currently)
                let query_params = qg.parameters();
                // TODO(malte): this does not yet cover the case when there are multiple query
                // parameters, which compound key support on Reader nodes.
                if !query_params.is_empty() {
                    //assert_eq!(query_params.len(), 1);
                    let key_column = query_params.iter().next().unwrap();
                    mig.maintain(leaf_na,
                                 self.field_to_columnid(leaf_na, &key_column.name).unwrap());
                } else {
                    // no query parameters, so we index on the first (and often only) column
                    mig.maintain(leaf_na, 0);
                }
            }
            debug!(mig.log, format!("Added final node for query named \"{}\"", name);
                   "node" => leaf_na.as_global().index());
            new_filter_nodes.push(leaf_na);

            // Finally, store the query graph and the corresponding `NodeAddress`
            self.query_graphs.push((qg.clone(), leaf_na));

            // finally, we output all the nodes we generated
            nodes_added = new_filter_nodes.into_iter()
                .chain(join_nodes.into_iter())
                .chain(func_nodes.into_iter())
                .collect();
        }

        (nodes_added, leaf_na)
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
        let new_join_view = get_node(&inc, &mig, &format!("q_{:x}_n2", qid));
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
        let res =
            inc.add_query("SELECT COUNT(votes.userid) AS votes FROM votes GROUP BY votes.aid;",
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
        let agg_view = get_node(&inc, &mig, &format!("q_{:x}_n2", qid));
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
        let qid = query_id_hash(&["computed_columns", "votes"], &[],
                                &[&Column {
                                    name: String::from("count"),
                                    alias: Some(String::from("count")),
                                    table: None,
                                    function: Some(FunctionExpression::Count(
                                            FieldExpression::Seq(
                                                vec![Column::from("votes.userid")]))),
                                }]);
        let proj_helper_view = get_node(&inc, &mig, &format!("q_{:x}_n2_prj_hlpr", qid));
        assert_eq!(proj_helper_view.fields(), &["userid", "grp"]);
        assert_eq!(proj_helper_view.description(), format!("π[1, lit: 0]"));
        // check aggregation view
        let agg_view = get_node(&inc, &mig, &format!("q_{:x}_n2", qid));
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
        let agg_view = get_node(&inc, &mig, &format!("q_{:x}_n2", qid));
        assert_eq!(agg_view.fields(), &["userid", "count"]);
        assert_eq!(agg_view.description(), format!("|*| γ[0]"));
        // check edge view -- note that it's not actually currently possible to read from
        // this for a lack of key (the value would be the key)
        let edge_view = get_node(&inc, &mig, &res.unwrap().name);
        assert_eq!(edge_view.fields(), &["count"]);
        assert_eq!(edge_view.description(), format!("π[1]"));
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
