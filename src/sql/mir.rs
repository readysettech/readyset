use flow::core::{NodeAddress, DataType};
use mir::{GroupedNodeType, MirNode, MirNodeType};
// TODO(malte): remove if possible
pub use mir::{FlowNode, MirNodeRef, MirQuery};
use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator, TableKey,
              SqlQuery};
use nom_sql::{SelectStatement, LimitClause, OrderClause};
use sql::query_graph::{QueryGraph, QueryGraphEdge};

use slog;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::rc::Rc;
use std::vec::Vec;

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

#[derive(Clone, Debug)]
pub struct SqlToMirConverter {
    base_schemas: HashMap<String, Vec<(usize, Vec<Column>)>>,
    nodes: HashMap<(String, usize), MirNodeRef>,
    log: slog::Logger,
    schema_version: usize,
}

impl Default for SqlToMirConverter {
    fn default() -> Self {
        SqlToMirConverter {
            base_schemas: HashMap::default(),
            nodes: HashMap::default(),
            log: slog::Logger::root(slog::Discard, None),
            schema_version: 0,
        }
    }
}

impl SqlToMirConverter {
    pub fn with_logger(log: slog::Logger) -> Self {
        SqlToMirConverter { log: log, ..Default::default() }
    }

    /// Converts a condition tree stored in the `ConditionExpr` returned by the SQL parser into a
    /// vector of conditions that `shortcut` understands.
    fn to_conditions(&self,
                     ct: &ConditionTree,
                     n: &MirNodeRef)
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
        let num_columns = n.borrow().columns().len();
        let mut filter = vec![None; num_columns];
        filter[n.borrow()
            .columns()
            .iter()
            .position(|c| *c == l)
            .unwrap()] = Some((ct.operator.clone(), DataType::from(r)));
        filter
    }

    pub fn get_flow_node_address(&self, name: &str, version: usize) -> Option<NodeAddress> {
        match self.nodes.get(&(name.to_string(), version)) {
            None => panic!(format!("node ({}, {}) not found!", name, version)),
            Some(ref node) => {
                match node.borrow().flow_node {
                    None => panic!(format!("no flow node on ({}, {})", name, version)),
                    Some(ref flow_node) => Some(flow_node.address()),
                }
            }
        }
    }

    pub fn named_base_to_mir(&mut self, name: &str, query: &SqlQuery) -> MirQuery {
        match *query {
            SqlQuery::CreateTable(ref ctq) => {
                assert_eq!(name, ctq.table.name);
                let n = self.make_base_node(&name, &ctq.fields, ctq.keys.as_ref());
                let rcn = Rc::new(RefCell::new(n));
                self.nodes.insert((String::from(name), self.schema_version), rcn.clone());
                MirQuery::singleton(name, rcn)
            }
            SqlQuery::Insert(ref iq) => {
                assert_eq!(name, iq.table.name);
                let (cols, _): (Vec<Column>, Vec<String>) = iq.fields
                    .iter()
                    .cloned()
                    .unzip();
                let n = self.make_base_node(&name, &cols, None);
                let rcn = Rc::new(RefCell::new(n));
                self.nodes.insert((String::from(name), self.schema_version), rcn.clone());
                MirQuery::singleton(name, rcn)
            }
            _ => panic!("expected base-yielding query!"),
        }
    }

    pub fn named_query_to_mir(&mut self,
                              name: &str,
                              sq: &SelectStatement,
                              qg: &QueryGraph)
                              -> MirQuery {
        let nodes = self.make_nodes_for_selection(&name, sq, qg);
        let mut roots = Vec::new();
        let mut leaves = Vec::new();
        for mn in nodes {
            self.nodes.insert((String::from(mn.borrow().name()), self.schema_version),
                              mn.clone());
            trace!(self.log,
                   "Added MIR node ({}, v{}): {:?}",
                   mn.borrow().name(),
                   self.schema_version,
                   mn);
            if mn.borrow().ancestors().len() == 0 {
                // root
                roots.push(mn.clone());
            }
            if mn.borrow().children().len() == 0 {
                // leaf
                leaves.push(mn);
            }
        }
        assert_eq!(leaves.len(), 1);

        MirQuery {
            name: String::from(name),
            roots: roots,
            leaf: leaves.into_iter().next().unwrap(),
        }
    }

    pub fn upgrade_schema(&mut self, new_version: usize) {
        assert!(new_version > self.schema_version);
        self.schema_version = new_version;
    }

    fn make_base_node(&mut self,
                      name: &str,
                      cols: &Vec<Column>,
                      keys: Option<&Vec<TableKey>>)
                      -> MirNode {
        // have we seen a base of this name before?
        if self.base_schemas.contains_key(name) {
            let mut existing_schemas: Vec<(usize, Vec<Column>)> = self.base_schemas[name].clone();
            existing_schemas.sort_by_key(|&(sv, _)| sv);

            for (existing_sv, ref schema) in existing_schemas {
                // TODO(malte): check the keys too
                if schema == cols {
                    // exact match, so reuse the existing base node
                    info!(self.log,
                          "base table for {} already exists with identical schema in version {}; reusing it.",
                          name,
                          existing_sv);
                    return MirNode::reuse(self.nodes[&(String::from(name), existing_sv)].clone(),
                                          self.schema_version);
                } else {
                    // match, but schema is different, so we'll need to either:
                    //  1) reuse the existing node, but add an upgrader for any changes in the column
                    //     set, or
                    //  2) give up and just make a new node
                    error!(self.log,
                           "base table for {} already exists in version {}, but has a different schema!",
                           name,
                           existing_sv);
                }
            }
        }

        // all columns on a base must have the base as their table
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
        // TODO(malte): support >1 pkey
        assert!(primary_keys.len() <= 1);

        // remember the schema for this version
        let mut base_schemas = self.base_schemas.entry(String::from(name)).or_insert(Vec::new());
        base_schemas.push((self.schema_version, cols.clone()));

        // make node
        if !primary_keys.is_empty() {
            match **primary_keys.iter().next().unwrap() {
                TableKey::PrimaryKey(ref key_cols) => {
                    debug!(self.log,
                           "Assigning primary key ({}) for base {}",
                           key_cols.iter()
                               .map(|c| c.name.as_str())
                               .collect::<Vec<_>>()
                               .join(", "),
                           name);
                    MirNode::new(name,
                                 self.schema_version,
                                 cols.clone(),
                                 MirNodeType::Base { keys: key_cols.clone() },
                                 vec![],
                                 vec![])
                }
                _ => unreachable!(),
            }
        } else {
            MirNode::new(name,
                         self.schema_version,
                         cols.clone(),
                         MirNodeType::Base { keys: vec![] },
                         vec![],
                         vec![])
        }
    }

    fn make_filter_nodes(&mut self,
                         name: &str,
                         parent: MirNodeRef,
                         predicates: &Vec<ConditionTree>)
                         -> Vec<MirNodeRef> {
        let mut new_nodes = vec![];

        let mut prev_node = parent;
        for (i, cond) in predicates.iter().enumerate() {
            // convert ConditionTree to a chain of Filter operators.
            // TODO(malte): this doesn't handle OR or AND correctly: needs a nested loop
            let filter = self.to_conditions(cond, &prev_node);
            let parent_fields = prev_node.borrow()
                .columns()
                .iter()
                .cloned()
                .collect();
            let f_name = format!("{}_f{}", name, i);
            let n = MirNode::new(&f_name,
                                 self.schema_version,
                                 parent_fields,
                                 MirNodeType::Filter { conditions: filter },
                                 vec![prev_node.clone()],
                                 vec![]);
            let rcn = Rc::new(RefCell::new(n));
            prev_node.borrow_mut().add_child(rcn.clone());
            new_nodes.push(rcn.clone());
            prev_node = rcn;
        }

        new_nodes
    }


    fn make_function_node(&mut self,
                          name: &str,
                          func_col: &Column,
                          group_cols: Vec<&Column>,
                          parent: MirNodeRef)
                          -> MirNodeRef {
        use ops::grouped::aggregate::Aggregation;
        use ops::grouped::extremum::Extremum;
        use nom_sql::FunctionExpression::*;
        use nom_sql::FieldExpression::*;

        let mknode = |cols: &Vec<Column>, t: GroupedNodeType| {
            // No support for multi-columns functions at this point
            assert_eq!(cols.len(), 1);

            let over = cols.iter().next().unwrap();

            self.make_grouped_node(name, &func_col, (parent, &over), group_cols, t)
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

    fn make_grouped_node(&mut self,
                         name: &str,
                         computed_col: &Column,
                         over: (MirNodeRef, &Column),
                         group_by: Vec<&Column>,
                         node_type: GroupedNodeType)
                         -> MirNodeRef {
        let parent_node = over.0;

        // Resolve column IDs in parent
        let over_col = over.1;
        /*let group_col = group_by.iter()
            .map(|c| self.field_to_columnid(parent_ni, &c.name).unwrap())
            .collect::<Vec<_>>();*/

        // The function node's set of output columns is the group columns plus the function
        // column
        let mut combined_columns = Vec::from_iter(group_by.iter().map(|c| match c.alias {
                                                                          Some(ref a) => {
                                                       Column {
                                                           name: a.clone(),
                                                           alias: Some(a.clone()),
                                                           table: c.table.clone(),
                                                           function: c.function.clone(),
                                                       }
                                                   }
                                                                          None => (*c).clone(),
                                                                      }));
        combined_columns.push(computed_col.clone());

        // make the new operator
        match node_type {
            GroupedNodeType::Aggregation(agg) => {
                let n = MirNode::new(name,
                                     self.schema_version,
                                     combined_columns,
                                     MirNodeType::Aggregation {
                                         on: over_col.clone(),
                                         group_by: group_by.into_iter().cloned().collect(),
                                         kind: agg,
                                     },
                                     vec![parent_node.clone()],
                                     vec![]);
                let rcn = Rc::new(RefCell::new(n));
                parent_node.borrow_mut().add_child(rcn.clone());
                rcn
            }
            GroupedNodeType::Extremum(extr) => {
                let n = MirNode::new(name,
                                     self.schema_version,
                                     combined_columns,
                                     MirNodeType::Extremum {
                                         on: over_col.clone(),
                                         group_by: group_by.into_iter().cloned().collect(),
                                         kind: extr,
                                     },
                                     vec![parent_node.clone()],
                                     vec![]);
                let rcn = Rc::new(RefCell::new(n));
                parent_node.borrow_mut().add_child(rcn.clone());
                rcn
            }
            GroupedNodeType::GroupConcat(sep) => {
                let n = MirNode::new(name,
                                     self.schema_version,
                                     combined_columns,
                                     MirNodeType::GroupConcat {
                                         on: over_col.clone(),
                                         separator: sep,
                                     },
                                     vec![parent_node.clone()],
                                     vec![]);
                let rcn = Rc::new(RefCell::new(n));
                parent_node.borrow_mut().add_child(rcn.clone());
                rcn
            }
        }
    }

    fn make_join_node(&mut self,
                      name: &str,
                      jps: &[ConditionTree],
                      left_node: MirNodeRef,
                      right_node: MirNodeRef)
                      -> MirNodeRef {
        let projected_cols_left = left_node.borrow()
            .columns()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let projected_cols_right = right_node.borrow()
            .columns()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let fields = projected_cols_left.into_iter()
            .chain(projected_cols_right.into_iter())
            .collect::<Vec<Column>>();

        // join columns need us to generate join group configs for the operator
        // TODO(malte): no multi-level joins yet
        let mut left_join_columns = Vec::new();
        let mut right_join_columns = Vec::new();
        for p in jps.iter() {
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
            left_join_columns.push(l_col);
            right_join_columns.push(r_col);
        }
        assert_eq!(left_join_columns.len(), right_join_columns.len());
        let inner = MirNodeType::Join {
            on_left: left_join_columns,
            on_right: right_join_columns,
            project: fields.clone(),
        };
        let n = MirNode::new(name,
                             self.schema_version,
                             fields,
                             inner,
                             vec![left_node.clone(), right_node.clone()],
                             vec![]);
        let rcn = Rc::new(RefCell::new(n));
        left_node.borrow_mut().add_child(rcn.clone());
        right_node.borrow_mut().add_child(rcn.clone());

        rcn
    }

    fn make_projection_helper(&mut self,
                              name: &str,
                              parent: MirNodeRef,
                              computed_col: &Column)
                              -> MirNodeRef {
        let target_cols = target_columns_from_computed_column(computed_col);
        // TODO(malte): we only support a single column argument at this point
        assert_eq!(target_cols.len(), 1);
        let fn_col = target_cols.last().unwrap();

        self.make_project_node(name,
                               parent,
                               vec![fn_col],
                               vec![(String::from("grp"), DataType::from(0 as i32))])
    }

    fn make_project_node(&mut self,
                         name: &str,
                         parent_node: MirNodeRef,
                         proj_cols: Vec<&Column>,
                         literals: Vec<(String, DataType)>)
                         -> MirNodeRef {
        //assert!(proj_cols.iter().all(|c| c.table == parent_name));

        let literal_names: Vec<String> = literals.iter().map(|&(ref n, _)| n.clone()).collect();
        let fields = proj_cols.clone()
            .into_iter()
            .cloned()
            .chain(literal_names.into_iter().map(|n| {
                Column {
                    name: n,
                    alias: None,
                    table: Some(String::from(name)),
                    function: None,
                }
            }))
            .collect();

        let n = MirNode::new(name,
                             self.schema_version,
                             fields,
                             MirNodeType::Project {
                                 emit: proj_cols.into_iter().cloned().collect(),
                                 literals: literals,
                             },
                             vec![parent_node.clone()],
                             vec![]);
        let rcn = Rc::new(RefCell::new(n));
        parent_node.borrow_mut().add_child(rcn.clone());
        rcn
    }

    fn make_topk_node(&mut self,
                      name: &str,
                      parent: MirNodeRef,
                      group_by: Vec<&Column>,
                      order: &Option<OrderClause>,
                      limit: &LimitClause)
                      -> MirNodeRef {
        let combined_columns = parent.borrow()
            .columns()
            .iter()
            .cloned()
            .collect();

        let order = match *order {
            Some(ref o) => Some(o.columns.clone()),
            None => None,
        };

        assert_eq!(limit.offset, 0); // Non-zero offset not supported

        // make the new operator and record its metadata
        let node = MirNode::new(name,
                                self.schema_version,
                                combined_columns,
                                MirNodeType::TopK {
                                    order: order,
                                    group_by: group_by.into_iter().cloned().collect(),
                                    k: limit.limit as usize,
                                    offset: 0,
                                },
                                vec![parent.clone()],
                                vec![]);

        let rcn = Rc::new(RefCell::new(node));
        parent.borrow_mut().add_child(rcn.clone());

        rcn
    }

    /// Returns list of nodes added
    fn make_nodes_for_selection(&mut self,
                                name: &str,
                                st: &SelectStatement,
                                qg: &QueryGraph)
                                -> Vec<MirNodeRef> {
        use std::collections::HashMap;

        let leaf_node: MirNodeRef;
        let mut nodes_added: Vec<MirNodeRef>;
        let mut new_node_count = 0;

        // Canonical operator order: B-J-G-F-P-R
        // (Base, Join, GroupBy, Filter, Project, Reader)
        {
            // 0. Base nodes (always reused)
            let mut base_nodes: HashMap<&str, MirNodeRef> = HashMap::default();
            let mut sorted_rels: Vec<&str> = qg.relations
                .keys()
                .map(String::as_str)
                .collect();
            sorted_rels.sort();
            for rel in &sorted_rels {
                // the node holding computed columns doesn't have a base
                if *rel == "computed_columns" {
                    continue;
                }

                let existing = self.nodes.get(&(String::from(*rel), self.schema_version));
                let base_for_rel = match existing {
                    None => {
                        panic!("Query \"{}\" refers to unknown base \"{}\" node at v{}",
                               name,
                               rel,
                               self.schema_version)
                    }
                    Some(bmn) => MirNode::reuse(bmn.clone(), self.schema_version),
                };
                base_nodes.insert(*rel, Rc::new(RefCell::new(base_for_rel)));
            }

            // 1. Generate join nodes for the query. This starts out by joining two of the base
            //    nodes corresponding to relations in the first join predicate, and then continues
            //    to join the result against previously unseen tables from the remaining
            //    predicates. Note that no (src, dst) pair ever occurs twice, since we've already
            //    previously moved all predicates pertaining to src/dst joins onto a single edge.
            let mut join_nodes: Vec<MirNodeRef> = Vec::new();
            let mut joined_tables = HashSet::new();
            let mut sorted_edges: Vec<(&(String, String), &QueryGraphEdge)> =
                qg.edges.iter().collect();
            sorted_edges.sort_by_key(|k| &(k.0).0);
            let mut prev_node = None;
            for &(&(ref src, ref dst), edge) in &sorted_edges {
                match *edge {
                    // Edge represents a LEFT JOIN
                    QueryGraphEdge::LeftJoin(_) => unimplemented!(),
                    // Edge represents a JOIN
                    QueryGraphEdge::Join(ref jps) => {
                        let left_node;
                        let right_node;

                        if joined_tables.contains(src) && joined_tables.contains(dst) {
                            // We have already handled *both* tables that are part of the join.
                            // This should never occur, because their join predicates must be
                            // associated with the same query graph edge.
                            unreachable!();
                        } else if joined_tables.contains(src) {
                            // join left against previous join, right against base
                            left_node = prev_node.unwrap();
                            right_node = base_nodes[dst.as_str()].clone();
                        } else if joined_tables.contains(dst) {
                            // join right against previous join, left against base
                            left_node = base_nodes[src.as_str()].clone();
                            right_node = prev_node.unwrap();
                        } else {
                            // We've seen neither of these tables before
                            // If we already have a join in prev_ni, we must assume that some
                            // future join will bring these unrelated join arms together.
                            // TODO(malte): make that actually work out...
                            left_node = base_nodes[src.as_str()].clone();
                            right_node = base_nodes[dst.as_str()].clone();
                        };
                        // make node
                        let jn = self.make_join_node(&format!("q_{:x}_n{}",
                                                              qg.signature().hash,
                                                              new_node_count),
                                                     jps,
                                                     left_node,
                                                     right_node);
                        join_nodes.push(jn.clone());
                        new_node_count += 1;
                        prev_node = Some(jn);

                        // we've now joined both tables
                        joined_tables.insert(src);
                        joined_tables.insert(dst);
                    }
                    // Edge represents a GROUP BY, which we handle later
                    QueryGraphEdge::GroupBy(_) => (),
                }
            }

            // 3. Grouped and function nodes
            let mut func_nodes: Vec<MirNodeRef> = Vec::new();
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
                                let table = gb_cols.iter()
                                    .next()
                                    .unwrap()
                                    .table
                                    .as_ref()
                                    .unwrap();
                                assert!(gb_cols.iter().all(|c| c.table.as_ref().unwrap() == table));

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
                                    let gb_and_param_cols: Vec<_> =
                                        gb_cols.iter().chain(param_cols.into_iter()).collect();


                                    let parent_node = match prev_node {
                                        // If no explicit parent node is specified, we extract
                                        // the base node from the "over" column's specification
                                        None => base_nodes[over_table].clone(),
                                        // We have an explicit parent node (likely a projection helper), so use that
                                        Some(node) => node,
                                    };

                                    let n = self.make_function_node(&format!("q_{:x}_n{}",
                                                                             qg.signature().hash,
                                                                             new_node_count),
                                                                    fn_col,
                                                                    gb_and_param_cols,
                                                                    parent_node);
                                    prev_node = Some(n.clone());
                                    func_nodes.push(n);
                                    grouped_fn_columns.insert(fn_col);
                                    new_node_count += 1;
                                }
                            }
                        }
                    }
                    // Function columns without GROUP BY
                    for computed_col in computed_cols_cgn.columns
                            .iter()
                            .filter(|c| !grouped_fn_columns.contains(c))
                            .collect::<Vec<_>>() {

                        let agg_node_name =
                            &format!("q_{:x}_n{}", qg.signature().hash, new_node_count);

                        let over_cols = target_columns_from_computed_column(computed_col);
                        let over_table = over_cols.iter()
                            .next()
                            .unwrap()
                            .table
                            .as_ref()
                            .unwrap()
                            .as_str();

                        let ref proj_cols_from_target_table = qg.relations
                            .get(over_table)
                            .as_ref()
                            .unwrap()
                            .columns;

                        let parent_node = match prev_node {
                            Some(ref node) => node.clone(),
                            None => base_nodes[over_table].clone(),
                        };

                        let (group_cols, parent_node) = if proj_cols_from_target_table.is_empty() {
                            // slightly messy hack: if there are no group columns and the table on
                            // which we compute has no projected columns in the output, we make one
                            // up a group column by adding an extra projection node
                            let proj_name = format!("{}_prj_hlpr", agg_node_name);
                            let proj =
                                self.make_projection_helper(&proj_name, parent_node, computed_col);

                            func_nodes.push(proj.clone());
                            new_node_count += 1;

                            let bogo_group_col = Column::from(format!("{}.grp", proj_name)
                                                                  .as_str());
                            (vec![bogo_group_col], proj)
                        } else {
                            (proj_cols_from_target_table.clone(), parent_node)
                        };
                        let n = self.make_function_node(agg_node_name,
                                                        computed_col,
                                                        group_cols.iter().collect(),
                                                        parent_node);
                        func_nodes.push(n);
                        new_node_count += 1;
                    }
                }
            }

            // 3. Generate the necessary filter node for each relation node in the query graph.
            let mut filter_nodes = Vec::new();
            // Need to iterate over relations in a deterministic order, as otherwise nodes will be
            // added in a different order every time, which will yield different node identifiers
            // and make it difficult for applications to check what's going on.
            let mut sorted_rels: Vec<&String> = qg.relations.keys().collect();
            sorted_rels.sort();
            for rel in &sorted_rels {
                let qgn = &qg.relations[*rel];
                // we've already handled computed columns
                if *rel != "computed_columns" {
                    // the following conditional is required to avoid "empty" nodes (without any
                    // projected columns) that are required as inputs to joins
                    if !qgn.predicates.is_empty() {
                        // add a filter chain for each query graph node's predicates
                        let parent = match prev_node {
                            None => base_nodes[rel.as_str()].clone(),
                            Some(pn) => pn,
                        };
                        let fns = self.make_filter_nodes(&format!("q_{:x}_n{}",
                                                                  qg.signature().hash,
                                                                  new_node_count),
                                                         parent,
                                                         &qgn.predicates);
                        assert!(fns.len() > 0);
                        new_node_count += fns.len();
                        prev_node = Some(fns.iter()
                                             .last()
                                             .unwrap()
                                             .clone());
                        filter_nodes.extend(fns);
                    }
                }
            }

            // 4. Get the final node
            let mut final_node: MirNodeRef = if !filter_nodes.is_empty() {
                filter_nodes.last().unwrap().clone()
            } else if !func_nodes.is_empty() {
                // TODO(malte): This won't work if computed columns are used within JOIN clauses
                func_nodes.last().unwrap().clone()
            } else if !join_nodes.is_empty() {
                join_nodes.last().unwrap().clone()
            } else {
                // no join, filter, or function node --> base node is parent
                assert_eq!(sorted_rels.len(), 1);
                base_nodes[sorted_rels.last().unwrap().as_str()].clone()
            };

            // 4. Potentially insert TopK node below the final node
            if let Some(ref limit) = st.limit {
                let group_by = qg.parameters();

                let node = self.make_topk_node(&format!("q_{:x}_n{}",
                                                        qg.signature().hash,
                                                        new_node_count),
                                               final_node,
                                               group_by,
                                               &st.order,
                                               limit);
                func_nodes.push(node.clone());
                final_node = node;
                new_node_count += 1;
            }

            // should have counted all nodes added, except for the base nodes (which reuse)
            debug_assert_eq!(new_node_count,
                             join_nodes.len() + func_nodes.len() + filter_nodes.len());
            // we're now done with the query, so remember all the nodes we've added so far
            nodes_added = base_nodes.into_iter()
                .map(|(_, n)| n)
                .chain(join_nodes.into_iter())
                .chain(func_nodes.into_iter())
                .chain(filter_nodes.into_iter())
                .collect();

            // 5. Generate leaf views that expose the query result
            let projected_columns: Vec<Column> = sorted_rels.iter().fold(Vec::new(), |mut v, s| {
                v.extend(qg.relations[*s].columns.clone().into_iter());
                v
            });

            // translate aliases on leaf columns only
            let fields = projected_columns.iter()
                .map(|c| match c.alias {
                         Some(ref a) => {
                             Column {
                                 name: a.clone(),
                                 table: c.table.clone(),
                                 alias: Some(a.clone()),
                                 function: c.function.clone(),
                             }
                         }
                         None => c.clone(),
                     })
                .collect::<Vec<Column>>();
            let leaf_project_node =
                self.make_project_node(&format!("q_{:x}_n{}", qg.signature().hash, new_node_count),
                                       final_node,
                                       fields.iter().collect(),
                                       vec![]);
            nodes_added.push(leaf_project_node.clone());

            // We always materialize leaves of queries (at least currently), so add a
            // `MaterializedLeaf` node keyed on the query parameters.
            let query_params = qg.parameters();
            let ln = MirNode::new(name,
                                  self.schema_version,
                                  leaf_project_node.borrow()
                                      .columns()
                                      .iter()
                                      .cloned()
                                      .collect(),
                                  MirNodeType::Leaf {
                                      node: leaf_project_node.clone(),
                                      keys: query_params.into_iter().cloned().collect(),
                                  },
                                  vec![leaf_project_node.clone()],
                                  vec![]);
            leaf_node = Rc::new(RefCell::new(ln));
            leaf_project_node.borrow_mut().add_child(leaf_node.clone());
            nodes_added.push(leaf_node);

            debug!(self.log,
                   format!("Added final MIR node for query named \"{}\"", name));
        }

        // finally, we output all the nodes we generated
        nodes_added
    }
}
