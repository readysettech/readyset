use flow::core::{NodeAddress, DataType};
use mir::{MirNode, MirNodeType};
// TODO(malte): remove if possible
pub use mir::{FlowNode, MirNodeRef, MirQuery};
use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator, TableKey,
              SqlQuery};
use nom_sql::{SelectStatement, LimitClause, OrderType, OrderClause};
use ops;
use sql::query_graph::{QueryGraph, QueryGraphEdge, QueryGraphNode};

use slog;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::vec::Vec;

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

#[derive(Clone, Debug)]
pub struct SqlToMirConverter {
    base_schemas: HashMap<String, Vec<(usize, Vec<Column>)>>,
    nodes: HashMap<(String, usize), MirNodeRef>,
    node_fields: HashMap<NodeAddress, Vec<String>>,
    log: slog::Logger,
    schema_version: usize,
}

impl Default for SqlToMirConverter {
    fn default() -> Self {
        SqlToMirConverter {
            base_schemas: HashMap::default(),
            nodes: HashMap::default(),
            node_fields: HashMap::default(),
            log: slog::Logger::root(slog::Discard, None),
            schema_version: 0,
        }
    }
}

impl SqlToMirConverter {
    pub fn with_logger(log: slog::Logger) -> Self {
        SqlToMirConverter { log: log, ..Default::default() }
    }

    /// TODO(malte): modify once `SqlToMirConverter` has a better intermediate graph representation.
    fn fields_for(&self, na: NodeAddress) -> &[String] {
        self.node_fields[&na].as_slice()
    }

    /// TODO(malte): modify once `SqlToMirConverter` has a better intermediate graph representation.
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
            self.nodes.insert((String::from(name), self.schema_version), mn.clone());
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
                                 MirNodeType::Base(cols.clone(), key_cols.clone()),
                                 vec![],
                                 vec![])
                }
                _ => unreachable!(),
            }
        } else {
            MirNode::new(name,
                         self.schema_version,
                         cols.clone(),
                         MirNodeType::Base(cols.clone(), vec![]),
                         vec![],
                         vec![])
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
            left_join_columns.push(l_col);
            right_join_columns.push(r_col);
        }
        assert_eq!(left_join_columns.len(), right_join_columns.len());
        let inner = MirNodeType::Join(left_join_columns, right_join_columns, fields.clone());
        let n = MirNode::new(name,
                             self.schema_version,
                             fields,
                             inner,
                             vec![left_node, right_node],
                             vec![]);
        //mig.add_ingredient(String::from(name), fields.as_slice(), j);
        // println!("added join on {:?} and {:?}: {}", left_ni, right_ni, name);
        //self.node_addresses.insert(String::from(name), n);
        //self.node_fields.insert(n, fields);
        Rc::new(RefCell::new(n))
    }

    fn make_nodes_for_selection(&mut self,
                                name: &str,
                                st: &SelectStatement,
                                qg: &QueryGraph)
                                -> Vec<MirNodeRef> {
        use std::collections::HashMap;

        let nodes_added: Vec<MirNodeRef>;
        let mut new_node_count = 0;

        // J-G-F-P
        {
            // 0. Roots
            let mut base_nodes: HashMap<&str, MirNodeRef> = HashMap::default();
            let mut sorted_rels: Vec<&str> = qg.relations
                .keys()
                .map(String::as_str)
                .collect();
            sorted_rels.sort();
            for rel in &sorted_rels {
                let mut base_for_rel = match self.nodes.get(&(String::from(*rel),
                                       self.schema_version)) {
                    None => panic!("Query \"{}\" refers to unknown base \"{}\" node", name, rel),
                    Some(bmn) => MirNode::reuse(bmn.clone(), self.schema_version),
                };
                base_nodes.insert(*rel, Rc::new(RefCell::new(base_for_rel)));
            }

            // 2. Generate join nodes for the query. This starts out by joining two of the filter
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

            // 4. Filters
            let mut filter_nodes = HashMap::<String, Vec<MirNodeRef>>::new();

            // 5. Get the final node
            let mut final_na: MirNodeRef = if !func_nodes.is_empty() {
                // XXX(malte): This won't work if (a) there are multiple function nodes in the
                // query, or (b) computed columns are used within JOIN clauses
                assert!(func_nodes.len() <= 2);
                func_nodes.last().unwrap().clone()
            } else if !join_nodes.is_empty() {
                join_nodes.last().unwrap().clone()
            } else if !filter_nodes.is_empty() {
                assert_eq!(filter_nodes.len(), 1);
                let filter = filter_nodes.iter()
                    .next()
                    .as_ref()
                    .unwrap()
                    .1
                    .clone();
                assert_ne!(filter.len(), 0);
                filter.last().unwrap().clone()
            } else {
                // no join, filter, or function node --> base node is parent
                assert_eq!(sorted_rels.len(), 1);
                base_nodes[sorted_rels.last().unwrap()].clone()
            };


            // 6. Potentially insert TopK node below the final node

            // 7. Generate leaf views that expose the query result

            // XXX(malte): hack
            join_nodes
        }
    }
}
