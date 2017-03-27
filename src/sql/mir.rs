use flow::core::{NodeAddress, DataType};
use mir::{MirNode, MirNodeRef, MirNodeType, MirQuery};
use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator, TableKey,
              SqlQuery};
use nom_sql::{SelectStatement, LimitClause, OrderType, OrderClause};
use ops;

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

    pub fn named_query_to_mir(&mut self, name: &str, query: SqlQuery) -> MirQuery {
        match query {
            SqlQuery::CreateTable(ctq) => {
                assert_eq!(name, ctq.table.name);
                let n = self.make_base_node(&name, &ctq.fields, ctq.keys.as_ref());
                let rcn = Rc::new(RefCell::new(n));
                self.nodes.insert((String::from(name), self.schema_version), rcn.clone());
                MirQuery::singleton(name, rcn)
            }
            SqlQuery::Insert(iq) => {
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
            SqlQuery::Select(sq) => {
                let nodes = self.make_nodes_for_selection(&sq, &name);
                let mut roots = Vec::new();
                let mut leaves = Vec::new();
                for mn in nodes {
                    let rcn = Rc::new(RefCell::new(mn));
                    self.nodes.insert((String::from(name), self.schema_version), rcn.clone());
                    if rcn.borrow().ancestors().len() == 0 {
                        // root
                        roots.push(rcn.clone());
                    }
                    if rcn.borrow().children().len() == 0 {
                        // leaf
                        leaves.push(rcn);
                    }
                }
                assert_eq!(leaves.len(), 1);

                MirQuery {
                    name: String::from(name),
                    roots: roots,
                    leaf: leaves.into_iter().next().unwrap(),
                }
            }
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
                           "Assigning primary key {:?} for base {}",
                           key_cols,
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

    /// Return is (`new_nodes`, `leaf_node`).
    fn make_nodes_for_selection(&mut self, st: &SelectStatement, name: &str) -> Vec<MirNode> {
        vec![]
    }
}
