use dataflow::ops::join::JoinType;
use dataflow::ops::union;
use mir::node::node_inner::MirNodeInner;
use mir::node::{GroupedNodeType, MirNode};
use mir::query::MirQuery;
use mir::{Column, MirNodeRef};
use nom_sql::analysis::ReferredColumns;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use tracing::{debug, error, info, trace, warn};

use crate::controller::sql::query_graph::{OutputColumn, QueryGraph};
use crate::controller::sql::query_signature::Signature;
use crate::controller::sql::query_utils::extract_limit;
use nom_sql::{
    BinaryOperator, ColumnSpecification, CompoundSelectOperator, CreateTableStatement, Expression,
    FieldDefinitionExpression, FunctionExpression, LimitClause, Literal, OrderClause,
    SelectStatement, TableKey, UnaryOperator,
};

use itertools::Itertools;

use std::collections::{HashMap, HashSet};

use std::ops::Deref;
use std::vec::Vec;

use crate::errors::internal_err;
use crate::ReadySetResult;
use noria::{internal, invariant, invariant_eq, unsupported, DataType, ReadySetError};

use super::query_graph::JoinPredicate;

mod grouped;
mod join;

fn sanitize_leaf_column(c: &mut Column, view_name: &str) {
    c.table = Some(view_name.to_string());
    c.function = None;
    c.aliases = vec![];
}

fn value_columns_needed_for_predicates(
    value_columns: &[OutputColumn],
    predicates: &[Expression],
) -> Vec<(Column, OutputColumn)> {
    let pred_columns: HashSet<Column> = predicates
        .iter()
        .flat_map(|p| p.referred_columns())
        .map(|col| col.clone().into())
        .collect();

    value_columns
        .iter()
        .filter_map(|oc| match *oc {
            OutputColumn::Expression(ref ec) => Some((
                Column {
                    name: ec.name.clone(),
                    table: ec.table.clone(),
                    function: None,
                    aliases: vec![],
                },
                oc.clone(),
            )),
            OutputColumn::Literal(ref lc) => Some((
                Column {
                    name: lc.name.clone(),
                    table: lc.table.clone(),
                    function: None,
                    aliases: vec![],
                },
                oc.clone(),
            )),
            OutputColumn::Data { .. } => None,
        })
        .filter(|(c, _)| pred_columns.contains(c))
        .collect()
}

fn default_row_for_select(st: &SelectStatement) -> Option<Vec<DataType>> {
    // If this is an aggregated query AND it does not contain a GROUP BY clause,
    // set default values based on the aggregation (or lack thereof) on each
    // individual field
    if !st.contains_aggregate_select() || st.group_by.is_some() {
        return None;
    }
    Some(
        st.fields
            .iter()
            .map(|f| match f {
                FieldDefinitionExpression::Expression {
                    expr: Expression::Call(func),
                    ..
                } => match func {
                    FunctionExpression::Avg { .. } => DataType::None,
                    FunctionExpression::Count { .. } => DataType::Int(0),
                    FunctionExpression::CountStar => DataType::Int(0),
                    FunctionExpression::Sum { .. } => DataType::None,
                    FunctionExpression::Max(..) => DataType::None,
                    FunctionExpression::Min(..) => DataType::None,
                    FunctionExpression::GroupConcat { .. } => DataType::None,
                    FunctionExpression::Cast(..) => DataType::None,
                    FunctionExpression::Call { .. } => DataType::None,
                },
                _ => DataType::None,
            })
            .collect(),
    )
}

/// Configuration for how SQL is converted to MIR
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(crate) struct Config {
    /// If set to `true`, a SQL `ORDER BY` with `LIMIT` will emit a [`TopK`][] node, which
    /// currently crashes if it ever receives negative deltas that bring /// it below K records (see
    /// [ENG-56][]). If set to `false`, the SQL conversion process to return a
    /// [`ReadySetError::Unsupported`] (causing the adapter to send the query to fallback).
    /// Defaults to `false`.
    ///
    /// [`TopK`]: MirNodeInner::TopK
    /// [ENG-56]: https://readysettech.atlassian.net/browse/ENG-56
    pub(crate) allow_topk: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self { allow_topk: false }
    }
}

#[derive(Clone, Debug)]
pub(super) struct SqlToMirConverter {
    config: Config,
    base_schemas: HashMap<String, Vec<(usize, Vec<ColumnSpecification>)>>,
    current: HashMap<String, usize>,
    nodes: HashMap<(String, usize), MirNodeRef>,
    schema_version: usize,
}

impl Default for SqlToMirConverter {
    fn default() -> Self {
        SqlToMirConverter {
            config: Default::default(),
            base_schemas: HashMap::default(),
            current: HashMap::default(),
            nodes: HashMap::default(),
            schema_version: 0,
        }
    }
}

impl SqlToMirConverter {
    /// Set the [`Config`]
    pub(crate) fn set_config(&mut self, config: Config) {
        self.config = config;
    }

    fn get_view(&self, view_name: &str) -> Result<MirNodeRef, ReadySetError> {
        self.current
            .get(view_name)
            .ok_or_else(|| ReadySetError::ViewNotFound(view_name.into()))
            .and_then(|v| match self.nodes.get(&(String::from(view_name), *v)) {
                None => internal!("Inconsistency: view {} does not exist at v{}", view_name, v),
                Some(bmn) => Ok(MirNode::reuse(bmn.clone(), self.schema_version)),
            })
    }

    pub(super) fn add_leaf_below(
        &mut self,
        prior_leaf: MirNodeRef,
        name: &str,
        params: &[Column],
        project_columns: Option<Vec<Column>>,
    ) -> MirQuery {
        // hang off the previous logical leaf node
        let parent_columns: Vec<Column> = prior_leaf.borrow().columns().to_vec();
        let parent = MirNode::reuse(prior_leaf, self.schema_version);

        let (reproject, columns): (bool, Vec<Column>) = match project_columns {
            // parent is a projection already, so no need to reproject; just reuse its columns
            None => (false, parent_columns),
            // parent is not a projection, so we need to reproject to the columns passed to us
            Some(pc) => (true, pc.into_iter().chain(params.iter().cloned()).collect()),
        };

        let n = if reproject {
            // add a (re-)projection and then another leaf
            MirNode::new(
                &format!("{}_reproject", name),
                self.schema_version,
                columns.clone(),
                MirNodeInner::Project {
                    emit: columns.clone(),
                    literals: vec![],
                    expressions: vec![],
                },
                vec![parent.clone()],
                vec![],
            )
        } else {
            // add an identity node and then another leaf
            MirNode::new(
                &format!("{}_id", name),
                self.schema_version,
                columns.clone(),
                MirNodeInner::Identity,
                vec![parent.clone()],
                vec![],
            )
        };

        let new_leaf = MirNode::new(
            name,
            self.schema_version,
            columns
                .into_iter()
                .map(|mut c| {
                    sanitize_leaf_column(&mut c, name);
                    c
                })
                .collect(),
            MirNodeInner::leaf(n.clone(), Vec::from(params)),
            vec![n],
            vec![],
        );

        // always register leaves
        self.current.insert(String::from(name), self.schema_version);
        self.nodes
            .insert((String::from(name), self.schema_version), new_leaf.clone());

        // wrap in a (very short) query to return
        MirQuery {
            name: String::from(name),
            roots: vec![parent],
            leaf: new_leaf,
        }
    }

    pub(super) fn compound_query_to_mir(
        &mut self,
        name: &str,
        sqs: Vec<&MirQuery>,
        op: CompoundSelectOperator,
        order: &Option<OrderClause>,
        limit: &Option<LimitClause>,
        has_leaf: bool,
    ) -> ReadySetResult<MirQuery> {
        let union_name = if !has_leaf && limit.is_none() {
            String::from(name)
        } else {
            format!("{}_union", name)
        };
        let mut final_node = match op {
            CompoundSelectOperator::Union => self.make_union_node(
                &union_name,
                &sqs.iter().map(|mq| mq.leaf.clone()).collect::<Vec<_>>()[..],
                union::DuplicateMode::UnionAll,
            )?,
            _ => internal!(),
        };
        let node_id = (union_name, self.schema_version);
        self.nodes
            .entry(node_id)
            .or_insert_with(|| final_node.clone());

        // we use these columns for intermediate nodes
        let columns: Vec<Column> = final_node.borrow().columns().to_vec();
        // we use these columns for whichever node ends up being the leaf
        let sanitized_columns: Vec<Column> = columns
            .clone()
            .into_iter()
            .map(|mut c| {
                sanitize_leaf_column(&mut c, name);
                c
            })
            .collect();

        if let Some(limit) = limit.as_ref() {
            let (topk_name, topk_columns) = if !has_leaf {
                (String::from(name), sanitized_columns.iter().collect())
            } else {
                (format!("{}_topk", name), columns.iter().collect())
            };
            let topk_node =
                self.make_topk_node(&topk_name, final_node, topk_columns, order, limit)?;
            let node_id = (topk_name, self.schema_version);
            self.nodes
                .entry(node_id)
                .or_insert_with(|| topk_node.clone());
            final_node = topk_node;
        }

        let leaf_node = if has_leaf {
            MirNode::new(
                name,
                self.schema_version,
                sanitized_columns,
                MirNodeInner::leaf(final_node.clone(), vec![]),
                vec![final_node],
                vec![],
            )
        } else {
            final_node.borrow_mut().columns = sanitized_columns;
            final_node
        };

        self.current
            .insert(String::from(leaf_node.borrow().name()), self.schema_version);
        let node_id = (String::from(name), self.schema_version);
        self.nodes
            .entry(node_id)
            .or_insert_with(|| leaf_node.clone());

        Ok(MirQuery {
            name: String::from(name),
            roots: sqs.iter().fold(Vec::new(), |mut acc, mq| {
                acc.extend(mq.roots.iter().cloned());
                acc
            }),
            leaf: leaf_node,
        })
    }

    // pub(super) viz for tests
    pub(super) fn get_flow_node_address(&self, name: &str, version: usize) -> Option<NodeIndex> {
        match self.nodes.get(&(name.to_string(), version)) {
            None => None,
            Some(node) => node
                .borrow()
                .flow_node
                .as_ref()
                .map(|flow_node| flow_node.address()),
        }
    }

    pub(super) fn get_leaf(&self, name: &str) -> Option<NodeIndex> {
        match self.current.get(name) {
            None => None,
            Some(v) => self.get_flow_node_address(name, *v),
        }
    }

    pub(super) fn named_base_to_mir(
        &mut self,
        name: &str,
        ctq: &CreateTableStatement,
    ) -> ReadySetResult<MirQuery> {
        invariant_eq!(name, ctq.table.name);
        let n = self.make_base_node(name, &ctq.fields, ctq.keys.as_ref())?;
        let node_id = (String::from(name), self.schema_version);
        use std::collections::hash_map::Entry;
        if let Entry::Vacant(e) = self.nodes.entry(node_id) {
            self.current.insert(String::from(name), self.schema_version);
            e.insert(n.clone());
        }
        Ok(MirQuery::singleton(name, n))
    }

    pub(super) fn remove_query(&mut self, name: &str, mq: &MirQuery) -> ReadySetResult<()> {
        use std::collections::VecDeque;

        let v = self
            .current
            .remove(name)
            .ok_or_else(|| internal_err(format!("no query named \"{}\"?", name)))?;

        let nodeid = (name.to_owned(), v);
        let leaf_mn = self.nodes.remove(&nodeid).ok_or_else(|| {
            internal_err(format!("could not find MIR node {:?} for removal", nodeid))
        })?;

        invariant_eq!(leaf_mn.borrow().name, mq.leaf.borrow().name);

        // traverse the MIR query backwards, removing any nodes that we still have registered.
        let mut q = VecDeque::new();
        q.push_back(leaf_mn);

        while let Some(mnr) = q.pop_front() {
            let n = mnr.borrow_mut();
            q.extend(n.ancestors.clone());
            // node may not be registered, so don't bother checking return
            match n.inner {
                MirNodeInner::Reuse { .. } | MirNodeInner::Base { .. } => (),
                _ => {
                    self.nodes.remove(&(n.name.to_owned(), v));
                }
            }
        }
        Ok(())
    }

    pub(super) fn remove_base(&mut self, name: &str, mq: &MirQuery) -> ReadySetResult<()> {
        info!(%name, "Removing base node");
        self.remove_query(name, mq)?;
        if self.base_schemas.remove(name).is_none() {
            warn!(%name, "Attempted to remove non-existent base node");
        }
        Ok(())
    }

    pub(super) fn named_query_to_mir(
        &mut self,
        name: &str,
        sq: &SelectStatement,
        qg: &QueryGraph,
        has_leaf: bool,
    ) -> Result<MirQuery, ReadySetError> {
        let nodes = self.make_nodes_for_selection(name, sq, qg, has_leaf)?;
        let mut roots = Vec::new();
        let mut leaves = Vec::new();
        for mn in nodes.into_iter() {
            let node_id = (String::from(mn.borrow().name()), self.schema_version);
            // only add the node if we don't have it registered at this schema version already. If
            // we don't do this, we end up adding the node again for every re-use of it, with
            // increasingly deeper chains of nested `MirNode::Reuse` structures.
            self.nodes.entry(node_id).or_insert_with(|| mn.clone());

            if mn.borrow().ancestors().is_empty() {
                // root
                roots.push(mn.clone());
            }
            if mn.borrow().children().is_empty() {
                // leaf
                leaves.push(mn);
            }
        }
        if leaves.len() != 1 {
            internal!("expected just one leaf! leaves: {:?}", leaves);
        }
        #[allow(clippy::unwrap_used)] // checked above
        let leaf = leaves.into_iter().next().unwrap();
        self.current
            .insert(String::from(leaf.borrow().name()), self.schema_version);

        Ok(MirQuery {
            name: String::from(name),
            roots,
            leaf,
        })
    }

    pub(super) fn upgrade_schema(&mut self, new_version: usize) -> ReadySetResult<()> {
        invariant!(new_version > self.schema_version);
        self.schema_version = new_version;
        Ok(())
    }

    fn make_base_node(
        &mut self,
        name: &str,
        cols: &[ColumnSpecification],
        keys: Option<&Vec<TableKey>>,
    ) -> ReadySetResult<MirNodeRef> {
        // have we seen a base of this name before?
        if let Some(mut existing_schemas) = self.base_schemas.get(name).cloned() {
            existing_schemas.sort_by_key(|&(sv, _)| sv);
            // newest schema first
            existing_schemas.reverse();

            #[allow(clippy::never_loop)]
            for (existing_version, ref schema) in existing_schemas {
                // TODO(malte): check the keys too
                if &schema[..] == cols {
                    // exact match, so reuse the existing base node
                    info!(
                        %name,
                        %existing_version,
                        "base table already exists with identical schema; reusing it.",
                    );
                    let node_key = (String::from(name), existing_version);
                    let existing_node = self.nodes.get(&node_key).cloned().ok_or_else(|| {
                        internal_err(format!("could not find MIR node {:?} for reuse", node_key))
                    })?;
                    return Ok(MirNode::reuse(existing_node, self.schema_version));
                } else {
                    // match, but schema is different, so we'll need to either:
                    //  1) reuse the existing node, but add an upgrader for any changes in the
                    //     column set, or
                    //  2) give up and just make a new node
                    info!(
                        %name,
                        %existing_version,
                        "base table already exists, but has a different schema!",
                    );

                    // Find out if this is a simple case of adding or removing a column
                    let mut columns_added = Vec::new();
                    let mut columns_removed = Vec::new();
                    let mut columns_unchanged = Vec::new();
                    for c in cols {
                        if !schema.contains(c) {
                            // new column
                            columns_added.push(c);
                        } else {
                            columns_unchanged.push(c);
                        }
                    }
                    for c in schema {
                        if !cols.contains(c) {
                            // dropped column
                            columns_removed.push(c);
                        }
                    }

                    if !columns_unchanged.is_empty()
                        && (!columns_added.is_empty() || !columns_removed.is_empty())
                    {
                        error!(
                            %name,
                            ?columns_added,
                            ?columns_removed,
                            %existing_version
                        );
                        let node_key = (String::from(name), existing_version);
                        let existing_node =
                            self.nodes.get(&node_key).cloned().ok_or_else(|| {
                                internal_err(format!(
                                    "couldn't find MIR node {:?} in add/remove cols",
                                    node_key
                                ))
                            })?;

                        let mut columns: Vec<ColumnSpecification> = existing_node
                            .borrow()
                            .column_specifications()
                            .iter()
                            .map(|&(ref cs, _)| cs.clone())
                            .collect();
                        for added in &columns_added {
                            columns.push((*added).clone());
                        }
                        for removed in &columns_removed {
                            let pos =
                                columns
                                    .iter()
                                    .position(|cc| cc == *removed)
                                    .ok_or_else(|| {
                                        internal_err(format!(
                                            "couldn't find column \"{:#?}\", \
                                             which we're removing",
                                            removed
                                        ))
                                    })?;
                            columns.remove(pos);
                        }
                        invariant_eq!(
                            columns.len(),
                            existing_node.borrow().columns().len() + columns_added.len()
                                - columns_removed.len()
                        );

                        // remember the schema for this version
                        let base_schemas = self.base_schemas.entry(String::from(name)).or_default();
                        base_schemas.push((self.schema_version, columns));

                        return Ok(MirNode::adapt_base(
                            existing_node,
                            columns_added,
                            columns_removed,
                        ));
                    } else {
                        info!("base table has complex schema change");
                        break;
                    }
                }
            }
        }

        // all columns on a base must have the base as their table
        invariant!(cols
            .iter()
            .all(|c| c.column.table == Some(String::from(name))));

        // primary keys can either be specified directly (at the end of CREATE TABLE), or inline
        // with the definition of a field (i.e., as a ColumnConstraint).
        // We assume here that an earlier rewrite pass has coalesced all primary key definitions in
        // the TableKey structure passed in via `keys`.
        let primary_keys = match keys {
            None => vec![],
            Some(keys) => keys
                .iter()
                .filter_map(|k| match *k {
                    ref k @ TableKey::PrimaryKey { .. } => Some(k),
                    _ => None,
                })
                .collect(),
        };
        invariant!(primary_keys.len() <= 1);

        // remember the schema for this version
        let base_schemas = self.base_schemas.entry(String::from(name)).or_default();
        base_schemas.push((self.schema_version, cols.to_vec()));

        // make node
        Ok(if let Some(pk) = primary_keys.first() {
            match **pk {
                TableKey::PrimaryKey { ref columns, .. } => {
                    debug!(
                        %name,
                        primary_key = %columns.iter().map(|c| c.name.as_str()).join(", "),
                        "Assigning primary key for base",
                    );
                    MirNode::new(
                        name,
                        self.schema_version,
                        cols.iter().map(|cs| Column::from(&cs.column)).collect(),
                        MirNodeInner::Base {
                            column_specs: cols.iter().map(|cs| (cs.clone(), None)).collect(),
                            keys: columns.iter().map(Column::from).collect(),
                            adapted_over: None,
                        },
                        vec![],
                        vec![],
                    )
                }
                _ => internal!(),
            }
        } else {
            MirNode::new(
                name,
                self.schema_version,
                cols.iter().map(|cs| Column::from(&cs.column)).collect(),
                MirNodeInner::Base {
                    column_specs: cols.iter().map(|cs| (cs.clone(), None)).collect(),
                    keys: vec![],
                    adapted_over: None,
                },
                vec![],
                vec![],
            )
        })
    }

    fn make_union_node(
        &self,
        name: &str,
        ancestors: &[MirNodeRef],
        duplicate_mode: union::DuplicateMode,
    ) -> ReadySetResult<MirNodeRef> {
        let mut emit: Vec<Vec<Column>> = Vec::new();
        invariant!(ancestors.len() > 1, "union must have more than 1 ancestors");

        #[allow(clippy::unwrap_used)] // checked above
        let ucols: Vec<Column> = ancestors.first().unwrap().borrow().columns().to_vec();
        let num_ucols = ucols.len();

        // Find columns present in all ancestors
        // XXX(malte): this currently matches columns by **name** rather than by table and name,
        // which can go wrong if there are multiple columns of the same name in the inputs to the
        // union. Unfortunately, we have to do it by name here because the nested queries in
        // compound SELECT rewrite the table name on their output columns.
        let mut selected_cols = HashSet::new();
        for c in ucols {
            if ancestors
                .iter()
                .all(|a| a.borrow().columns().iter().any(|ac| *ac.name == c.name))
            {
                selected_cols.insert(c.name.clone());
            } else {
                internal!(
                    "column with name '{}' not found all union ancestors: all ancestors' \
                     output columns must have the same names",
                    c.name
                );
            }
        }
        invariant_eq!(
            num_ucols,
            selected_cols.len(),
            "union drops ancestor columns"
        );

        for ancestor in ancestors.iter() {
            let mut acols: Vec<Column> = Vec::new();
            for ac in ancestor.borrow().columns() {
                if selected_cols.contains(&ac.name) && !acols.iter().any(|c| ac.name == c.name) {
                    acols.push(ac.clone());
                }
            }
            emit.push(acols.clone());
        }

        invariant!(
            emit.iter().all(|e| e.len() == selected_cols.len()),
            "all ancestors columns must have the same size, but got emit: {:?}, selected: {:?}",
            emit,
            selected_cols
        );

        invariant!(!emit.is_empty());

        #[allow(clippy::unwrap_used)] // checked above
        Ok(MirNode::new(
            name,
            self.schema_version,
            emit.first().unwrap().clone(),
            MirNodeInner::Union {
                emit,
                duplicate_mode,
            },
            ancestors.to_vec(),
            vec![],
        ))
    }

    fn make_union_from_same_base(
        &self,
        name: &str,
        ancestors: Vec<MirNodeRef>,
        columns: Vec<Column>,
        duplicate_mode: union::DuplicateMode,
    ) -> ReadySetResult<MirNodeRef> {
        invariant!(ancestors.len() > 1, "union must have more than 1 ancestors");
        trace!(%name, ?columns, "Added union node");
        let emit = ancestors.iter().map(|_| columns.clone()).collect();

        Ok(MirNode::new(
            name,
            self.schema_version,
            columns,
            MirNodeInner::Union {
                emit,
                duplicate_mode,
            },
            ancestors,
            vec![],
        ))
    }

    fn make_filter_node(
        &self,
        name: &str,
        parent: MirNodeRef,
        conditions: Expression,
    ) -> MirNodeRef {
        let fields = parent.borrow().columns().to_vec();
        trace!(%name, %conditions, "Added filter node");
        MirNode::new(
            name,
            self.schema_version,
            fields,
            MirNodeInner::Filter { conditions },
            vec![parent],
            vec![],
        )
    }

    fn make_aggregate_node(
        &self,
        name: &str,
        func_col: &Column,
        group_cols: Vec<&Column>,
        parent: MirNodeRef,
        projected_exprs: &HashMap<Expression, String>,
    ) -> ReadySetResult<Vec<MirNodeRef>> {
        use dataflow::ops::grouped::aggregate::Aggregation;
        use dataflow::ops::grouped::extremum::Extremum;
        use nom_sql::FunctionExpression::*;

        let mut out_nodes = Vec::new();

        let mknode = |over: &Column, t: GroupedNodeType, distinct: bool| {
            if distinct {
                let new_name = format!("{}_d{}", name, out_nodes.len());
                let mut dist_col = vec![over];
                dist_col.extend(group_cols.clone());
                let node = self.make_distinct_node(&new_name, parent, dist_col.clone());
                out_nodes.push(node.clone());
                out_nodes.push(self.make_grouped_node(name, func_col, (node, over), group_cols, t));
            } else {
                out_nodes.push(self.make_grouped_node(
                    name,
                    func_col,
                    (parent, over),
                    group_cols,
                    t,
                ));
            }
            out_nodes
        };

        let func = func_col.function.as_ref().ok_or_else(|| {
            internal_err(format!(
                "aggregate col {:?} was not a function",
                func_col.function
            ))
        })?;
        Ok(match *func.deref() {
            // TODO: support more types of filter expressions
            // CH: https://app.clubhouse.io/readysettech/story/193
            Sum {
                expr: box Expression::Column(ref col),
                distinct,
            } => mknode(
                &Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::Sum),
                distinct,
            ),
            Sum { ref expr, distinct } => mknode(
                // TODO(celine): replace with ParentRef
                &Column::named(projected_exprs.get(expr).cloned().ok_or_else(|| {
                    internal_err(format!("projected_exprs does not contain {:?}", expr))
                })?),
                GroupedNodeType::Aggregation(Aggregation::Sum),
                distinct,
            ),
            CountStar => {
                internal!("COUNT(*) should have been rewritten earlier!")
            }
            Count {
                expr: box Expression::Column(ref col),
                distinct,
                count_nulls,
            } => mknode(
                &Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::Count { count_nulls }),
                distinct,
            ),
            Count {
                ref expr,
                distinct,
                count_nulls,
            } => mknode(
                // TODO(celine): replace with ParentRef
                &Column::named(projected_exprs.get(expr).cloned().ok_or_else(|| {
                    internal_err(format!("projected_exprs does not contain {:?}", expr))
                })?),
                GroupedNodeType::Aggregation(Aggregation::Count { count_nulls }),
                distinct,
            ),
            Avg {
                expr: box Expression::Column(ref col),
                distinct,
            } => mknode(
                &Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::Avg),
                distinct,
            ),
            Avg { ref expr, distinct } => mknode(
                // TODO(celine): replace with ParentRef
                &Column::named(projected_exprs.get(expr).cloned().ok_or_else(|| {
                    internal_err(format!("projected_exprs does not contain {:?}", expr))
                })?),
                GroupedNodeType::Aggregation(Aggregation::Avg),
                distinct,
            ),
            // TODO(atsakiris): Support Filters for Extremum/GroupConcat
            // CH: https://app.clubhouse.io/readysettech/story/198
            Max(box Expression::Column(ref col)) => mknode(
                &Column::from(col),
                GroupedNodeType::Extremum(Extremum::Max),
                false,
            ),
            Max(ref expr) => mknode(
                // TODO(celine): replace with ParentRef
                &Column::named(projected_exprs.get(expr).cloned().ok_or_else(|| {
                    internal_err(format!("projected_exprs does not contain {:?}", expr))
                })?),
                GroupedNodeType::Extremum(Extremum::Max),
                false,
            ),
            Min(box Expression::Column(ref col)) => mknode(
                &Column::from(col),
                GroupedNodeType::Extremum(Extremum::Min),
                false,
            ),
            Min(ref expr) => mknode(
                // TODO(celine): replace with ParentRef
                &Column::named(projected_exprs.get(expr).cloned().ok_or_else(|| {
                    internal_err(format!("projected_exprs does not contain {:?}", expr))
                })?),
                GroupedNodeType::Extremum(Extremum::Min),
                false,
            ),
            GroupConcat {
                expr: box Expression::Column(ref col),
                ref separator,
            } => mknode(
                &Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::GroupConcat {
                    separator: separator.clone(),
                }),
                false,
            ),
            _ => internal!("{:?}", func),
        })
    }

    fn make_grouped_node(
        &self,
        name: &str,
        computed_col: &Column,
        over: (MirNodeRef, &Column),
        group_by: Vec<&Column>,
        node_type: GroupedNodeType,
    ) -> MirNodeRef {
        let (parent_node, over_col) = over;

        // The aggregate node's set of output columns is the group columns plus the function
        // column
        let mut combined_columns = group_by
            .iter()
            .map(|c| (*c).clone())
            .collect::<Vec<Column>>();
        combined_columns.push(computed_col.clone());

        // make the new operator
        match node_type {
            GroupedNodeType::Aggregation(agg) => MirNode::new(
                name,
                self.schema_version,
                combined_columns,
                MirNodeInner::Aggregation {
                    on: over_col.clone(),
                    group_by: group_by.into_iter().cloned().collect(),
                    kind: agg,
                },
                vec![parent_node],
                vec![],
            ),
            GroupedNodeType::Extremum(extr) => MirNode::new(
                name,
                self.schema_version,
                combined_columns,
                MirNodeInner::Extremum {
                    on: over_col.clone(),
                    group_by: group_by.into_iter().cloned().collect(),
                    kind: extr,
                },
                vec![parent_node],
                vec![],
            ),
        }
    }

    fn make_join_node(
        &self,
        name: &str,
        join_predicates: &[JoinPredicate],
        left_node: MirNodeRef,
        right_node: MirNodeRef,
        kind: JoinType,
    ) -> ReadySetResult<MirNodeRef> {
        // TODO(malte): this is where we overproject join columns in order to increase reuse
        // opportunities. Technically, we need to only project those columns here that the query
        // actually needs; at a minimum, we could start with just the join colums, relying on the
        // automatic column pull-down to retrieve the remaining columns required.
        let projected_cols_left = left_node.borrow().columns().to_vec();
        let projected_cols_right = right_node.borrow().columns().to_vec();
        let mut fields = projected_cols_left
            .into_iter()
            .chain(projected_cols_right.into_iter())
            .collect::<Vec<Column>>();

        // join columns need us to generate join group configs for the operator
        let mut left_join_columns = Vec::new();
        let mut right_join_columns = Vec::new();

        for jp in join_predicates {
            let mut l_col = match jp.left {
                Expression::Column(ref f) => Column::from(f),
                _ => unsupported!("no multi-level joins yet"),
            };
            let r_col = match jp.right {
                Expression::Column(ref f) => Column::from(f),
                _ => unsupported!("no multi-level joins yet"),
            };

            if kind == JoinType::Inner {
                // for inner joins, don't duplicate the join column in the output, but instead add
                // aliases to the columns that represent it going forward (viz., the left-side join
                // column)
                l_col.add_alias(&r_col);
                // add the alias to all instances of `l_col` in `fields` (there might be more than one
                // if `l_col` is explicitly projected multiple times)
                fields = fields
                    .into_iter()
                    .filter_map(|mut f| {
                        if f == r_col {
                            // drop instances of right-side column
                            None
                        } else if f == l_col {
                            // add alias for right-side column to any left-side column
                            // N.B.: since `l_col` is already aliased, need to check this *after*
                            // checking for equivalence with `r_col` (by now, `l_col` == `r_col` via
                            // alias), so `f == l_col` also triggers if `f` is in `l_col.aliases`.
                            f.add_alias(&r_col);
                            Some(f)
                        } else {
                            // keep unaffected columns
                            Some(f)
                        }
                    })
                    .collect();
            }

            left_join_columns.push(l_col);
            right_join_columns.push(r_col);
        }

        invariant_eq!(left_join_columns.len(), right_join_columns.len());
        let inner = match kind {
            JoinType::Inner => MirNodeInner::Join {
                on_left: left_join_columns,
                on_right: right_join_columns,
                project: fields.clone(),
            },
            JoinType::Left => MirNodeInner::LeftJoin {
                on_left: left_join_columns,
                on_right: right_join_columns,
                project: fields.clone(),
            },
        };
        trace!(?inner, "Added join node");
        Ok(MirNode::new(
            name,
            self.schema_version,
            fields,
            inner,
            vec![left_node, right_node],
            vec![],
        ))
    }

    fn make_join_aggregates_node(
        &self,
        name: &str,
        aggregates: &[MirNodeRef; 2],
    ) -> ReadySetResult<MirNodeRef> {
        let fields = aggregates
            .iter()
            .map(|node| node.borrow().columns().to_owned())
            .flatten()
            .unique()
            .collect::<Vec<Column>>();

        let inner = MirNodeInner::JoinAggregates;
        trace!(?inner, "Added join node");
        Ok(MirNode::new(
            name,
            self.schema_version,
            fields,
            inner,
            aggregates.to_vec(),
            vec![],
        ))
    }

    fn make_projection_helper(
        &self,
        name: &str,
        parent: MirNodeRef,
        fn_cols: Vec<&Column>,
    ) -> MirNodeRef {
        self.make_project_node(
            name,
            parent,
            fn_cols,
            vec![],
            vec![(String::from("grp"), DataType::from(0i32))],
            false,
        )
    }

    fn make_project_node(
        &self,
        name: &str,
        parent_node: MirNodeRef,
        proj_cols: Vec<&Column>,
        expressions: Vec<(String, Expression)>,
        literals: Vec<(String, DataType)>,
        is_leaf: bool,
    ) -> MirNodeRef {
        // TODO(eta): why was this commented out?
        //assert!(proj_cols.iter().all(|c| c.table == parent_name));

        let fields = proj_cols
            .clone()
            .into_iter()
            .map(|c| {
                let mut c = c.clone();
                // if this is the leaf node of a query, it represents a view, so we rewrite the
                // table name here.
                if is_leaf {
                    sanitize_leaf_column(&mut c, name);
                }
                c
            })
            .chain(
                expressions
                    .iter()
                    .map(|&(ref n, _)| n.clone())
                    .chain(literals.iter().map(|&(ref n, _)| n.clone()))
                    .map(|n| {
                        if is_leaf {
                            Column::new(Some(name), &n)
                        } else {
                            Column::new(None, &n)
                        }
                    }),
            )
            .collect();

        let emit_cols = proj_cols.into_iter().cloned().collect();

        MirNode::new(
            name,
            self.schema_version,
            fields,
            MirNodeInner::Project {
                emit: emit_cols,
                literals,
                expressions,
            },
            vec![parent_node],
            vec![],
        )
    }

    #[cfg(feature = "param_filter")]
    fn make_param_filter_node(
        &self,
        name: &str,
        parent_node: MirNodeRef,
        col: &Column,
        emit_key: &Column,
        operator: &BinaryOperator,
    ) -> MirNodeRef {
        let fields = parent_node
            .borrow()
            .columns()
            .iter()
            .cloned()
            .chain(std::iter::once(emit_key.clone()))
            .collect();

        MirNode::new(
            name,
            self.schema_version,
            fields,
            MirNodeInner::ParamFilter {
                col: col.clone(),
                emit_key: emit_key.clone(),
                operator: *operator,
            },
            vec![parent_node],
            vec![],
        )
    }

    fn make_distinct_node(
        &self,
        name: &str,
        parent: MirNodeRef,
        group_by: Vec<&Column>,
    ) -> MirNodeRef {
        let group_by: Vec<_> = group_by.into_iter().cloned().collect();
        let mut columns = group_by.clone();
        // When Distinct gets converted to a Count flow node in mir_to_flow it will count the
        // occurances up for us, and put that in a hidden column. Rather than store the count in a
        // hidden column, we should be explicit and create that column now. It will be omitted in
        // the final output due to absence in the projected columns list.
        columns.push(Column {
            name: "__distinct_count".to_owned(),
            table: None,
            function: None,
            aliases: vec![],
        });

        // make the new operator and record its metadata
        MirNode::new(
            name,
            self.schema_version,
            columns,
            MirNodeInner::Distinct { group_by },
            vec![parent],
            vec![],
        )
    }

    fn make_topk_node(
        &self,
        name: &str,
        parent: MirNodeRef,
        group_by: Vec<&Column>,
        order: &Option<OrderClause>,
        limit: &LimitClause,
    ) -> ReadySetResult<MirNodeRef> {
        if !self.config.allow_topk {
            unsupported!("TopK is not supported");
        }

        let combined_columns = parent.borrow().columns().to_vec();

        let order = order.as_ref().map(|o| {
            o.columns
                .iter()
                .map(|(c, o)| (Column::from(c), *o))
                .collect()
        });

        if let Some(offset) = &limit.offset {
            if offset != &Expression::Literal(0.into()) {
                unsupported!("TopK nodes don't support OFFSET yet ({} supplied)", offset)
            }
        }

        let k = extract_limit(limit)?;

        // make the new operator and record its metadata
        Ok(MirNode::new(
            name,
            self.schema_version,
            combined_columns,
            MirNodeInner::TopK {
                order,
                group_by: group_by.into_iter().cloned().collect(),
                k,
                offset: 0,
            },
            vec![parent],
            vec![],
        ))
    }

    fn make_predicate_nodes(
        &self,
        name: &str,
        parent: MirNodeRef,
        ce: &Expression,
        nc: usize,
    ) -> ReadySetResult<Vec<MirNodeRef>> {
        let mut pred_nodes: Vec<MirNodeRef> = Vec::new();
        let output_cols = parent.borrow().columns().to_vec();
        match ce {
            Expression::BinaryOp { lhs, op, rhs } => {
                match op {
                    BinaryOperator::And => {
                        let left = self.make_predicate_nodes(name, parent, lhs, nc)?;
                        invariant!(!left.is_empty());
                        #[allow(clippy::unwrap_used)] // checked above
                        let right = self.make_predicate_nodes(
                            name,
                            left.last().unwrap().clone(),
                            rhs,
                            nc + left.len(),
                        )?;

                        pred_nodes.extend(left);
                        pred_nodes.extend(right);
                    }
                    BinaryOperator::Or => {
                        let left = self.make_predicate_nodes(name, parent.clone(), lhs, nc)?;
                        let right =
                            self.make_predicate_nodes(name, parent, rhs, nc + left.len())?;

                        debug!("Creating union node for `or` predicate");

                        invariant!(!left.is_empty());
                        invariant!(!right.is_empty());
                        #[allow(clippy::unwrap_used)] // checked above
                        let last_left = left.last().unwrap().clone();
                        #[allow(clippy::unwrap_used)] // checked above
                        let last_right = right.last().unwrap().clone();
                        let union = self.make_union_from_same_base(
                            &format!("{}_un{}", name, nc + left.len() + right.len()),
                            vec![last_left, last_right],
                            output_cols,
                            // the filters might overlap, so we need to set BagUnion mode which
                            // removes rows in one side that exist in the other
                            union::DuplicateMode::BagUnion,
                        )?;

                        pred_nodes.extend(left);
                        pred_nodes.extend(right);
                        pred_nodes.push(union);
                    }
                    _ => {
                        // currently, we only support filter-like
                        // comparison operations, no nested-selections
                        let f =
                            self.make_filter_node(&format!("{}_f{}", name, nc), parent, ce.clone());
                        pred_nodes.push(f);
                    }
                }
            }
            Expression::UnaryOp {
                op: UnaryOperator::Not | UnaryOperator::Neg,
                ..
            } => internal!("negation should have been removed earlier"),
            Expression::Literal(_) | Expression::Column(_) => {
                let f = self.make_filter_node(
                    &format!("{}_f{}", name, nc),
                    parent,
                    Expression::BinaryOp {
                        lhs: Box::new(ce.clone()),
                        op: BinaryOperator::NotEqual,
                        rhs: Box::new(Expression::Literal(Literal::Integer(0))),
                    },
                );
                pred_nodes.push(f);
            }
            Expression::Between { .. } => internal!("BETWEEN should have been removed earlier"),
            Expression::Exists(_) => unsupported!("exists unsupported"),
            Expression::Call(_) => {
                internal!("Function calls should have been handled by projection earlier")
            }
            Expression::CaseWhen { .. } => unsupported!("CASE WHEN not supported in filters"),
            Expression::NestedSelect(_) => unsupported!("Nested selects not supported in filters"),
            Expression::In { .. } => internal!("IN should have been removed earlier"),
        }

        Ok(pred_nodes)
    }

    fn predicates_above_group_by<'a>(
        &self,
        name: &str,
        column_to_predicates: &HashMap<Column, Vec<&'a Expression>>,
        over_col: Column,
        parent: MirNodeRef,
        created_predicates: &mut Vec<&'a Expression>,
    ) -> ReadySetResult<Vec<MirNodeRef>> {
        let mut predicates_above_group_by_nodes = Vec::new();
        let mut prev_node = parent;

        let ces = column_to_predicates.get(&over_col).unwrap();
        for ce in ces {
            if !created_predicates.contains(ce) {
                let mpns = self.make_predicate_nodes(
                    &format!("{}_mp{}", name, predicates_above_group_by_nodes.len()),
                    prev_node.clone(),
                    ce,
                    0,
                )?;
                invariant!(!mpns.is_empty());
                #[allow(clippy::unwrap_used)] // checked above
                {
                    prev_node = mpns.last().unwrap().clone();
                }
                predicates_above_group_by_nodes.extend(mpns);
                created_predicates.push(ce);
            }
        }

        Ok(predicates_above_group_by_nodes)
    }

    fn make_value_project_node(
        &self,
        qg: &QueryGraph,
        prev_node: Option<MirNodeRef>,
        node_count: usize,
    ) -> ReadySetResult<Option<MirNodeRef>> {
        let arith_and_lit_columns_needed =
            value_columns_needed_for_predicates(&qg.columns, &qg.global_predicates);

        Ok(if !arith_and_lit_columns_needed.is_empty() {
            let projected_expressions: Vec<(String, Expression)> = arith_and_lit_columns_needed
                .iter()
                .filter_map(|&(_, ref oc)| match oc {
                    OutputColumn::Expression(ref ac) => {
                        Some((ac.name.clone(), ac.expression.clone()))
                    }
                    OutputColumn::Data { .. } => None,
                    OutputColumn::Literal(_) => None,
                })
                .collect();
            let projected_literals: Vec<(String, DataType)> = arith_and_lit_columns_needed
                .iter()
                .map(|&(_, ref oc)| -> ReadySetResult<_> {
                    match oc {
                        OutputColumn::Expression(_) => Ok(None),
                        OutputColumn::Data { .. } => Ok(None),
                        OutputColumn::Literal(ref lc) => {
                            Ok(Some((lc.name.clone(), DataType::try_from(&lc.value)?)))
                        }
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect();

            // prev_node must be set at this point
            let parent = match prev_node {
                None => internal!(),
                Some(pn) => pn,
            };

            let passthru_cols: Vec<_> = parent.borrow().columns().to_vec();
            let projected = self.make_project_node(
                &format!("q_{:x}_n{}", qg.signature().hash, node_count),
                parent,
                passthru_cols.iter().collect(),
                projected_expressions,
                projected_literals,
                false,
            );

            Some(projected)
        } else {
            None
        })
    }

    /// Returns list of nodes added
    #[allow(clippy::cognitive_complexity)]
    fn make_nodes_for_selection(
        &mut self,
        name: &str,
        st: &SelectStatement,
        qg: &QueryGraph,
        has_leaf: bool,
    ) -> Result<Vec<MirNodeRef>, ReadySetError> {
        // TODO: make this take &self!
        use crate::controller::sql::mir::grouped::make_grouped;
        use crate::controller::sql::mir::grouped::{
            make_expressions_above_grouped, make_predicates_above_grouped,
        };
        use crate::controller::sql::mir::join::make_joins;

        let mut nodes_added: Vec<MirNodeRef>;
        let mut new_node_count = 0;

        // Canonical operator order: B-J-F-G-P-R
        // (Base, Join, Filter, GroupBy, Project, Reader)
        {
            let mut node_for_rel: HashMap<&str, MirNodeRef> = HashMap::default();

            // 0. Base nodes (always reused)
            let mut base_nodes: Vec<MirNodeRef> = Vec::new();
            let mut sorted_rels: Vec<&str> = qg.relations.keys().map(String::as_str).collect();
            sorted_rels.sort_unstable();
            for rel in &sorted_rels {
                if *rel == "computed_columns" {
                    continue;
                }

                let base_for_rel = self.get_view(rel)?;

                base_nodes.push(base_for_rel.clone());
                node_for_rel.insert(*rel, base_for_rel);
            }

            let join_nodes = make_joins(
                self,
                &format!("q_{:x}", qg.signature().hash),
                qg,
                &node_for_rel,
                new_node_count,
            )?;

            new_node_count += join_nodes.len();

            let mut prev_node = match join_nodes.last() {
                Some(n) => Some(n.clone()),
                None => {
                    invariant_eq!(base_nodes.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked above
                    Some(base_nodes.last().unwrap().clone())
                }
            };

            // 2. If we're aggregating on expressions rather than directly on columns, project out
            // those expressions before the aggregate itself
            let expressions_above_grouped = make_expressions_above_grouped(
                self,
                &format!("q_{:x}", qg.signature().hash),
                qg,
                new_node_count,
                &mut prev_node,
            );

            if !expressions_above_grouped.is_empty() {
                new_node_count += 1;
            }

            // 3. Get columns used by each predicate. This will be used to check
            // if we need to reorder predicates before group_by nodes.
            let mut column_to_predicates: HashMap<Column, Vec<&Expression>> = HashMap::new();

            for rel in &sorted_rels {
                if *rel == "computed_columns" {
                    continue;
                }

                let qgn = qg.relations.get(*rel).ok_or_else(|| {
                    internal_err(format!("couldn't find {:?} in qg relations", rel))
                })?;
                for pred in qgn.predicates.iter().chain(&qg.global_predicates) {
                    for col in pred.referred_columns() {
                        column_to_predicates
                            .entry(col.into())
                            .or_default()
                            .push(pred);
                    }
                }
            }

            // 3a. Reorder some predicates before group by nodes
            // FIXME(malte): This doesn't currently work correctly with arithmetic and literal
            // projections that form input to these filters -- these need to be lifted above them
            // (and above the aggregations).
            let (created_predicates, predicates_above_group_by_nodes) =
                make_predicates_above_grouped(
                    self,
                    &format!("q_{:x}", qg.signature().hash),
                    qg,
                    &node_for_rel,
                    new_node_count,
                    &column_to_predicates,
                    &mut prev_node,
                )?;

            new_node_count += predicates_above_group_by_nodes.len();

            nodes_added = base_nodes
                .into_iter()
                .chain(join_nodes.into_iter())
                .chain(predicates_above_group_by_nodes.into_iter())
                .collect();
            let mut added_bogokey = false;

            let mut predicate_nodes = Vec::new();
            // 5. Generate the necessary filter nodes for local predicates associated with each
            // relation node in the query graph.
            //
            // Need to iterate over relations in a deterministic order, as otherwise nodes will be
            // added in a different order every time, which will yield different node identifiers
            // and make it difficult for applications to check what's going on.
            for rel in &sorted_rels {
                let qgn = qg.relations.get(*rel).ok_or_else(|| {
                    internal_err(format!("qg relations did not contain {:?}", rel))
                })?;
                // we've already handled computed columns
                if *rel == "computed_columns" {
                    continue;
                }

                // the following conditional is required to avoid "empty" nodes (without any
                // projected columns) that are required as inputs to joins
                if !qgn.predicates.is_empty() {
                    // add a predicate chain for each query graph node's predicates
                    for (i, ref p) in qgn.predicates.iter().enumerate() {
                        if created_predicates.contains(p) {
                            continue;
                        }

                        let parent = match prev_node {
                            None => node_for_rel.get(rel).cloned().ok_or_else(|| {
                                internal_err(format!("node_for_rel did not contain {:?}", rel))
                            })?,
                            Some(pn) => pn,
                        };

                        let fns = self.make_predicate_nodes(
                            &format!("q_{:x}_n{}_p{}", qg.signature().hash, new_node_count, i,),
                            parent,
                            p,
                            0,
                        )?;

                        invariant!(!fns.is_empty());
                        new_node_count += fns.len();
                        #[allow(clippy::unwrap_used)] // checked above
                        {
                            prev_node = Some(fns.iter().last().unwrap().clone());
                        }
                        predicate_nodes.extend(fns);
                    }
                }
            }

            let num_local_predicates = predicate_nodes.len();

            // 6. Determine literals and expressions that global predicates depend
            //    on and add them here; remembering that we've already added them-
            if let Some(projected) =
                self.make_value_project_node(qg, prev_node.clone(), new_node_count)?
            {
                new_node_count += 1;
                nodes_added.push(projected.clone());
                prev_node = Some(projected);
            }

            // 7. Global predicates
            for (i, ref p) in qg.global_predicates.iter().enumerate() {
                if created_predicates.contains(p) {
                    continue;
                }

                let parent = match prev_node {
                    None => internal!(),
                    Some(pn) => pn,
                };

                let fns = self.make_predicate_nodes(
                    &format!(
                        "q_{:x}_n{}_{}",
                        qg.signature().hash,
                        new_node_count,
                        num_local_predicates + i,
                    ),
                    parent,
                    p,
                    0,
                )?;

                invariant!(!fns.is_empty());
                new_node_count += fns.len();
                #[allow(clippy::unwrap_used)] // checked above
                {
                    prev_node = Some(fns.iter().last().unwrap().clone());
                }
                predicate_nodes.extend(fns);
            }

            // 8. Add function and grouped nodes
            let mut func_nodes: Vec<MirNodeRef> = make_grouped(
                self,
                &format!("q_{:x}", qg.signature().hash),
                qg,
                &node_for_rel,
                new_node_count,
                &mut prev_node,
                false,
                &expressions_above_grouped,
            )?;

            new_node_count += func_nodes.len();

            // 9. Get the final node
            let mut final_node: MirNodeRef = match prev_node {
                Some(n) => n,
                None => {
                    // no join, filter, or function node --> base node is parent
                    invariant_eq!(sorted_rels.len(), 1);
                    #[allow(clippy::unwrap_used)]
                    node_for_rel
                        .get(sorted_rels.last().unwrap())
                        .cloned()
                        .ok_or_else(|| {
                            internal_err("node_for_rel does not contain final node rel")
                        })?
                }
            };

            // 10. Potentially insert TopK node below the final node
            // XXX(malte): this adds a bogokey if there are no parameter columns to do the TopK
            // over, but we could end up in a stick place if we reconcile/combine multiple
            // queries (due to security universes or due to compound select queries) that do
            // not all have the bogokey!
            if let Some(ref limit) = st.limit {
                let group_by = if qg.parameters().is_empty() {
                    // need to add another projection to introduce a bogokey to group by
                    let cols: Vec<_> = final_node.borrow().columns().to_vec();
                    let table = format!("q_{:x}_n{}", qg.signature().hash, new_node_count);
                    let bogo_project = self.make_project_node(
                        &table,
                        final_node.clone(),
                        cols.iter().collect(),
                        vec![],
                        vec![("bogokey".into(), DataType::from(0i32))],
                        false,
                    );
                    new_node_count += 1;
                    nodes_added.push(bogo_project.clone());
                    final_node = bogo_project;
                    added_bogokey = true;
                    vec![Column::new(None, "bogokey")]
                } else {
                    qg.parameters()
                        .into_iter()
                        .map(|(col, _)| Column::from(col))
                        .collect()
                };

                let topk_node = self.make_topk_node(
                    &format!("q_{:x}_n{}", qg.signature().hash, new_node_count),
                    final_node,
                    group_by.iter().collect(),
                    &st.order,
                    limit,
                )?;
                func_nodes.push(topk_node.clone());
                final_node = topk_node;
                new_node_count += 1;
            }

            // we're now done with the query, so remember all the nodes we've added so far
            nodes_added.extend(func_nodes);
            nodes_added.extend(predicate_nodes);

            // 10. Generate leaf views that expose the query result
            let mut projected_columns: Vec<Column> = qg
                .columns
                .iter()
                .filter_map(|oc| match *oc {
                    OutputColumn::Expression(_) => None,
                    OutputColumn::Data {
                        ref column,
                        alias: ref name,
                    } => Some(Column::from(column).aliased_as(name.clone())),
                    OutputColumn::Literal(_) => None,
                })
                .collect();

            // We may already have added some of the expression and literal columns
            let (_, already_computed): (Vec<_>, Vec<_>) =
                value_columns_needed_for_predicates(&qg.columns, &qg.global_predicates)
                    .into_iter()
                    .unzip();
            let projected_expressions: Vec<(String, Expression)> = qg
                .columns
                .iter()
                .filter_map(|oc| match *oc {
                    OutputColumn::Expression(ref ac) => {
                        if !already_computed.contains(oc) {
                            Some((ac.name.clone(), ac.expression.clone()))
                        } else {
                            projected_columns.push(Column::new(None, &ac.name));
                            None
                        }
                    }
                    OutputColumn::Data { .. } => None,
                    OutputColumn::Literal(_) => None,
                })
                .collect();
            let mut projected_literals: Vec<(String, DataType)> = qg
                .columns
                .iter()
                .map(|oc| -> ReadySetResult<_> {
                    match *oc {
                        OutputColumn::Expression(_) => Ok(None),
                        OutputColumn::Data { .. } => Ok(None),
                        OutputColumn::Literal(ref lc) => {
                            if !already_computed.contains(oc) {
                                Ok(Some((lc.name.clone(), DataType::try_from(&lc.value)?)))
                            } else {
                                projected_columns.push(Column::new(None, &lc.name));
                                Ok(None)
                            }
                        }
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect();

            if added_bogokey {
                projected_columns.push(Column::new(None, "bogokey"));
            }

            // Convert the query parameters to an ordered list of columns that will comprise the
            // lookup key if a leaf node is attached.
            let key_columns = match qg.parameters()[..] {
                _ if !has_leaf => {
                    // If no leaf node is to be attached, the key_columns are unnecessary.
                    None
                }

                [] => {
                    // If there are no parameters, use a dummy "bogokey" for the lookup key.
                    // Ensure the "bogokey" is projected if that has not been done already.
                    if !projected_columns.contains(&Column::new(None, "bogokey")) {
                        projected_literals.push(("bogokey".into(), DataType::from(0i32)));
                    }
                    Some(vec![Column::new(None, "bogokey")])
                }

                #[cfg(feature = "param_filter")]
                [(column, operator @ (BinaryOperator::ILike | BinaryOperator::Like))] => {
                    // If parameters have non equality operators, insert a ParamFilter node to
                    // support key lookups over the query, and use the ParamFilter's emitted
                    // key column as the lookup key. Only a subset of non equality operators are
                    // supported.
                    let filter_key_column = Column::new(None, "__filter_key");
                    final_node = self.make_param_filter_node(
                        &format!("q_{:x}_n{}{}", qg.signature().hash, new_node_count, uformat),
                        final_node,
                        &Column::from(column),
                        &filter_key_column,
                        operator,
                    );
                    nodes_added.push(final_node.clone());
                    projected_columns.push(filter_key_column.clone());
                    new_node_count += 1;
                    Some(vec![filter_key_column])
                }

                _ => {
                    // If parameters have equality operators only, ensure their columns are
                    // projected and collect them into the lookup key.
                    for (pc, op) in qg.parameters() {
                        if *op != BinaryOperator::Equal {
                            unsupported!("Unsupported binary operator `{}`; only direct equality is supported", op);
                        }
                        let pc = Column::from(pc);
                        if !projected_columns.contains(&pc) {
                            projected_columns.push(pc);
                        }
                    }
                    Some(
                        qg.parameters()
                            .into_iter()
                            .map(|(col, _)| Column::from(col))
                            .collect(),
                    )
                }
            };

            if st.distinct {
                let name = if has_leaf {
                    format!("q_{:x}_n{}", qg.signature().hash, new_node_count)
                } else {
                    format!("{}_d{}", name, new_node_count)
                };
                let distinct_node = self.make_distinct_node(
                    &name,
                    final_node.clone(),
                    projected_columns.iter().collect(),
                );
                nodes_added.push(distinct_node.clone());
                final_node = distinct_node;
                new_node_count += 1;
            }

            let ident = if has_leaf {
                format!("q_{:x}_n{}", qg.signature().hash, new_node_count)
            } else {
                String::from(name)
            };

            let leaf_project_node = self.make_project_node(
                &ident,
                final_node,
                projected_columns.iter().collect(),
                projected_expressions,
                projected_literals,
                !has_leaf,
            );

            nodes_added.push(leaf_project_node.clone());

            if let Some(key_columns) = key_columns {
                // We are supposed to add a `MaterializedLeaf` node keyed on the query
                // parameters. For purely internal views (e.g., subqueries), this is not set.
                let columns = leaf_project_node
                    .borrow()
                    .columns()
                    .iter()
                    .cloned()
                    .map(|mut c| {
                        sanitize_leaf_column(&mut c, name);
                        c
                    })
                    .collect();

                let leaf_node = MirNode::new(
                    name,
                    self.schema_version,
                    columns,
                    MirNodeInner::Leaf {
                        node: leaf_project_node.clone(),
                        keys: key_columns,
                        order_by: st.order.as_ref().map(|order| {
                            order
                                .columns
                                .iter()
                                .cloned()
                                .map(|(col, ot)| (col.into(), ot))
                                .collect()
                        }),
                        limit: st.limit.as_ref().map(extract_limit).transpose()?,
                        returned_cols: Some({
                            let mut cols = st.fields
                                .iter()
                                .map(|expression| -> ReadySetResult<_> {
                                    match expression {
                                        FieldDefinitionExpression::All
                                        | FieldDefinitionExpression::AllInTable(_) => {
                                            internal!("All expression should have been desugared at this point")
                                        }
                                        FieldDefinitionExpression::Expression {
                                            alias: Some(alias),
                                            ..
                                        } => {
                                            Ok(Column::named(alias.clone()))
                                        }
                                        FieldDefinitionExpression::Expression {
                                            expr: Expression::Column(c),
                                            ..
                                        } => Ok(Column::from(c)),
                                        FieldDefinitionExpression::Expression {
                                            expr,
                                            ..
                                        } => Ok(Column::named(expr.to_string())),
                                    }
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            cols.retain(|e| e.name != "bogokey");
                            cols
                        }),
                        default_row: default_row_for_select(st),
                    },
                    vec![leaf_project_node],
                    vec![],
                );
                nodes_added.push(leaf_node);
            }

            debug!(%name, "Added final MIR node for query");
        }
        // finally, we output all the nodes we generated
        Ok(nodes_added)
    }
}
