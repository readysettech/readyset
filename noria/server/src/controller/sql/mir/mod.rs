use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::vec::Vec;

use ::serde::{Deserialize, Serialize};
use common::IndexType;
use dataflow::ops::grouped::aggregate::Aggregation;
use dataflow::ops::union;
use launchpad::redacted::Sensitive;
use lazy_static::lazy_static;
use mir::node::node_inner::MirNodeInner;
use mir::node::{GroupedNodeType, MirNode};
use mir::query::MirQuery;
pub use mir::Column;
use mir::MirNodeRef;
use nom_sql::analysis::ReferredColumns;
use nom_sql::{
    BinaryOperator, ColumnSpecification, CompoundSelectOperator, CreateTableStatement, Expression,
    FieldDefinitionExpression, FunctionExpression, LimitClause, Literal, OrderClause, OrderType,
    SelectStatement, SqlIdentifier, TableKey, UnaryOperator,
};
use noria::ViewPlaceholder;
use noria_data::DataType;
use noria_errors::{internal, internal_err, invariant, invariant_eq, unsupported, ReadySetError};
use noria_sql_passes::is_correlated;
use petgraph::graph::NodeIndex;
use tracing::{debug, error, trace, warn};

use super::query_graph::{extract_limit_offset, JoinPredicate};
use crate::controller::sql::mir::grouped::{
    make_expressions_above_grouped, make_grouped, make_predicates_above_grouped,
    post_lookup_aggregates,
};
use crate::controller::sql::mir::join::{make_cross_joins, make_joins};
use crate::controller::sql::query_graph::{to_query_graph, OutputColumn, Pagination, QueryGraph};
use crate::controller::sql::query_signature::Signature;
use crate::ReadySetResult;

mod grouped;
mod join;
pub(in crate::controller::sql) mod serde;

lazy_static! {
    pub static ref PAGE_NUMBER_COL: SqlIdentifier = "__page_number".into();
}

fn value_columns_needed_for_predicates(
    value_columns: &[OutputColumn],
    predicates: &[Expression],
) -> Vec<(Column, OutputColumn)> {
    let pred_columns: Vec<Column> = predicates
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
                    aliases: vec![],
                },
                oc.clone(),
            )),
            OutputColumn::Literal(ref lc) => Some((
                Column {
                    name: lc.name.clone(),
                    table: lc.table.clone(),
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
                    FunctionExpression::Call { .. } => DataType::None,
                },
                _ => DataType::None,
            })
            .collect(),
    )
}

/// Kinds of joins in MIR
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Inner joins - see [`MirNodeInner::InnerJoin`]
    Inner,
    /// Left joins - see [`MirNodeInner::LeftJoin`]
    Left,
    /// Dependent joins - see [`MirNodeInner::DependentJoin`]
    Dependent,
}

/// Configuration for how SQL is converted to MIR
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default)]
pub(crate) struct Config {
    /// If set to `true`, a SQL `ORDER BY` with `LIMIT` will emit a [`TopK`][] node. If set to
    /// `false`, the SQL conversion process returns a [`ReadySetError::Unsupported`], causing the
    /// adapter to send the query to fallback. Defaults to `false`.
    ///
    /// [`TopK`]: MirNodeInner::TopK
    pub(crate) allow_topk: bool,

    /// If set to 'true', a SQL 'ORDER BY' with 'LIMIT' and 'OFFSET' will emit a ['Paginate'][]
    /// node. If set to 'false', the SQL conversion process returns a
    /// ['ReadySetError::Unsupported'], causing the adapter to send the query to fallback. Defaults
    /// to 'false'.
    ///
    /// ['Paginate']: MirNodeInner::Paginate
    pub(crate) allow_paginate: bool,

    /// Enable support for mixing equality and range comparisons in a query. Support for mixed
    /// comparisons is currently unfinished, so these queries may return incorrect results.
    pub(crate) allow_mixed_comparisons: bool,
}

#[derive(Clone, Debug, Default)]
pub(super) struct SqlToMirConverter {
    pub(in crate::controller::sql) config: Config,
    pub(in crate::controller::sql) base_schemas:
        HashMap<SqlIdentifier, Vec<(usize, Vec<ColumnSpecification>)>>,
    pub(in crate::controller::sql) current: HashMap<SqlIdentifier, usize>,
    pub(in crate::controller::sql) nodes: HashMap<(SqlIdentifier, usize), MirNodeRef>,
    pub(in crate::controller::sql) schema_version: usize,
}

impl SqlToMirConverter {
    /// Returns a reference to the `nodes` map field
    /// Needed for deserialization, as the [`SqlToMirConverter`] is the
    /// source of truth to know the [`MirNodeRef`]s that compose the
    /// MIR graph.
    pub(super) fn nodes(&self) -> &HashMap<(SqlIdentifier, usize), MirNodeRef> {
        &self.nodes
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    /// Set the [`Config`]
    pub(crate) fn set_config(&mut self, config: Config) {
        self.config = config;
    }

    fn get_view(&self, view_name: &str) -> ReadySetResult<MirNodeRef> {
        let v = self
            .current
            .get(view_name)
            .ok_or_else(|| ReadySetError::ViewNotFound(view_name.into()))?;
        let node = self
            .nodes
            .get(&(SqlIdentifier::from(view_name), *v))
            .ok_or_else(|| {
                internal_err(format!(
                    "Inconsistency: view {} does not exist at v{}",
                    view_name, v
                ))
            })?;

        let reuse_node = MirNode::reuse(node.clone(), self.schema_version);

        Ok(reuse_node)
    }

    pub(super) fn add_leaf_below(
        &mut self,
        prior_leaf: MirNodeRef,
        name: &SqlIdentifier,
        params: &[Column],
        index_type: IndexType,
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
                format!("{}_reproject", name).into(),
                self.schema_version,
                MirNodeInner::Project {
                    emit: columns,
                    literals: vec![],
                    expressions: vec![],
                },
                vec![MirNodeRef::downgrade(&parent)],
                vec![],
            )
        } else {
            // add an identity node and then another leaf
            MirNode::new(
                format!("{}_id", name).into(),
                self.schema_version,
                MirNodeInner::Identity,
                vec![MirNodeRef::downgrade(&parent)],
                vec![],
            )
        };

        // TODO(DAN): placeholder to key mappings are not fully supported for reuse
        let params: Vec<(_, ViewPlaceholder)> = params
            .iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    c.clone(),
                    ViewPlaceholder::OneToOne(i + 1 /* placeholders are 1-based */),
                )
            })
            .collect();
        let new_leaf = MirNode::new(
            name.clone(),
            self.schema_version,
            MirNodeInner::leaf(params, index_type),
            vec![MirNodeRef::downgrade(&n)],
            vec![],
        );

        // always register leaves
        self.current.insert(name.clone(), self.schema_version);
        self.nodes
            .insert((name.clone(), self.schema_version), new_leaf.clone());

        // wrap in a (very short) query to return
        MirQuery {
            name: name.clone(),
            roots: vec![parent],
            leaf: new_leaf,
        }
    }

    pub(super) fn compound_query_to_mir(
        &mut self,
        name: &SqlIdentifier,
        sqs: Vec<&MirQuery>,
        op: CompoundSelectOperator,
        order: &Option<OrderClause>,
        limit: &Option<LimitClause>,
        has_leaf: bool,
    ) -> ReadySetResult<MirQuery> {
        let union_name = if !has_leaf && limit.is_none() {
            name.clone()
        } else {
            format!("{}_union", name).into()
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

        if let Some(limit) = limit {
            let (limit, offset) = extract_limit_offset(limit)?;
            let make_topk = offset.is_none();
            let paginate_name = if has_leaf {
                if make_topk {
                    format!("{}_topk", name)
                } else {
                    format!("{}_paginate", name)
                }
                .into()
            } else {
                name.clone()
            };

            // Either a topk or paginate node
            let group_by = final_node.borrow().columns();
            let paginate_node = self
                .make_paginate_node(
                    &paginate_name,
                    final_node,
                    group_by,
                    &order.as_ref().map(|o| {
                        o.order_by
                            .iter()
                            .map(|(e, ot)| (e.clone(), ot.unwrap_or(OrderType::OrderAscending)))
                            .collect()
                    }),
                    limit,
                    make_topk,
                )?
                .last()
                .unwrap()
                .clone();
            let node_id = (paginate_name, self.schema_version);
            self.nodes
                .entry(node_id)
                .or_insert_with(|| paginate_node.clone());
            final_node = paginate_node;
        }

        let alias_table = MirNode::new(
            if has_leaf {
                format!("{}_alias_table", name).into()
            } else {
                name.clone()
            },
            self.schema_version,
            MirNodeInner::AliasTable {
                table: name.clone(),
            },
            vec![MirNodeRef::downgrade(&final_node)],
            vec![],
        );

        // TODO: Initialize leaf with ordering?
        let leaf_node = if has_leaf {
            MirNode::new(
                name.clone(),
                self.schema_version,
                MirNodeInner::leaf(
                    vec![],
                    // TODO: is this right?
                    IndexType::HashMap,
                ),
                vec![MirNodeRef::downgrade(&alias_table)],
                vec![],
            )
        } else {
            alias_table
        };

        self.current
            .insert(leaf_node.borrow().name().clone(), self.schema_version);
        let node_id = (name.clone(), self.schema_version);
        self.nodes
            .entry(node_id)
            .or_insert_with(|| leaf_node.clone());

        Ok(MirQuery {
            name: name.clone(),
            roots: sqs.iter().fold(Vec::new(), |mut acc, mq| {
                acc.extend(mq.roots.iter().cloned());
                acc
            }),
            leaf: leaf_node,
        })
    }

    // pub(super) viz for tests
    pub(super) fn get_flow_node_address(
        &self,
        name: &SqlIdentifier,
        version: usize,
    ) -> Option<NodeIndex> {
        match self.nodes.get(&(name.clone(), version)) {
            None => None,
            Some(node) => node
                .borrow()
                .flow_node
                .as_ref()
                .map(|flow_node| flow_node.address()),
        }
    }

    pub(super) fn get_leaf(&self, name: &SqlIdentifier) -> Option<NodeIndex> {
        match self.current.get(name) {
            None => None,
            Some(v) => self.get_flow_node_address(name, *v),
        }
    }

    pub(super) fn named_base_to_mir(
        &mut self,
        name: &SqlIdentifier,
        ctq: &CreateTableStatement,
    ) -> ReadySetResult<MirQuery> {
        invariant_eq!(ctq.table.name, name);
        let n = self.make_base_node(name, &ctq.fields, ctq.keys.as_ref())?;
        let node_id = (name.clone(), self.schema_version);
        use std::collections::hash_map::Entry;
        if let Entry::Vacant(e) = self.nodes.entry(node_id) {
            self.current.insert(name.clone(), self.schema_version);
            e.insert(n.clone());
        }
        Ok(MirQuery::singleton(name, n))
    }

    pub(super) fn remove_query(
        &mut self,
        name: &SqlIdentifier,
        mq: &MirQuery,
    ) -> ReadySetResult<()> {
        use std::collections::VecDeque;

        let v = self
            .current
            .remove(name)
            .ok_or_else(|| internal_err(format!("no query named \"{}\"?", name)))?;

        let nodeid = (name.clone(), v);
        let leaf_mn = self.nodes.remove(&nodeid).ok_or_else(|| {
            internal_err(format!("could not find MIR node {:?} for removal", nodeid))
        })?;

        invariant_eq!(leaf_mn.borrow().name, mq.leaf.borrow().name);

        // traverse the MIR query backwards, removing any nodes that we still have registered.
        let mut q = VecDeque::new();
        q.push_back(leaf_mn);

        while let Some(mnr) = q.pop_front() {
            let n = mnr.borrow_mut();
            q.extend(n.ancestors.iter().map(|n| n.upgrade().unwrap()));
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

    pub(super) fn remove_base(
        &mut self,
        name: &SqlIdentifier,
        mq: &MirQuery,
    ) -> ReadySetResult<()> {
        debug!(%name, "Removing base node");
        self.remove_query(name, mq)?;
        if self.base_schemas.remove(name).is_none() {
            warn!(%name, "Attempted to remove non-existent base node");
        }
        Ok(())
    }

    pub(super) fn named_query_to_mir(
        &mut self,
        name: &SqlIdentifier,
        sq: &SelectStatement,
        qg: &QueryGraph,
        has_leaf: bool,
    ) -> Result<MirQuery, ReadySetError> {
        let nodes = self.make_nodes_for_selection(name, sq, qg, has_leaf)?;
        let mut roots = Vec::new();
        let mut leaves = Vec::new();
        for mn in nodes.into_iter() {
            let node_id = (mn.borrow().name().clone(), self.schema_version);
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
        self.current.insert(name.clone(), self.schema_version);

        Ok(MirQuery {
            name: name.clone(),
            roots,
            leaf,
        })
    }

    fn make_base_node(
        &mut self,
        name: &SqlIdentifier,
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
                    debug!(
                        %name,
                        %existing_version,
                        "base table already exists with identical schema; reusing it.",
                    );
                    let node_key = (name.clone(), existing_version);
                    let existing_node = self.nodes.get(&node_key).cloned().ok_or_else(|| {
                        internal_err(format!("could not find MIR node {:?} for reuse", node_key))
                    })?;
                    return Ok(MirNode::reuse(existing_node, self.schema_version));
                } else {
                    // match, but schema is different, so we'll need to either:
                    //  1) reuse the existing node, but add an upgrader for any changes in the
                    //     column set, or
                    //  2) give up and just make a new node
                    debug!(
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
                        let node_key = (name.clone(), existing_version);
                        let existing_node =
                            self.nodes.get(&node_key).cloned().ok_or_else(|| {
                                internal_err(format!(
                                    "couldn't find MIR node {:?} in add/remove cols",
                                    node_key
                                ))
                            })?;

                        let mut columns: Vec<ColumnSpecification> = existing_node
                            .borrow()
                            .column_specifications()?
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
                        let base_schemas = self.base_schemas.entry(name.clone()).or_default();
                        base_schemas.push((self.schema_version, columns));

                        return Ok(MirNode::adapt_base(
                            existing_node,
                            columns_added,
                            columns_removed,
                        ));
                    } else {
                        warn!("base table has complex schema change");
                        break;
                    }
                }
            }
        }

        // all columns on a base must have the base as their table
        invariant!(cols.iter().all(|c| c
            .column
            .table
            .as_ref()
            .map(|t| t == name)
            .unwrap_or(false)));

        // primary keys can either be specified directly (at the end of CREATE TABLE), or inline
        // with the definition of a field (i.e., as a ColumnConstraint).
        // We assume here that an earlier rewrite pass has coalesced all primary key definitions in
        // the TableKey structure.

        // For our unique keys, we want the very first key to be the primary key if present, as it
        // will be used as the primary index.
        // TODO: failing that we want to index on a unique integer key, failing that on whatever.

        let (primary_key, unique_keys) = match keys {
            None => (None, vec![].into()),
            Some(keys) => {
                let primary_key = keys.iter().find_map(|k| match k {
                    TableKey::PrimaryKey { columns, .. } => {
                        Some(columns.iter().map(Column::from).collect::<Box<[Column]>>())
                    }
                    _ => None,
                });

                let unique_keys = keys.iter().filter_map(|k| match k {
                    TableKey::UniqueKey { columns, .. } => {
                        Some(columns.iter().map(Column::from).collect::<Box<[Column]>>())
                    }
                    _ => None,
                });

                (primary_key, unique_keys.collect::<Box<[_]>>())
            }
        };

        // remember the schema for this version
        let base_schemas = self.base_schemas.entry(name.clone()).or_default();
        base_schemas.push((self.schema_version, cols.to_vec()));

        Ok(MirNode::new(
            name.clone(),
            self.schema_version,
            MirNodeInner::Base {
                column_specs: cols.iter().map(|cs| (cs.clone(), None)).collect(),
                primary_key,
                unique_keys,
                adapted_over: None,
            },
            vec![],
            vec![],
        ))
    }

    fn make_union_node(
        &self,
        name: &SqlIdentifier,
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
                .all(|a| a.borrow().columns().iter().any(|ac| c.name == ac.name))
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
                    acols.push(Column::named(ac.name));
                }
            }
            emit.push(acols);
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
            name.clone(),
            self.schema_version,
            MirNodeInner::Union {
                emit,
                duplicate_mode,
            },
            ancestors.iter().map(MirNodeRef::downgrade).collect(),
            vec![],
        ))
    }

    fn make_union_from_same_base(
        &self,
        name: &SqlIdentifier,
        ancestors: Vec<MirNodeRef>,
        columns: Vec<Column>,
        duplicate_mode: union::DuplicateMode,
    ) -> ReadySetResult<MirNodeRef> {
        invariant!(ancestors.len() > 1, "union must have more than 1 ancestors");
        trace!(%name, ?columns, "Added union node");
        let emit = ancestors.iter().map(|_| columns.clone()).collect();

        Ok(MirNode::new(
            name.clone(),
            self.schema_version,
            MirNodeInner::Union {
                emit,
                duplicate_mode,
            },
            ancestors.iter().map(MirNodeRef::downgrade).collect(),
            vec![],
        ))
    }

    fn make_filter_node(
        &self,
        name: &SqlIdentifier,
        parent: MirNodeRef,
        conditions: Expression,
    ) -> MirNodeRef {
        trace!(%name, %conditions, "Added filter node");
        MirNode::new(
            name.clone(),
            self.schema_version,
            MirNodeInner::Filter { conditions },
            vec![MirNodeRef::downgrade(&parent)],
            vec![],
        )
    }

    fn make_aggregate_node(
        &self,
        name: &SqlIdentifier,
        func_col: Column,
        function: FunctionExpression,
        group_cols: Vec<Column>,
        parent: MirNodeRef,
        projected_exprs: &HashMap<Expression, SqlIdentifier>,
    ) -> ReadySetResult<Vec<MirNodeRef>> {
        use dataflow::ops::grouped::extremum::Extremum;
        use nom_sql::FunctionExpression::*;

        macro_rules! mk_error {
            ($expression:expr) => {
                internal_err(format!(
                    "projected_exprs does not contain {:?}",
                    Sensitive($expression)
                ))
            };
        }

        let mut out_nodes = Vec::new();

        let mknode = |over: Column, t: GroupedNodeType, distinct: bool| {
            if distinct {
                let new_name: SqlIdentifier = format!("{}_d{}", name, out_nodes.len()).into();
                let mut dist_col = vec![over.clone()];
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

        Ok(match function {
            Sum {
                expr: box Expression::Column(col),
                distinct,
            } => mknode(
                Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::Sum),
                distinct,
            ),
            Sum { expr, distinct } => mknode(
                // TODO(celine): replace with ParentRef
                Column::named(
                    projected_exprs
                        .get(&expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(&*expr))?,
                ),
                GroupedNodeType::Aggregation(Aggregation::Sum),
                distinct,
            ),
            CountStar => {
                internal!("COUNT(*) should have been rewritten earlier!")
            }
            Count {
                expr: box Expression::Column(col),
                distinct,
                count_nulls,
            } => mknode(
                Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::Count { count_nulls }),
                distinct,
            ),
            Count {
                ref expr,
                distinct,
                count_nulls,
            } => mknode(
                // TODO(celine): replace with ParentRef
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(&*expr))?,
                ),
                GroupedNodeType::Aggregation(Aggregation::Count { count_nulls }),
                distinct,
            ),
            Avg {
                expr: box Expression::Column(col),
                distinct,
            } => mknode(
                Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::Avg),
                distinct,
            ),
            Avg { ref expr, distinct } => mknode(
                // TODO(celine): replace with ParentRef
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(&*expr))?,
                ),
                GroupedNodeType::Aggregation(Aggregation::Avg),
                distinct,
            ),
            // TODO(atsakiris): Support Filters for Extremum/GroupConcat
            // CH: https://app.clubhouse.io/readysettech/story/198
            Max(box Expression::Column(col)) => mknode(
                Column::from(col),
                GroupedNodeType::Extremum(Extremum::Max),
                false,
            ),
            Max(ref expr) => mknode(
                // TODO(celine): replace with ParentRef
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(&*expr))?,
                ),
                GroupedNodeType::Extremum(Extremum::Max),
                false,
            ),
            Min(box Expression::Column(col)) => mknode(
                Column::from(col),
                GroupedNodeType::Extremum(Extremum::Min),
                false,
            ),
            Min(ref expr) => mknode(
                // TODO(celine): replace with ParentRef
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(&*expr))?,
                ),
                GroupedNodeType::Extremum(Extremum::Min),
                false,
            ),
            GroupConcat {
                expr: box Expression::Column(col),
                separator,
            } => mknode(
                Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::GroupConcat { separator }),
                false,
            ),
            _ => {
                internal!("not an aggregate: {:?}", Sensitive(&function));
            }
        })
    }

    fn make_grouped_node(
        &self,
        name: &SqlIdentifier,
        output_column: Column,
        (parent_node, on): (MirNodeRef, Column),
        group_by: Vec<Column>,
        node_type: GroupedNodeType,
    ) -> MirNodeRef {
        match node_type {
            GroupedNodeType::Aggregation(kind) => MirNode::new(
                name.clone(),
                self.schema_version,
                MirNodeInner::Aggregation {
                    on,
                    group_by,
                    output_column,
                    kind,
                },
                vec![MirNodeRef::downgrade(&parent_node)],
                vec![],
            ),
            GroupedNodeType::Extremum(kind) => MirNode::new(
                name.clone(),
                self.schema_version,
                MirNodeInner::Extremum {
                    on,
                    group_by,
                    output_column,
                    kind,
                },
                vec![MirNodeRef::downgrade(&parent_node)],
                vec![],
            ),
        }
    }

    fn make_join_node(
        &self,
        name: &SqlIdentifier,
        join_predicates: &[JoinPredicate],
        left_node: MirNodeRef,
        right_node: MirNodeRef,
        kind: JoinKind,
    ) -> ReadySetResult<MirNodeRef> {
        // TODO(malte): this is where we overproject join columns in order to increase reuse
        // opportunities. Technically, we need to only project those columns here that the query
        // actually needs; at a minimum, we could start with just the join colums, relying on the
        // automatic column pull-down to retrieve the remaining columns required.
        let projected_cols_left = left_node.borrow().columns().to_vec();
        let projected_cols_right = right_node.borrow().columns().to_vec();
        let mut project = projected_cols_left
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

            if kind == JoinKind::Inner {
                // for inner joins, don't duplicate the join column in the output, but instead add
                // aliases to the columns that represent it going forward (viz., the left-side join
                // column)
                l_col.add_alias(&r_col);
                // add the alias to all instances of `l_col` in `fields` (there might be more than
                // one if `l_col` is explicitly projected multiple times)
                project = project
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
            JoinKind::Inner => MirNodeInner::Join {
                on_left: left_join_columns,
                on_right: right_join_columns,
                project,
            },
            JoinKind::Left => MirNodeInner::LeftJoin {
                on_left: left_join_columns,
                on_right: right_join_columns,
                project,
            },
            JoinKind::Dependent => MirNodeInner::DependentJoin {
                on_left: left_join_columns,
                on_right: right_join_columns,
                project,
            },
        };
        trace!(?inner, "Added join node");
        Ok(MirNode::new(
            name.clone(),
            self.schema_version,
            inner,
            vec![
                MirNodeRef::downgrade(&left_node),
                MirNodeRef::downgrade(&right_node),
            ],
            vec![],
        ))
    }

    fn make_join_aggregates_node(
        &self,
        name: &SqlIdentifier,
        aggregates: &[MirNodeRef; 2],
    ) -> ReadySetResult<MirNodeRef> {
        trace!("Added join aggregates node");
        Ok(MirNode::new(
            name.clone(),
            self.schema_version,
            MirNodeInner::JoinAggregates,
            aggregates.iter().map(MirNodeRef::downgrade).collect(),
            vec![],
        ))
    }

    fn make_projection_helper(
        &self,
        name: &SqlIdentifier,
        parent: MirNodeRef,
        fn_cols: Vec<Column>,
    ) -> MirNodeRef {
        self.make_project_node(
            name,
            parent,
            fn_cols,
            vec![],
            vec![("grp".into(), DataType::from(0i32))],
        )
    }

    fn make_project_node(
        &self,
        name: &SqlIdentifier,
        parent_node: MirNodeRef,
        emit: Vec<Column>,
        expressions: Vec<(SqlIdentifier, Expression)>,
        literals: Vec<(SqlIdentifier, DataType)>,
    ) -> MirNodeRef {
        MirNode::new(
            name.clone(),
            self.schema_version,
            MirNodeInner::Project {
                emit,
                literals,
                expressions,
            },
            vec![MirNodeRef::downgrade(&parent_node)],
            vec![],
        )
    }

    fn make_distinct_node(
        &self,
        name: &SqlIdentifier,
        parent: MirNodeRef,
        group_by: Vec<Column>,
    ) -> MirNodeRef {
        MirNode::new(
            name.clone(),
            self.schema_version,
            MirNodeInner::Distinct { group_by },
            vec![MirNodeRef::downgrade(&parent)],
            vec![],
        )
    }

    fn make_paginate_node(
        &self,
        name: &SqlIdentifier,
        mut parent: MirNodeRef,
        group_by: Vec<Column>,
        order: &Option<Vec<(Expression, OrderType)>>,
        limit: usize,
        is_topk: bool,
    ) -> ReadySetResult<Vec<MirNodeRef>> {
        if !self.config.allow_topk && is_topk {
            unsupported!("TopK is not supported");
        } else if !self.config.allow_paginate && !is_topk {
            unsupported!("Paginate is not supported");
        }

        // Gather a list of expressions we need to evaluate before the paginate node
        let mut exprs_to_project = vec![];
        let order = order.as_ref().map(|oc| {
            oc.iter()
                .map(|(expr, ot)| {
                    (
                        match expr {
                            Expression::Column(col) => Column::from(col),
                            expr => {
                                let col = Column::named(expr.to_string());
                                if parent.borrow().column_id_for_column(&col).err().iter().any(
                                    |err| matches!(err, ReadySetError::NonExistentColumn { .. }),
                                ) {
                                    // Only project the expression if we haven't already
                                    exprs_to_project.push(expr.clone());
                                }
                                col
                            }
                        },
                        *ot,
                    )
                })
                .collect()
        });

        let mut nodes = vec![];

        // If we're ordering on non-column expressions, add an extra node to project those first
        if !exprs_to_project.is_empty() {
            let parent_columns = parent.borrow().columns();
            let project_node = self.make_project_node(
                &format!("{}_proj", name).into(),
                parent.clone(),
                parent_columns,
                exprs_to_project
                    .into_iter()
                    .map(|expr| (expr.to_string().into(), expr))
                    .collect(),
                vec![],
            );
            nodes.push(project_node.clone());
            parent = project_node;
        }

        // make the new operator and record its metadata
        let paginate_node = if is_topk {
            MirNode::new(
                name.clone(),
                self.schema_version,
                MirNodeInner::TopK {
                    order,
                    group_by,
                    limit,
                },
                vec![MirNodeRef::downgrade(&parent)],
                vec![],
            )
        } else {
            MirNode::new(
                name.clone(),
                self.schema_version,
                MirNodeInner::Paginate {
                    order,
                    group_by,
                    limit,
                },
                vec![MirNodeRef::downgrade(&parent)],
                vec![],
            )
        };

        nodes.push(paginate_node);

        Ok(nodes)
    }

    fn make_predicate_nodes(
        &self,
        name: &SqlIdentifier,
        parent: MirNodeRef,
        ce: &Expression,
        nc: usize,
    ) -> ReadySetResult<Vec<MirNodeRef>> {
        let mut pred_nodes: Vec<MirNodeRef> = Vec::new();
        let output_cols = parent.borrow().columns().to_vec();
        match ce {
            Expression::BinaryOp {
                lhs,
                op: BinaryOperator::And,
                rhs,
            } => {
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
            Expression::BinaryOp {
                lhs,
                op: BinaryOperator::Or,
                rhs,
            } => {
                let left = self.make_predicate_nodes(name, parent.clone(), lhs, nc)?;
                let right = self.make_predicate_nodes(name, parent, rhs, nc + left.len())?;

                debug!("Creating union node for `or` predicate");

                invariant!(!left.is_empty());
                invariant!(!right.is_empty());
                #[allow(clippy::unwrap_used)] // checked above
                let last_left = left.last().unwrap().clone();
                #[allow(clippy::unwrap_used)] // checked above
                let last_right = right.last().unwrap().clone();
                let union = self.make_union_from_same_base(
                    &format!("{}_un{}", name, nc + left.len() + right.len()).into(),
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
            Expression::UnaryOp {
                op: UnaryOperator::Not | UnaryOperator::Neg,
                ..
            } => internal!("negation should have been removed earlier"),
            Expression::Literal(_) | Expression::Column(_) => {
                let f = self.make_filter_node(
                    &format!("{}_f{}", name, nc).into(),
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
            Expression::Exists(subquery) => {
                let qg = to_query_graph(subquery)?;
                let nodes = self.make_nodes_for_selection(name, subquery, &qg, false)?;
                let subquery_leaf = nodes
                    .iter()
                    .find(|n| n.borrow().children().is_empty())
                    .ok_or_else(|| internal_err("MIR query missing leaf!"))?
                    .clone();
                pred_nodes.extend(nodes);

                // -> π[lit: 0, lit: 0]
                let group_proj = self.make_project_node(
                    &format!("{}_prj_hlpr", name).into(),
                    subquery_leaf,
                    vec![],
                    vec![],
                    vec![
                        ("__count_val".into(), DataType::from(0u32)),
                        ("__count_grp".into(), DataType::from(0u32)),
                    ],
                );
                pred_nodes.push(group_proj.clone());
                // -> [0, 0] for each row

                // -> |0| γ[1]
                let exists_count_col = Column::named("__exists_count");
                let exists_count_node = self.make_grouped_node(
                    &format!("{}_count", name).into(),
                    exists_count_col,
                    (group_proj, Column::named("__count_val")),
                    vec![Column::named("__count_grp")],
                    GroupedNodeType::Aggregation(Aggregation::Count { count_nulls: true }),
                );
                pred_nodes.push(exists_count_node.clone());
                // -> [0, <count>] for each row

                // -> σ[c1 > 0]
                let gt_0_filter = self.make_filter_node(
                    &format!("{}_count_gt_0", name).into(),
                    exists_count_node,
                    Expression::BinaryOp {
                        lhs: Box::new(Expression::Column("__exists_count".into())),
                        op: BinaryOperator::Greater,
                        rhs: Box::new(Expression::Literal(Literal::Integer(0))),
                    },
                );
                pred_nodes.push(gt_0_filter.clone());

                // left -> π[...left, lit: 0]
                let parent_columns = parent.borrow().columns().to_vec();
                let left_literal_join_key_proj = self.make_project_node(
                    &format!("{}_join_key", name).into(),
                    parent,
                    parent_columns,
                    vec![],
                    vec![("__exists_join_key".into(), DataType::from(0u32))],
                );
                pred_nodes.push(left_literal_join_key_proj.clone());

                // -> ⋈ on: l.__exists_join_key ≡ r.__count_grp
                let exists_join = self.make_join_node(
                    &format!("{}_join", name).into(),
                    &[JoinPredicate {
                        left: Expression::Column("__exists_join_key".into()),
                        right: Expression::Column("__count_grp".into()),
                    }],
                    left_literal_join_key_proj,
                    gt_0_filter,
                    if is_correlated(subquery) {
                        JoinKind::Dependent
                    } else {
                        JoinKind::Inner
                    },
                )?;

                pred_nodes.push(exists_join);
            }
            Expression::Call(_) => {
                internal!("Function calls should have been handled by projection earlier")
            }
            Expression::NestedSelect(_) => unsupported!("Nested selects not supported in filters"),
            _ => {
                let f =
                    self.make_filter_node(&format!("{}_f{}", name, nc).into(), parent, ce.clone());
                pred_nodes.push(f);
            }
        }

        Ok(pred_nodes)
    }

    fn predicates_above_group_by<'a>(
        &self,
        name: &SqlIdentifier,
        column_to_predicates: &HashMap<nom_sql::Column, Vec<&'a Expression>>,
        over_col: &nom_sql::Column,
        parent: MirNodeRef,
        created_predicates: &mut Vec<&'a Expression>,
    ) -> ReadySetResult<Vec<MirNodeRef>> {
        let mut predicates_above_group_by_nodes = Vec::new();
        let mut prev_node = parent;

        let ces = column_to_predicates.get(over_col).unwrap();
        for ce in ces {
            // If we have two aggregates over the same column, we will skip this step for the
            // second aggregate
            if !created_predicates.contains(ce) {
                let mpns = self.make_predicate_nodes(
                    &format!("{}_mp{}", name, predicates_above_group_by_nodes.len()).into(),
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
            let projected_expressions: Vec<(SqlIdentifier, Expression)> =
                arith_and_lit_columns_needed
                    .iter()
                    .filter_map(|&(_, ref oc)| match oc {
                        OutputColumn::Expression(ref ac) => {
                            Some((ac.name.clone(), ac.expression.clone()))
                        }
                        OutputColumn::Data { .. } => None,
                        OutputColumn::Literal(_) => None,
                    })
                    .collect();
            let projected_literals: Vec<(SqlIdentifier, DataType)> = arith_and_lit_columns_needed
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
                &format!("q_{:x}_n{}", qg.signature().hash, node_count).into(),
                parent,
                passthru_cols,
                projected_expressions,
                projected_literals,
            );

            Some(projected)
        } else {
            None
        })
    }

    /// Returns list of nodes added
    #[allow(clippy::cognitive_complexity)]
    fn make_nodes_for_selection(
        &self,
        name: &SqlIdentifier,
        st: &SelectStatement,
        qg: &QueryGraph,
        has_leaf: bool,
    ) -> Result<Vec<MirNodeRef>, ReadySetError> {
        // TODO: make this take &self!

        let mut nodes_added: Vec<MirNodeRef> = vec![];
        let mut new_node_count = 0;

        // Canonical operator order: B-J-F-G-P-R
        // (Base, Join, Filter, GroupBy, Project, Reader)
        {
            let mut node_for_rel: HashMap<&SqlIdentifier, MirNodeRef> = HashMap::default();
            let mut correlated_relations: HashSet<&SqlIdentifier> = Default::default();

            // Convert the query parameters to an ordered list of columns that will comprise the
            // lookup key if a leaf node is attached.
            let view_key = qg.view_key(self.config())?;

            // 0. Base nodes (always reused)
            let mut base_nodes: Vec<MirNodeRef> = Vec::new();
            let mut sorted_rels: Vec<&SqlIdentifier> = qg.relations.keys().collect();
            sorted_rels.sort_unstable();
            for rel_name in &sorted_rels {
                let base_for_rel =
                    if let Some((subgraph, subquery)) = &qg.relations[*rel_name].subgraph {
                        let sub_nodes =
                            self.make_nodes_for_selection(rel_name, subquery, subgraph, false)?;
                        let leaf = sub_nodes
                            .iter()
                            .find(|n| n.borrow().children().is_empty())
                            .ok_or_else(|| internal_err("MIR query missing leaf!"))?
                            .clone();
                        nodes_added.extend(sub_nodes);
                        if is_correlated(subquery) {
                            correlated_relations.insert(rel_name);
                        }
                        leaf
                    } else {
                        self.get_view(rel_name)?
                    };

                nodes_added.push(base_for_rel.clone());

                let alias_table_node_name = format!(
                    "q_{:x}_{}_alias_table_{}",
                    qg.signature().hash,
                    base_for_rel.borrow().name(),
                    rel_name
                )
                .into();
                let alias_table_node = MirNode::new(
                    alias_table_node_name,
                    self.schema_version,
                    MirNodeInner::AliasTable {
                        table: (*rel_name).clone(),
                    },
                    vec![MirNodeRef::downgrade(&base_for_rel)],
                    vec![],
                );

                base_nodes.push(alias_table_node.clone());
                node_for_rel.insert(*rel_name, alias_table_node);
            }

            let join_nodes = make_joins(
                self,
                &format!("q_{:x}", qg.signature().hash).into(),
                qg,
                &node_for_rel,
                &correlated_relations,
                new_node_count,
            )?;

            new_node_count += join_nodes.len();

            let mut prev_node = match join_nodes.last() {
                Some(n) => Some(n.clone()),
                None => {
                    invariant!(!base_nodes.is_empty());
                    if base_nodes.len() > 1 {
                        // If we have more than one base node, that means we have a list of tables
                        // that don't have (obvious) join clauses we can pull out of the conditions.
                        // So we need to make no-condition (cross) joins for those tables.
                        //
                        // Later, the optimizer might decide to add conditions to the joins anyway.
                        make_cross_joins(
                            self,
                            &format!("q_{:x}", qg.signature().hash),
                            &mut new_node_count,
                            base_nodes.clone(),
                            &correlated_relations,
                        )?
                        .last()
                        .cloned()
                    } else {
                        #[allow(clippy::unwrap_used)] // checked above
                        Some(base_nodes.last().unwrap().clone())
                    }
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
            let mut column_to_predicates: HashMap<nom_sql::Column, Vec<&Expression>> =
                HashMap::new();

            for rel in &sorted_rels {
                let qgn = qg.relations.get(*rel).ok_or_else(|| {
                    internal_err(format!("couldn't find {:?} in qg relations", rel))
                })?;
                for pred in qgn.predicates.iter().chain(&qg.global_predicates) {
                    for col in pred.referred_columns() {
                        column_to_predicates
                            .entry(col.clone())
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
                    &format!("q_{:x}", qg.signature().hash).into(),
                    qg,
                    &node_for_rel,
                    new_node_count,
                    &column_to_predicates,
                    &mut prev_node,
                )?;

            new_node_count += predicates_above_group_by_nodes.len();

            nodes_added.extend(
                base_nodes
                    .into_iter()
                    .chain(join_nodes.into_iter())
                    .chain(predicates_above_group_by_nodes.into_iter()),
            );

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
                            &format!("q_{:x}_n{}_p{}", qg.signature().hash, new_node_count, i)
                                .into(),
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
                    )
                    .into(),
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
                &format!("q_{:x}", qg.signature().hash).into(),
                qg,
                &node_for_rel,
                new_node_count,
                &mut prev_node,
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

            // 10. Potentially insert TopK or Paginate node below the final node
            // XXX(malte): this adds a bogokey if there are no parameter columns to do the TopK
            // over, but we could end up in a stick place if we reconcile/combine multiple
            // queries (due to security universes or due to compound select queries) that do
            // not all have the bogokey!

            // Indicates whether the final project should include a bogokey. This is required when
            // we have a TopK node that groups on a bogokey. However, it is not required for
            // Paginate nodes that group on a bogokey, as they will project a page number field
            let mut bogo_in_final_projection = false;
            let mut create_paginate = false;
            if let Some(Pagination {
                order,
                limit,
                offset,
            }) = qg.pagination.as_ref()
            {
                let make_topk = offset.is_none();
                let group_by = if qg.parameters().is_empty() {
                    // need to add another projection to introduce a bogokey to group by if there
                    // are no query parameters
                    let cols: Vec<_> = final_node.borrow().columns().to_vec();
                    let table = format!("q_{:x}_n{}", qg.signature().hash, new_node_count).into();
                    let bogo_project = self.make_project_node(
                        &table,
                        final_node.clone(),
                        cols,
                        vec![],
                        vec![("bogokey".into(), DataType::from(0i32))],
                    );
                    new_node_count += 1;
                    nodes_added.push(bogo_project.clone());
                    final_node = bogo_project;
                    // Indicates whether we need a bogokey at the leaf node. This is the case for
                    // topk nodes that group by a bogokey. However, this is not the case for
                    // paginate nodes as they will project a page number
                    bogo_in_final_projection = make_topk;
                    create_paginate = !make_topk;
                    vec![Column::new(None, "bogokey")]
                } else {
                    // view key will have the offset parameter if it exists. We must filter it out
                    // of the group by, because the column originates at this node
                    view_key
                        .columns
                        .iter()
                        .filter_map(|(col, _)| {
                            if col.name != *PAGE_NUMBER_COL {
                                Some(col.clone())
                            } else {
                                None
                            }
                        })
                        .collect()
                };

                // Order by expression projections and either a topk or paginate node
                let paginate_nodes = self.make_paginate_node(
                    &format!("q_{:x}_n{}", qg.signature().hash, new_node_count).into(),
                    final_node,
                    group_by,
                    order,
                    *limit,
                    make_topk,
                )?;
                func_nodes.extend(paginate_nodes.clone());
                final_node = paginate_nodes.last().unwrap().clone();
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
            let projected_expressions: Vec<(SqlIdentifier, Expression)> = qg
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
            let mut projected_literals: Vec<(SqlIdentifier, DataType)> = qg
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

            // Bogokey will not be added to post-paginate project nodes
            if bogo_in_final_projection {
                projected_columns.push(Column::new(None, "bogokey"));
            }

            if has_leaf {
                if qg.parameters().is_empty()
                    && !create_paginate
                    && !projected_columns.contains(&Column::new(None, "bogokey"))
                {
                    projected_literals.push(("bogokey".into(), DataType::from(0i32)));
                } else {
                    for (column, _) in &view_key.columns {
                        if !projected_columns.contains(column) {
                            projected_columns.push(column.clone())
                        }
                    }
                }
            }

            if st.distinct {
                let name = if has_leaf {
                    format!("q_{:x}_n{}", qg.signature().hash, new_node_count).into()
                } else {
                    format!("{}_d{}", name, new_node_count).into()
                };
                let distinct_node =
                    self.make_distinct_node(&name, final_node.clone(), projected_columns.clone());
                nodes_added.push(distinct_node.clone());
                final_node = distinct_node;
            }

            let leaf_project_node = self.make_project_node(
                &if has_leaf {
                    format!("q_{:x}_leaf_project", qg.signature().hash).into()
                } else {
                    name.into()
                },
                final_node,
                projected_columns,
                projected_expressions,
                projected_literals,
            );
            nodes_added.push(leaf_project_node.clone());

            if has_leaf {
                // We are supposed to add a `Leaf` node keyed on the query parameters. For purely
                // internal views (e.g., subqueries), this is not set.

                let aggregates = if view_key.index_type != IndexType::HashMap {
                    post_lookup_aggregates(qg, st, name)?
                } else {
                    None
                };

                let leaf_node = MirNode::new(
                    name.clone(),
                    self.schema_version,
                    MirNodeInner::Leaf {
                        keys: view_key
                            .columns
                            .into_iter()
                            .map(|(col, placeholder)| (col, placeholder))
                            .collect(),
                        index_type: view_key.index_type,
                        order_by: st.order.as_ref().map(|order| {
                            order
                                .order_by
                                .iter()
                                .cloned()
                                .map(|(expr, ot)| {
                                    (
                                        match expr {
                                            Expression::Column(col) => Column::from(col),
                                            expr => Column::named(expr.to_string()),
                                        },
                                        ot.unwrap_or(OrderType::OrderAscending),
                                    )
                                })
                                .collect()
                        }),
                        limit: qg.pagination.as_ref().map(|p| p.limit),
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
                        aggregates,
                    },
                    vec![MirNodeRef::downgrade(&leaf_project_node)],
                    vec![],
                );
                nodes_added.push(leaf_node);
            }

            debug!(%name, "Added final MIR node for query");
        }
        // finally, we output all the nodes we generated
        Ok(nodes_added)
    }

    /// Upgrades the schema version of the MIR nodes.
    pub(super) fn upgrade_version(&mut self) {
        self.schema_version += 1;
    }
}
