use mir::node::{GroupedNodeType, MirNode, MirNodeType};
use mir::query::MirQuery;
use mir::{Column, MirNodeRef};
use noria::DataType;
use petgraph::graph::NodeIndex;
// TODO(malte): remove if possible
use dataflow::ops::filter::FilterCondition;
use dataflow::ops::join::JoinType;

use crate::controller::sql::query_graph::{OutputColumn, QueryGraph};
use crate::controller::sql::query_signature::Signature;
use nom_sql::{
    ArithmeticExpression, ColumnSpecification, CompoundSelectOperator, ConditionBase,
    ConditionExpression, ConditionTree, Literal, Operator, SqlQuery, TableKey,
};
use nom_sql::{LimitClause, OrderClause, SelectStatement};

use slog;
use std::collections::{HashMap, HashSet};

use std::ops::Deref;
use std::vec::Vec;

use crate::controller::sql::security::Universe;
use crate::controller::sql::UniverseId;

mod grouped;
mod join;
mod rewrite;
mod security;

fn sanitize_leaf_column(c: &mut Column, view_name: &str) {
    c.table = Some(view_name.to_string());
    c.function = None;
    c.aliases = vec![];
}

/// Returns all collumns used in a predicate
fn predicate_columns(ce: &ConditionExpression) -> HashSet<Column> {
    use nom_sql::ConditionExpression::*;

    let mut cols = HashSet::new();
    match *ce {
        LogicalOp(ref ct) | ComparisonOp(ref ct) => {
            cols.extend(predicate_columns(&ct.left));
            cols.extend(predicate_columns(&ct.right));
        }
        Base(ConditionBase::Field(ref c)) => {
            cols.insert(Column::from(c));
        }
        Bracketed(ref ce) => {
            cols.extend(predicate_columns(&ce));
        }
        NegationOp(_) => unreachable!("negations should have been eliminated"),
        _ => (),
    }

    cols
}

fn value_columns_needed_for_predicates(
    value_columns: &[OutputColumn],
    predicates: &[ConditionExpression],
) -> Vec<(Column, OutputColumn)> {
    let pred_columns: HashSet<_> = predicates.iter().fold(HashSet::new(), |mut acc, p| {
        acc.extend(predicate_columns(p));
        acc
    });

    value_columns
        .iter()
        .filter_map(|oc| match *oc {
            OutputColumn::Arithmetic(ref ac) => Some((
                Column {
                    name: ac.name.clone(),
                    table: ac.table.clone(),
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
            OutputColumn::Data(_) => None,
        })
        .filter(|(c, _)| pred_columns.contains(c))
        .collect()
}

#[derive(Clone, Debug)]
pub(super) struct SqlToMirConverter {
    base_schemas: HashMap<String, Vec<(usize, Vec<ColumnSpecification>)>>,
    current: HashMap<String, usize>,
    log: slog::Logger,
    nodes: HashMap<(String, usize), MirNodeRef>,
    schema_version: usize,

    /// Universe in which the conversion is happening
    universe: Universe,
}

impl Default for SqlToMirConverter {
    fn default() -> Self {
        SqlToMirConverter {
            base_schemas: HashMap::default(),
            current: HashMap::default(),
            log: slog::Logger::root(slog::Discard, o!()),
            nodes: HashMap::default(),
            schema_version: 0,
            universe: Universe::default(),
        }
    }
}

impl SqlToMirConverter {
    pub(super) fn with_logger(log: slog::Logger) -> Self {
        SqlToMirConverter {
            log,
            ..Default::default()
        }
    }

    /// Set universe in which the conversion will happen.
    /// We need this, because different universes will have different
    /// security policies and therefore different nodes that are not
    /// represent in the the query graph
    pub(super) fn set_universe(&mut self, universe: Universe) {
        self.universe = universe;
    }

    /// Set the universe to a policy-free universe
    pub(super) fn clear_universe(&mut self) {
        self.universe = Universe::default();
    }

    fn get_view(&self, view_name: &str) -> Result<MirNodeRef, String> {
        self.current
            .get(view_name)
            .ok_or_else(|| format!("Query refers to unknown view \"{}\"", view_name))
            .and_then(|v| match self.nodes.get(&(String::from(view_name), *v)) {
                None => Err(format!(
                    "Inconsistency: view \"{}\" does not exist at v{}",
                    view_name, v
                )),
                Some(bmn) => Ok(MirNode::reuse(bmn.clone(), self.schema_version)),
            })
    }

    pub fn add_nodes(&mut self, nodes: Vec<MirNodeRef>) {
        for node in nodes {
            let node_id = (String::from(node.borrow().name()), self.schema_version);
            self.nodes.entry(node_id).or_insert_with(|| node.clone());
            self.current.insert(String::from(node.borrow().name()), self.schema_version);
        }
    }

    fn logical_op_to_conditions(
        &self,
        ct: &ConditionTree,
        columns: &mut Vec<Column>,
        n: &MirNodeRef,
    ) -> Vec<(usize, FilterCondition)> {
        match ct.operator {
            Operator::And => {
                let mut left_filter = match ct.left.as_ref() {
                    ConditionExpression::LogicalOp(ref ct2) => self.logical_op_to_conditions(ct2, columns, n),
                    ConditionExpression::ComparisonOp(ref ct2) => self.to_conditions(ct2, columns, n),
                    _ => unimplemented!(),
                };
                let mut right_filter = match ct.right.as_ref() {
                    ConditionExpression::LogicalOp(ref ct2) => self.logical_op_to_conditions(ct2, columns, n),
                    ConditionExpression::ComparisonOp(ref ct2) => self.to_conditions(ct2, columns, n),
                    _ => unimplemented!(),
                };
                left_filter.append(&mut right_filter);
                left_filter
            }
            _ => unimplemented!()
        }
    }

    /// Converts a condition tree stored in the `ConditionExpr` returned by the SQL parser
    /// and adds its to a vector of conditions.
    fn to_conditions(
        &self,
        ct: &ConditionTree,
        columns: &mut Vec<Column>,
        n: &MirNodeRef,
    ) -> Vec<(usize, FilterCondition)> {
        use std::cmp::max;

        // TODO(malte): we only support one level of condition nesting at this point :(
        let l = match *ct.left.as_ref() {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
            _ => unimplemented!(),
        };
        use dataflow::ops::filter;
        let f = match *ct.right.as_ref() {
            ConditionExpression::Base(ConditionBase::Literal(Literal::Integer(ref i))) => {
                FilterCondition::Comparison(
                    ct.operator.clone(),
                    filter::Value::Constant(DataType::from(*i)),
                )
            }
            ConditionExpression::Base(ConditionBase::Literal(Literal::String(ref s))) => {
                FilterCondition::Comparison(
                    ct.operator.clone(),
                    filter::Value::Constant(DataType::from(s.clone())),
                )
            }
            ConditionExpression::Base(ConditionBase::Literal(Literal::Null)) => {
                FilterCondition::Comparison(
                    ct.operator.clone(),
                    filter::Value::Constant(DataType::None),
                )
            }
            ConditionExpression::Base(ConditionBase::LiteralList(ref ll)) => {
                FilterCondition::In(ll.iter().map(|l| DataType::from(l.clone())).collect())
            }
            ConditionExpression::Base(ConditionBase::Field(ref f)) => {
                // NOTE(jon): the uwnrap here is almost certainly wrong given the business
                // that goes on further down where it appens a column in magical circumstances.
                // also, what if two columns share a name, but differ in .table?
                let fi = columns.iter().rposition(|c| *c.name == f.name).unwrap();
                FilterCondition::Comparison(ct.operator.clone(), filter::Value::Column(fi))
            }
            _ => unimplemented!(),
        };

        let absolute_column_ids: Vec<usize> = columns
            .iter()
            .map(|c| n.borrow().column_id_for_column(c, None))
            .collect();
        let max_column_id = *absolute_column_ids.iter().max().unwrap();
        let num_columns = max(columns.len(), max_column_id + 1);
        let mut filters = Vec::new();

        match columns.iter().rposition(|c| *c.name == l.name) {
            None => {
                // Might occur if the column doesn't exist in the parent; e.g., for aggregations.
                // We assume that the column is appended at the end.
                columns.push(Column::from(l));
                filters.push((num_columns, f));
            }
            Some(pos) => {
                let index = absolute_column_ids[pos];
                filters.push((index, f));
            }
        }

        filters
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
                MirNodeType::Project {
                    emit: columns.clone(),
                    literals: vec![],
                    arithmetic: vec![],
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
                MirNodeType::Identity,
                vec![parent.clone()],
                vec![],
            )
        };

        let new_leaf = MirNode::new(
            name,
            self.schema_version,
            columns
                .clone()
                .into_iter()
                .map(|mut c| {
                    sanitize_leaf_column(&mut c, name);
                    c
                })
                .collect(),
            MirNodeType::Leaf {
                node: parent.clone(),
                keys: Vec::from(params),
            },
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
    ) -> MirQuery {
        let union_name = if !has_leaf && limit.is_none() {
            String::from(name)
        } else {
            format!("{}_union", name)
        };
        let mut final_node = match op {
            CompoundSelectOperator::Union => self.make_union_node(
                &union_name,
                &sqs.iter().map(|mq| mq.leaf.clone()).collect::<Vec<_>>()[..],
            ),
            _ => unimplemented!(),
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

        if limit.is_some() {
            let (topk_name, topk_columns) = if !has_leaf {
                (String::from(name), sanitized_columns.iter().collect())
            } else {
                (format!("{}_topk", name), columns.iter().collect())
            };
            let topk_node = self.make_topk_node(
                &topk_name,
                final_node,
                topk_columns,
                order,
                limit.as_ref().unwrap(),
            );
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
                MirNodeType::Leaf {
                    node: final_node.clone(),
                    keys: vec![],
                },
                vec![final_node.clone()],
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

        MirQuery {
            name: String::from(name),
            roots: sqs.iter().fold(Vec::new(), |mut acc, mq| {
                acc.extend(mq.roots.iter().cloned());
                acc
            }),
            leaf: leaf_node,
        }
    }

    // pub(super) viz for tests
    pub(super) fn get_flow_node_address(&self, name: &str, version: usize) -> Option<NodeIndex> {
        match self.nodes.get(&(name.to_string(), version)) {
            None => None,
            Some(ref node) => match node.borrow().flow_node {
                None => None,
                Some(ref flow_node) => Some(flow_node.address()),
            },
        }
    }

    pub(super) fn get_leaf(&self, name: &str) -> Option<NodeIndex> {
        match self.current.get(name) {
            None => None,
            Some(v) => self.get_flow_node_address(name, *v),
        }
    }

    pub(super) fn named_base_to_mir(&mut self, name: &str, query: &SqlQuery) -> MirQuery {
        match *query {
            SqlQuery::CreateTable(ref ctq) => {
                assert_eq!(name, ctq.table.name);
                let n = self.make_base_node(&name, &ctq.fields, ctq.keys.as_ref());
                let node_id = (String::from(name), self.schema_version);
                use std::collections::hash_map::Entry;
                if let Entry::Vacant(e) = self.nodes.entry(node_id) {
                    self.current.insert(String::from(name), self.schema_version);
                    e.insert(n.clone());
                }
                MirQuery::singleton(name, n)
            }
            _ => panic!("expected CREATE TABLE query!"),
        }
    }

    pub(super) fn remove_query(&mut self, name: &str, mq: &MirQuery) {
        use std::collections::VecDeque;

        let v = self
            .current
            .remove(name)
            .unwrap_or_else(|| panic!("no query named \"{}\"?", name));

        let nodeid = (name.to_owned(), v);
        let leaf_mn = self.nodes.remove(&nodeid).unwrap();

        assert_eq!(leaf_mn.borrow().name, mq.leaf.borrow().name);

        // traverse the MIR query backwards, removing any nodes that we still have registered.
        let mut q = VecDeque::new();
        q.push_back(leaf_mn);

        while !q.is_empty() {
            let mnr = q.pop_front().unwrap();
            let n = mnr.borrow_mut();
            q.extend(n.ancestors.clone());
            // node may not be registered, so don't bother checking return
            match n.inner {
                MirNodeType::Reuse { .. } | MirNodeType::Base { .. } => (),
                _ => {
                    self.nodes.remove(&(n.name.to_owned(), v));
                }
            }
        }
    }

    pub(super) fn remove_base(&mut self, name: &str, mq: &MirQuery) {
        info!(self.log, "Removing base {} from SqlTomirconverter", name);
        self.remove_query(name, mq);
        if self.base_schemas.remove(name).is_none() {
            warn!(
                self.log,
                "Attempted to remove non-existant base node {} from SqlToMirconverter", name
            );
        }
    }

    pub(super) fn named_query_to_mir(
        &mut self,
        name: &str,
        sq: &SelectStatement,
        qg: &QueryGraph,
        has_leaf: bool,
        universe: UniverseId,
    ) -> Result<
        (
            bool,
            MirQuery,
            Option<HashMap<(String, Option<String>), String>>,
            String,
        ),
        String,
    > {
        let (sec, nodes, table_mapping, base_name) =
            self.make_nodes_for_selection(&name, sq, qg, has_leaf, universe)?;
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
        assert_eq!(
            leaves.len(),
            1,
            "expected just one leaf! leaves: {:?}",
            leaves
        );
        let leaf = leaves.into_iter().next().unwrap();
        self.current
            .insert(String::from(leaf.borrow().name()), self.schema_version);

        Ok((
            sec,
            MirQuery {
                name: String::from(name),
                roots,
                leaf,
            },
            table_mapping,
            base_name,
        ))
    }

    pub(super) fn upgrade_schema(&mut self, new_version: usize) {
        assert!(new_version > self.schema_version);
        self.schema_version = new_version;
    }

    fn make_base_node(
        &mut self,
        name: &str,
        cols: &[ColumnSpecification],
        keys: Option<&Vec<TableKey>>,
    ) -> MirNodeRef {
        // have we seen a base of this name before?
        if self.base_schemas.contains_key(name) {
            let mut existing_schemas: Vec<(usize, Vec<ColumnSpecification>)> =
                self.base_schemas[name].clone();
            existing_schemas.sort_by_key(|&(sv, _)| sv);
            // newest schema first
            existing_schemas.reverse();

            #[warn(clippy::never_loop)]
            for (existing_sv, ref schema) in existing_schemas {
                // TODO(malte): check the keys too
                if &schema[..] == cols {
                    // exact match, so reuse the existing base node
                    info!(
                        self.log,
                        "base table for {} already exists with identical \
                         schema in version {}; reusing it.",
                        name,
                        existing_sv
                    );
                    let existing_node = self.nodes[&(String::from(name), existing_sv)].clone();
                    return MirNode::reuse(existing_node, self.schema_version);
                } else {
                    // match, but schema is different, so we'll need to either:
                    //  1) reuse the existing node, but add an upgrader for any changes in the
                    //     column set, or
                    //  2) give up and just make a new node
                    info!(
                        self.log,
                        "base table for {} already exists in version {}, \
                         but has a different schema!",
                        name,
                        existing_sv
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
                            self.log,
                            "base {}: add columns {:?}, remove columns {:?} over v{}",
                            name,
                            columns_added,
                            columns_removed,
                            existing_sv
                        );
                        let existing_node = self.nodes[&(String::from(name), existing_sv)].clone();

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
                                    .unwrap_or_else(|| {
                                        panic!(
                                            "couldn't find column \"{:#?}\", \
                                             which we're removing",
                                            removed
                                        )
                                    });
                            columns.remove(pos);
                        }
                        assert_eq!(
                            columns.len(),
                            existing_node.borrow().columns().len() + columns_added.len()
                                - columns_removed.len()
                        );

                        // remember the schema for this version
                        let base_schemas = self.base_schemas.entry(String::from(name)).or_default();
                        base_schemas.push((self.schema_version, columns.clone()));

                        return MirNode::adapt_base(existing_node, columns_added, columns_removed);
                    } else {
                        info!(self.log, "base table has complex schema change");
                        break;
                    }
                }
            }
        }

        // all columns on a base must have the base as their table
        assert!(cols
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
                    ref k @ TableKey::PrimaryKey(..) => Some(k),
                    _ => None,
                })
                .collect(),
        };
        assert!(primary_keys.len() <= 1);

        // remember the schema for this version
        let base_schemas = self.base_schemas.entry(String::from(name)).or_default();
        base_schemas.push((self.schema_version, cols.to_vec()));

        // make node
        if !primary_keys.is_empty() {
            match **primary_keys.iter().next().unwrap() {
                TableKey::PrimaryKey(ref key_cols) => {
                    debug!(
                        self.log,
                        "Assigning primary key ({}) for base {}",
                        key_cols
                            .iter()
                            .map(|c| c.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", "),
                        name
                    );
                    MirNode::new(
                        name,
                        self.schema_version,
                        cols.iter().map(|cs| Column::from(&cs.column)).collect(),
                        MirNodeType::Base {
                            column_specs: cols.iter().map(|cs| (cs.clone(), None)).collect(),
                            keys: key_cols.iter().map(Column::from).collect(),
                            adapted_over: None,
                        },
                        vec![],
                        vec![],
                    )
                }
                _ => unreachable!(),
            }
        } else {
            MirNode::new(
                name,
                self.schema_version,
                cols.iter().map(|cs| Column::from(&cs.column)).collect(),
                MirNodeType::Base {
                    column_specs: cols.iter().map(|cs| (cs.clone(), None)).collect(),
                    keys: vec![],
                    adapted_over: None,
                },
                vec![],
                vec![],
            )
        }
    }

    fn make_union_node(&self, name: &str, ancestors: &[MirNodeRef]) -> MirNodeRef {
        let mut emit: Vec<Vec<Column>> = Vec::new();
        assert!(ancestors.len() > 1, "union must have more than 1 ancestors");

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
                panic!(
                    "column with name '{}' not found all union ancestors: all ancestors' \
                     output columns must have the same names",
                    c.name
                );
            }
        }
        assert_eq!(
            num_ucols,
            selected_cols.len(),
            "union drops ancestor columns"
        );

        for ancestor in ancestors.iter() {
            let mut acols: Vec<Column> = Vec::new();
            for ac in ancestor.borrow().columns() {
                if selected_cols.contains(&ac.name)
                    && acols.iter().find(|c| ac.name == *c.name).is_none()
                {
                    acols.push(ac.clone());
                }
            }
            emit.push(acols.clone());
        }

        assert!(
            emit.iter().all(|e| e.len() == selected_cols.len()),
            "all ancestors columns must have the same size, but got emit: {:?}, selected: {:?}",
            emit,
            selected_cols
        );

        MirNode::new(
            name,
            self.schema_version,
            emit.first().unwrap().clone(),
            MirNodeType::Union { emit },
            ancestors.to_vec(),
            vec![],
        )
    }

    // Creates union node for universe creation - returns the resulting node ref and a universe table mapping
    fn make_union_node_sec(
        &self,
        name: &str,
        ancestors: &[MirNodeRef],
    ) -> (
        MirNodeRef,
        Option<HashMap<(String, Option<String>), String>>,
    ) {
        let mut emit: Vec<Vec<Column>> = Vec::new();
        assert!(ancestors.len() > 1, "union must have more than 1 ancestors");

        let ucols: Vec<Column> = ancestors.first().unwrap().borrow().columns().to_vec();
        let num_ucols = ucols.len();

        let mut selected_cols = HashSet::new();
        let mut selected_col_objects = HashSet::new();
        for c in ucols {
            if ancestors
                .iter()
                .all(|a| a.borrow().columns().iter().any(|ac| *ac.name == c.name))
            {
                selected_cols.insert(c.name.clone());
                selected_col_objects.insert(c.clone());
            }
        }

        let mut precedent_table = " ".to_string();
        for col in selected_col_objects.clone() {
            match &col.table {
                Some(x) => {
                    debug!(
                        self.log,
                        "Selected column {} from table {} for UNION.", col.name, x
                    );
                    precedent_table = col.table.unwrap();
                }
                None => {
                    debug!(
                        self.log,
                        "Selected column {} with no table name for UNION.", col.name
                    );
                    precedent_table = "None".to_string();
                }
            }
        }

        assert_eq!(
            num_ucols,
            selected_cols.len(),
            "union drops ancestor columns"
        );

        let mut table_mapping = HashMap::new();

        for ancestor in ancestors.iter() {
            let mut acols: Vec<Column> = Vec::new();
            for ac in ancestor.borrow().columns() {
                if selected_cols.contains(&ac.name)
                    && acols.iter().find(|c| ac.name == *c.name).is_none()
                {
                    acols.push(ac.clone());
                }
            }

            for col in acols.clone() {
                match col.table {
                    Some(x) => {
                        debug!(
                            self.log,
                            "About to push column {} from table {} onto emit.", col.name, x
                        );
                        let col_n: String = col.name.to_owned();
                        let tab_n: String = x.to_owned();
                        let key = (col_n, Some(tab_n));

                        table_mapping.insert(key, precedent_table.to_string());
                    }
                    None => {
                        debug!(self.log, "About to push column {} onto emit.", col.name);
                        table_mapping
                            .insert((col.name.to_owned(), None), precedent_table.to_string());
                    }
                }
            }

            emit.push(acols.clone());
        }

        assert!(
            emit.iter().all(|e| e.len() == selected_cols.len()),
            "all ancestors columns must have the same size, but got emit: {:?}, selected: {:?}",
            emit,
            selected_cols
        );

        (
            MirNode::new(
                name,
                self.schema_version,
                emit.first().unwrap().clone(),
                MirNodeType::Union { emit },
                ancestors.to_vec(),
                vec![],
            ),
            Some(table_mapping),
        )
    }

    fn make_union_from_same_base(
        &self,
        name: &str,
        ancestors: Vec<MirNodeRef>,
        columns: Vec<Column>,
    ) -> MirNodeRef {
        assert!(ancestors.len() > 1, "union must have more than 1 ancestors");
        trace!(self.log, "Added union node wiht columns {:?}", columns);
        let emit = ancestors.iter().map(|_| columns.clone()).collect();

        MirNode::new(
            name,
            self.schema_version,
            columns,
            MirNodeType::Union { emit },
            ancestors.clone(),
            vec![],
        )
    }

    fn make_filter_node(&self, name: &str, parent: MirNodeRef, cond: &ConditionTree) -> MirNodeRef {
        let mut fields = parent.borrow().columns().to_vec();

        let filter = self.to_conditions(cond, &mut fields, &parent);
        trace!(
            self.log,
            "Added filter node {} with condition {:?}",
            name,
            filter
        );
        MirNode::new(
            name,
            self.schema_version,
            fields,
            MirNodeType::Filter { conditions: filter },
            vec![parent.clone()],
            vec![],
        )
    }

    fn make_function_node(
        &self,
        name: &str,
        func_col: &Column,
        group_cols: Vec<&Column>,
        parent: MirNodeRef,
    ) -> Vec<MirNodeRef> {
        use dataflow::ops::grouped::aggregate::Aggregation;
        use dataflow::ops::grouped::filteraggregate::FilterAggregation;
        use dataflow::ops::grouped::extremum::Extremum;
        use nom_sql::FunctionExpression::*;

        let mut out_nodes = Vec::new();

        let mknode = |over: &Column, over_else: Option<Literal>, t: GroupedNodeType, distinct: bool, cond: Option<&ConditionExpression>| {
            if distinct {
                let new_name = name.to_owned() + "_distinct";
                let mut dist_col = Vec::new();
                dist_col.push(over);
                dist_col.extend(group_cols.clone());
                let node = self.make_distinct_node(&new_name, parent, dist_col.clone());
                out_nodes.push(node.clone());
                out_nodes.push(self.make_grouped_node(
                    name,
                    &func_col,
                    (node, &over, over_else),
                    group_cols,
                    t,
                    cond,
                ));
                out_nodes
            } else {
                out_nodes.push(self.make_grouped_node(
                    name,
                    &func_col,
                    (parent, &over, over_else),
                    group_cols,
                    t,
                    cond,
                ));
                out_nodes
            }
        };

        let func = func_col.function.as_ref().unwrap();
        match *func.deref() {
            Sum(ref col, distinct) => mknode(
                &Column::from(col),
                None,
                GroupedNodeType::Aggregation(Aggregation::SUM),
                distinct,
                None,
            ),
            SumFilter(ref col, ref else_val, ref condition) => mknode(
                &Column::from(col),
                else_val.clone(),
                GroupedNodeType::FilterAggregation(FilterAggregation::SUM),
                false,
                Some(condition),
            ),
            Count(ref col, distinct) => mknode(
                &Column::from(col),
                None,
                GroupedNodeType::Aggregation(Aggregation::COUNT),
                distinct,
                None,
            ),
            CountStar => {
                // XXX(malte): there is no "over" column, but our aggregation operators' API
                // requires one to be specified, so we earlier rewrote it to use the last parent
                // column (see passes/count_star_rewrite.rs). However, this isn't *entirely*
                // faithful to COUNT(*) semantics, because COUNT(*) is supposed to count all
                // rows including those with NULL values, and we don't have a mechanism to do that
                // (but we also don't have a NULL value, so maybe we're okay).
                panic!("COUNT(*) should have been rewritten earlier!")
            },
            CountFilter(ref col, ref else_val, ref condition) => mknode(
                &Column::from(col),
                else_val.clone(),
                GroupedNodeType::FilterAggregation(FilterAggregation::COUNT),
                false,
                Some(condition),
            ),
            Max(ref col) => mknode(
                &Column::from(col),
                None,
                GroupedNodeType::Extremum(Extremum::MAX),
                false,
                None,
            ),
            Min(ref col) => mknode(
                &Column::from(col),
                None,
                GroupedNodeType::Extremum(Extremum::MIN),
                false,
                None,
            ),
            GroupConcat(ref col, ref separator) => mknode(
                &Column::from(col),
                None,
                GroupedNodeType::GroupConcat(separator.clone()),
                false,
                None,
            ),
            _ => unimplemented!(),
        }
    }

    fn make_grouped_node(
        &self,
        name: &str,
        computed_col: &Column,
        over: (MirNodeRef, &Column, Option<Literal>),
        group_by: Vec<&Column>,
        node_type: GroupedNodeType,
        condition: Option<&ConditionExpression>
    ) -> MirNodeRef {
        let parent_node = over.0;

        // Resolve column IDs in parent
        let over_col = over.1;
        let else_val = over.2;

        // The function node's set of output columns is the group columns plus the function
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
                MirNodeType::Aggregation {
                    on: over_col.clone(),
                    group_by: group_by.into_iter().cloned().collect(),
                    kind: agg,
                },
                vec![parent_node.clone()],
                vec![],
            ),
            GroupedNodeType::Extremum(extr) => MirNode::new(
                name,
                self.schema_version,
                combined_columns,
                MirNodeType::Extremum {
                    on: over_col.clone(),
                    group_by: group_by.into_iter().cloned().collect(),
                    kind: extr,
                },
                vec![parent_node.clone()],
                vec![],
            ),
            GroupedNodeType::FilterAggregation(filter_agg) => {
                use nom_sql::ConditionExpression::*;

                let cond = condition.expect("Filter aggregation must have condition!");
                let mut fields = parent_node.borrow().columns().to_vec();
                let filter = match *cond {
                    LogicalOp(ref ct) => self.logical_op_to_conditions(ct, &mut fields, &parent_node),
                    ComparisonOp(ref ct) => self.to_conditions(ct, &mut fields, &parent_node),
                    Bracketed(_) => unimplemented!(),
                    NegationOp(_) => unreachable!("negation should have been removed earlier"),
                    Base(_) => unreachable!("dangling base predicate"),
                    Arithmetic(_) => unimplemented!(),
                };
                MirNode::new(
                    name,
                    self.schema_version,
                    combined_columns,
                    MirNodeType::FilterAggregation {
                        on: over_col.clone(),
                        else_on: else_val.clone(),
                        group_by: group_by.into_iter().cloned().collect(),
                        kind: filter_agg,
                        conditions: filter,
                    },
                    vec![parent_node.clone()],
                    vec![],
                )
            },
            GroupedNodeType::GroupConcat(sep) => MirNode::new(
                name,
                self.schema_version,
                combined_columns,
                MirNodeType::GroupConcat {
                    on: over_col.clone(),
                    separator: sep,
                },
                vec![parent_node.clone()],
                vec![],
            ),
        }
    }

    fn make_join_node(
        &self,
        name: &str,
        jp: &ConditionTree,
        left_node: MirNodeRef,
        right_node: MirNodeRef,
        kind: JoinType,
    ) -> MirNodeRef {
        // TODO(malte): this is where we overproject join columns in order to increase reuse
        // opportunities. Technically, we need to only project those columns here that the query
        // actually needs; at a minimum, we could start with just the join colums, relying on the
        // automatic column pull-down to retrieve the remaining columns required.
        let projected_cols_left = left_node.borrow().columns().to_vec();
        let projected_cols_right = right_node.borrow().columns().to_vec();
        let fields = projected_cols_left
            .into_iter()
            .chain(projected_cols_right.into_iter())
            .collect::<Vec<Column>>();

        // join columns need us to generate join group configs for the operator
        // TODO(malte): no multi-level joins yet
        let mut left_join_columns = Vec::new();
        let mut right_join_columns = Vec::new();

        // equi-join only
        assert!(jp.operator == Operator::Equal || jp.operator == Operator::In);
        let mut l_col = match *jp.left {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => Column::from(f),
            _ => unimplemented!(),
        };
        let r_col = match *jp.right {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => Column::from(f),
            _ => unimplemented!(),
        };

        // don't duplicate the join column in the output, but instead add aliases to the columns
        // that represent it going forward (viz., the left-side join column)
        l_col.add_alias(&r_col);
        // add the alias to all instances of `l_col` in `fields` (there might be more than one
        // if `l_col` is explicitly projected multiple times)
        let fields: Vec<Column> = fields
            .into_iter()
            .filter_map(|mut f| {
                if f == r_col {
                    // drop instances of right-side column
                    None
                } else if f == l_col {
                    // add alias for right-side column to any left-side column
                    // N.B.: since `l_col` is already aliased, need to check this *after* checking
                    // for equivalence with `r_col` (by now, `l_col` == `r_col` via alias), so
                    // `f == l_col` also triggers if `f` is in `l_col.aliases`.
                    f.add_alias(&r_col);
                    Some(f)
                } else {
                    // keep unaffected columns
                    Some(f)
                }
            })
            .collect();

        left_join_columns.push(l_col);
        right_join_columns.push(r_col);

        assert_eq!(left_join_columns.len(), right_join_columns.len());
        let inner = match kind {
            JoinType::Inner => MirNodeType::Join {
                on_left: left_join_columns,
                on_right: right_join_columns,
                project: fields.clone(),
            },
            JoinType::Left => MirNodeType::LeftJoin {
                on_left: left_join_columns,
                on_right: right_join_columns,
                project: fields.clone(),
            },
        };
        trace!(self.log, "Added join node {:?}", inner);
        MirNode::new(
            name,
            self.schema_version,
            fields,
            inner,
            vec![left_node.clone(), right_node.clone()],
            vec![],
        )
    }

    fn make_projection_helper(
        &self,
        name: &str,
        parent: MirNodeRef,
        fn_col: &Column,
    ) -> MirNodeRef {
        self.make_project_node(
            name,
            parent,
            vec![fn_col],
            vec![],
            vec![(String::from("grp"), DataType::from(0 as i32))],
            false,
        )
    }

    fn make_project_node(
        &self,
        name: &str,
        parent_node: MirNodeRef,
        proj_cols: Vec<&Column>,
        arithmetic: Vec<(String, ArithmeticExpression)>,
        literals: Vec<(String, DataType)>,
        is_leaf: bool,
    ) -> MirNodeRef {
        //assert!(proj_cols.iter().all(|c| c.table == parent_name));

        let names: Vec<String> = arithmetic
            .iter()
            .map(|&(ref n, _)| n.clone())
            .chain(literals.iter().map(|&(ref n, _)| n.clone()))
            .collect();

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
            .chain(names.into_iter().map(|n| {
                if is_leaf {
                    Column::new(Some(&name), &n)
                } else {
                    Column::new(None, &n)
                }
            }))
            .collect();

        let emit_cols = proj_cols.into_iter().cloned().collect();

        MirNode::new(
            name,
            self.schema_version,
            fields,
            MirNodeType::Project {
                emit: emit_cols,
                literals,
                arithmetic,
            },
            vec![parent_node.clone()],
            vec![],
        )
    }

    fn make_distinct_node(
        &self,
        name: &str,
        parent: MirNodeRef,
        group_by: Vec<&Column>,
    ) -> MirNodeRef {
        let combined_columns = parent.borrow().columns().to_vec();

        // make the new operator and record its metadata
        MirNode::new(
            name,
            self.schema_version,
            combined_columns,
            MirNodeType::Distinct {
                group_by: group_by.into_iter().cloned().collect(),
            },
            vec![parent.clone()],
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
    ) -> MirNodeRef {
        let combined_columns = parent.borrow().columns().to_vec();

        let order = match *order {
            Some(ref o) => Some(
                o.columns
                    .iter()
                    .map(|(c, o)| (Column::from(c), o.clone()))
                    .collect(),
            ),
            None => None,
        };

        assert_eq!(limit.offset, 0); // Non-zero offset not supported

        // make the new operator and record its metadata
        MirNode::new(
            name,
            self.schema_version,
            combined_columns,
            MirNodeType::TopK {
                order,
                group_by: group_by.into_iter().cloned().collect(),
                k: limit.limit as usize,
                offset: 0,
            },
            vec![parent.clone()],
            vec![],
        )
    }

    fn make_predicate_nodes(
        &self,
        name: &str,
        parent: MirNodeRef,
        ce: &ConditionExpression,
        nc: usize,
    ) -> Vec<MirNodeRef> {
        use nom_sql::ConditionExpression::*;

        let mut pred_nodes: Vec<MirNodeRef> = Vec::new();
        let output_cols = parent.borrow().columns().to_vec();
        match *ce {
            LogicalOp(ref ct) => {
                let (left, right);
                match ct.operator {
                    Operator::And => {
                        left = self.make_predicate_nodes(name, parent.clone(), &*ct.left, nc);

                        right = self.make_predicate_nodes(
                            name,
                            left.last().unwrap().clone(),
                            &*ct.right,
                            nc + left.len(),
                        );

                        pred_nodes.extend(left.clone());
                        pred_nodes.extend(right.clone());
                    }
                    Operator::Or => {
                        left = self.make_predicate_nodes(name, parent.clone(), &*ct.left, nc);

                        right = self.make_predicate_nodes(
                            name,
                            parent.clone(),
                            &*ct.right,
                            nc + left.len(),
                        );

                        debug!(self.log, "Creating union node for `or` predicate");

                        let last_left = left.last().unwrap().clone();
                        let last_right = right.last().unwrap().clone();
                        let union = self.make_union_from_same_base(
                            &format!("{}_un", name),
                            vec![last_left, last_right],
                            output_cols,
                        );

                        pred_nodes.extend(left.clone());
                        pred_nodes.extend(right.clone());
                        pred_nodes.push(union);
                    }
                    _ => unreachable!("LogicalOp operator is {:?}", ct.operator),
                }
            }
            ComparisonOp(ref ct) => {
                // currently, we only support filter-like
                // comparison operations, no nested-selections
                let f = self.make_filter_node(&format!("{}_f{}", name, nc), parent, ct);

                pred_nodes.push(f);
            }
            Bracketed(ref inner) => {
                pred_nodes.extend(self.make_predicate_nodes(name, parent, &*inner, nc));
            }
            NegationOp(_) => unreachable!("negation should have been removed earlier"),
            Base(_) => unreachable!("dangling base predicate"),
            Arithmetic(_) => unimplemented!(),
        }

        pred_nodes
    }

    fn predicates_above_group_by<'a>(
        &self,
        name: &str,
        column_to_predicates: &HashMap<Column, Vec<&'a ConditionExpression>>,
        over_col: Column,
        parent: MirNodeRef,
        created_predicates: &mut Vec<&'a ConditionExpression>,
    ) -> Vec<MirNodeRef> {
        let mut predicates_above_group_by_nodes = Vec::new();
        let mut prev_node = parent.clone();

        let ces = column_to_predicates.get(&over_col).unwrap();
        for ce in ces {
            if !created_predicates.contains(ce) {
                let mpns = self.make_predicate_nodes(
                    &format!("{}_mp{}", name, predicates_above_group_by_nodes.len()),
                    prev_node.clone(),
                    ce,
                    0,
                );
                assert!(!mpns.is_empty());
                prev_node = mpns.last().unwrap().clone();
                predicates_above_group_by_nodes.extend(mpns);
                created_predicates.push(ce);
            }
        }

        predicates_above_group_by_nodes
    }

    fn make_value_project_node(
        &self,
        qg: &QueryGraph,
        prev_node: Option<MirNodeRef>,
        node_count: usize,
        universe: &str,
    ) -> Option<MirNodeRef> {
        let arith_and_lit_columns_needed =
            value_columns_needed_for_predicates(&qg.columns, &qg.global_predicates);

        if !arith_and_lit_columns_needed.is_empty() {
            let projected_arithmetic: Vec<(String, ArithmeticExpression)> =
                arith_and_lit_columns_needed
                    .iter()
                    .filter_map(|&(_, ref oc)| match oc {
                        OutputColumn::Arithmetic(ref ac) => {
                            Some((ac.name.clone(), ac.expression.clone()))
                        }
                        OutputColumn::Data(_) => None,
                        OutputColumn::Literal(_) => None,
                    })
                    .collect();
            let projected_literals: Vec<(String, DataType)> = arith_and_lit_columns_needed
                .iter()
                .filter_map(|&(_, ref oc)| match oc {
                    OutputColumn::Arithmetic(_) => None,
                    OutputColumn::Data(_) => None,
                    OutputColumn::Literal(ref lc) => {
                        Some((lc.name.clone(), DataType::from(&lc.value)))
                    }
                })
                .collect();

            // prev_node must be set at this point
            let parent = match prev_node {
                None => unreachable!(),
                Some(pn) => pn,
            };

            let passthru_cols: Vec<_> = parent.borrow().columns().to_vec();
            let projected = self.make_project_node(
                &format!("q_{:x}_n{}{}", qg.signature().hash, node_count, universe),
                parent.clone(),
                passthru_cols.iter().collect(),
                projected_arithmetic,
                projected_literals,
                false,
            );

            Some(projected)
        } else {
            None
        }
    }

    /// Returns list of nodes added
    #[allow(clippy::cognitive_complexity)]
    fn make_nodes_for_selection(
        &mut self,
        name: &str,
        st: &SelectStatement,
        qg: &QueryGraph,
        has_leaf: bool,
        universe: UniverseId,
    ) -> Result<
        (
            bool,
            Vec<MirNodeRef>,
            Option<HashMap<(String, Option<String>), String>>,
            String,
        ),
        String,
    > {
        // TODO: make this take &self!
        use crate::controller::sql::mir::grouped::make_grouped;
        use crate::controller::sql::mir::grouped::make_predicates_above_grouped;
        use crate::controller::sql::mir::join::make_joins;

        let mut nodes_added: Vec<MirNodeRef>;
        let mut new_node_count = 0;

        let (uid, _) = universe.clone();

        let uformat = if uid == "global".into() {
            String::from("")
        } else {
            format!("_u{}", uid.to_string())
        };

        let mut table_mapping = None;
        let mut sec_round = false;
        let mut union_base_name = " ".to_string();

        // Canonical operator order: B-J-G-F-P-R
        // (Base, Join, GroupBy, Filter, Project, Reader)
        {
            let mut node_for_rel: HashMap<&str, MirNodeRef> = HashMap::default();

            // 0. Base nodes (always reused)
            let mut base_nodes: Vec<MirNodeRef> = Vec::new();
            let mut sorted_rels: Vec<&str> = qg.relations.keys().map(String::as_str).collect();
            sorted_rels.sort();
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
                &format!("q_{:x}{}", qg.signature().hash, uformat),
                qg,
                &node_for_rel,
                new_node_count,
            );

            new_node_count += join_nodes.len();

            let mut prev_node = match join_nodes.last() {
                Some(n) => Some(n.clone()),
                None => {
                    assert_eq!(base_nodes.len(), 1);
                    Some(base_nodes.last().unwrap().clone())
                }
            };

            // 2. Get columns used by each predicate. This will be used to check
            // if we need to reorder predicates before group_by nodes.
            let mut column_to_predicates: HashMap<Column, Vec<&ConditionExpression>> =
                HashMap::new();

            for rel in &sorted_rels {
                if *rel == "computed_columns" {
                    continue;
                }

                let qgn = &qg.relations[*rel];
                for pred in &qgn.predicates {
                    let cols = predicate_columns(pred);

                    for col in cols {
                        column_to_predicates.entry(col).or_default().push(pred);
                    }
                }

                for pred in &qg.global_predicates {
                    let cols = predicate_columns(pred);

                    for col in cols {
                        column_to_predicates.entry(col).or_default().push(pred);
                    }
                }
            }

            // 2a. Reorder some predicates before group by nodes
            // FIXME(malte): This doesn't currently work correctly with arithmetic and literal
            // projections that form input to these filters -- these need to be lifted above them
            // (and above the aggregations).
            let (created_predicates, predicates_above_group_by_nodes) =
                make_predicates_above_grouped(
                    self,
                    &format!("q_{:x}{}", qg.signature().hash, uformat),
                    &qg,
                    &node_for_rel,
                    new_node_count,
                    &column_to_predicates,
                    &mut prev_node,
                );

            new_node_count += predicates_above_group_by_nodes.len();

            // 3. Create security boundary
            use crate::controller::sql::mir::security::SecurityBoundary;
            let (last_policy_nodes, policy_nodes) = self.make_security_boundary(
                universe.clone(),
                &mut node_for_rel,
                prev_node.clone(),
            )?;

            let mut ancestors = self.universe.member_of.iter().fold(
                Ok(vec![]),
                |acc: Result<_, String>, (gname, gids)| {
                    acc.and_then(|mut acc| {
                        let group_views: Result<Vec<_>, String> = gids
                            .iter()
                            .filter_map(|gid| {
                                // This is a little annoying, but because of the way we name universe queries,
                                // we need to strip the view name of the _u{uid} suffix
                                let root = name.trim_end_matches(&uformat);
                                if root == name {
                                    None
                                } else {
                                    let view_name = format!(
                                        "{}_{}{}",
                                        root,
                                        gname.to_string(),
                                        gid.to_string()
                                    );
                                    Some(self.get_view(&view_name))
                                }
                            })
                            .collect();

                        trace!(&self.log, "group views {:?}", group_views);
                        acc.extend(group_views?);
                        Ok(acc)
                    })
                },
            )?;

            nodes_added = base_nodes
                .into_iter()
                .chain(join_nodes.into_iter())
                .chain(predicates_above_group_by_nodes.into_iter())
                .chain(policy_nodes.into_iter())
                .chain(ancestors.clone().into_iter())
                .collect();

            // For each policy chain, create a version of the query
            // All query versions, including group queries will be reconciled at the end
            for n in last_policy_nodes.iter() {
                prev_node = Some(n.clone());

                // 3. Add function and grouped nodes
                let mut func_nodes: Vec<MirNodeRef> = make_grouped(
                    self,
                    &format!("q_{:x}{}", qg.signature().hash, uformat),
                    &qg,
                    &node_for_rel,
                    new_node_count,
                    &mut prev_node,
                    false,
                );

                new_node_count += func_nodes.len();

                let mut predicate_nodes = Vec::new();
                // 4. Generate the necessary filter nodes for local predicates associated with each
                // relation node in the query graph.
                //
                // Need to iterate over relations in a deterministic order, as otherwise nodes will be
                // added in a different order every time, which will yield different node identifiers
                // and make it difficult for applications to check what's going on.
                for rel in &sorted_rels {
                    let qgn = &qg.relations[*rel];
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
                                None => node_for_rel[rel].clone(),
                                Some(pn) => pn,
                            };

                            let fns = self.make_predicate_nodes(
                                &format!(
                                    "q_{:x}_n{}_p{}{}",
                                    qg.signature().hash,
                                    new_node_count,
                                    i,
                                    uformat
                                ),
                                parent,
                                p,
                                0,
                            );

                            assert!(!fns.is_empty());
                            new_node_count += fns.len();
                            prev_node = Some(fns.iter().last().unwrap().clone());
                            predicate_nodes.extend(fns);
                        }
                    }
                }

                let num_local_predicates = predicate_nodes.len();

                // 5. Determine literals and arithmetic expressions that global predicates depend
                //    on and add them here; remembering that we've already added them-
                if let Some(projected) =
                    self.make_value_project_node(&qg, prev_node.clone(), new_node_count, &uformat)
                {
                    new_node_count += 1;
                    nodes_added.push(projected.clone());
                    prev_node = Some(projected);
                }

                // 5. Global predicates
                for (i, ref p) in qg.global_predicates.iter().enumerate() {
                    if created_predicates.contains(p) {
                        continue;
                    }

                    let parent = match prev_node {
                        None => unimplemented!(),
                        Some(pn) => pn,
                    };

                    let fns = self.make_predicate_nodes(
                        &format!(
                            "q_{:x}_n{}_{}{}",
                            qg.signature().hash,
                            new_node_count,
                            num_local_predicates + i,
                            uformat,
                        ),
                        parent,
                        p,
                        0,
                    );

                    assert!(!fns.is_empty());
                    new_node_count += fns.len();
                    prev_node = Some(fns.iter().last().unwrap().clone());
                    predicate_nodes.extend(fns);
                }

                // 6. Get the final node
                let mut final_node: MirNodeRef = if prev_node.is_some() {
                    prev_node.unwrap().clone()
                } else {
                    // no join, filter, or function node --> base node is parent
                    assert_eq!(sorted_rels.len(), 1);
                    node_for_rel[sorted_rels.last().unwrap()].clone()
                };

                // 7. Potentially insert TopK node below the final node
                // XXX(malte): this adds a bogokey if there are no parameter columns to do the TopK
                // over, but we could end up in a stick place if we reconcile/combine multiple
                // queries (due to security universes or due to compound select queries) that do
                // not all have the bogokey!
                if let Some(ref limit) = st.limit {
                    let group_by = if qg.parameters().is_empty() {
                        // need to add another projection to introduce a bogokey to group by
                        let cols: Vec<_> = final_node.borrow().columns().to_vec();
                        let table =
                            format!("q_{:x}_n{}{}", qg.signature().hash, new_node_count, uformat);
                        let bogo_project = self.make_project_node(
                            &table,
                            final_node.clone(),
                            cols.iter().collect(),
                            vec![],
                            vec![("bogokey".into(), DataType::from(0 as i32))],
                            false,
                        );
                        new_node_count += 1;
                        nodes_added.push(bogo_project.clone());
                        final_node = bogo_project;

                        vec![Column::new(None, "bogokey")]
                    } else {
                        qg.parameters().into_iter().map(Column::from).collect()
                    };

                    let topk_node = self.make_topk_node(
                        &format!("q_{:x}_n{}{}", qg.signature().hash, new_node_count, uformat),
                        final_node,
                        group_by.iter().collect(),
                        &st.order,
                        limit,
                    );
                    func_nodes.push(topk_node.clone());
                    final_node = topk_node;
                    new_node_count += 1;
                }

                // we're now done with the query, so remember all the nodes we've added so far
                nodes_added.extend(func_nodes);
                nodes_added.extend(predicate_nodes);

                ancestors.push(final_node);
            }

            let final_node = if ancestors.len() > 1 {
                // If we have multiple queries, reconcile them.
                sec_round = true;
                if uid != "global".into() {
                    sec_round = true;
                }

                let (nodes, tables, union_base_node_name) = self.reconcile(
                    &format!("q_{:x}{}", qg.signature().hash, uformat),
                    &qg,
                    &ancestors,
                    new_node_count,
                    sec_round,
                );

                if sec_round {
                    table_mapping = tables;
                    union_base_name = union_base_node_name;
                }

                new_node_count += nodes.len();
                nodes_added.extend(nodes.clone());
                nodes.last().unwrap().clone()
            } else {
                ancestors.last().unwrap().clone()
            };

            let final_node_cols: Vec<Column> = final_node.borrow().columns().to_vec();
            // 8. Generate leaf views that expose the query result
            let mut projected_columns: Vec<Column> = if universe.1.is_none() {
                qg.columns
                    .iter()
                    .filter_map(|oc| match *oc {
                        OutputColumn::Arithmetic(_) => None,
                        OutputColumn::Data(ref c) => Some(Column::from(c)),
                        OutputColumn::Literal(_) => None,
                    })
                    .collect()
            } else {
                // If we are creating a query for a group universe, we project
                // all columns in the final node. When a user universe that
                // belongs to this group, the proper projection and leaf node
                // will be added.
                final_node_cols.to_vec()
            };

            for pc in qg.parameters() {
                let pc = Column::from(pc);
                if !projected_columns.contains(&pc) {
                    projected_columns.push(pc);
                }
            }

            // We may already have added some of the arithmetic and literal columns
            let (_, already_computed): (Vec<_>, Vec<_>) =
                value_columns_needed_for_predicates(&qg.columns, &qg.global_predicates)
                    .into_iter()
                    .unzip();
            let projected_arithmetic: Vec<(String, ArithmeticExpression)> = qg
                .columns
                .iter()
                .filter_map(|oc| match *oc {
                    OutputColumn::Arithmetic(ref ac) => {
                        if !already_computed.contains(oc) {
                            Some((ac.name.clone(), ac.expression.clone()))
                        } else {
                            projected_columns.push(Column::new(None, &ac.name));
                            None
                        }
                    }
                    OutputColumn::Data(_) => None,
                    OutputColumn::Literal(_) => None,
                })
                .collect();
            let mut projected_literals: Vec<(String, DataType)> = qg
                .columns
                .iter()
                .filter_map(|oc| match *oc {
                    OutputColumn::Arithmetic(_) => None,
                    OutputColumn::Data(_) => None,
                    OutputColumn::Literal(ref lc) => {
                        if !already_computed.contains(oc) {
                            Some((lc.name.clone(), DataType::from(&lc.value)))
                        } else {
                            projected_columns.push(Column::new(None, &lc.name));
                            None
                        }
                    }
                })
                .collect();

            // if this query does not have any parameters, we must add a bogokey
            let has_bogokey = if has_leaf && qg.parameters().is_empty() {
                // only add the bogokey if we haven't already added it prior to a TopK above
                if !projected_columns.contains(&Column::new(None, "bogokey")) {
                    projected_literals.push(("bogokey".into(), DataType::from(0 as i32)));
                }
                true
            } else {
                false
            };

            let ident = if has_leaf {
                format!("q_{:x}_n{}{}", qg.signature().hash, new_node_count, uformat)
            } else {
                String::from(name)
            };

            let leaf_project_node = self.make_project_node(
                &ident,
                final_node,
                projected_columns.iter().collect(),
                projected_arithmetic,
                projected_literals,
                !has_leaf,
            );

            nodes_added.push(leaf_project_node.clone());

            if has_leaf {
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

                let query_params = if has_bogokey {
                    vec![Column::new(None, "bogokey")]
                } else {
                    qg.parameters().into_iter().map(Column::from).collect()
                };

                let leaf_node = MirNode::new(
                    name,
                    self.schema_version,
                    columns,
                    MirNodeType::Leaf {
                        node: leaf_project_node.clone(),
                        keys: query_params,
                    },
                    vec![leaf_project_node.clone()],
                    vec![],
                );
                nodes_added.push(leaf_node);
            }

            debug!(
                self.log,
                "Added final MIR node for query named \"{}\"", name
            );
        }
        // finally, we output all the nodes we generated
        Ok((sec_round, nodes_added, table_mapping, union_base_name))
    }
}
