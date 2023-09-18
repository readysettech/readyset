use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;

use dataflow_state::{MaterializedNodeState, PointKey, SnapshotMode};
use itertools::Itertools;
use nom_sql::Relation;
use readyset_client::{Modification, Operation, TableOperation};
use readyset_data::{DfValue, DfValueKind};
use readyset_errors::ReadySetResult;
use readyset_util::redacted::Sensitive;
use readyset_util::Indices;
use replication_offset::ReplicationOffset;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace};
use vec_map::VecMap;

use crate::node::Column;
use crate::prelude::*;
use crate::processing::LookupIndex;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SetSnapshotMode {
    EnterSnapshotMode,
    FinishSnapshotMode,
}

/// A batch of writes to be persisted to the state backing a [`Base`] node
#[derive(Debug, PartialEq, Eq)]
pub struct BaseWrite {
    /// The records to be written
    pub records: Records,

    /// The replication offset to optionally be set.
    ///
    /// `None` in this field means that no changes to the previously-set replication offset should
    /// be made
    ///
    /// See [the documentation for PersistentState](::readyset_dataflow::state::persistent_state)
    /// for more information about replication offsets.
    pub replication_offset: Option<ReplicationOffset>,

    /// Optionally enter or exit the snapshot mode for this table
    pub set_snapshot_mode: Option<SetSnapshotMode>,
}

impl From<Records> for BaseWrite {
    fn from(records: Records) -> Self {
        Self {
            records,
            replication_offset: None,
            set_snapshot_mode: None,
        }
    }
}

/// Base is used to represent the root nodes of the ReadySet data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[must_use]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Base {
    primary_key: Option<Box<[usize]>>,
    unique_keys: Vec<Box<[usize]>>,

    defaults: Vec<DfValue>,
    dropped: Vec<usize>,
    unmodified: bool,
    permissive_writes: bool,
}

impl Base {
    /// Create a non-durable base node operator.
    pub fn new() -> Self {
        Default::default()
    }

    pub(in crate::node) fn take(&self) -> Self {
        Clone::clone(self)
    }

    pub fn with_default_values(mut self, defaults: Vec<DfValue>) -> Self {
        self.defaults = defaults;
        self
    }

    /// Assign a known primary key to the base, a primary key can't contain NULL columns
    pub fn with_primary_key<K: Into<Box<[usize]>>>(mut self, primary_key: K) -> Self {
        self.primary_key = Some(primary_key.into());
        self
    }

    /// Add a set of known unique keys to base, per SQL specification a key with NULL columns
    /// can be repeat multiple times even as a unique key
    pub fn with_unique_keys<C: AsRef<[usize]>, K: IntoIterator<Item = C>>(
        mut self,
        unique_keys: K,
    ) -> Self {
        self.unique_keys = unique_keys.into_iter().map(|c| c.as_ref().into()).collect();
        self
    }

    pub fn primary_key(&self) -> Option<&[usize]> {
        self.primary_key.as_deref()
    }

    /// Return the list of all unique indices in this base, including the primary key and
    /// the unique keys. If primary key is set it will be the first in the list.
    pub fn all_unique_keys(&self) -> Vec<Box<[usize]>> {
        self.primary_key
            .iter()
            .map(AsRef::as_ref)
            .chain(self.unique_keys.iter().map(AsRef::as_ref))
            .map(|key| key.into())
            .collect()
    }

    /// Add a new column to this base node.
    pub fn add_column(&mut self, default: DfValue) -> ReadySetResult<usize> {
        invariant!(
            !self.defaults.is_empty(),
            "cannot add columns to base nodes without\
             setting default values for initial columns"
        );
        self.defaults.push(default);
        self.unmodified = false;
        Ok(self.defaults.len() - 1)
    }

    /// Drop a column from this base node.
    pub fn drop_column(&mut self, column: usize) -> ReadySetResult<()> {
        invariant!(
            !self.defaults.is_empty(),
            "cannot drop columns from base nodes without\
             setting default values for initial columns"
        );
        invariant!(column < self.defaults.len());
        self.unmodified = false;

        // note that we don't need to *do* anything for dropped columns when we receive records.
        // the only thing that matters is that new Mutators remember to inject default values for
        // dropped columns.
        self.dropped.push(column);
        Ok(())
    }

    pub fn get_dropped(&self) -> VecMap<DfValue> {
        self.dropped
            .iter()
            .map(|&col| (col, self.defaults[col].clone()))
            .collect()
    }

    /// Extend a row with default values if it does not match the current schema length
    pub(crate) fn fix(&self, row: &mut Vec<DfValue>) {
        if self.unmodified {
            return;
        }

        if row.len() != self.defaults.len() {
            let rlen = row.len();
            row.extend(self.defaults.iter().skip(rlen).cloned());
        }
    }

    /// Process table operations for a base table that doesn't have a key, such tables can
    /// have multiple copies of the same row, and delete operations are free to remove any of them
    fn process_unkeyed(
        &mut self,
        db: &MaterializedNodeState,
        operations: Vec<TableOperation>,
    ) -> ReadySetResult<BaseWrite> {
        // Keep track of the maximal replication offset in the list, if any
        let mut replication_offset: Option<ReplicationOffset> = None;
        let mut set_snapshot_mode: Option<SetSnapshotMode> = None;

        // This is a non keyed table, can only apply non-keyed operations
        let mut records = Vec::with_capacity(operations.len());
        for op in operations {
            match op {
                TableOperation::Insert(mut row) => {
                    self.fix(&mut row);
                    records.push(Record::Positive(row));
                }
                TableOperation::DeleteRow { mut row } => {
                    self.fix(&mut row);
                    records.push(Record::Negative(row));
                }
                TableOperation::SetReplicationOffset(offset) => {
                    offset.try_max_into(&mut replication_offset)?;
                }
                TableOperation::SetSnapshotMode(s) => {
                    set_snapshot_mode = Some(if s {
                        SetSnapshotMode::EnterSnapshotMode
                    } else {
                        SetSnapshotMode::FinishSnapshotMode
                    })
                }
                TableOperation::Truncate => {
                    records.clear();
                    let mut all_records = db.all_records();
                    records.extend(all_records.read().iter().map(|r| Record::Negative(r)));
                }
                TableOperation::DeleteByKey { .. }
                | TableOperation::InsertOrUpdate { .. }
                | TableOperation::Update { .. } => {
                    internal!("unkeyed base got keyed operation {:?}", op);
                }
            }
        }

        Ok(BaseWrite {
            records: records.into(),
            replication_offset,
            set_snapshot_mode,
        })
    }

    /// Compute the deltas required to apply the list of the provided `TableOperation` to the base
    /// table
    pub(in crate::node) fn process_ops(
        &mut self,
        our_index: LocalNodeIndex,
        columns: &[Column],
        mut ops: Vec<TableOperation>,
        state: &StateMap,
        snapshot_mode: SnapshotMode,
        name: Relation,
    ) -> ReadySetResult<BaseWrite> {
        trace!(node = %our_index, base_ops = ?ops);
        for op in ops.iter_mut() {
            apply_table_op_coercions(op, columns, self.primary_key())?;
        }

        let db = match state.get(our_index) {
            Some(x) => x,
            None => internal!("base nodes must always be materialized"),
        };

        let key_cols = match &self.primary_key {
            Some(key) if !ops.is_empty() => key.as_ref(),
            _ => return self.process_unkeyed(db, ops),
        };

        let mut n_ops = ops.len();
        // Sort all of the operations lexicographically by key types, all unkeyed operations will
        // move to the front of the vector (which can only be `SetReplicationOffset`), for
        // the rest of the operations it will group them by their key value.
        ops.sort_by(|a, b| {
            key_of(key_cols, a).cmp(key_of(key_cols, b)).then_with(|| {
                // Put all the Truncate ops last so we can process them separately
                if *a == TableOperation::Truncate {
                    Ordering::Greater
                } else if *b == TableOperation::Truncate {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
        });
        let mut ops = ops.into_iter().peekable();

        // First compute the replication offset
        let mut replication_offset: Option<ReplicationOffset> = None;
        let mut set_snapshot_mode: Option<SetSnapshotMode> = None;

        while let Some(op) = ops.peek() {
            // Process all of the `SetReplicationOffset` and `SetSnapshotMode` ops, then proceed to
            // the keyed operations as usual
            match op {
                TableOperation::SetReplicationOffset(offset) => {
                    offset.try_max_into(&mut replication_offset)?;
                    ops.next();
                    n_ops -= 1;
                }
                TableOperation::SetSnapshotMode(s) => {
                    set_snapshot_mode = Some(if *s {
                        SetSnapshotMode::EnterSnapshotMode
                    } else {
                        SetSnapshotMode::FinishSnapshotMode
                    });
                    ops.next();
                    n_ops -= 1;
                }
                _ => break,
            }
        }

        let mut results = vec![];

        let mut truncated = false;
        while let Some(TableOperation::Truncate) = ops.peek() {
            n_ops -= 1;
            ops.next();
            if !truncated {
                debug!("Truncating base");
                truncated = true;
                let mut all_records = db.all_records();
                results.extend(all_records.read().iter().map(|r| Record::Negative(r)));
            }
        }

        // Group the operations by their key, so we can process each group independently
        let ops = ops.group_by(|op| key_of(key_cols, op).cloned().collect::<Vec<_>>());
        results.reserve(n_ops);

        /// [`TouchedKey`] indicates if the given key was previously deleted or inserted as part of
        /// the current batch that was not yet persisted to the base node.
        enum TouchedKey<'a> {
            Deleted,
            Inserted(Cow<'a, [DfValue]>),
        }
        let mut touched_keys: HashMap<Vec<DfValue>, TouchedKey> = HashMap::new();
        let mut failed_log = FailedOpLogger::new(name);

        for (key, ops) in &ops {
            // It is not enough to check the persisted value for the key, as it may have been
            // changed in previous iteration, therefore we have to check it was not
            // changed in one of the outstanding records
            let stored_value = if snapshot_mode.is_enabled() {
                // In snapshot mode don't check the currently store values as it doesn't matter for
                // correctness but imposes a heavy toll on batched writes
                None
            } else {
                match touched_keys.get(&key) {
                    Some(TouchedKey::Inserted(row)) => Some(row.clone()), /* Row was added in previous iteration */
                    Some(TouchedKey::Deleted) => None,                    /* Row was deleted */
                    // previously
                    None => match db.lookup(key_cols, &PointKey::from(key.clone())) {
                        LookupResult::Missing => internal!(),
                        LookupResult::Some(rows) if rows.is_empty() => None,
                        LookupResult::Some(rows) if rows.len() == 1 => rows.into_iter().next(),
                        LookupResult::Some(rows) => {
                            internal!(
                                "key {:?} not unique; num_rows={}",
                                Sensitive(&key),
                                rows.len()
                            );
                        }
                    },
                }
            };

            // Current value for the given key following the operations that were already applied to
            // it
            let mut value = stored_value.clone();

            for op in ops {
                match op {
                    TableOperation::Insert(row) if value.is_none() => value = Some(Cow::Owned(row)),
                    TableOperation::Insert(_) => {
                        failed_log.failed_insert();
                    }
                    TableOperation::DeleteRow { row } if value == Some(Cow::Borrowed(&row)) => {
                        // Delete the row, but only if it fully matches the current row
                        value = None;
                    }
                    TableOperation::DeleteRow { row } => {
                        failed_log.failed_delete(row, value.as_deref());
                    }
                    TableOperation::DeleteByKey { .. } => value = None,

                    TableOperation::InsertOrUpdate { row, .. } if value.is_none() => {
                        value = Some(Cow::Owned(row))
                    }
                    TableOperation::InsertOrUpdate { update, .. }
                    | TableOperation::Update { update, .. }
                        if value.is_some() =>
                    {
                        if let Some(updated) = value.as_mut().map(Cow::to_mut) {
                            for (col, op) in update.into_iter().enumerate() {
                                // XXX: make sure user doesn't update primary key?
                                match op {
                                    Modification::Set(v) => updated[col] = v,
                                    Modification::Apply(op, v) => {
                                        let old: i128 = <i128>::try_from(updated[col].clone())?;
                                        let delta: i128 = <i128>::try_from(v)?;
                                        updated[col] = match op {
                                            Operation::Add => DfValue::try_from(old + delta)?,
                                            Operation::Sub => DfValue::try_from(old - delta)?,
                                        };
                                    }
                                    Modification::None => {}
                                }
                            }
                        }
                    }
                    TableOperation::Update { .. } => {
                        failed_log.failed_update();
                    }
                    TableOperation::SetSnapshotMode(_)
                    | TableOperation::SetReplicationOffset(_)
                    | TableOperation::InsertOrUpdate { .. }
                    | TableOperation::Truncate => {
                        // This is unreachable, because all of those cases are handled above
                    }
                }
            }

            // Finished processing operations for this key
            if stored_value != value {
                // If the stored value and the new computed value differ we need to update the
                // stored value
                if let Some(row) = stored_value {
                    // First delete the existing value, if any
                    touched_keys.insert(key, TouchedKey::Deleted); // We don't remove here so we know not to look in db
                    results.push(Record::Negative(row.into_owned()));
                }
                if let Some(row) = value {
                    // Second store the new value, if any
                    touched_keys.insert(
                        row.cloned_indices(key_cols.to_vec())
                            .map_err(|_| ReadySetError::InvalidRecordLength)?,
                        TouchedKey::Inserted(row.clone()),
                    );
                    results.push(Record::Positive(row.into_owned()));
                }
            }
        }

        for r in &mut results {
            self.fix(r);
        }

        // We allow permissive writes if we are running without an upstream.
        // If we are allowing permissive writes, we treat failed writes as no-ops
        // instead of errors, using our base tables as a pseudo-upstream
        // If we are replicating, we don't expect any failed writes, so we treat any
        // as an error.
        if self.permissive_writes {
            failed_log.ensure_no_failed_ops()?;
        }

        Ok(BaseWrite {
            records: results.into(),
            replication_offset,
            set_snapshot_mode,
        })
    }

    pub(in crate::node) fn suggest_indexes(&self, n: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        if let Some(pk) = &self.primary_key {
            HashMap::from([(
                n,
                LookupIndex::Strict(Index::hash_map(pk.as_ref().to_vec())),
            )])
        } else {
            HashMap::new()
        }
    }
}

impl Default for Base {
    fn default() -> Self {
        Base {
            primary_key: None,
            unique_keys: Vec::new(),
            defaults: Vec::new(),
            dropped: Vec::new(),
            unmodified: true,
            permissive_writes: false,
        }
    }
}

fn key_val(i: usize, col: usize, r: &TableOperation) -> Option<&DfValue> {
    match *r {
        TableOperation::Insert(ref row) => Some(&row[col]),
        TableOperation::DeleteByKey { ref key } => Some(&key[i]),
        TableOperation::DeleteRow { ref row } => Some(&row[col]),
        TableOperation::Update { ref key, .. } => Some(&key[i]),
        TableOperation::InsertOrUpdate { ref row, .. } => Some(&row[col]),
        TableOperation::SetReplicationOffset(_)
        | TableOperation::SetSnapshotMode(_)
        | TableOperation::Truncate => None,
    }
}

fn key_of<'a>(key_cols: &'a [usize], r: &'a TableOperation) -> impl Iterator<Item = &'a DfValue> {
    key_cols
        .iter()
        .enumerate()
        .filter_map(move |(i, col)| key_val(i, *col, r))
}

/// Coerce values in the table operation to the correct types for the underlying schema, if
/// needed.
///
/// This is used to handle cases where certain types may have a different user-visible
/// representation than their underlying database representation (for example, inserting enum
/// values, which appear as strings to the user but must be stored as integers in the database).
///
/// Note that the actual type-specific logic is implemented as a [`DfValue`] method, so as to keep
/// type logic out of the base node code.
fn apply_table_op_coercions(
    op: &mut TableOperation,
    columns: &[Column],
    primary_key: Option<&[usize]>,
) -> ReadySetResult<()> {
    let coerce_row = |row: &mut Vec<DfValue>| {
        for (val, col) in row.iter_mut().zip(columns) {
            val.maybe_coerce_for_table_op(col.ty())?;
        }
        Ok(())
    };

    let coerce_update = |update: &mut Vec<Modification>| {
        for (modification, col) in update.iter_mut().zip(columns) {
            match modification {
                Modification::Set(val) | Modification::Apply(_, val) => {
                    val.maybe_coerce_for_table_op(col.ty())?
                }
                Modification::None => {}
            }
        }
        Ok(())
    };

    let coerce_key = |key: &mut Vec<DfValue>| {
        if let Some(pk) = primary_key {
            for (val, col_idx) in key.iter_mut().zip(pk) {
                let col = columns
                    .get(*col_idx)
                    .ok_or_else(|| internal_err!("Key column index out of bounds"))?;
                val.maybe_coerce_for_table_op(col.ty())?;
            }
        }
        Ok(())
    };

    match op {
        TableOperation::Insert(row) | TableOperation::DeleteRow { row } => coerce_row(row),
        TableOperation::InsertOrUpdate { row, update } => {
            coerce_row(row)?;
            coerce_update(update)
        }
        TableOperation::Update { update, key } => {
            coerce_update(update)?;
            coerce_key(key)
        }
        TableOperation::DeleteByKey { key } => coerce_key(key),
        TableOperation::Truncate
        | TableOperation::SetReplicationOffset(_)
        | TableOperation::SetSnapshotMode(_) => Ok(()),
    }
}

/// A helper to log information about failed table updates without leaking data
pub(crate) struct FailedOpLogger {
    insert_existing: usize,
    update_non_existing: usize,
    delete_non_existing: usize,
    // Maps from (column index, deleted data type, actual data type) to count for columns with
    // different types
    delete_type_mismatch: HashMap<(usize, Option<DfValueKind>, Option<DfValueKind>), usize>,
    // Maps from (column inxed, deleted data type) to count for columns with different values
    delete_row_data_mismatch: HashMap<(usize, DfValueKind), usize>,
    table: Relation,
}

impl FailedOpLogger {
    pub(crate) fn new(table: Relation) -> Self {
        Self {
            insert_existing: Default::default(),
            update_non_existing: Default::default(),
            delete_non_existing: Default::default(),
            delete_type_mismatch: Default::default(),
            delete_row_data_mismatch: Default::default(),
            table,
        }
    }

    fn failed_delete(&mut self, deleted_row: Vec<DfValue>, current_row: Option<&[DfValue]>) {
        use itertools::EitherOrBoth;

        match current_row {
            None => self.delete_non_existing += 1,
            Some(row) => {
                for (col, vals) in deleted_row.iter().zip_longest(row.iter()).enumerate() {
                    let (del, cur) = match vals {
                        EitherOrBoth::Both(d, c) => (Some(d), Some(c)),
                        EitherOrBoth::Left(d) => (Some(d), None),
                        EitherOrBoth::Right(c) => (None, Some(c)),
                    };

                    if del.map(DfValueKind::from) != cur.map(DfValueKind::from) {
                        *self
                            .delete_type_mismatch
                            .entry((col, del.map(DfValueKind::from), cur.map(DfValueKind::from)))
                            .or_default() += 1;
                        // Only report the first inconsistency in the row
                        break;
                    } else if del != cur {
                        *self
                            .delete_row_data_mismatch
                            // Unwrap ok because del has to be Some in else statement
                            .entry((col, DfValueKind::from(del.unwrap())))
                            .or_default() += 1;
                        // Only report the first inconsistency in the row
                        break;
                    }
                }
            }
        }
    }

    fn failed_insert(&mut self) {
        self.insert_existing += 1;
    }

    fn failed_update(&mut self) {
        self.update_non_existing += 1;
    }

    fn has_failed_inserts(&self) -> bool {
        self.insert_existing != 0
    }

    fn has_failed_deletes_or_updates(&self) -> bool {
        !(self.update_non_existing == 0
            && self.delete_non_existing == 0
            && self.delete_type_mismatch.is_empty()
            && self.delete_row_data_mismatch.is_empty())
    }

    fn ensure_no_failed_ops(self) -> ReadySetResult<()> {
        if self.has_failed_deletes_or_updates() || self.has_failed_inserts() {
            self.log_errors();
            return Err(ReadySetError::FailedBaseOps { table: self.table });
        }

        Ok(())
    }

    pub fn log_errors(&self) {
        if self.has_failed_inserts() {
            // If we failed to insert an op to the base table, our state is no longer correct, even
            // if we are allowing permissive writes.
            error!(
                node = %self.table.display_unquoted(),
                insert = %self.insert_existing,
                "Table failed to apply insert operations"
            );
        }

        // If we are allowing permissive writes, we should have never called this function for
        // failed deletes or updates, so any we see here we log an error for.
        if self.has_failed_deletes_or_updates() {
            error!(
                table = %self.table.display_unquoted(),
                update = %self.update_non_existing,
                delete_non_existing = %self.delete_non_existing,
                delete_type_mismatch = %self.delete_type_mismatch.len(),
                delete_data_mismatch = %self.delete_row_data_mismatch.len(),
                "Table failed to apply update or delete operations"
            );

            if !self.delete_type_mismatch.is_empty() || !self.delete_row_data_mismatch.is_empty() {
                error!(
                    table = %self.table.display_unquoted(),
                    type_mismatch = ?self.delete_type_mismatch,
                    data_mismatch = ?self.delete_row_data_mismatch,
                    "Table failed to apply delete operations"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works_default() {
        let b = Base::default();

        assert!(b.primary_key.is_none());
        assert!(b.unique_keys.is_empty());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert!(b.unmodified);
    }

    #[test]
    fn it_works_new() {
        let b = Base::new();

        assert!(b.primary_key.is_none());
        assert!(b.unique_keys.is_empty());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert!(b.unmodified);
    }

    mod process {
        use std::convert::TryInto;

        use dataflow_state::MaterializedNodeState;
        use readyset_data::DfType;

        use super::*;
        use crate::node::Column as DfColumn;

        fn test_lots_of_changes_in_same_batch(mut state: MaterializedNodeState) {
            use crate::node;
            use crate::prelude::*;

            // most of this is from MockGraph
            let mut graph = Graph::new();
            let source = graph.add_node(Node::new(
                "source",
                vec![DfColumn::new("".into(), DfType::Unknown, None)],
                node::NodeType::Source,
            ));

            let b = Base::new().with_primary_key([0, 2]);
            let global = graph.add_node(Node::new(
                "b",
                vec![
                    DfColumn::new("x".into(), DfType::Unknown, None),
                    DfColumn::new("y".into(), DfType::Unknown, None),
                    DfColumn::new("z".into(), DfType::Unknown, None),
                ],
                b,
            ));
            graph.add_edge(source, global, ());
            let local = LocalNodeIndex::make(0_u32);
            let mut ip: IndexPair = global.into();
            ip.set_local(local);
            graph
                .node_weight_mut(global)
                .unwrap()
                .set_finalized_addr(ip);

            let mut remap = HashMap::new();
            remap.insert(global, ip);
            graph.node_weight_mut(global).unwrap().on_commit(&remap);
            graph.node_weight_mut(global).unwrap().add_to(0.into());

            for (_, lookup_index) in graph[global].suggest_indexes(global) {
                match lookup_index {
                    LookupIndex::Strict(index) => state.add_index(index, None),
                    LookupIndex::Weak(index) => state.add_weak_index(index),
                }
            }

            let mut states = StateMap::new();
            states.insert(local, state);
            let n = graph[global].take();
            let mut n = n.finalize(&graph);

            let name = n.name().clone();
            let one = move |u: Vec<TableOperation>| {
                let mut m = n
                    .get_base_mut()
                    .unwrap()
                    .process_ops(
                        local,
                        &[
                            Column::new("a".into(), DfType::Int, None),
                            Column::new("a".into(), DfType::DEFAULT_TEXT, None),
                            Column::new("a".into(), DfType::Int, None),
                        ],
                        u,
                        &states,
                        SnapshotMode::SnapshotModeDisabled,
                        name,
                    )
                    .unwrap()
                    .records;
                node::materialize(&mut m, None, None, states.get_mut(local)).unwrap();
                m
            };

            assert_eq!(
                one(vec![
                    TableOperation::Insert(vec![1.into(), "a".try_into().unwrap(), 1.into()]),
                    TableOperation::Insert(vec![2.into(), "2a".try_into().unwrap(), 1.into()]),
                    TableOperation::Insert(vec![3.into(), "3a".try_into().unwrap(), 1.into()]),
                    TableOperation::DeleteByKey {
                        key: vec![1.into(), 1.into()],
                    },
                    TableOperation::Insert(vec![1.into(), "b".try_into().unwrap(), 1.into()]),
                    TableOperation::InsertOrUpdate {
                        row: vec![1.into(), "c".try_into().unwrap(), 1.into()],
                        update: vec![
                            Modification::None,
                            Modification::Set("never".try_into().unwrap()),
                            Modification::None,
                        ],
                    },
                    TableOperation::InsertOrUpdate {
                        row: vec![1.into(), "also never".try_into().unwrap(), 1.into()],
                        update: vec![
                            Modification::None,
                            Modification::Set("d".try_into().unwrap()),
                            Modification::None,
                        ],
                    },
                    TableOperation::DeleteRow {
                        row: vec![3.into(), "3a".try_into().unwrap(), 1.into()]
                    },
                    TableOperation::Update {
                        key: vec![1.into(), 1.into()],
                        update: vec![
                            Modification::None,
                            Modification::Set("e".try_into().unwrap()),
                            Modification::None,
                        ],
                    },
                    TableOperation::Update {
                        key: vec![2.into(), 1.into()],
                        update: vec![
                            Modification::None,
                            Modification::Set("2x".try_into().unwrap()),
                            Modification::None,
                        ],
                    },
                    TableOperation::DeleteByKey {
                        key: vec![1.into(), 1.into()],
                    },
                    TableOperation::DeleteByKey {
                        key: vec![2.into(), 1.into()],
                    },
                ]),
                Records::default()
            );
        }

        #[test]
        fn lots_of_changes_in_same_batch() {
            let state = MemoryState::default();
            test_lots_of_changes_in_same_batch(MaterializedNodeState::Memory(state));
        }

        #[test]
        fn lots_of_changes_in_same_batch_persistent() {
            let state = PersistentState::new(
                String::from("lots_of_changes_in_same_batch_persistent"),
                Vec::<Box<[usize]>>::new(),
                &PersistenceParameters::default(),
            )
            .unwrap();

            test_lots_of_changes_in_same_batch(MaterializedNodeState::Persistent(state));
        }

        #[test]
        fn delete_row_unkeyed() {
            let mut b = Base::new();

            let ni = LocalNodeIndex::make(0u32);

            let state = MaterializedNodeState::Persistent(
                PersistentState::new(
                    String::from("delete_row_not_in_batch"),
                    Vec::<Box<[usize]>>::new(),
                    &PersistenceParameters::default(),
                )
                .unwrap(),
            );

            let mut state_map = NodeMap::new();
            state_map.insert(ni, state);

            let table = Relation {
                name: "test".into(),
                schema: None,
            };
            assert_eq!(
                b.process_ops(
                    ni,
                    &[],
                    vec![
                        TableOperation::Insert(vec![1.into(), 2.into(), 3.into()]),
                        TableOperation::DeleteRow {
                            row: vec![2.into(), 3.into(), 4.into()]
                        }
                    ],
                    &state_map,
                    SnapshotMode::SnapshotModeDisabled,
                    table,
                )
                .unwrap(),
                BaseWrite {
                    records: vec![
                        Record::Positive(vec![1.into(), 2.into(), 3.into()]),
                        Record::Negative(vec![2.into(), 3.into(), 4.into()])
                    ]
                    .into(),
                    replication_offset: None,
                    set_snapshot_mode: None,
                }
            )
        }

        #[test]
        fn delete_row_not_in_batch_keyed() {
            let mut b = Base::new().with_primary_key([0]);

            let ni = LocalNodeIndex::make(0u32);

            let mut state = MaterializedNodeState::Persistent(
                PersistentState::new(
                    String::from("delete_row_not_in_batch_keyed"),
                    Vec::<Box<[usize]>>::new(),
                    &PersistenceParameters::default(),
                )
                .unwrap(),
            );

            state.add_index(Index::hash_map(vec![0]), None);

            let mut recs = vec![Record::Positive(vec![2.into(), 3.into(), 4.into()])].into();
            state.process_records(&mut recs, None, None).unwrap();

            let mut state_map = NodeMap::new();
            state_map.insert(ni, state);

            let table = Relation {
                name: "test".into(),
                schema: None,
            };
            assert_eq!(
                b.process_ops(
                    ni,
                    &[],
                    vec![
                        TableOperation::Insert(vec![1.into(), 2.into(), 3.into()]),
                        TableOperation::DeleteRow {
                            row: vec![2.into(), 3.into(), 4.into()]
                        }
                    ],
                    &state_map,
                    SnapshotMode::SnapshotModeDisabled,
                    table,
                )
                .unwrap(),
                BaseWrite {
                    records: vec![
                        Record::Positive(vec![1.into(), 2.into(), 3.into()]),
                        Record::Negative(vec![2.into(), 3.into(), 4.into()])
                    ]
                    .into(),
                    replication_offset: None,
                    set_snapshot_mode: None,
                }
            )
        }

        #[test]
        fn delete_after_key_update() {
            let mut b = Base::new().with_primary_key([0]);

            let ni = LocalNodeIndex::make(0u32);

            let mut state = MaterializedNodeState::Persistent(
                PersistentState::new(
                    String::from("delete_after_key_update"),
                    Vec::<Box<[usize]>>::new(),
                    &PersistenceParameters::default(),
                )
                .unwrap(),
            );

            state.add_index(Index::hash_map(vec![0]), None);

            let mut recs = vec![Record::Positive(vec![2.into(), 3.into(), 4.into()])].into();
            state.process_records(&mut recs, None, None).unwrap();

            let mut state_map = NodeMap::new();
            state_map.insert(ni, state);

            let table = Relation {
                name: "test".into(),
                schema: None,
            };
            assert_eq!(
                b.process_ops(
                    ni,
                    &[Column::new("a".into(), DfType::Int, None)],
                    vec![
                        TableOperation::Update {
                            key: vec![2.into()],
                            update: vec![
                                Modification::Set(3.into()),
                                Modification::None,
                                Modification::None
                            ]
                        },
                        TableOperation::DeleteByKey {
                            key: vec![3.into()]
                        }
                    ],
                    &state_map,
                    SnapshotMode::SnapshotModeDisabled,
                    table,
                )
                .unwrap(),
                BaseWrite {
                    records: vec![
                        Record::Negative(vec![2.into(), 3.into(), 4.into()]),
                        Record::Positive(vec![3.into(), 3.into(), 4.into()]),
                        Record::Negative(vec![3.into(), 3.into(), 4.into()])
                    ]
                    .into(),
                    replication_offset: None,
                    set_snapshot_mode: None,
                }
            )
        }

        #[test]
        fn truncate() {
            let mut b = Base::new().with_primary_key([0]);
            let ni = LocalNodeIndex::make(0u32);
            let mut state = MaterializedNodeState::Persistent(
                PersistentState::new(
                    "truncate".into(),
                    Vec::<Box<[usize]>>::new(),
                    &PersistenceParameters::default(),
                )
                .unwrap(),
            );

            state.add_index(Index::hash_map(vec![0]), None);

            let mut recs = vec![
                Record::Positive(vec![1.into(), "a".into()]),
                Record::Positive(vec![1.into(), "b".into()]),
                Record::Positive(vec![2.into(), "b".into()]),
                Record::Positive(vec![3.into(), "c".into()]),
            ]
            .into();
            state.process_records(&mut recs, None, None).unwrap();

            let mut state_map = NodeMap::new();
            state_map.insert(ni, state);

            let table = Relation {
                name: "test".into(),
                schema: None,
            };
            let res = b
                .process_ops(
                    ni,
                    &[],
                    vec![TableOperation::Truncate],
                    &state_map,
                    SnapshotMode::SnapshotModeDisabled,
                    table,
                )
                .unwrap();
            assert_eq!(
                res,
                BaseWrite {
                    records: vec![
                        Record::Negative(vec![1.into(), "a".into()]),
                        Record::Negative(vec![1.into(), "b".into()]),
                        Record::Negative(vec![2.into(), "b".into()]),
                        Record::Negative(vec![3.into(), "c".into()]),
                    ]
                    .into(),
                    replication_offset: None,
                    set_snapshot_mode: None
                }
            );
        }

        #[test]
        fn truncate_unkeyed() {
            let mut b = Base::new();
            let ni = LocalNodeIndex::make(0u32);
            let mut state = MaterializedNodeState::Persistent(
                PersistentState::new(
                    "truncate".into(),
                    Vec::<Box<[usize]>>::new(),
                    &PersistenceParameters::default(),
                )
                .unwrap(),
            );

            state.add_index(Index::hash_map(vec![0]), None);
            let mut recs = vec![
                Record::Positive(vec![1.into(), "a".into()]),
                Record::Positive(vec![1.into(), "b".into()]),
                Record::Positive(vec![2.into(), "b".into()]),
                Record::Positive(vec![3.into(), "c".into()]),
            ]
            .into();
            state.process_records(&mut recs, None, None).unwrap();

            let mut state_map = NodeMap::new();
            state_map.insert(ni, state);

            let table = Relation {
                name: "test".into(),
                schema: None,
            };
            let res = b
                .process_ops(
                    ni,
                    &[],
                    vec![TableOperation::Truncate],
                    &state_map,
                    SnapshotMode::SnapshotModeDisabled,
                    table,
                )
                .unwrap();
            assert_eq!(
                res,
                BaseWrite {
                    records: vec![
                        Record::Negative(vec![1.into(), "a".into()]),
                        Record::Negative(vec![1.into(), "b".into()]),
                        Record::Negative(vec![2.into(), "b".into()]),
                        Record::Negative(vec![3.into(), "c".into()]),
                    ]
                    .into(),
                    replication_offset: None,
                    set_snapshot_mode: None
                }
            );
        }
    }
}
