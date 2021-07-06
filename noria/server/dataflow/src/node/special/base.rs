use crate::prelude::*;
use itertools::Itertools;
use launchpad::Indices;
use maplit::hashmap;
use noria::errors::ReadySetResult;
use noria::{internal, Modification, Operation, ReplicationOffset, TableOperation};
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use vec_map::VecMap;

/// A batch of writes to be persisted to the state backing a [`Base`] node
#[derive(Debug, PartialEq)]
pub struct BaseWrite {
    /// The records to be written
    pub records: Records,

    /// The replication offset to optionally be set.
    ///
    /// `None` in this field means that no changes to the previously-set replication offset should
    /// be made
    ///
    /// See [the documentation for PersistentState](::noria_dataflow::state::persistent_state) for
    /// more information about replication offsets.
    pub replication_offset: Option<ReplicationOffset>,
}

impl From<Records> for BaseWrite {
    fn from(records: Records) -> Self {
        Self {
            records,
            replication_offset: None,
        }
    }
}

/// Base is used to represent the root nodes of the Noria data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug, Serialize, Deserialize)]
pub struct Base {
    primary_key: Option<Vec<usize>>,

    defaults: Vec<DataType>,
    dropped: Vec<usize>,
    unmodified: bool,
}

impl Base {
    /// Create a non-durable base node operator.
    pub fn new(defaults: Vec<DataType>) -> Self {
        Base {
            defaults,
            ..Base::default()
        }
    }

    /// Builder with a known primary key.
    pub fn with_key(mut self, primary_key: Vec<usize>) -> Base {
        self.primary_key = Some(primary_key);
        self
    }

    pub fn key(&self) -> Option<&[usize]> {
        self.primary_key.as_ref().map(|cols| &cols[..])
    }

    /// Add a new column to this base node.
    pub fn add_column(&mut self, default: DataType) -> ReadySetResult<usize> {
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
            "cannot add columns to base nodes without\
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

    pub fn get_dropped(&self) -> VecMap<DataType> {
        self.dropped
            .iter()
            .map(|&col| (col, self.defaults[col].clone()))
            .collect()
    }

    /// Extend a row with default values if it does not match the current schema length
    pub(crate) fn fix(&self, row: &mut Vec<DataType>) {
        if self.unmodified {
            return;
        }

        if row.len() != self.defaults.len() {
            let rlen = row.len();
            row.extend(self.defaults.iter().skip(rlen).cloned());
        }
    }
}

/// A Base clone must have a different unique_id so that no two copies write to the same file.
/// Resetting the writer to None in the original copy is not enough to guarantee that, as the
/// original object can still re-open the log file on-demand from Base::persist_to_log.
impl Clone for Base {
    fn clone(&self) -> Base {
        Base {
            primary_key: self.primary_key.clone(),

            defaults: self.defaults.clone(),
            dropped: self.dropped.clone(),
            unmodified: self.unmodified,
        }
    }
}

impl Default for Base {
    fn default() -> Self {
        Base {
            primary_key: None,

            defaults: Vec::new(),
            dropped: Vec::new(),
            unmodified: true,
        }
    }
}

fn key_val(i: usize, col: usize, r: &TableOperation) -> Option<&DataType> {
    match *r {
        TableOperation::Insert(ref row) => Some(&row[col]),
        TableOperation::DeleteByKey { ref key } => Some(&key[i]),
        TableOperation::DeleteRow { ref row } => Some(&row[col]),
        TableOperation::Update { ref key, .. } => Some(&key[i]),
        TableOperation::InsertOrUpdate { ref row, .. } => Some(&row[col]),
        TableOperation::SetReplicationOffset(_) => None,
    }
}

fn key_of<'a>(key_cols: &'a [usize], r: &'a TableOperation) -> impl Iterator<Item = &'a DataType> {
    key_cols
        .iter()
        .enumerate()
        .filter_map(move |(i, col)| key_val(i, *col, r))
}

impl Base {
    pub(in crate::node) fn take(&self) -> Self {
        Clone::clone(self)
    }

    /// Process table operations for a base table that doesn't have a key, such tables can
    /// have multiple copies of the same row, and delete operations are free to remove any of them
    fn process_unkeyed(&mut self, operations: Vec<TableOperation>) -> ReadySetResult<BaseWrite> {
        // Keep track of the maximal replication offset in the list, if any
        let mut replication_offset: Option<ReplicationOffset> = None;

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
                _ => {
                    internal!("unkeyed base got keyed operation {:?}", op);
                }
            }
        }

        Ok(BaseWrite {
            records: records.into(),
            replication_offset,
        })
    }

    /// Compute the deltas required to apply the list of the provided `TableOperation` to the base table
    pub(in crate::node) fn process(
        &mut self,
        our_index: LocalNodeIndex,
        mut ops: Vec<TableOperation>,
        state: &StateMap,
    ) -> ReadySetResult<BaseWrite> {
        if self.primary_key.is_none() || ops.is_empty() {
            return self.process_unkeyed(ops);
        }

        let mut n_ops = ops.len();
        let key_cols = &self.primary_key.as_ref().unwrap()[..];
        // Sort all of the operations lexicographically by key types, all unkeyed operations will move
        // to the front of the vector (which can only be `SetReplicationOffset`), for the rest of the operations
        // it will group them by their key value.
        ops.sort_by(|a, b| key_of(key_cols, a).cmp(key_of(key_cols, b)));
        let mut ops = ops.into_iter().peekable();

        // First compute the replication offset
        let mut replication_offset: Option<ReplicationOffset> = None;
        while let Some(TableOperation::SetReplicationOffset(offset)) = ops.peek() {
            // Process all of the `SetReplicationOffset` ops, then proceed to the keyed operations as usual
            offset.try_max_into(&mut replication_offset)?;
            ops.next();
            n_ops -= 1;
        }

        // starting record state
        let db = match state.get(our_index) {
            Some(x) => x,
            None => internal!("base with primary key must be materialized"),
        };

        // Group the operations by their key, so we can process each group independently
        let ops = ops.group_by(|op| key_of(key_cols, op).cloned().collect::<Vec<_>>());
        let mut results = Vec::with_capacity(n_ops);
        /// [`TouchedKey`] indicates if the given key was previously deleted or inserted as part of the current batch that
        /// was not yet persisted to the base node.
        enum TouchedKey<'a> {
            Deleted,
            Inserted(Cow<'a, [DataType]>),
        }
        let mut touched_keys: HashMap<Vec<DataType>, TouchedKey> = HashMap::new();

        for (key, ops) in &ops {
            // It is not enough to check the persisted value for the key, as it may have been changed in previous iteration,
            // therefore we have to check it was not changed in one of the outstanding records
            let stored_value = match touched_keys.get(&key) {
                Some(TouchedKey::Inserted(row)) => Some(row.clone()), // Row was added in previous iteration
                Some(TouchedKey::Deleted) => None,                    // Row was deleted previously
                None => match db.lookup(key_cols, &KeyType::from(&key)) {
                    LookupResult::Missing => internal!(),
                    LookupResult::Some(rows) if rows.is_empty() => None,
                    LookupResult::Some(rows) if rows.len() == 1 => rows.into_iter().next(),
                    LookupResult::Some(rows) => {
                        internal!("key {:?} not unique; n={}", key, rows.len())
                    }
                },
            };

            // Current value for the given key following the operations that were already applied to it
            let mut value = stored_value.clone();

            for op in ops {
                match op {
                    TableOperation::Insert(row) if value.is_none() => value = Some(Cow::Owned(row)),
                    TableOperation::DeleteByKey { .. } => value = None,
                    TableOperation::DeleteRow { row } if value == Some(Cow::Borrowed(&row)) => {
                        // Delete the row, but only if it fully matches the current row
                        value = None;
                    }
                    TableOperation::SetReplicationOffset(offset) => {
                        offset.try_max_into(&mut replication_offset)?
                    }
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
                                            Operation::Add => DataType::try_from(old + delta)?,
                                            Operation::Sub => DataType::try_from(old - delta)?,
                                        };
                                    }
                                    Modification::None => {}
                                }
                            }
                        }
                    }
                    op => eprintln!("Base ignoring operation: {:?}", op),
                }
            }

            // Finished processing operations for this key
            if stored_value != value {
                // If the stored value and the new computed value differ we need to update the stored value
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

        Ok(BaseWrite {
            records: results.into(),
            replication_offset,
        })
    }

    pub(in crate::node) fn suggest_indexes(&self, n: NodeIndex) -> HashMap<NodeIndex, Index> {
        if let Some(ref pk) = self.primary_key {
            hashmap! {
                n => Index::hash_map(pk.clone())
            }
        } else {
            HashMap::new()
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

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    #[test]
    fn it_works_new() {
        let b = Base::new(vec![]);

        assert!(b.primary_key.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    mod process {
        use super::*;

        fn test_lots_of_changes_in_same_batch(mut state: Box<dyn State>) {
            use crate::node;
            use crate::prelude::*;

            // most of this is from MockGraph
            let mut graph = Graph::new();
            let source = graph.add_node(Node::new(
                "source",
                &["because-type-inference"],
                node::NodeType::Source,
            ));

            let b = Base::new(vec![]).with_key(vec![0, 2]);
            let global = graph.add_node(Node::new("b", &["x", "y", "z"], b));
            graph.add_edge(source, global, ());
            let local = unsafe { LocalNodeIndex::make(0_u32) };
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

            for (_, index) in graph[global].suggest_indexes(global) {
                state.add_key(&index, None);
            }

            let mut states = StateMap::new();
            states.insert(local, state);
            let n = graph[global].take();
            let mut n = n.finalize(&graph);

            let mut one = move |u: Vec<TableOperation>| {
                let mut m = n
                    .get_base_mut()
                    .unwrap()
                    .process(local, u, &states)
                    .unwrap()
                    .records;
                node::materialize(&mut m, None, None, states.get_mut(local));
                m
            };

            assert_eq!(
                one(vec![
                    TableOperation::Insert(vec![1.into(), "a".into(), 1.into()]),
                    TableOperation::Insert(vec![2.into(), "2a".into(), 1.into()]),
                    TableOperation::Insert(vec![3.into(), "3a".into(), 1.into()]),
                    TableOperation::DeleteByKey {
                        key: vec![1.into(), 1.into()],
                    },
                    TableOperation::Insert(vec![1.into(), "b".into(), 1.into()]),
                    TableOperation::InsertOrUpdate {
                        row: vec![1.into(), "c".into(), 1.into()],
                        update: vec![
                            Modification::None,
                            Modification::Set("never".into()),
                            Modification::None,
                        ],
                    },
                    TableOperation::InsertOrUpdate {
                        row: vec![1.into(), "also never".into(), 1.into()],
                        update: vec![
                            Modification::None,
                            Modification::Set("d".into()),
                            Modification::None,
                        ],
                    },
                    TableOperation::DeleteRow {
                        row: vec![3.into(), "3a".into(), 1.into()]
                    },
                    TableOperation::Update {
                        key: vec![1.into(), 1.into()],
                        update: vec![
                            Modification::None,
                            Modification::Set("e".into()),
                            Modification::None,
                        ],
                    },
                    TableOperation::Update {
                        key: vec![2.into(), 1.into()],
                        update: vec![
                            Modification::None,
                            Modification::Set("2x".into()),
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
            test_lots_of_changes_in_same_batch(Box::new(state));
        }

        #[test]
        fn lots_of_changes_in_same_batch_persistent() {
            let state = PersistentState::new(
                String::from("lots_of_changes_in_same_batch_persistent"),
                None,
                &PersistenceParameters::default(),
            );

            test_lots_of_changes_in_same_batch(Box::new(state));
        }

        #[test]
        fn delete_row_unkeyed() {
            let mut b = Base::new(vec![]);

            let ni = unsafe { LocalNodeIndex::make(0u32) };

            let state: Box<dyn State> = Box::new(PersistentState::new(
                String::from("delete_row_not_in_batch"),
                None,
                &PersistenceParameters::default(),
            ));

            let mut state_map = Map::new();
            state_map.insert(ni, state);

            assert_eq!(
                b.process(
                    ni,
                    vec![
                        TableOperation::Insert(vec![1.into(), 2.into(), 3.into()]),
                        TableOperation::DeleteRow {
                            row: vec![2.into(), 3.into(), 4.into()]
                        }
                    ],
                    &state_map
                )
                .unwrap(),
                BaseWrite {
                    records: vec![
                        Record::Positive(vec![1.into(), 2.into(), 3.into()]),
                        Record::Negative(vec![2.into(), 3.into(), 4.into()])
                    ]
                    .into(),
                    replication_offset: None,
                }
            )
        }

        #[test]
        fn delete_row_not_in_batch_keyed() {
            let mut b = Base::new(vec![]).with_key(vec![0]);

            let ni = unsafe { LocalNodeIndex::make(0u32) };

            let mut state: Box<dyn State> = Box::new(PersistentState::new(
                String::from("delete_row_not_in_batch_keyed"),
                None,
                &PersistenceParameters::default(),
            ));

            state.add_key(&Index::hash_map(vec![0]), None);

            let mut recs = vec![Record::Positive(vec![2.into(), 3.into(), 4.into()])].into();
            state.process_records(&mut recs, None, None);

            let mut state_map = Map::new();
            state_map.insert(ni, state);

            assert_eq!(
                b.process(
                    ni,
                    vec![
                        TableOperation::Insert(vec![1.into(), 2.into(), 3.into()]),
                        TableOperation::DeleteRow {
                            row: vec![2.into(), 3.into(), 4.into()]
                        }
                    ],
                    &state_map
                )
                .unwrap(),
                BaseWrite {
                    records: vec![
                        Record::Positive(vec![1.into(), 2.into(), 3.into()]),
                        Record::Negative(vec![2.into(), 3.into(), 4.into()])
                    ]
                    .into(),
                    replication_offset: None,
                }
            )
        }

        #[test]
        fn delete_after_key_update() {
            let mut b = Base::new(vec![]).with_key(vec![0]);

            let ni = unsafe { LocalNodeIndex::make(0u32) };

            let mut state: Box<dyn State> = Box::new(PersistentState::new(
                String::from("delete_after_key_update"),
                None,
                &PersistenceParameters::default(),
            ));

            state.add_key(&Index::hash_map(vec![0]), None);

            let mut recs = vec![Record::Positive(vec![2.into(), 3.into(), 4.into()])].into();
            state.process_records(&mut recs, None, None);

            let mut state_map = Map::new();
            state_map.insert(ni, state);

            assert_eq!(
                b.process(
                    ni,
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
                    &state_map
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
                }
            )
        }
    }
}
