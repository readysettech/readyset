//! We define `PersistentStateHandle` in a submodule to prevent the `persistent_state` module from
//! accessing its private members. This is done to avoid deadlocks: since `shared_state` and `db`
//! are locked separately, it is possible for threads to hold write locks separately on each and
//! then request a write lock on the other. `PersistentStateHandle`'s API requires that threads
//! already hold a write lock on the RocksDB handle before acquiring a write lock on the shared
//! state, which prevents deadlocks.
use std::sync::Arc;

use common::IndexType;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use readyset_data::DfValue;
use replication_offset::ReplicationOffset;
use rocksdb::{self, DB};
use tracing::debug;

use super::{deserialize_row, PersistentState, SharedState, PK_CF};
use crate::{PointKey, RecordResult};

/// A handle that can cloned and shared between threads to safely read from the
/// [`PersistentState`] concurrently.
#[derive(Clone)]
pub struct PersistentStateHandle {
    /// The replication offset used to make sure the read handle received all forward
    /// processing messages for state, if the replication offset of the read handle is
    /// behind that of the base table (`inner.replication_offset`), lookups will result
    /// in a miss.
    pub replication_offset: Option<ReplicationOffset>,
    shared_state: Arc<RwLock<SharedState>>,
    /// The handle to the RocksDB database we are reading from
    db: Arc<RwLock<DB>>,
}

pub(super) struct PersistentStateReadGuard<'a> {
    pub(super) db: RwLockReadGuard<'a, DB>,
    pub(super) shared_state: RwLockReadGuard<'a, SharedState>,
}

pub(super) struct PersistentStateWriteGuard<'a> {
    pub(super) db: RwLockWriteGuard<'a, DB>,
    pub(super) shared_state: RwLockWriteGuard<'a, SharedState>,
}

impl PersistentStateHandle {
    pub(super) fn new(
        shared_state: SharedState,
        db: DB,
        replication_offset: Option<ReplicationOffset>,
    ) -> Self {
        Self {
            shared_state: Arc::new(RwLock::new(shared_state)),
            db: Arc::new(RwLock::new(db)),
            replication_offset,
        }
    }

    /// Acquires read locks on both the RocksDB handle and the shared state. The decision not
    /// to expose the ability to acquire a read lock on the shared state alone was deliberate:
    /// by requiring that a thread already have a read lock on the RocksDB handle before
    /// acquiring a read lock on the shared state, we eliminate the potential for deadlocks.
    pub(super) fn inner(&self) -> PersistentStateReadGuard {
        PersistentStateReadGuard {
            db: self.db.read(),
            shared_state: self.shared_state.read(),
        }
    }

    /// Acquires write locks on both the RocksDB handle and the shared state. The decision not
    /// to expose the ability to acquire a write lock on the shared state alone was deliberate:
    /// by requiring that a thread already have a write lock on the RocksDB handle before
    /// acquiring a write lock on the shared state, we eliminate the potential for deadlocks.
    pub(super) fn inner_mut(&self) -> PersistentStateWriteGuard {
        PersistentStateWriteGuard {
            db: self.db.write(),
            shared_state: self.shared_state.write(),
        }
    }

    pub(super) fn db(&self) -> RwLockReadGuard<'_, DB> {
        self.db.read()
    }

    /// Perform a lookup for multiple equal keys at once. The results are returned in the order
    /// of the original keys.
    pub(super) fn lookup_multi<'a>(
        &'a self,
        columns: &[usize],
        keys: &[PointKey],
    ) -> Vec<RecordResult<'a>> {
        if keys.is_empty() {
            return vec![];
        }
        let inner = self.inner();

        let index = inner.shared_state.index(IndexType::HashMap, columns);
        let is_primary = index.is_primary;

        let cf = inner.db.cf_handle(&index.column_family).unwrap();
        // Create an iterator once, reuse it for each key
        let mut iter = inner.db.raw_iterator_cf(cf);
        let mut iter_primary = if !is_primary {
            Some(
                inner.db.raw_iterator_cf(
                    inner
                        .db
                        .cf_handle(PK_CF)
                        .expect("Primary key column family not found"),
                ),
            )
        } else {
            None
        };

        keys.iter()
            .map(|k| {
                let prefix = PersistentState::serialize_prefix(k);
                let mut rows = Vec::new();

                let is_unique = index.is_unique && !k.has_null();

                iter.seek(&prefix); // Find the next key

                while iter.key().map(|k| k.starts_with(&prefix)).unwrap_or(false) {
                    let val = match &mut iter_primary {
                        Some(iter_primary) => {
                            // If we have a primary iterator, it means this is a secondary index
                            // and we need to lookup by the
                            // primary key next
                            iter_primary.seek(iter.value().unwrap());
                            deserialize_row(iter_primary.value().unwrap())
                        }
                        None => deserialize_row(iter.value().unwrap()),
                    };

                    rows.push(val);

                    if is_unique {
                        // We know that there is only one row for this index
                        break;
                    }

                    iter.next();
                }

                RecordResult::Owned(rows)
            })
            .collect()
    }

    /// Looks up rows in an index
    /// If the index is the primary index, the lookup gets the rows from the primary index
    /// directly. If the index is a secondary index, we will first lookup the primary
    /// index keys from that secondary index, then perform a lookup into the primary
    /// index
    pub(super) fn do_lookup(&self, columns: &[usize], key: &PointKey) -> Option<Vec<Vec<DfValue>>> {
        let inner = self.inner();
        if self.replication_offset < inner.shared_state.replication_offset {
            // We are checking the replication offset under a read lock, and the lock remains in
            // place until after the read completed, guaranteeing that no write takes place. An
            // alternative would be to use a transaction that reads the log offset from the meta
            // with the value.
            debug!("Consistency miss in PersistentStateHandle");
            return None;
        }
        let index = inner.shared_state.index(IndexType::HashMap, columns);

        let cf = inner.db.cf_handle(&index.column_family).unwrap();
        let primary_cf = if !index.is_primary {
            Some(inner.db.cf_handle(PK_CF).unwrap())
        } else {
            None
        };

        let prefix = PersistentState::serialize_prefix(key);

        if index.is_unique && !key.has_null() {
            // This is a unique key, so we know there's only one row to retrieve
            let value = inner.db.get_pinned_cf(cf, &prefix).unwrap();
            Some(match (value, primary_cf) {
                (None, _) => vec![],
                (Some(value), None) => vec![deserialize_row(value)],
                (Some(pk), Some(primary_cf)) => vec![deserialize_row(
                    inner
                        .db
                        .get_pinned_cf(primary_cf, pk)
                        .unwrap()
                        .expect("Existing primary key"),
                )],
            })
        } else {
            // This could correspond to more than one value, so we'll use a prefix_iterator,
            // for each row
            let mut rows = Vec::new();
            let mut opts = rocksdb::ReadOptions::default();
            opts.set_prefix_same_as_start(true);

            let mut iter = inner.db.raw_iterator_cf_opt(cf, opts);
            let mut iter_primary = primary_cf.map(|pcf| inner.db.raw_iterator_cf(pcf));

            iter.seek(&prefix);

            while let Some(value) = iter.value() {
                let raw_row = match &mut iter_primary {
                    Some(iter_primary) => {
                        iter_primary.seek(value);
                        iter_primary.value().expect("Existing primary key")
                    }
                    None => value,
                };

                rows.push(deserialize_row(raw_row));
                iter.next();
            }

            Some(rows)
        }
    }
}
