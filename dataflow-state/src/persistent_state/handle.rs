//! `PersistentStateHandle` provides concurrent access to both the RocksDB database and the
//! Rust-side shared metadata (`SharedState`). The `DB` is wrapped in `Arc<DB>` — RocksDB is
//! inherently thread-safe, and the `multi-threaded-cf` feature enables `&self` for CF operations.
//! Only `SharedState` requires a lock (`RwLock`) for concurrent metadata access (replication
//! offset, WAL state, index list).
//!
//! # Threading Model
//!
//! Two resources are shared across threads:
//!
//! 1. **`Arc<DB>`** — RocksDB handle, thread-safe without a Rust-side lock. Reads (`get`,
//!    `raw_iterator`) and writes (`write`, `flush_wal`) can proceed concurrently. Column family
//!    creation/deletion (`create_cf`, `drop_cf`) is also safe via the `multi-threaded-cf` feature
//!    but callers must ensure no concurrent `cf_handle()` lookups on a CF being dropped.
//!
//! 2. **`Arc<RwLock<SharedState>>`** — Readyset-side metadata (replication offset, WAL state, index
//!    list). All readers and writers of this metadata must acquire the lock.
//!
//! ## Lock Ordering
//!
//! When both resources are needed, always acquire `SharedState` first, then access `DB`:
//!
//! ```text
//! SharedState (read or write)  →  DB operations
//! ```
//!
//! Never hold a `SharedState` lock while waiting on another `SharedState` lock (no re-entrancy).
//!
//! ## Threads
//!
//! - **Dataflow worker** (`&mut PersistentState`): Mutates state via `insert`, `remove`,
//!   `set_replication_offset`, `add_index`, `enable_snapshot_mode`. Exclusive `&mut self` access
//!   guarantees no concurrent mutations, but concurrent readers (via cloned handles) may exist.
//!
//! - **WalFlusher** (background thread): Periodically flushes/syncs the WAL. Reads `SharedState`
//!   to check WAL status, performs DB I/O, then writes back via `shared_state_mut()`.
//!
//! - **MetricsReporter** (background thread): Reads `SharedState` to enumerate CFs, then queries
//!   DB properties.
//!
//! - **Reader handles** (cloned `PersistentStateHandle`): Perform lookups via `do_lookup`,
//!   `lookup_multi`, `lookup_range`. Acquire `SharedState` read lock to check replication offset
//!   and resolve index metadata, then perform DB reads.
use std::sync::Arc;

use common::IndexType;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use readyset_data::DfValue;
use replication_offset::ReplicationOffset;
use rocksdb::DB;
use tracing::debug;

use super::{deserialize_row, PersistentState, SharedState, PK_CF};
use crate::{PointKey, RecordResult};

/// A handle that can be cloned and shared between threads to safely read from the
/// [`PersistentState`] concurrently.
#[derive(Clone)]
pub struct PersistentStateHandle {
    /// The replication offset used to make sure the read handle received all forward
    /// processing messages for state, if the replication offset of the read handle is
    /// behind that of the base table (`shared_state.replication_offset`), lookups will result
    /// in a miss.
    pub replication_offset: Option<ReplicationOffset>,
    shared_state: Arc<RwLock<SharedState>>,
    /// The handle to the RocksDB database we are reading from
    db: Arc<DB>,
}

impl PersistentStateHandle {
    pub(super) fn new(
        shared_state: SharedState,
        db: DB,
        replication_offset: Option<ReplicationOffset>,
    ) -> Self {
        Self {
            shared_state: Arc::new(RwLock::new(shared_state)),
            db: Arc::new(db),
            replication_offset,
        }
    }

    /// Returns a reference to the underlying RocksDB database.
    pub(super) fn db(&self) -> &DB {
        &self.db
    }

    /// Acquires a read lock on the shared metadata state.
    pub(super) fn shared_state(&self) -> RwLockReadGuard<'_, SharedState> {
        self.shared_state.read()
    }

    /// Acquires a write lock on the shared metadata state.
    pub(super) fn shared_state_mut(&self) -> RwLockWriteGuard<'_, SharedState> {
        self.shared_state.write()
    }

    /// Perform a lookup for multiple equal keys at once. The results are returned in the order
    /// of the original keys.
    pub(super) fn lookup_multi(
        &self,
        columns: &[usize],
        keys: &[PointKey],
    ) -> Vec<RecordResult<'_>> {
        debug!("lookup_multi: columns: {:?}, keys: {:?}", columns, keys);
        if keys.is_empty() {
            return vec![];
        }
        let db = self.db();

        // Extract index metadata under lock, then drop before I/O
        let (cf_name, is_primary, is_unique_vec) = {
            let ss = self.shared_state();
            let index = ss.index(IndexType::HashMap, columns);
            (
                index.column_family.clone(),
                index.is_primary,
                keys.iter()
                    .map(|k| index.is_unique && !k.has_null())
                    .collect::<Vec<_>>(),
            )
        };

        let cf = db.cf_handle(&cf_name).unwrap();
        // Create an iterator once, reuse it for each key
        let mut iter = db.raw_iterator_cf(&cf);
        let mut iter_primary = if !is_primary {
            Some(
                db.raw_iterator_cf(
                    &db.cf_handle(PK_CF)
                        .expect("Primary key column family not found"),
                ),
            )
        } else {
            None
        };

        keys.iter()
            .zip(&is_unique_vec)
            .map(|(k, &is_unique)| {
                let prefix = PersistentState::serialize_prefix(k);
                let mut rows = Vec::new();

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
        debug!("do_lookup: columns: {:?}, key: {:?}", columns, key);
        let db = self.db();

        // Extract index metadata under lock, then drop before I/O
        let (cf_name, is_primary, is_unique) = {
            let ss = self.shared_state();
            if self.replication_offset < ss.replication_offset {
                // We check the replication offset under a SharedState read lock, which prevents
                // concurrent updates to the offset (via shared_state_mut()) but does NOT prevent
                // concurrent RocksDB writes — the DB is accessed via Arc<DB> without a Rust-side
                // lock. RocksDB itself is thread-safe, so reads see a consistent snapshot.
                debug!("Consistency miss in PersistentStateHandle");
                return None;
            }
            let index = ss.index(IndexType::HashMap, columns);
            (
                index.column_family.clone(),
                index.is_primary,
                index.is_unique,
            )
        };

        let cf = db.cf_handle(&cf_name).unwrap();
        let primary_cf = if !is_primary {
            Some(db.cf_handle(PK_CF).unwrap())
        } else {
            None
        };

        let prefix = PersistentState::serialize_prefix(key);

        if is_unique && !key.has_null() {
            // This is a unique key, so we know there's only one row to retrieve
            let value = db.get_pinned_cf(&cf, &prefix).unwrap();
            Some(match (value, primary_cf) {
                (None, _) => vec![],
                (Some(value), None) => vec![deserialize_row(value)],
                (Some(pk), Some(primary_cf)) => vec![deserialize_row(
                    db.get_pinned_cf(&primary_cf, pk)
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

            let mut iter = db.raw_iterator_cf_opt(&cf, opts);
            let mut iter_primary = primary_cf.map(|pcf| db.raw_iterator_cf(&pcf));

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
