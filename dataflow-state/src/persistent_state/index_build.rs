//! Online index building functionality for PersistentState.
//!
//! This module implements snapshot-based online index building (OIB), which allows
//! new secondary indices to be built in the background while the table remains
//! available for reads and writes.
//!
//! ## Sidekick RocksDB Architecture
//!
//! Secondary indices are built in a separate, short-lived "sidekick" RocksDB instance
//! that lives alongside the primary DB. This eliminates write pipeline contention:
//! OIB bulk writes never touch the primary's memtable, and the primary's WAL remains
//! clean (no `disable_wal` gaps).
//!
//! ## Algorithm Steps
//!
//! 1. **Prepare**: Record the WAL sequence number, persist a pending-build marker,
//!    and create empty column families on the primary for each new index.
//! 2. **Snapshot scan**: Open a sidekick DB, take a primary snapshot, and scan all
//!    existing rows into the sidekick's CFs (secondary key → PK mapping).
//! 3. **WAL catch-up**: Replay WAL entries written since step 1 into the sidekick,
//!    applying Puts and Deletes to keep secondary keys consistent.
//! 4. **Transfer + activate**: Write sidekick data into SST files, ingest them into
//!    the primary, then activate the indices under a write lock.
//! 5. **Closure drain**: Still holding the write lock, replay any WAL entries that
//!    arrived during the transfer window directly into the primary.

use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;

use readyset_client::TableStatus;
use readyset_errors::{internal_err, ReadySetResult};
use readyset_sql::ast::{Relation, SqlIdentifier};
use rocksdb::{WriteBatch, WriteBatchIteratorCf};
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

pub use super::handle::PersistentStateHandle;
use super::{IndexEpoch, PersistentMeta, PersistentState, SharedState, META_KEY};

/// Initial capacity for WAL operation collector. Typical write batches contain
/// fewer than 64 operations, so this avoids reallocations in most cases.
const TYPICAL_WAL_OPS_PER_BATCH: usize = 64;

/// Number of secondary-index operations to accumulate in a [`WriteBatch`]
/// before flushing to RocksDB during WAL catch-up. Larger values amortize
/// the per-write overhead at the cost of a bigger in-memory batch.
const WAL_CATCHUP_FLUSH_THRESHOLD: usize = 4096;

/// Maximum number of entries allowed in the `recent_puts` cache during WAL
/// catch-up. If exceeded, the index build fails gracefully rather than
/// consuming unbounded memory.
const MAX_RECENT_PUTS: usize = 1_000_000;

/// Components needed for background index building, cloned from `PersistentState`
/// so that a copy can be moved into the build thread.
#[derive(Clone)]
pub struct IndexBuildContext {
    db: PersistentStateHandle,
    name: SqlIdentifier,
    table: Option<Relation>,
    default_options: rocksdb::Options,
    table_status_tx: Option<UnboundedSender<(Relation, TableStatus)>>,
    index_build_status: Arc<AtomicIndexBuildStatus>,
    /// Cooperative shutdown flag. The domain sets this to request cancellation;
    /// the build thread polls it in hot loops via `check_shutdown()`.
    pub(crate) shutdown_requested: Arc<AtomicBool>,
    epoch: IndexEpoch,
}

impl std::fmt::Debug for IndexBuildContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexBuildContext")
            .field("name", &self.name)
            .field("table", &self.table)
            .field("index_build_status", &self.index_build_status)
            .field("shutdown_requested", &self.shutdown_requested)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

impl IndexBuildContext {
    /// Returns `Err` if shutdown has been requested. Call at phase boundaries
    /// and periodically in hot loops.
    fn check_shutdown(&self) -> ReadySetResult<()> {
        if self.shutdown_requested.load(Ordering::Acquire) {
            Err(internal_err!(
                "Index build cancelled due to shutdown request"
            ))
        } else {
            Ok(())
        }
    }

    /// Builds a [`PersistentMeta`] from the IndexBuildContext fields.
    ///
    /// Uses `shared_state.replication_offset` (the current offset) rather than the cloned
    /// handle's snapshot, which may be stale if writes occurred during a background index build.
    fn build_meta(&self, shared_state: &SharedState) -> PersistentMeta<'static> {
        PersistentMeta::new(
            shared_state,
            self.epoch,
            shared_state.replication_offset.clone().map(Cow::Owned),
        )
    }

    /// Persists a [`PersistentMeta`] with a sync write (fsync). This ensures
    /// the meta survives OS/hardware crashes, not just process crashes.
    fn save_meta_sync(db: &rocksdb::DB, meta: &PersistentMeta<'_>) -> ReadySetResult<()> {
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        let mut batch = rocksdb::WriteBatch::default();
        let serialized = serde_json::to_string(meta)
            .map_err(|e| internal_err!("Failed to serialize persistent meta: {e}"))?;
        batch.put(META_KEY, serialized);
        db.write_opt(batch, &write_opts)
            .map_err(|e| internal_err!("Failed to persist meta with sync write: {e}"))?;
        Ok(())
    }
}

// =============================================================================
// PersistentState impl block for index building methods
// =============================================================================

impl PersistentState {
    /// Returns the current [`IndexBuildStatus`].
    pub fn index_build_status(&self) -> IndexBuildStatus {
        self.index_build_status.load()
    }
}

/// Status of a background index build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum IndexBuildStatus {
    /// Build is currently running.
    InProgress = 0,
    /// Build completed successfully; indices are active.
    Succeeded = 1,
    /// Build failed; indices were not activated.
    Failed = 2,
}

impl TryFrom<u8> for IndexBuildStatus {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, u8> {
        match v {
            0 => Ok(Self::InProgress),
            1 => Ok(Self::Succeeded),
            2 => Ok(Self::Failed),
            other => Err(other),
        }
    }
}

/// Atomic wrapper around [`IndexBuildStatus`] for lock-free cross-thread access.
pub(super) struct AtomicIndexBuildStatus(AtomicU8);

impl AtomicIndexBuildStatus {
    pub(super) fn new(status: IndexBuildStatus) -> Self {
        Self(AtomicU8::new(status as u8))
    }

    pub(super) fn load(&self) -> IndexBuildStatus {
        IndexBuildStatus::try_from(self.0.load(Ordering::Acquire))
            .unwrap_or(IndexBuildStatus::Failed)
    }

    fn store(&self, status: IndexBuildStatus) {
        self.0.store(status as u8, Ordering::Release);
    }

    /// Atomically transitions from `expected` to `new`. Returns `Ok(expected)`
    /// on success, or `Err(actual)` if the current value wasn't `expected`.
    ///
    /// The `Result<IndexBuildStatus, IndexBuildStatus>` mirrors the standard
    /// `AtomicU8::compare_exchange` API: on failure the `Err` carries the
    /// actual value that was observed, which callers can use to decide whether
    /// to retry with a different expected state.
    fn compare_exchange(
        &self,
        expected: IndexBuildStatus,
        new: IndexBuildStatus,
    ) -> Result<IndexBuildStatus, IndexBuildStatus> {
        self.0
            .compare_exchange(
                expected as u8,
                new as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|v| IndexBuildStatus::try_from(v).unwrap_or(IndexBuildStatus::Failed))
            .map_err(|v| IndexBuildStatus::try_from(v).unwrap_or(IndexBuildStatus::Failed))
    }
}

impl std::fmt::Debug for AtomicIndexBuildStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.load().fmt(f)
    }
}

/// Creates a completion channel for coordinating index build lifecycle.
///
/// The build thread holds the [`SyncSender`]; the domain holds the [`Receiver`].
/// When the build finishes (or panics), the sender is dropped, which unblocks
/// `recv_timeout()` on the domain side.
pub(crate) fn build_completion_channel() -> (SyncSender<()>, Receiver<()>) {
    mpsc::sync_channel(1)
}

/// Drop guard that marks an index build as [`IndexBuildStatus::Failed`] on drop.
///
/// Call [`mark_succeeded`](Self::mark_succeeded) on the success path so that
/// `Drop` writes `Succeeded` instead. On early `?` returns or panic unwinds
/// the guard fires without `mark_succeeded`, correctly recording failure.
///
/// The guard also holds the sending half of the completion channel. When the
/// guard is dropped, the sender is dropped too, which unblocks any
/// `recv_timeout()` on the receiving end.
struct IndexBuildGuard {
    status: Arc<AtomicIndexBuildStatus>,
    /// Dropped on guard drop to signal completion. The value is never sent;
    /// the receiver detects completion via `RecvTimeoutError::Disconnected`.
    _completion_tx: SyncSender<()>,
    succeeded: bool,
}

impl IndexBuildGuard {
    fn new(status: Arc<AtomicIndexBuildStatus>, completion_tx: SyncSender<()>) -> Self {
        Self {
            status,
            _completion_tx: completion_tx,
            succeeded: false,
        }
    }

    /// Mark the build as succeeded. Must be called before the guard is dropped
    /// on the success path.
    fn mark_succeeded(&mut self) {
        self.succeeded = true;
    }
}

impl Drop for IndexBuildGuard {
    fn drop(&mut self) {
        let result = if self.succeeded {
            IndexBuildStatus::Succeeded
        } else {
            IndexBuildStatus::Failed
        };
        // Update observable state before dropping _completion_tx, so that any
        // thread unblocked by the channel disconnect sees the final status.
        self.status.store(result);
        // _completion_tx is dropped here, unblocking the receiver.
    }
}

/// A single WAL operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WalOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Filters and collects WAL batch operations for a specific column family.
///
/// Implements [`WriteBatchIteratorCf`] so it can be passed to
/// `WriteBatch::iterate_cf()`. Only operations targeting `target_cf_id`
/// are collected; operations on other CFs are silently skipped.
///
/// Operations are stored in a single vector to preserve their order within
/// each batch. This is critical for correctness: interleaved put/delete
/// operations on the same key must be applied in the original order.
///
/// Each operation copies its key and value into owned `Vec<u8>`s.
/// This is unfortunate for high-throughput WAL replay, but unavoidable:
/// the `WriteBatchIteratorCf` trait provides only borrowed `&[u8]` slices
/// whose lifetimes do not outlive the callback. We mitigate this by
/// reusing the collector's capacity across batches via `clear()`.
#[derive(Debug)]
pub(super) struct WalOperationCollector {
    /// The column family ID to filter for.
    pub(super) target_cf_id: u32,
    /// Collected operations in order.
    pub(super) operations: Vec<WalOperation>,
    /// Set to true if `merge_cf()` is called for our target CF.
    ///
    /// We cannot return an error from the `WriteBatchIteratorCf` callbacks
    /// (they return `()`), so we record the problem here and check it after
    /// `iterate_cf()` returns. A merge operation would mean the index build
    /// cannot correctly replay the WAL — the caller must abort the build
    /// rather than silently producing an incomplete index.
    pub(super) had_unexpected_op: bool,
}

impl WalOperationCollector {
    pub(super) fn new(target_cf_id: u32) -> Self {
        Self {
            target_cf_id,
            operations: Vec::with_capacity(TYPICAL_WAL_OPS_PER_BATCH),
            had_unexpected_op: false,
        }
    }

    /// Clears the operations vector while retaining allocated capacity.
    /// This allows reusing the collector across multiple batches.
    pub(super) fn clear(&mut self) {
        self.operations.clear();
    }
}

impl WriteBatchIteratorCf for WalOperationCollector {
    fn put_cf(&mut self, cf_id: u32, key: &[u8], value: &[u8]) {
        if cf_id == self.target_cf_id {
            self.operations.push(WalOperation::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            });
        }
    }

    fn delete_cf(&mut self, cf_id: u32, key: &[u8]) {
        if cf_id == self.target_cf_id {
            self.operations
                .push(WalOperation::Delete { key: key.to_vec() });
        }
    }

    fn merge_cf(&mut self, cf_id: u32, _key: &[u8], _value: &[u8]) {
        if cf_id == self.target_cf_id {
            error!("Unexpected merge operation in WAL for target column family");
            self.had_unexpected_op = true;
        }
    }
}

/// Statistics from WAL catch-up operation.
#[derive(Debug, Default)]
pub(super) struct WalCatchUpStats {
    /// Number of put operations successfully applied to pending indices.
    pub(super) puts_applied: u64,
    /// Number of delete operations applied to pending indices.
    pub(super) deletes_applied: u64,
    /// Number of delete operations skipped (row not found in cache or snapshot).
    pub(super) deletes_skipped: u64,
}

/// Returns the RocksDB-internal column family ID for a named CF.
///
/// RocksDB assigns monotonically increasing CF IDs that are **never reused**,
/// even after a column family is dropped and recreated with the same name.
/// The WAL's `WriteBatchIteratorCf` callbacks report operations using these
/// internal IDs, so we must query the actual ID rather than hardcoding it.
///
/// There is no direct API to query a CF's ID, so we use an indirect probe:
/// create a temporary `WriteBatch` with one `put_cf`, then iterate it via
/// `WriteBatchIteratorCf` to observe the CF ID that RocksDB encoded. The
/// batch is never written to the database.
fn cf_id(db: &rocksdb::DB, cf_name: &str) -> ReadySetResult<u32> {
    let cf = db
        .cf_handle(cf_name)
        .ok_or_else(|| internal_err!("column family '{}' not found", cf_name))?;

    // Create a tiny WriteBatch with a single put_cf, then iterate it
    // to observe the CF ID that RocksDB encodes into the batch.
    let mut probe = WriteBatch::default();
    probe.put_cf(&cf, b"__cf_id_probe__", b"");

    struct CfIdProbe(Option<u32>);
    impl WriteBatchIteratorCf for CfIdProbe {
        fn put_cf(&mut self, cf_id: u32, _key: &[u8], _value: &[u8]) {
            self.0 = Some(cf_id);
        }
        fn delete_cf(&mut self, _cf_id: u32, _key: &[u8]) {}
        fn merge_cf(&mut self, _cf_id: u32, _key: &[u8], _value: &[u8]) {}
    }

    let mut collector = CfIdProbe(None);
    probe.iterate_cf(&mut collector);
    collector
        .0
        .ok_or_else(|| internal_err!("failed to determine CF ID for '{}'", cf_name))
}
