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
//!    existing rows into the sidekick's CFs (secondary key -> PK mapping).
//! 3. **WAL catch-up**: Replay WAL entries written since step 1 into the sidekick,
//!    applying Puts and Deletes to keep secondary keys consistent.
//! 4. **Transfer + activate**: Write sidekick data into SST files, ingest them into
//!    the primary, then activate the indices under a write lock.
//! 5. **Closure drain**: Still holding the write lock, replay any WAL entries that
//!    arrived during the transfer window directly into the primary.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bincode::Options;
use failpoint_macros::set_failpoint;
use metrics::counter;
use readyset_client::internal::Index;
use readyset_client::TableStatus;
use readyset_data::DfValue;
use readyset_errors::{internal_err, ReadySetResult};
use readyset_sql::ast::{Relation, SqlIdentifier};
use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, IngestExternalFileOptions, ReadOptions,
    SstFileWriter, WriteBatch, WriteBatchIteratorCf,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info, warn};

pub use super::handle::PersistentStateHandle;
use super::{
    build_key, check_if_index_is_unique, recorded, IndexEpoch, IndexParams, PendingBuildMeta,
    PersistentIndex, PersistentMeta, PersistentState, SharedState, META_KEY, PENDING_BUILD_KEY,
    PK_CF,
};
use crate::persistent_state::{bulk_scan_read_opts, AutoCompact};

/// Initial capacity for WAL operation collector. Typical write batches contain
/// fewer than 64 operations, so this avoids reallocations in most cases.
const TYPICAL_WAL_OPS_PER_BATCH: usize = 64;

/// Number of secondary-index operations to accumulate in a [`WriteBatch`]
/// before writing to RocksDB during WAL catch-up. Larger values amortize
/// the per-write overhead at the cost of a bigger in-memory batch.
const WAL_CATCHUP_WRITE_THRESHOLD: usize = 4096;

/// Maximum number of entries allowed in the `recent_puts` cache during WAL
/// catch-up. If exceeded, the index build fails gracefully rather than
/// consuming unbounded memory.
const MAX_RECENT_PUTS: usize = 1_000_000;

/// Monotonic counter for generating unique sidekick directory names.
static SIDEKICK_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Number of SST entries between shutdown checks during transfer.
const TRANSFER_SHUTDOWN_CHECK_INTERVAL: u64 = 100_000;

/// Maximum size (in bytes) of a single SST file produced during the sidekick
/// transfer phase. Matches RocksDB's default `target_file_size_base` (64 MB).
///
/// A single monolithic SST covering the entire key range forces RocksDB to
/// read the whole file for any compaction that touches even a small key range
/// overlap, and doubles storage temporarily during that compaction. Splitting
/// at this boundary produces files that align with RocksDB's expectations for
/// L6, enabling efficient incremental compaction of individual key ranges.
const TRANSFER_SST_MAX_BYTES: u64 = 64 * 1024 * 1024;

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
    /// Shared slot for the completion channel receiver. `build_indices` creates
    /// the channel and stores the receiver here; `shut_down()` takes it to wait.
    pub(crate) completion_rx: Arc<std::sync::Mutex<Option<Receiver<()>>>>,
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

    /// Mark that an index build is starting by writing a [`PendingBuildMeta`]
    /// to [`PENDING_BUILD_KEY`]. This is stored separately from
    /// [`PersistentMeta`] so that it never races with the domain thread's
    /// replication-offset writes to [`META_KEY`].
    ///
    /// If the process crashes mid-build, startup recovery reads this key
    /// and drops the partial column families.
    pub(super) fn mark_pending_build(
        &self,
        column_families: Vec<String>,
        sequence_number: u64,
    ) -> ReadySetResult<()> {
        let db = self.db.db();
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let pending = PendingBuildMeta {
            column_families,
            start_sequence_number: sequence_number,
            start_time_unix_secs: start_time,
        };

        let serialized = serde_json::to_string(&pending)
            .map_err(|e| internal_err!("Failed to serialize PendingBuildMeta: {e}"))?;

        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        let mut batch = rocksdb::WriteBatch::default();
        batch.put(PENDING_BUILD_KEY, serialized);
        db.write_opt(batch, &write_opts)
            .map_err(|e| internal_err!("Failed to persist pending build marker: {e}"))?;
        Ok(())
    }

    /// Clear the pending build marker by deleting [`PENDING_BUILD_KEY`].
    pub(super) fn clear_pending_build(&self) -> ReadySetResult<()> {
        let db = self.db.db();
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete(PENDING_BUILD_KEY);
        db.write_opt(batch, &write_opts)
            .map_err(|e| internal_err!("Failed to clear pending build marker: {e}"))?;
        Ok(())
    }

    /// Activates pending indices by persisting the updated metadata to disk,
    /// then adding them to shared_state.indices.
    ///
    /// Disk is written first so that a write failure doesn't leave in-memory
    /// state out of sync. The index metadata update and pending-build marker
    /// deletion are written in a single [`WriteBatch`] for atomicity — if we
    /// crash between them, we'd either have active indices AND a stale pending
    /// marker (causing crash recovery to drop valid CFs) or neither.
    fn activate_pending_indices(
        &self,
        shared_state: &mut SharedState,
        pending_indices: Vec<PersistentIndex>,
    ) -> ReadySetResult<()> {
        let db = self.db.db();

        // Temporarily push pending indices so build_meta serializes them,
        // then roll back if anything fails (serialization or disk write).
        let rollback_count = pending_indices.len();
        for idx in &pending_indices {
            shared_state.indices.push(idx.clone());
        }

        let result = (|| -> ReadySetResult<()> {
            let meta = self.build_meta(shared_state);
            let serialized = serde_json::to_string(&meta)
                .map_err(|e| internal_err!("Failed to serialize persistent meta: {e}"))?;

            let mut write_opts = rocksdb::WriteOptions::default();
            write_opts.set_sync(true);
            let mut batch = rocksdb::WriteBatch::default();
            batch.put(META_KEY, serialized);
            batch.delete(PENDING_BUILD_KEY);

            db.write_opt(batch, &write_opts)
                .map_err(|e| internal_err!("Failed to persist activated indices: {e}"))?;

            Ok(())
        })();

        if result.is_err() {
            shared_state
                .indices
                .truncate(shared_state.indices.len() - rollback_count);
        }

        result
    }

    /// Creates pending column families for the new indices without adding them to shared_state.
    fn create_secondary_pending(
        &self,
        indices: &[(Index, bool)],
        start_cf_number: usize,
    ) -> ReadySetResult<Vec<PersistentIndex>> {
        let db = self.db.db();
        let mut created: Vec<PersistentIndex> = Vec::with_capacity(indices.len());

        for (i, (index, is_unique)) in indices.iter().enumerate() {
            info!(
                base = %self.name,
                ?index,
                is_unique,
                "Creating pending secondary index"
            );

            let index_params = IndexParams::from(index);
            let cf_name = (start_cf_number + i).to_string();
            let persistent = PersistentIndex {
                column_family: cf_name.clone(),
                is_unique: *is_unique,
                is_primary: false,
                index: index.clone(),
            };

            if let Err(e) = db.create_cf(
                &cf_name,
                &index_params.make_rocksdb_options(&self.default_options),
            ) {
                // Cleanup already-created column families
                for idx in &created {
                    if let Err(cleanup_err) = db.drop_cf(&idx.column_family) {
                        error!(
                            error = %cleanup_err,
                            cf = %idx.column_family,
                            "Failed to cleanup orphaned column family"
                        );
                    }
                }
                return Err(internal_err!(
                    "Failed to create column family {cf_name} for pending index: {e}"
                ));
            }

            created.push(persistent);
        }

        Ok(created)
    }

    /// Cleans up pending indices by dropping their column families and
    /// clearing the pending build marker.
    fn cleanup_pending_indices(&self, pending_indices: &[PersistentIndex]) {
        let db = self.db.db();

        for idx in pending_indices {
            info!(
                base = %self.name,
                column_family = %idx.column_family,
                ?idx.index,
                "Cleaning up failed pending index"
            );
            if let Err(e) = db.drop_cf(&idx.column_family) {
                error!(
                    error = %e,
                    column_family = %idx.column_family,
                    "Failed to drop column family during cleanup"
                );
            }
        }

        if let Err(e) = self.clear_pending_build() {
            error!(error = %e, "Failed to clear pending build during cleanup");
        }
    }

    /// Catches up pending indices from WAL entries since `since_seq`, writing
    /// secondary index entries to `write_db` (which may be the sidekick).
    ///
    /// For each `Put` in the WAL, the deserialized row is used to compute
    /// secondary keys and insert them. For each `Delete`, the row is looked up
    /// first in a `recent_puts` cache, then in a primary snapshot (if
    /// provided), then in the primary DB directly.
    ///
    /// * `disable_wal` – if true, catch-up writes bypass the WAL (appropriate
    ///   for the sidekick DB which doesn't need WAL). Set to false when writing
    ///   to the primary DB (e.g. during the closure drain after activation),
    ///   where WAL entries must be preserved for crash recovery.
    fn catch_up_from_wal(
        &self,
        primary_db: &rocksdb::DB,
        write_db: &rocksdb::DB,
        snapshot: Option<&rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>>,
        since_seq: u64,
        pending_indices: &[PersistentIndex],
        disable_wal: bool,
    ) -> ReadySetResult<(WalCatchUpStats, u64)> {
        let primary_cf_id = cf_id(primary_db, PK_CF)?;
        let base_label = self.name.to_string();

        let wal_puts_counter = counter!(recorded::OIB_WAL_PUTS, "base" => base_label.clone());
        let wal_deletes_counter = counter!(recorded::OIB_WAL_DELETES, "base" => base_label);

        // Flush the WAL so get_updates_since can see buffered entries.
        // false = no fsync; we only need the entries in the OS page cache,
        // not durably on disk.
        primary_db
            .flush_wal(false)
            .map_err(|e| internal_err!("Failed to flush WAL before catch-up iteration: {e}"))?;

        let wal_iter = primary_db
            .get_updates_since(since_seq)
            .map_err(|e| internal_err!("Failed to iterate WAL from sequence {since_seq}: {e}"))?;

        let pending_cf_handles: Vec<_> = pending_indices
            .iter()
            .map(|idx| {
                write_db.cf_handle(&idx.column_family).ok_or_else(|| {
                    internal_err!(
                        "Pending index column family '{}' not found during WAL catch-up",
                        idx.column_family
                    )
                })
            })
            .collect::<ReadySetResult<Vec<_>>>()?;

        let pk_cf = primary_db
            .cf_handle(PK_CF)
            .ok_or_else(|| internal_err!("No primary index exists during WAL catch-up"))?;

        let mut stats = WalCatchUpStats::default();
        let mut write_batch = WriteBatch::default();
        let mut batch_ops = 0usize;
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.disable_wal(disable_wal);
        let mut last_seen_seq = since_seq;

        let mut collector = WalOperationCollector::new(primary_cf_id);
        let bincode_opts = bincode::options();

        // Cache of rows inserted during WAL catch-up. When a row is inserted
        // then deleted within the WAL window, the sidekick may not have it yet
        // (if it arrived after the scan), so we cache puts to handle deletes.
        let mut recent_puts: HashMap<Vec<u8>, Vec<DfValue>> = HashMap::new();

        for batch_result in wal_iter {
            self.check_shutdown()?;

            let (seq, batch) =
                batch_result.map_err(|e| internal_err!("Failed to read WAL batch: {e}"))?;

            if seq <= since_seq {
                continue;
            }

            last_seen_seq = seq;
            collector.clear();
            batch.iterate_cf(&mut collector);

            if collector.had_unexpected_op {
                antithesis_sdk::assert_unreachable!(
                    "no merge ops in WAL during index build catch-up",
                    &serde_json::json!({ "sequence": seq })
                );
                return Err(internal_err!(
                    "WAL batch at sequence {} contained an unexpected operation type \
                     (e.g. merge); aborting index build to avoid producing an incomplete index",
                    seq
                ));
            }

            for op in collector.operations.drain(..) {
                match op {
                    WalOperation::Put { key, value } => {
                        let row: Vec<DfValue> = bincode_opts.deserialize(&value).map_err(|e| {
                            internal_err!(
                                "Failed to deserialize row from WAL put at sequence {}: {}",
                                seq,
                                e
                            )
                        })?;

                        write_secondary_keys(
                            &row,
                            &key,
                            pending_indices,
                            &pending_cf_handles,
                            &mut write_batch,
                            WriteOp::Put,
                        );

                        if recent_puts.len() <= MAX_RECENT_PUTS {
                            recent_puts.insert(key, row);
                        }
                        batch_ops += 1;
                        stats.puts_applied += 1;
                        wal_puts_counter.increment(1);
                    }
                    WalOperation::Delete { key } => {
                        // Look up the row so we can compute secondary keys.
                        // Priority: recent_puts cache > primary snapshot > primary DB.
                        // The sidekick doesn't have the PK CF (only secondary CFs),
                        // so we must use the primary for row lookups.
                        let row = if let Some(row) = recent_puts.remove(&key) {
                            Some(row)
                        } else if let Some(snapshot) = snapshot {
                            snapshot
                                .get_cf(&pk_cf, &key)
                                .map_err(|e| {
                                    internal_err!(
                                        "RocksDB error looking up row in snapshot during WAL \
                                         delete catch-up: {e}"
                                    )
                                })?
                                .map(|bytes| {
                                    bincode::options()
                                        .deserialize::<Vec<DfValue>>(bytes.as_ref())
                                        .map_err(|e| {
                                            internal_err!(
                                                "Failed to deserialize row during WAL delete \
                                                 catch-up (snapshot): {e}"
                                            )
                                        })
                                })
                                .transpose()?
                        } else {
                            primary_db
                                .get_cf(&pk_cf, &key)
                                .map_err(|e| {
                                    internal_err!(
                                        "RocksDB error looking up row during WAL delete \
                                         catch-up: {e}"
                                    )
                                })?
                                .map(|bytes| {
                                    bincode::options()
                                        .deserialize::<Vec<DfValue>>(bytes.as_ref())
                                        .map_err(|e| {
                                            internal_err!(
                                                "Failed to deserialize row during WAL delete \
                                                 catch-up (primary): {e}"
                                            )
                                        })
                                })
                                .transpose()?
                        };

                        if let Some(row) = row {
                            write_secondary_keys(
                                &row,
                                &key,
                                pending_indices,
                                &pending_cf_handles,
                                &mut write_batch,
                                WriteOp::Delete,
                            );
                            batch_ops += 1;
                            stats.deletes_applied += 1;
                            wal_deletes_counter.increment(1);
                        } else {
                            stats.deletes_skipped += 1;
                        }
                    }
                }

                if batch_ops >= WAL_CATCHUP_WRITE_THRESHOLD {
                    write_db
                        .write_opt(std::mem::take(&mut write_batch), &write_opts)
                        .map_err(|e| internal_err!("Failed to write WAL catch-up batch: {e}"))?;
                    batch_ops = 0;
                }
            }
        }

        if !write_batch.is_empty() {
            write_db
                .write_opt(write_batch, &write_opts)
                .map_err(|e| internal_err!("Failed to write final WAL catch-up batch: {e}"))?;
        }

        info!(
            since_seq,
            last_seen_seq,
            puts_applied = stats.puts_applied,
            deletes_applied = stats.deletes_applied,
            deletes_skipped = stats.deletes_skipped,
            pending_indices = pending_indices.len(),
            "WAL catch-up completed"
        );

        Ok((stats, last_seen_seq))
    }

    fn prepare_pending_build(
        &self,
        indices: &[(Index, bool)],
    ) -> ReadySetResult<(Vec<PersistentIndex>, u64)> {
        if let (Some(table_status_tx), Some(table)) = (&self.table_status_tx, &self.table) {
            if let Err(err) =
                table_status_tx.send((table.clone(), TableStatus::CreatingIndex(None)))
            {
                error!(
                    error = %err,
                    table = %table.display_unquoted(),
                    "Failed to notify controller of new index",
                );
            }
        }

        let db = self.db.db();

        // CF names are sequential integers starting from 0. `indices.len()`
        // is safe as the starting name because:
        // - Only one index build runs at a time (enforced by `index_build_status`)
        // - Pending indices are never pushed to `shared_state.indices`, so
        //   `len()` doesn't change during a build
        // - Failed builds clean up their CFs via `cleanup_pending_indices`
        // - Crash recovery uses `PendingBuildMeta` to drop orphaned CFs on restart
        // - Even if a name did collide, `create_cf` returns an error (no silent
        //   corruption)
        let start_cf_number = self.db.shared_state().indices.len();

        let cf_names: Vec<String> = (0..indices.len())
            .map(|i| (start_cf_number + i).to_string())
            .collect();

        let sequence_number = db.latest_sequence_number();
        self.mark_pending_build(cf_names, sequence_number)?;

        let pending_indices = match self.create_secondary_pending(indices, start_cf_number) {
            Ok(indices) => indices,
            Err(e) => {
                if let Err(meta_err) = self.clear_pending_build() {
                    error!(error = %meta_err, "Failed to clear pending build after CF creation failure");
                }
                return Err(e);
            }
        };

        Ok((pending_indices, sequence_number))
    }

    pub fn build_indices(&self, indices: Vec<(Index, bool)>) -> ReadySetResult<()> {
        antithesis_sdk::assert_sometimes!(
            true,
            "online index build exercised",
            &serde_json::json!({})
        );

        // Create the completion channel and guard *before* the failpoint so
        // that early returns (including failpoint-triggered ones) always drop
        // the guard, which drops the sender and unblocks any shutdown waiter.
        let (completion_tx, completion_rx) = build_completion_channel();
        *self.completion_rx.lock().expect("poisoned") = Some(completion_rx);
        let mut guard = IndexBuildGuard::new(self.index_build_status.clone(), completion_tx);

        set_failpoint!(readyset_util::failpoints::ONLINE_INDEX_BUILD_START, |_| {
            Err(internal_err!(
                "online-index-build-start failpoint triggered"
            ))
        });
        // Record WAL sequence number, persist pending-build marker,
        // create empty CFs on primary.
        let (pending_indices, sequence_number) = self.prepare_pending_build(&indices)?;

        // If the build fails, clean up the pending CFs.
        let result = self.run_build(&pending_indices, sequence_number);
        if result.is_err() {
            self.cleanup_pending_indices(&pending_indices);
        }
        result?;

        guard.mark_succeeded();
        Ok(())
    }

    /// Runs the core index build: scan, WAL catch-up, transfer, and activate.
    ///
    /// All secondary index writes go to a separate sidekick RocksDB instance,
    /// eliminating write pipeline contention with the primary. After the build,
    /// the sidekick's data is transferred to the primary via SstFileWriter+ingest.
    fn run_build(
        &self,
        pending_indices: &[PersistentIndex],
        snapshot_seq: u64,
    ) -> ReadySetResult<()> {
        let db = self.db.db();
        let db_path = db.path().to_path_buf();

        // Open sidekick DB with matching CFs
        let sidekick = open_sidekick(&db_path, pending_indices, &self.default_options)?;
        let snapshot = db.snapshot();

        // Scan snapshot -> sidekick
        let scan_start = Instant::now();
        let pk_cf = db
            .cf_handle(PK_CF)
            .ok_or_else(|| internal_err!("primary key column family '{}' not found", PK_CF))?;
        let iter = snapshot.raw_iterator_cf_opt(&pk_cf, bulk_scan_read_opts());

        let estimated_rows = match db.property_int_value_cf(&pk_cf, "rocksdb.estimate-num-keys") {
            Ok(Some(n)) => n as usize,
            Ok(None) => 0,
            Err(e) => {
                tracing::warn!(error = %e, "failed to estimate row count for progress reporting");
                0
            }
        };

        self.check_shutdown()?;

        // Write to sidekick, not primary.
        PersistentState::populate_secondary_from_iter(
            &sidekick.db,
            iter,
            pending_indices,
            &self.name,
            &self.table,
            &self.table_status_tx,
            estimated_rows,
            AutoCompact::Enable,
            Some(&self.shutdown_requested),
        )?;
        let scan_duration = scan_start.elapsed();
        info!(
            base = %self.name,
            scan_ms = scan_duration.as_millis(),
            "snapshot scan complete"
        );

        set_failpoint!(
            readyset_util::failpoints::ONLINE_INDEX_BUILD_POST_SCAN,
            |_| {
                Err(internal_err!(
                    "online-index-build-post-scan failpoint triggered"
                ))
            }
        );

        // WAL catch-up: two rounds to minimize closure drain lock hold time.
        //
        // Round 1 replays everything accumulated during the snapshot scan.
        // Round 2 replays whatever trickled in during round 1. This shrinks the
        // dirty window so the closure drain — which holds the domain write lock —
        // only needs to replay a minimal number of writes.
        //
        // A single round + closure drain would produce identical results, but
        // the two-round approach keeps the write-lock hold time predictably
        // small regardless of write rate during the scan.
        self.check_shutdown()?;
        let (_stats, last_seq) = self.catch_up_from_wal(
            db,
            &sidekick.db,
            Some(&snapshot),
            snapshot_seq,
            pending_indices,
            true, // disable_wal: sidekick doesn't need WAL
        )?;

        // Release the snapshot before the second catch-up -- it's passed as
        // None anyway, and dropping it frees RocksDB resources sooner.
        drop(snapshot);

        let (_stats, final_seq) = self.catch_up_from_wal(
            db,
            &sidekick.db,
            None, // falls through to primary DB for delete lookups
            last_seq,
            pending_indices,
            true, // disable_wal: sidekick doesn't need WAL
        )?;

        // Transfer sidekick -> primary via SST ingest
        self.transfer_sidekick_to_primary(&sidekick, db, &self.default_options, pending_indices)?;

        // Close and clean up sidekick
        let sidekick_path = sidekick.path.clone();
        drop(sidekick);

        // Clean up sidekick directory
        if let Err(e) = fs::remove_dir_all(&sidekick_path) {
            // Not fatal -- crash recovery cleans up on next startup
            warn!(
                error = %e,
                path = %sidekick_path.display(),
                "Failed to remove sidekick directory after ingestion"
            );
        }

        set_failpoint!(
            readyset_util::failpoints::ONLINE_INDEX_BUILD_PRE_ACTIVATE,
            |_| {
                Err(internal_err!(
                    "online-index-build-pre-activate failpoint triggered"
                ))
            }
        );

        // Activate indices and close the write gap under a single write-lock hold.
        //
        // Why a write lock is required here:
        //   Between the end of transfer and the start of this closure drain,
        //   concurrent domain inserts may have written new rows to the primary
        //   PK CF. Those inserts do NOT write secondary keys for the pending
        //   indices because they aren't activated yet. The closure drain replays
        //   this gap from the WAL, writing directly to the primary.
        //
        //   Holding the write lock for the entire activate+drain window ensures:
        //   (a) no new inserts land in the gap while we're draining it, and
        //   (b) readers see a consistent view -- indices are only visible after
        //       all secondary keys are fully caught up.
        let mut shared_state = self.db.shared_state_mut();
        self.activate_pending_indices(&mut shared_state, pending_indices.to_vec())?;
        let (_stats, _) = self.catch_up_from_wal(
            db,
            db,   // primary is both source and dest
            None, // no snapshot needed
            final_seq,
            pending_indices,
            false, // WAL enabled for primary writes
        )?;
        drop(shared_state);

        // Re-enable auto-compaction with raised L0 thresholds. Since we
        // ingested compacted SSTs, there should be very few L0 files, but
        // we still raise thresholds as a safety measure.
        for index in pending_indices {
            if let Some(cf) = db.cf_handle(&index.column_family) {
                if let Err(err) = db.set_options_cf(
                    &cf,
                    &[
                        ("level0_slowdown_writes_trigger", "512"),
                        ("level0_stop_writes_trigger", "1024"),
                    ],
                ) {
                    warn!(
                        %err,
                        table = %self.name,
                        cf = %index.column_family,
                        "Failed to raise L0 thresholds after index build",
                    );
                }
                if let Err(err) = db.set_options_cf(&cf, &[("disable_auto_compactions", "false")]) {
                    error!(
                        %err,
                        table = %self.name,
                        cf = %index.column_family,
                        "Failed to re-enable auto-compaction after index build",
                    );
                }
            }
        }

        Ok(())
    }
}

// =============================================================================
// PersistentState impl block for index building methods
// =============================================================================

impl PersistentState {
    /// Prepares indices for background building by filtering and creating primary index if needed.
    ///
    /// Returns `Some(indices_with_uniqueness)` if there are indices to build, `None` otherwise.
    /// Each element is a tuple of (Index, is_unique).
    pub fn prepare_indices_for_build(
        &mut self,
        strict: Vec<(Index, Option<Vec<super::Tag>>)>,
        weak: Vec<Index>,
    ) -> ReadySetResult<Option<Vec<(Index, bool)>>> {
        let mut indices = Vec::new();
        let mut shared_state = self.db.shared_state_mut();
        let mut seen_indices = HashSet::new();

        for (index, tags) in strict
            .into_iter()
            .chain(weak.into_iter().map(|x| (x, None)))
        {
            if tags.is_some() {
                return Err(internal_err!("Base tables can't be partial"));
            }
            let existing = shared_state.indices.iter().any(|pi| pi.index == index);
            if existing {
                continue;
            }

            // Skip if we've already seen this index in this batch (deduplicates strict/weak)
            if !seen_indices.insert(index.clone()) {
                continue;
            }

            let uniq = check_if_index_is_unique(&self.unique_keys, &index.columns);
            if shared_state.indices.is_empty() {
                self.add_primary_index_with_state(&mut shared_state, &index.columns, uniq)?;
                // Primary indices can only be HashMaps, so if this is our first index and it's
                // *not* a HashMap index, add another secondary index of the correct index type
                if index.index_type == common::IndexType::HashMap {
                    continue;
                }
            }
            indices.push((index, uniq));
        }

        if indices.is_empty() {
            Ok(None)
        } else {
            Ok(Some(indices))
        }
    }

    /// Returns the current [`IndexBuildStatus`].
    pub fn index_build_status(&self) -> IndexBuildStatus {
        self.index_build_status.load()
    }

    /// Atomically marks that an index build is in progress.
    ///
    /// Returns `Ok(())` if the transition succeeded (previous status was
    /// `Succeeded` or `Failed`). Returns `Err` if a build is already in
    /// progress, enforcing the single-build-at-a-time invariant.
    ///
    /// This should be called before spawning a background index build task.
    pub fn mark_index_build_in_progress(&self) -> ReadySetResult<()> {
        // Try transitioning from Succeeded first (the common case).
        if self
            .index_build_status
            .compare_exchange(IndexBuildStatus::Succeeded, IndexBuildStatus::InProgress)
            .is_ok()
        {
            return Ok(());
        }
        // Try transitioning from Failed (retry after a previous failure).
        if self
            .index_build_status
            .compare_exchange(IndexBuildStatus::Failed, IndexBuildStatus::InProgress)
            .is_ok()
        {
            return Ok(());
        }
        // Already InProgress — another build is running.
        Err(internal_err!(
            "Cannot start index build: another build is already in progress"
        ))
    }

    /// Creates an IndexBuildContext for background index building.
    pub fn create_index_build_context(&self) -> IndexBuildContext {
        IndexBuildContext {
            db: self.db.clone(),
            name: self.name.clone(),
            table: self.table.clone(),
            default_options: self.default_options.clone(),
            table_status_tx: self.table_status_tx.clone(),
            index_build_status: self.index_build_status.clone(),
            shutdown_requested: self.shutdown_requested.clone(),
            completion_rx: self.build_completion_rx.clone(),
            epoch: self.epoch,
        }
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

/// Whether to put or delete secondary keys in a [`WriteBatch`].
enum WriteOp {
    Put,
    Delete,
}

/// Computes and writes secondary index keys for a single row into `batch`.
///
/// For each pending index, computes the secondary key from the row, serializes
/// it (using the unique or non-unique encoding), and adds a put or delete to
/// the batch.
fn write_secondary_keys(
    row: &[DfValue],
    pk_key: &[u8],
    pending_indices: &[PersistentIndex],
    cf_handles: &[impl rocksdb::AsColumnFamilyRef],
    batch: &mut WriteBatch,
    op: WriteOp,
) {
    for (idx, cf) in pending_indices.iter().zip(cf_handles.iter()) {
        let index_key = build_key(row, &idx.index.columns);
        let serialized_key = if idx.is_unique && !index_key.has_null() {
            PersistentState::serialize_prefix(&index_key)
        } else {
            PersistentState::serialize_secondary(&index_key, pk_key)
        };
        match op {
            WriteOp::Put => batch.put_cf(cf, &serialized_key, pk_key),
            WriteOp::Delete => batch.delete_cf(cf, &serialized_key),
        }
    }
}

// =============================================================================
// Sidekick RocksDB helpers
// =============================================================================

/// A temporary RocksDB instance used to build secondary indices in isolation.
/// Lives at `<primary_path>.oib/` alongside the primary DB directory.
struct SidekickDb {
    db: rocksdb::DB,
    path: PathBuf,
}

impl std::fmt::Debug for SidekickDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SidekickDb")
            .field("path", &self.path)
            .finish()
    }
}

/// Opens a sidekick RocksDB instance for bulk-writing secondary index entries.
///
/// Each sidekick gets a unique directory name (`<primary_path>.oib-<pid>-<counter>`)
/// to avoid LOCK collisions when multiple OIBs target the same base table
/// concurrently. Leftover directories from previous crashes are cleaned up
/// before opening.
///
/// The sidekick is tuned for bulk-write throughput: large memtables, parallel
/// background jobs, and no WAL (crash = restart the build).
/// Removes any leftover sidekick directories matching `<primary_path>.oib-*`.
///
/// Called from two places:
/// - `PersistentState::new_inner` (startup): cleans up leftovers from a previous
///   process that crashed mid-build.
/// - `open_sidekick` (before opening a new sidekick): cleans up leftovers from a
///   previous build attempt within the same process (e.g. a retry after failure).
pub(super) fn cleanup_sidekick_directories(primary_db_path: &std::path::Path) {
    let Some(parent) = primary_db_path.parent() else {
        return;
    };
    let Some(stem) = primary_db_path.file_name().and_then(|n| n.to_str()) else {
        return;
    };
    let prefix = format!("{}.oib-", stem);
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        if let Some(name) = entry.file_name().to_str() {
            if name.starts_with(&prefix) && entry.path().is_dir() {
                warn!(path = %entry.path().display(), "Removing leftover sidekick directory");
                let _ = fs::remove_dir_all(entry.path());
            }
        }
    }
}

fn open_sidekick(
    primary_db_path: &std::path::Path,
    pending_indices: &[PersistentIndex],
    base_options: &rocksdb::Options,
) -> ReadySetResult<SidekickDb> {
    cleanup_sidekick_directories(primary_db_path);

    let counter = SIDEKICK_COUNTER.fetch_add(1, Ordering::Relaxed);
    let sidekick_path =
        primary_db_path.with_extension(format!("db.oib-{}-{}", std::process::id(), counter));

    let ncpus = num_cpus::get() as i32;

    // Start from the primary's base options to ensure SST format compatibility
    // (block size, compression, format version, etc.). Then override for bulk writes.
    let mut opts = base_options.clone();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // VectorRep doesn't support concurrent writes, but we don't need them.
    opts.set_allow_concurrent_memtable_write(false);

    // Large memtables for fewer flushes -- the sidekick is ephemeral so we
    // can afford the memory.
    opts.set_write_buffer_size(256 * 1024 * 1024); // 256 MB per memtable
    opts.set_max_write_buffer_number(4);
    // NOTE: 1 GB total write buffer is sized for production servers with 16+ GB RAM.
    // On memory-constrained systems, this may need to be reduced via configuration.
    // Currently not configurable -- revisit if OIB is used on smaller instances.
    opts.set_db_write_buffer_size(1024 * 1024 * 1024); // 1 GB total

    // Background jobs for flush only (no compaction on sidekick).
    // Cap at 4 to avoid saturating I/O on large-core machines.
    opts.set_max_background_jobs(ncpus.min(4));

    // Disable WAL for the sidekick entirely -- on crash we restart the build.
    opts.set_manual_wal_flush(true);

    // Disable auto-compaction entirely -- the sidekick is only iterated
    // sequentially at the end, never queried, so compaction is wasted work.
    opts.set_disable_auto_compactions(true);
    opts.set_level_zero_file_num_compaction_trigger(1_000_000);
    opts.set_level_zero_slowdown_writes_trigger(1_000_000);
    opts.set_level_zero_stop_writes_trigger(1_000_000);

    // Build CF descriptors matching the primary's pending index CFs.
    // The comparator must match so ingested SSTs are compatible, but we
    // override the memtable, filters, and block cache for bulk-write perf.
    let mut cf_descriptors = vec![ColumnFamilyDescriptor::new(
        "default",
        rocksdb::Options::default(),
    )];
    for idx in pending_indices {
        let index_params = IndexParams::from(&idx.index);
        let mut cf_opts = index_params.make_rocksdb_options(base_options);

        // VectorRep: O(1) append during writes, single std::sort at flush.
        // Eliminates the 35% CPU cost of SkipList::FindLessThan in
        // HashLinkList flush path.
        cf_opts.set_memtable_factory(rocksdb::MemtableFactory::Vector);

        // No filters, no block cache -- the sidekick is write-only then
        // iterated once sequentially. Ribbon filter construction was ~10%
        // of CPU for zero benefit.
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_size(32 * 1024);
        block_opts.disable_cache();
        cf_opts.set_block_based_table_factory(&block_opts);

        cf_descriptors.push(ColumnFamilyDescriptor::new(&idx.column_family, cf_opts));
    }

    let db =
        rocksdb::DB::open_cf_descriptors(&opts, &sidekick_path, cf_descriptors).map_err(|e| {
            internal_err!(
                "Failed to open sidekick RocksDB at {}: {e}",
                sidekick_path.display()
            )
        })?;

    Ok(SidekickDb {
        db,
        path: sidekick_path,
    })
}

impl IndexBuildContext {
    /// Transfers all data from the sidekick's CFs to the corresponding CFs on the
    /// primary DB via `SstFileWriter` + `ingest_external_file_cf`. Each sidekick
    /// CF is flushed, iterated in sorted order, and written to one or more SST
    /// files (split at [`TRANSFER_SST_MAX_BYTES`] boundaries to match RocksDB's
    /// `target_file_size_base`). The SSTs are then ingested into the primary as
    /// file-move operations.
    ///
    /// Splitting avoids producing a single monolithic SST that would force
    /// RocksDB to read the entire file for any compaction touching even a small
    /// key range overlap. Multiple smaller SSTs enable efficient incremental
    /// compaction.
    ///
    /// Direct ingestion of the sidekick's compaction-output SSTs is not possible
    /// because RocksDB's `ingest_external_file` requires the
    /// `kExternalSstFileGlobalSeqnoPropertyName` property that only `SstFileWriter`
    /// produces ("Corruption: External file version not found").
    ///
    /// The `SstFileWriter` for each CF is created with CF-specific options
    /// (including custom comparators for BTreeMap indices).
    fn transfer_sidekick_to_primary(
        &self,
        sidekick: &SidekickDb,
        primary_db: &rocksdb::DB,
        base_options: &rocksdb::Options,
        pending_indices: &[PersistentIndex],
    ) -> ReadySetResult<u64> {
        let mut total_entries: u64 = 0;

        let mut ingest_opts = IngestExternalFileOptions::default();
        // Move the SST file into the primary's DB directory instead of copying.
        ingest_opts.set_move_files(true);

        for idx in pending_indices {
            self.check_shutdown()?;

            let cf_name = &idx.column_family;
            let sidekick_cf = sidekick.db.cf_handle(cf_name).ok_or_else(|| {
                internal_err!("Sidekick CF '{cf_name}' not found during transfer")
            })?;
            let primary_cf = primary_db
                .cf_handle(cf_name)
                .ok_or_else(|| internal_err!("Primary CF '{cf_name}' not found during transfer"))?;

            // Flush sidekick to ensure all memtable data is on disk.
            sidekick
                .db
                .flush_cf(&sidekick_cf)
                .map_err(|e| internal_err!("Failed to flush sidekick CF '{cf_name}': {e}"))?;

            // Build CF-specific options (includes custom comparator for BTreeMap).
            let cf_options = IndexParams::from(&idx.index).make_rocksdb_options(base_options);

            // Optimized read options for sequential scan: large readahead,
            // no block cache (ephemeral DB), and async I/O for prefetch.
            let mut read_opts = ReadOptions::default();
            read_opts.set_readahead_size(4 * 1024 * 1024);
            read_opts.fill_cache(false);
            read_opts.set_async_io(true);

            let mut iter = sidekick.db.raw_iterator_cf_opt(&sidekick_cf, read_opts);
            iter.seek_to_first();

            let mut sst_paths: Vec<PathBuf> = Vec::new();
            let mut cf_entries: u64 = 0;
            let mut sst_bytes: u64 = 0;
            let mut sst_writer: Option<SstFileWriter> = None;

            while iter.valid() {
                if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                    // Split output into multiple SST files at TRANSFER_SST_MAX_BYTES
                    // (default 64 MB, matching RocksDB's target_file_size_base).
                    // A single monolithic SST would force full-file reads for any
                    // compaction that overlaps even a small key range. Multiple
                    // smaller files let RocksDB compact individual ranges
                    // incrementally.
                    if sst_writer.is_none() || sst_bytes >= TRANSFER_SST_MAX_BYTES {
                        // Finish the current SST if one is open.
                        if let Some(mut writer) = sst_writer.take() {
                            writer.finish().map_err(|e| {
                                internal_err!("Failed to finish SST for CF '{cf_name}': {e}")
                            })?;
                        }

                        let sst_path = sidekick
                            .path
                            .join(format!("transfer_{cf_name}_{}.sst", sst_paths.len()));
                        let writer = SstFileWriter::create(&cf_options);
                        writer.open(&sst_path).map_err(|e| {
                            internal_err!(
                                "Failed to open SST writer at {}: {e}",
                                sst_path.display()
                            )
                        })?;
                        sst_paths.push(sst_path);
                        sst_bytes = 0;
                        sst_writer = Some(writer);
                    }

                    sst_writer
                        .as_mut()
                        .ok_or_else(|| internal_err!("SST writer missing for CF '{cf_name}'"))?
                        .put(key, value)
                        .map_err(|e| {
                            internal_err!("Failed to write to SST for CF '{cf_name}': {e}")
                        })?;
                    cf_entries += 1;
                    sst_bytes += (key.len() + value.len()) as u64;

                    if cf_entries.is_multiple_of(TRANSFER_SHUTDOWN_CHECK_INTERVAL) {
                        self.check_shutdown()?;
                    }
                }
                iter.next();
            }

            if let Err(e) = iter.status() {
                return Err(internal_err!(
                    "Error iterating sidekick CF '{cf_name}': {e}"
                ));
            }

            // Finish the last SST file.
            if let Some(mut writer) = sst_writer.take() {
                writer
                    .finish()
                    .map_err(|e| internal_err!("Failed to finish SST for CF '{cf_name}': {e}"))?;
            }

            if sst_paths.is_empty() {
                continue;
            }

            info!(
                base = %self.name,
                cf = %cf_name,
                entries = cf_entries,
                sst_files = sst_paths.len(),
                "transferring sidekick data to primary"
            );

            // Ingest all SST files into the primary's column family.
            let sst_refs: Vec<&std::path::Path> = sst_paths.iter().map(|p| p.as_path()).collect();
            primary_db
                .ingest_external_file_cf_opts(&primary_cf, &ingest_opts, sst_refs)
                .map_err(|e| {
                    internal_err!("Failed to ingest SSTs into primary CF '{cf_name}': {e}")
                })?;

            total_entries += cf_entries;
        }

        Ok(total_entries)
    }
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
