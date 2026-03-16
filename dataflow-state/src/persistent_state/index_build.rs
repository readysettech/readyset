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
    /// deletion are written in a single [`WriteBatch`] for atomicity -- if we
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
        // Already InProgress -- another build is running.
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
    /// cannot correctly replay the WAL -- the caller must abort the build
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

// =============================================================================
// Test helpers
// =============================================================================

#[cfg(test)]
impl IndexBuildContext {
    /// Performs setup + snapshot scan using a sidekick DB (same as production),
    /// returning a [`PendingIndexBuild`] that tests can inspect and later pass
    /// to [`catch_up_from_wal_for_test`] and [`activate_build`].
    pub(super) fn scan_snapshot(
        &self,
        indices: &[(Index, bool)],
    ) -> ReadySetResult<PendingIndexBuild> {
        let (pending_indices, sequence_number) = self.prepare_pending_build(indices)?;

        let db = self.db.db();
        let db_path = db.path().to_path_buf();
        let snapshot = db.snapshot();

        let pk_cf = db
            .cf_handle(PK_CF)
            .expect("primary key column family must exist");
        let iter = snapshot.raw_iterator_cf_opt(&pk_cf, bulk_scan_read_opts());

        let estimated_rows = db
            .property_int_value_cf(&pk_cf, "rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap_or(0) as usize;

        // Open a sidekick and scan into it -- same as production.
        // This keeps the primary's WAL clean (no disable_wal gaps).
        let sidekick = open_sidekick(&db_path, &pending_indices, &self.default_options)?;

        PersistentState::populate_secondary_from_iter(
            &sidekick.db,
            iter,
            &pending_indices,
            &self.name,
            &self.table,
            &self.table_status_tx,
            estimated_rows,
            AutoCompact::Enable,
            None, // no shutdown for test helper
        )?;

        // Keep the sidekick alive so it can be used for transfer during
        // activate_build. The sidekick's data will be ingested into the
        // primary at activation time, after WAL catch-up is complete.
        Ok(PendingIndexBuild {
            sequence_number,
            pending_indices,
            sidekick: Some(sidekick),
        })
    }

    /// WAL catch-up wrapper that acquires its own snapshot and writes
    /// directly to the primary DB (for test simplicity -- catch-up volume
    /// is small in tests so no contention concern).
    pub(super) fn catch_up_from_wal_for_test(
        &self,
        pending_build: &PendingIndexBuild,
    ) -> ReadySetResult<WalCatchUpStats> {
        let db = self.db.db();
        let snapshot = db.snapshot();
        let (stats, _last_seq) = self.catch_up_from_wal(
            db,
            db, // write_db = primary (no sidekick in tests)
            Some(&snapshot),
            pending_build.sequence_number,
            &pending_build.pending_indices,
            false, // disable_wal: false when writing to primary
        )?;
        Ok(stats)
    }

    /// Like [`catch_up_from_wal_for_test`] but also returns `last_seq` for
    /// chaining with [`closure_drain_for_test`].
    pub(super) fn catch_up_from_wal_for_test_with_seq(
        &self,
        pending_build: &PendingIndexBuild,
    ) -> ReadySetResult<(WalCatchUpStats, u64)> {
        let db = self.db.db();
        let snapshot = db.snapshot();
        self.catch_up_from_wal(
            db,
            db,
            Some(&snapshot),
            pending_build.sequence_number,
            &pending_build.pending_indices,
            false,
        )
    }

    /// Closure drain: catches up WAL writes from `since_seq` to current,
    /// writing directly to the primary DB CFs (which must already be active).
    /// Used by tests to verify the transfer-gap catch-up.
    pub(super) fn closure_drain_for_test(
        &self,
        since_seq: u64,
        pending_indices: &[PersistentIndex],
    ) -> ReadySetResult<WalCatchUpStats> {
        let db = self.db.db();
        let (stats, _) = self.catch_up_from_wal(
            db,
            db,   // primary is both source and dest
            None, // no snapshot needed
            since_seq,
            pending_indices,
            false, // WAL enabled for primary writes
        )?;
        Ok(stats)
    }

    /// Activates the indices from a [`PendingIndexBuild`].
    ///
    /// If the build holds a sidekick DB (from `scan_snapshot`), its data is
    /// transferred to the primary via SST ingestion before activation.
    pub(super) fn activate_build(&self, pending_build: PendingIndexBuild) -> ReadySetResult<()> {
        let db = self.db.db();

        // Transfer sidekick data to primary if present.
        if let Some(sidekick) = &pending_build.sidekick {
            self.transfer_sidekick_to_primary(
                sidekick,
                db,
                &self.default_options,
                &pending_build.pending_indices,
            )?;
        }
        // Drop sidekick (closes DB, but we don't remove the dir here -- it's a temp dir)
        if let Some(sidekick) = pending_build.sidekick {
            let sidekick_path = sidekick.path.clone();
            drop(sidekick);
            let _ = fs::remove_dir_all(&sidekick_path);
        }

        let mut shared_state = self.db.shared_state_mut();
        self.activate_pending_indices(&mut shared_state, pending_build.pending_indices)
    }
}

// =============================================================================
// Tests
// =============================================================================

/// Information about an in-progress index build, used by tests to inspect
/// and drive each step independently.
#[cfg(test)]
pub(super) struct PendingIndexBuild {
    /// The RocksDB sequence number at which the snapshot was taken.
    pub(super) sequence_number: u64,
    /// The indices being built (not yet in shared_state.indices).
    pub(super) pending_indices: Vec<PersistentIndex>,
    /// The sidekick DB holding scan data. Transfer to primary is deferred
    /// until activation so that WAL catch-up can proceed without WAL
    /// disruption from SST ingestion.
    sidekick: Option<SidekickDb>,
}

#[cfg(test)]
/// Reads the pending build marker from PENDING_BUILD_KEY, if present.
fn get_pending_build(db: &rocksdb::DB) -> Option<PendingBuildMeta> {
    db.get_pinned(PENDING_BUILD_KEY)
        .ok()
        .flatten()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Instant;

    use common::{IndexType, Record, Records};
    use readyset_client::internal::Index;
    use readyset_data::DfValue;

    use super::get_pending_build;
    use crate::persistent_state::tests::{insert, setup_persistent};
    use crate::persistent_state::{
        get_meta, DurabilityMode, PersistenceParameters, PersistenceType, PersistentState,
    };
    use crate::{LookupResult, PointKey, RecordResult, State};

    // =========================================================================
    // Test harness
    // =========================================================================

    /// A convenience wrapper that eliminates boilerplate setup in OIB tests.
    struct TestHarness {
        state: PersistentState,
    }

    impl TestHarness {
        /// Create a new test harness with primary index and `row_count` rows
        /// of `(pk, "value_{pk}")` data.
        fn new(name: &str, row_count: usize) -> Self {
            let mut state = setup_persistent(name, None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            for i in 0..row_count {
                insert(
                    &mut state,
                    vec![(i as i32).into(), format!("value_{}", i).into()],
                );
            }
            Self { state }
        }

        /// Create with custom unique keys.
        fn with_unique_keys(name: &str, unique_keys: &[usize], row_count: usize) -> Self {
            let mut state = setup_persistent(name, Some(unique_keys));
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            for i in 0..row_count {
                insert(
                    &mut state,
                    vec![(i as i32).into(), format!("value_{}", i).into()],
                );
            }
            Self { state }
        }

        /// Create with 3-column rows: (pk, "col1_{pk}", "col2_{pk}").
        fn new_three_cols(name: &str, row_count: usize) -> Self {
            let mut state = setup_persistent(name, None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            for i in 0..row_count {
                insert(
                    &mut state,
                    vec![
                        (i as i32).into(),
                        format!("col1_{}", i).into(),
                        format!("col2_{}", i).into(),
                    ],
                );
            }
            Self { state }
        }

        /// Insert a row.
        fn insert(&mut self, row: Vec<DfValue>) {
            insert(&mut self.state, row);
        }

        /// Insert N rows starting from `pk_start`: (pk, "{prefix}_{pk}").
        fn insert_range(&mut self, pk_start: i32, count: i32, prefix: &str) {
            for i in pk_start..(pk_start + count) {
                insert(
                    &mut self.state,
                    vec![i.into(), format!("{}_{}", prefix, i).into()],
                );
            }
        }

        /// Insert N 3-column rows starting from `pk_start`.
        fn insert_range_3col(&mut self, pk_start: i32, count: i32, prefix: &str) {
            for i in pk_start..(pk_start + count) {
                insert(
                    &mut self.state,
                    vec![
                        i.into(),
                        format!("{}_col1_{}", prefix, i).into(),
                        format!("{}_col2_{}", prefix, i).into(),
                    ],
                );
            }
        }

        /// Delete a row by providing the full row.
        fn delete(&mut self, row: Vec<DfValue>) {
            let mut records: Records = Record::Negative(row).into();
            self.state
                .process_records(&mut records, None, None)
                .expect("delete should succeed");
        }

        /// Look up by column index and key, assert exactly `expected` rows returned.
        fn assert_lookup_count(&self, col: usize, key: DfValue, expected: usize) {
            match self.state.lookup(&[col], &PointKey::Single(key.clone())) {
                LookupResult::Some(RecordResult::Owned(rows)) => {
                    assert_eq!(
                        rows.len(),
                        expected,
                        "lookup col={} key={:?}: expected {} rows, got {}",
                        col,
                        key,
                        expected,
                        rows.len()
                    );
                }
                other => panic!(
                    "lookup col={} key={:?}: expected Some(Owned), got {:?}",
                    col, key, other
                ),
            }
        }

        /// Look up by column index and key, assert 1 row with expected pk.
        fn assert_lookup_row(&self, col: usize, key: DfValue, expected_pk: DfValue) {
            match self.state.lookup(&[col], &PointKey::Single(key.clone())) {
                LookupResult::Some(RecordResult::Owned(rows)) => {
                    assert_eq!(
                        rows.len(),
                        1,
                        "lookup col={} key={:?}: expected 1 row, got {}",
                        col,
                        key,
                        rows.len()
                    );
                    assert_eq!(
                        rows[0][0], expected_pk,
                        "lookup col={} key={:?}: pk mismatch",
                        col, key
                    );
                }
                other => panic!(
                    "lookup col={} key={:?}: expected Some(Owned), got {:?}",
                    col, key, other
                ),
            }
        }

        /// Look up by column index and key, assert no rows.
        fn assert_lookup_empty(&self, col: usize, key: DfValue) {
            self.assert_lookup_count(col, key, 0);
        }

        /// Assert that all rows in range [pk_start, pk_start+count) exist in
        /// the secondary index on `col`.
        fn assert_secondary_range(&self, col: usize, pk_start: i32, count: i32, prefix: &str) {
            for i in pk_start..(pk_start + count) {
                let key = format!("{}_{}", prefix, i);
                self.assert_lookup_row(col, key.into(), i.into());
            }
        }
    }

    /// Helper to create a PersistentState at a specific path with permanent durability.
    /// This allows reopening the same database to test crash recovery scenarios.
    fn setup_persistent_at_path(path: &std::path::Path, name: &str) -> PersistentState {
        let unique_keys: Option<&[usize]> = None;
        let params = PersistenceParameters {
            mode: DurabilityMode::Permanent,
            storage_dir: Some(path.to_path_buf()),
            ..PersistenceParameters::default()
        };
        PersistentState::new(
            String::from(name),
            None,
            unique_keys,
            &params,
            PersistenceType::BaseTable,
            None,
        )
        .expect("should create persistent state")
    }

    /// Blocks until the index build finishes (succeeded or failed).
    /// Returns true if finished within `timeout`, false if timed out.
    fn wait_for_index_build(state: &PersistentState, timeout: std::time::Duration) -> bool {
        let start = Instant::now();
        while state.index_build_status() == crate::IndexBuildStatus::InProgress {
            if start.elapsed() > timeout {
                return false;
            }
            thread::sleep(std::time::Duration::from_millis(1));
        }
        true
    }

    // =========================================================================
    // online_index_build -- core lifecycle, WAL catch-up, high volume, batching
    // =========================================================================

    mod online_index_build {
        use std::thread;
        use std::time::Duration;

        use common::IndexType;
        use readyset_client::internal::Index;

        use readyset_data::DfValue;

        use super::{insert, setup_persistent, wait_for_index_build, TestHarness};
        use crate::persistent_state::PersistentState;
        use crate::{IndexBuildStatus, LookupResult, PointKey, RecordResult, State};

        /// Assert that looking up `key` in `cols` returns exactly `expected` rows.
        fn assert_lookup(state: &PersistentState, cols: &[usize], key: &PointKey, expected: usize) {
            match state.lookup(cols, key) {
                LookupResult::Some(RecordResult::Owned(rows)) => {
                    assert_eq!(
                        rows.len(),
                        expected,
                        "lookup cols={:?} key={:?}: expected {} rows, got {}",
                        cols,
                        key,
                        expected,
                        rows.len()
                    );
                }
                other => panic!(
                    "lookup cols={:?} key={:?}: expected Some(Owned), got {:?}",
                    cols, key, other
                ),
            }
        }

        #[test]
        fn add_secondary_records_sequence_number() {
            let h = TestHarness::new("add_secondary_records_seq", 10);

            let seq_before = h.state.db.db().latest_sequence_number();
            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            assert!(
                pending_build.sequence_number >= seq_before,
                "Pending build sequence {} should be >= seq_before {}",
                pending_build.sequence_number,
                seq_before
            );
            assert_eq!(pending_build.pending_indices.len(), 1);
            assert_eq!(pending_build.pending_indices[0].index.columns, vec![1]);
        }

        #[test]
        fn pending_indices_not_in_active_list() {
            let h = TestHarness::new("pending_not_active", 5);

            let initial_count = h.state.db.shared_state().indices.len();
            assert_eq!(initial_count, 1);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let _pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            assert_eq!(
                h.state.db.shared_state().indices.len(),
                initial_count,
                "Pending indices should not be added to active list during build"
            );
        }

        #[test]
        fn activate_pending_indices_adds_to_active_list() {
            let h = TestHarness::new("activate_adds_to_list", 5);
            let initial_count = h.state.db.shared_state().indices.len();

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            assert_eq!(h.state.db.shared_state().indices.len(), initial_count);

            ctx.activate_build(pending_build)
                .expect("activate_build should succeed");

            assert_eq!(h.state.db.shared_state().indices.len(), initial_count + 1);
            h.assert_lookup_row(1, "value_3".into(), 3.into());
        }

        #[test]
        fn writes_during_build_go_to_primary_only() {
            let mut h = TestHarness::new("writes_primary_only", 5);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");
            let seq_at_build = pending_build.sequence_number;

            h.insert_range(100, 5, "after_build");
            h.assert_lookup_count(0, 100.into(), 1);

            let current_seq = h.state.db.db().latest_sequence_number();
            assert!(current_seq > seq_at_build);
        }

        #[test]
        fn wal_catchup_applies_writes_during_build() {
            let mut h = TestHarness::new("wal_catchup_applies", 5);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            h.insert_range(100, 3, "during_build");

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("WAL catch-up should succeed");
            assert_eq!(stats.puts_applied, 3);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            h.assert_lookup_row(1, "value_2".into(), 2.into());
            h.assert_lookup_row(1, "during_build_101".into(), 101.into());
        }

        #[test]
        fn build_indices_catches_up_writes_after_prepare() {
            let mut h = TestHarness::new("catches_up_writes", 5);

            let strict = vec![(Index::new(IndexType::HashMap, vec![1]), None)];
            let indices = h
                .state
                .prepare_indices_for_build(strict, vec![])
                .unwrap()
                .expect("Should have indices to build");

            h.state.mark_index_build_in_progress().unwrap();
            let ctx = h.state.create_index_build_context();

            // Insert data after prepare but before build_indices
            h.insert_range(100, 3, "concurrent");

            ctx.build_indices(indices).unwrap();

            h.assert_lookup_row(1, "value_3".into(), 3.into());
            h.assert_lookup_row(1, "concurrent_101".into(), 101.into());
        }

        #[test]
        fn wait_for_index_build_success_and_timeout() {
            // Part 1: Successful wait
            {
                let mut state = setup_persistent("wait_build_success", None);
                state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
                state.mark_index_build_in_progress().unwrap();

                let status = state.index_build_status.clone();
                let handle = thread::spawn(move || {
                    thread::sleep(Duration::from_millis(50));
                    status.store(IndexBuildStatus::Succeeded);
                });

                let completed = wait_for_index_build(&state, Duration::from_secs(1));
                assert!(completed, "wait_for_index_build should return true");
                handle.join().unwrap();
            }

            // Part 2: Timeout
            {
                let mut state = setup_persistent("wait_build_timeout", None);
                state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
                state.mark_index_build_in_progress().unwrap();

                let completed = wait_for_index_build(&state, Duration::from_millis(200));
                assert!(
                    !completed,
                    "wait_for_index_build should return false on timeout"
                );
            }
        }

        #[test]
        fn mark_in_progress_rejects_concurrent_build() {
            let mut state = setup_persistent("reject_concurrent", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);

            state.mark_index_build_in_progress().unwrap();
            let result = state.mark_index_build_in_progress();
            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("already"),
                "error should mention 'already': {err_msg}"
            );
        }

        #[test]
        fn mark_in_progress_after_completed_or_failed() {
            let mut state = setup_persistent("mark_after_complete", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);

            state.mark_index_build_in_progress().unwrap();
            state.index_build_status.store(IndexBuildStatus::Succeeded);
            state.mark_index_build_in_progress().unwrap();
            state.index_build_status.store(IndexBuildStatus::Failed);
            state.mark_index_build_in_progress().unwrap();
        }

        #[test]
        fn snapshot_scan_includes_all_existing_rows() {
            let h = TestHarness::new("snapshot_includes_all", 10);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");
            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            h.assert_secondary_range(1, 0, 10, "value");
        }

        #[test]
        fn empty_indices_succeeds() {
            let h = TestHarness::new("empty_indices", 0);

            let ctx = h.state.create_index_build_context();
            let indices: Vec<(Index, bool)> = vec![];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");
            assert!(pending_build.pending_indices.is_empty());
        }

        #[test]
        fn multiple_indices_at_once() {
            let h = TestHarness::new_three_cols("multi_indices", 10);

            let ctx = h.state.create_index_build_context();
            let indices = vec![
                (Index::new(IndexType::HashMap, vec![1]), false),
                (Index::new(IndexType::HashMap, vec![2]), false),
            ];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");
            assert_eq!(pending_build.pending_indices.len(), 2);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            h.assert_lookup_row(1, "col1_5".into(), 5.into());
            h.assert_lookup_row(2, "col2_7".into(), 7.into());
        }

        #[test]
        fn wal_catchup_with_no_writes_during_build() {
            let h = TestHarness::new("wal_no_writes", 5);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 0);
            assert_eq!(stats.deletes_skipped, 0);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");
            h.assert_lookup_row(1, "value_3".into(), 3.into());
        }

        #[test]
        fn wal_catchup_multiple_indices() {
            let mut h = TestHarness::new_three_cols("wal_multi_indices", 3);

            let ctx = h.state.create_index_build_context();
            let indices = vec![
                (Index::new(IndexType::HashMap, vec![1]), false),
                (Index::new(IndexType::HashMap, vec![2]), false),
            ];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            h.insert_range_3col(100, 2, "new");

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 2);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");
            h.assert_lookup_row(1, "new_col1_100".into(), 100.into());
            h.assert_lookup_row(2, "new_col2_101".into(), 101.into());
        }

        #[test]
        fn wal_catchup_handles_unique_indices() {
            let mut h = TestHarness::new("wal_unique", 0);

            // Insert initial data with integer col1
            for i in 0..3 {
                h.insert(vec![i.into(), (i * 100).into()]);
            }

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), true)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            // Insert unique values during build
            for i in 100..102 {
                h.insert(vec![i.into(), (i * 100).into()]);
            }

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 2);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");
            h.assert_lookup_row(1, 10000.into(), 100.into());
        }

        #[test]
        fn wal_catchup_preserves_operation_order() {
            let mut h = TestHarness::new("wal_op_order", 3);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            // Interleaved ops on the same PK
            let row_foo: Vec<DfValue> = vec![100.into(), "foo".into()];
            let row_bar: Vec<DfValue> = vec![100.into(), "bar".into()];
            h.insert(row_foo.clone());
            h.delete(row_foo);
            h.insert(row_bar);
            h.insert(vec![200.into(), "simple".into()]);

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 3); // foo, bar, simple
            assert_eq!(stats.deletes_applied, 1);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");
            h.assert_lookup_row(1, "simple".into(), 200.into());
            h.assert_lookup_row(1, "bar".into(), 100.into());
        }

        #[test]
        fn wal_catchup_null_values_in_unique_index() {
            let mut h = TestHarness::with_unique_keys("wal_null_unique", &[1], 0);

            // Insert rows with NULL and non-NULL values in column 1
            h.insert(vec![1.into(), "alpha".into(), "extra_1".into()]);
            h.insert(vec![2.into(), DfValue::None, "extra_2".into()]);
            h.insert(vec![3.into(), "gamma".into(), "extra_3".into()]);
            h.insert(vec![4.into(), DfValue::None, "extra_4".into()]);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), true)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");
            assert!(pending_build.pending_indices[0].is_unique);

            // Insert more NULLs during build
            h.insert(vec![5.into(), DfValue::None, "extra_5".into()]);
            h.insert(vec![6.into(), DfValue::None, "extra_6".into()]);
            h.insert(vec![7.into(), "delta".into(), "extra_7".into()]);

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 3);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            // All 4 NULL rows should coexist
            match h.state.lookup(&[1], &PointKey::Single(DfValue::None)) {
                LookupResult::Some(RecordResult::Owned(rows)) => {
                    assert_eq!(rows.len(), 4, "All 4 NULL rows should coexist");
                    let mut pks: Vec<i64> = rows
                        .iter()
                        .map(|r| match &r[0] {
                            DfValue::Int(i) => *i,
                            other => panic!("Expected Int, got {:?}", other),
                        })
                        .collect();
                    pks.sort();
                    assert_eq!(pks, vec![2, 4, 5, 6]);
                }
                _ => panic!("Lookup of NULL should succeed"),
            }
            h.assert_lookup_row(1, "delta".into(), 7.into());
        }

        #[test]
        fn wal_catchup_with_unavailable_entries() {
            let h = TestHarness::new("wal_unavailable", 1);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let mut pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            // Tamper with sequence number to simulate WAL GC
            pending_build.sequence_number = u64::MAX;
            let result = ctx.catch_up_from_wal_for_test(&pending_build);
            assert!(result.is_err(), "Should fail with unavailable WAL entries");
        }

        /// Closure drain catches rows inserted during the transfer gap.
        #[test]
        fn wal_catchup_closure_drain() {
            let mut h = TestHarness::new("closure_drain", 5);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            // WAL catch-up writes
            h.insert_range(100, 3, "catchup");

            let (stats, last_seq) = ctx
                .catch_up_from_wal_for_test_with_seq(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 3);

            // Writes during the transfer gap (inserts AND deletes)
            h.insert_range(200, 5, "gap");
            h.delete(vec![201.into(), "gap_201".into()]);

            let pending_indices = pending_build.pending_indices.clone();
            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            // Closure drain
            let stats = ctx
                .closure_drain_for_test(last_seq, &pending_indices)
                .expect("closure drain should succeed");
            assert_eq!(stats.puts_applied, 5, "Should catch 5 gap inserts");
            assert_eq!(stats.deletes_applied, 1, "Should apply 1 gap delete");

            // Verify ALL rows are findable
            h.assert_secondary_range(1, 0, 5, "value");
            h.assert_secondary_range(1, 100, 3, "catchup");
            // gap rows: 200, 202, 203, 204 should exist; 201 deleted
            h.assert_lookup_row(1, "gap_200".into(), 200.into());
            h.assert_lookup_empty(1, "gap_201".into());
            h.assert_lookup_row(1, "gap_204".into(), 204.into());
        }

        // ---- WAL catch-up handles deletes and inserts after snapshot ----

        #[test]
        fn wal_catchup_handles_deletes_and_inserts_after_snapshot() {
            let mut h = TestHarness::new("wal_deletes_inserts", 100);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            // Delete rows 50-74 and insert rows 100-124
            for i in 50..75 {
                h.delete(vec![i.into(), format!("value_{}", i).into()]);
            }
            h.insert_range(100, 25, "new");

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert!(stats.deletes_skipped >= 25);
            assert_eq!(stats.puts_applied, 25);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            h.assert_lookup_row(1, "new_110".into(), 110.into());
            h.assert_lookup_empty(0, 60.into());
        }

        // ---- High volume tests ----

        #[test]
        fn wal_catchup_high_volume() {
            let mut h = TestHarness::new("high_volume", 10_000);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            h.insert_range(10_000, 5_000, "concurrent");

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 5_000);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            for (pk, val) in [
                (0, "value_0"),
                (9999, "value_9999"),
                (14999, "concurrent_14999"),
            ] {
                h.assert_lookup_row(1, val.into(), pk.into());
            }
        }

        #[test]
        fn high_volume_multiple_indices() {
            let mut state = setup_persistent("high_vol_multi", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);

            for i in 0..5_000 {
                insert(
                    &mut state,
                    vec![
                        i.into(),
                        format!("col1_{}", i).into(),
                        format!("col2_{}", i % 100).into(),
                    ],
                );
            }

            let ctx = state.create_index_build_context();
            let indices = vec![
                (Index::new(IndexType::HashMap, vec![1]), false),
                (Index::new(IndexType::HashMap, vec![2]), false),
                (Index::new(IndexType::HashMap, vec![1, 2]), false),
            ];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            for i in 5_000..7_000 {
                insert(
                    &mut state,
                    vec![
                        i.into(),
                        format!("col1_{}", i).into(),
                        format!("col2_{}", i % 100).into(),
                    ],
                );
            }

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 2_000);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            assert_lookup(&state, &[1], &PointKey::Single("col1_6000".into()), 1);
            assert_lookup(&state, &[2], &PointKey::Single("col2_50".into()), 70);
            assert_lookup(
                &state,
                &[1, 2],
                &PointKey::Double(("col1_6500".into(), "col2_0".into())),
                1,
            );
        }

        #[test]
        fn large_row_values_during_build() {
            let large_value: String = "x".repeat(100 * 1024);

            let mut state = setup_persistent("large_row_values", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            for i in 0..100 {
                insert(
                    &mut state,
                    vec![i.into(), format!("{}_{}", large_value, i).into()],
                );
            }

            let ctx = state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            for i in 100..150 {
                insert(
                    &mut state,
                    vec![i.into(), format!("{}_{}", large_value, i).into()],
                );
            }

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 50);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            // Verify the 100KB value round-trips exactly
            let expected_val = format!("{}_{}", large_value, 125);
            match state.lookup(&[1], &PointKey::Single(expected_val.clone().into())) {
                LookupResult::Some(RecordResult::Owned(rows)) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0], 125.into());
                    assert_eq!(rows[0][1], DfValue::from(expected_val.clone()));
                }
                _ => panic!("Large row should be accessible via secondary index"),
            }
        }

        // ---- WAL batch boundary tests (merged into online_index_build) ----

        #[test]
        fn wal_catchup_across_batch_boundaries() {
            let mut h = TestHarness::new("wal_batch_boundaries", 10);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            // Insert well over WAL_CATCHUP_WRITE_THRESHOLD (4096) rows
            h.insert_range(10, 8990, "batch");

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 8990);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            // Verify rows from different batches are accessible
            for i in [10, 500, 4095, 4096, 4097, 8999] {
                h.assert_lookup_row(1, format!("batch_{}", i).into(), i.into());
            }
        }

        // ---- Transfer gap write regression tests ----

        #[test]
        fn build_indices_captures_all_concurrent_writes() {
            let mut h = TestHarness::new("all_concurrent_writes", 10);

            let strict = vec![(Index::new(IndexType::HashMap, vec![1]), None)];
            let indices = h
                .state
                .prepare_indices_for_build(strict, vec![])
                .expect("prepare should succeed")
                .expect("Should have indices");

            h.insert_range(100, 10, "after_prepare");

            let state = h.state;
            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();
            ctx.build_indices(indices).unwrap();

            assert_eq!(state.index_build_status(), IndexBuildStatus::Succeeded);
            for i in 0..10 {
                assert_lookup(
                    &state,
                    &[1],
                    &PointKey::Single(format!("value_{}", i).into()),
                    1,
                );
            }
            for i in 100..110 {
                assert_lookup(
                    &state,
                    &[1],
                    &PointKey::Single(format!("after_prepare_{}", i).into()),
                    1,
                );
            }
        }

        #[test]
        fn build_indices_large_batch_production_path() {
            let mut h = TestHarness::new("large_batch_prod", 100);

            let strict = vec![(Index::new(IndexType::HashMap, vec![1]), None)];
            let indices = h
                .state
                .prepare_indices_for_build(strict, vec![])
                .expect("prepare should succeed")
                .expect("Should have indices");

            h.insert_range(1000, 5000, "bulk");

            let state = h.state;
            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();
            ctx.build_indices(indices).unwrap();

            assert_eq!(state.index_build_status(), IndexBuildStatus::Succeeded);
            for i in [0, 50, 99, 1000, 2048, 4095, 4096, 4097, 5999] {
                let key = if i < 100 {
                    format!("value_{}", i)
                } else {
                    format!("bulk_{}", i)
                };
                assert_lookup(&state, &[1], &PointKey::Single(key.into()), 1);
            }
        }

        #[test]
        fn build_indices_sends_creating_index_notification() {
            use readyset_client::TableStatus;
            use readyset_sql::ast::{Relation, SqlIdentifier};

            use crate::persistent_state::{PersistenceType, PersistentState};

            let table = Relation {
                schema: None,
                name: SqlIdentifier::from("test_table"),
            };
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut state = PersistentState::new(
                String::from("status_notify_test"),
                Some(table.clone()),
                None::<&[usize]>,
                &Default::default(),
                PersistenceType::BaseTable,
                Some(tx),
            )
            .expect("should create state");

            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            for i in 0..5 {
                insert(
                    &mut state,
                    vec![DfValue::from(i), format!("val_{}", i).into()],
                );
            }

            // Drain notifications from add_index
            while rx.try_recv().is_ok() {}

            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            ctx.build_indices(indices).unwrap();

            assert_eq!(state.index_build_status(), IndexBuildStatus::Succeeded);

            let mut notifications = Vec::new();
            while let Ok(msg) = rx.try_recv() {
                notifications.push(msg);
            }

            // Should have at least one CreatingIndex notification
            let creating_count = notifications
                .iter()
                .filter(|(_, status)| matches!(status, TableStatus::CreatingIndex(_)))
                .count();
            assert!(
                creating_count >= 1,
                "Should have at least 1 CreatingIndex notification, got {}",
                creating_count
            );

            // Check that at least one CreatingIndex has a progress value.
            // With small tables the build may complete before a progress
            // notification is emitted, so we only check when progress IS sent.
            let progress_values: Vec<_> = notifications
                .iter()
                .filter_map(|(_, status)| match status {
                    TableStatus::CreatingIndex(Some(p)) => Some(*p),
                    _ => None,
                })
                .collect();
            for p in &progress_values {
                assert!(
                    *p >= 0.0 && *p <= 1.0,
                    "progress should be in [0.0, 1.0], got {p}"
                );
            }

            // All notifications should be for our table
            for (notified_table, _) in &notifications {
                assert_eq!(notified_table, &table);
            }
        }

        // ---- WAL catch-up with only deletes ----

        #[test]
        fn wal_catchup_only_deletes() {
            let mut h = TestHarness::new("wal_only_deletes", 10);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            // Delete 3 rows, no new inserts
            for i in 3..6 {
                h.delete(vec![i.into(), format!("value_{}", i).into()]);
            }

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert!(
                stats.deletes_skipped >= 3,
                "Should skip at least 3 deletes, got {}",
                stats.deletes_skipped
            );
            assert_eq!(stats.puts_applied, 0, "No puts should be applied");

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            // Deletes during WAL catch-up are skipped (not applied to the
            // secondary) because the rows were already captured in the snapshot
            // scan. The primary PK CF no longer has these rows, so secondary
            // lookups that return them are "stale" -- but this is consistent
            // with how the blocking path behaves. Non-deleted rows are correct.
            h.assert_lookup_row(1, "value_0".into(), 0.into());
            h.assert_lookup_row(1, "value_9".into(), 9.into());
        }
    }

    // =========================================================================
    // wal_operation_collector_tests
    // =========================================================================

    mod wal_operation_collector_tests {
        use rocksdb::WriteBatchIteratorCf;

        use super::super::{WalOperation, WalOperationCollector};

        const TARGET_CF: u32 = 5;
        const OTHER_CF: u32 = 99;

        #[test]
        fn filters_puts_by_cf() {
            let mut collector = WalOperationCollector::new(TARGET_CF);
            collector.put_cf(TARGET_CF, b"key1", b"value1");
            collector.put_cf(TARGET_CF, b"key2", b"value2");
            collector.put_cf(OTHER_CF, b"key3", b"value3");
            assert_eq!(collector.operations.len(), 2);
            assert!(!collector.had_unexpected_op);
        }

        #[test]
        fn filters_deletes_by_cf() {
            let mut collector = WalOperationCollector::new(TARGET_CF);
            collector.delete_cf(TARGET_CF, b"key1");
            collector.delete_cf(OTHER_CF, b"key2");
            assert_eq!(collector.operations.len(), 1);
        }

        #[test]
        fn merge_sets_unexpected_flag() {
            let mut collector = WalOperationCollector::new(TARGET_CF);
            collector.merge_cf(OTHER_CF, b"key1", b"value1");
            assert!(!collector.had_unexpected_op);
            collector.merge_cf(TARGET_CF, b"key2", b"value2");
            assert!(collector.had_unexpected_op);
            assert!(collector.operations.is_empty());
        }

        #[test]
        fn clear_retains_capacity() {
            let mut collector = WalOperationCollector::new(TARGET_CF);
            for i in 0..10 {
                collector.put_cf(TARGET_CF, format!("key_{i}").as_bytes(), b"val");
            }
            assert_eq!(collector.operations.len(), 10);
            let capacity_before = collector.operations.capacity();
            collector.clear();
            assert_eq!(collector.operations.len(), 0);
            assert_eq!(collector.operations.capacity(), capacity_before);
        }

        #[test]
        fn unexpected_flag_survives_clear() {
            let mut collector = WalOperationCollector::new(TARGET_CF);
            collector.merge_cf(TARGET_CF, b"key", b"value");
            assert!(collector.had_unexpected_op);
            collector.clear();
            assert!(collector.had_unexpected_op);
        }

        #[test]
        fn interleaved_ops_preserve_order() {
            let mut collector = WalOperationCollector::new(TARGET_CF);
            collector.put_cf(TARGET_CF, b"key_a", b"val_1");
            collector.delete_cf(TARGET_CF, b"key_a");
            collector.put_cf(TARGET_CF, b"key_a", b"val_2");

            assert_eq!(collector.operations.len(), 3);
            assert!(matches!(&collector.operations[0], WalOperation::Put { .. }));
            assert!(matches!(
                &collector.operations[1],
                WalOperation::Delete { .. }
            ));
            assert!(matches!(&collector.operations[2], WalOperation::Put { .. }));
        }
    }

    // =========================================================================
    // crash_recovery_tests
    // =========================================================================

    mod crash_recovery_tests {
        use common::IndexType;
        use readyset_client::internal::Index;
        use readyset_data::DfValue;

        use super::{
            get_meta, get_pending_build, insert, setup_persistent_at_path, LookupResult, PointKey,
            RecordResult, State,
        };

        /// Parameterized crash recovery: tests three variants of interrupted builds.
        #[test]
        fn startup_recovery_variants() {
            // Variant 1: Normal interrupted build with existing CFs
            {
                let dir = tempfile::tempdir().expect("should create temp dir");
                let path = dir.path();
                let name = "recovery_normal";

                {
                    let mut state = setup_persistent_at_path(path, name);
                    state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
                    insert(&mut state, vec![1.into(), "val1".into()]);
                    insert(&mut state, vec![2.into(), "val2".into()]);

                    let ctx = state.create_index_build_context();
                    let db = state.db.db();
                    let seq = db.latest_sequence_number();
                    ctx.mark_pending_build(vec!["1".to_string()], seq).unwrap();
                    assert!(get_pending_build(db).is_some());
                }
                {
                    let state = setup_persistent_at_path(path, name);
                    let db = state.db.db();
                    assert!(get_pending_build(db).is_none());
                    match state.lookup(&[0], &PointKey::Single(1.into())) {
                        LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 1),
                        _ => panic!("Data should be accessible after recovery"),
                    }
                }
            }

            // Variant 2: Nonexistent CFs in pending marker
            {
                let dir = tempfile::tempdir().expect("should create temp dir");
                let path = dir.path();
                let name = "recovery_nonexistent";

                {
                    let mut state = setup_persistent_at_path(path, name);
                    state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
                    insert(&mut state, vec![1.into(), "val".into()]);

                    let ctx = state.create_index_build_context();
                    let db = state.db.db();
                    let seq = db.latest_sequence_number();
                    ctx.mark_pending_build(vec!["999".to_string(), "1000".to_string()], seq)
                        .unwrap();
                }
                {
                    let state = setup_persistent_at_path(path, name);
                    assert!(get_pending_build(state.db.db()).is_none());
                    match state.lookup(&[0], &PointKey::Single(1.into())) {
                        LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 1),
                        _ => panic!("Data should be accessible"),
                    }
                }
            }

            // Variant 3: Empty CF list in pending marker
            {
                let dir = tempfile::tempdir().expect("should create temp dir");
                let path = dir.path();
                let name = "recovery_empty_cfs";

                {
                    let mut state = setup_persistent_at_path(path, name);
                    state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
                    insert(&mut state, vec![1.into(), "val".into()]);

                    let ctx = state.create_index_build_context();
                    let db = state.db.db();
                    let seq = db.latest_sequence_number();
                    ctx.mark_pending_build(vec![], seq).unwrap();
                }
                {
                    let state = setup_persistent_at_path(path, name);
                    assert!(
                        get_pending_build(state.db.db()).is_none(),
                        "pending_build should be cleared with empty CF list"
                    );
                }
            }
        }

        #[test]
        fn startup_cleans_up_existing_orphan_cfs() {
            let dir = tempfile::tempdir().expect("should create temp dir");
            let path = dir.path();
            let name = "startup_orphan_cfs";

            {
                let mut state = setup_persistent_at_path(path, name);
                state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
                for i in 0..5 {
                    insert(
                        &mut state,
                        vec![DfValue::from(i), format!("val_{}", i).into()],
                    );
                }

                let ctx = state.create_index_build_context();
                let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
                let _pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

                let db = state.db.db();
                let pending = get_pending_build(db);
                assert!(pending.is_some());
                assert_eq!(pending.as_ref().unwrap().column_families, vec!["1"]);
                // Drop without activating - simulating crash
            }

            {
                let state = setup_persistent_at_path(path, name);
                let db = state.db.db();
                let meta = get_meta(db).expect("should get meta");

                assert!(get_pending_build(db).is_none());
                assert_eq!(meta.indices.len(), 0, "indices should be empty after wipe");

                // State is functional
                let mut state = state;
                state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
                for i in 0..5 {
                    insert(
                        &mut state,
                        vec![DfValue::from(i), format!("val_{}", i).into()],
                    );
                }
                state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
                match state.lookup(&[1], &PointKey::Single("val_2".into())) {
                    LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 1),
                    _ => panic!("New index should be functional after recovery"),
                }
            }
        }
    }

    // =========================================================================
    // pending_build_tests
    // =========================================================================

    mod pending_build_tests {
        use common::IndexType;
        use readyset_client::internal::Index;

        use super::{get_pending_build, insert, setup_persistent, TestHarness};
        use crate::State;

        #[test]
        fn pending_build_markers_are_persisted() {
            let mut state = setup_persistent("pending_markers", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            insert(&mut state, vec![1.into(), "val1".into()]);

            let ctx = state.create_index_build_context();
            let db = state.db.db();
            let seq = db.latest_sequence_number();
            let cf_names = vec!["1".to_string()];
            ctx.mark_pending_build(cf_names.clone(), seq).unwrap();

            let pending = get_pending_build(db);
            assert!(pending.is_some());
            assert_eq!(pending.as_ref().unwrap().column_families, cf_names);

            ctx.clear_pending_build().unwrap();
            assert!(get_pending_build(db).is_none());
        }

        #[test]
        fn pending_build_markers_full_flow() {
            let h = TestHarness::new("pending_full_flow", 3);

            {
                let db = h.state.db.db();
                assert!(get_pending_build(db).is_none());
            }

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            {
                let db = h.state.db.db();
                assert!(get_pending_build(db).is_some());
            }

            let stats = ctx
                .catch_up_from_wal_for_test(&pending_build)
                .expect("catch-up should succeed");
            assert_eq!(stats.puts_applied, 0);
            assert_eq!(stats.deletes_applied, 0);

            ctx.activate_build(pending_build)
                .expect("activate should succeed");

            {
                let db = h.state.db.db();
                assert!(get_pending_build(db).is_none());
            }
        }
    }

    // =========================================================================
    // build_indices_error_paths
    // =========================================================================

    mod build_indices_error_paths {
        use common::IndexType;
        use readyset_client::internal::Index;

        use super::{get_pending_build, TestHarness};
        use crate::persistent_state::PersistentState;
        use crate::{IndexBuildStatus, LookupResult, PointKey, RecordResult, State};

        fn assert_clean_state_after_failure(state: &PersistentState) {
            assert_eq!(state.index_build_status(), IndexBuildStatus::Failed);
            let db = state.db.db();
            assert!(get_pending_build(db).is_none());
        }

        #[test]
        fn prepare_failure_restores_flag_and_clears_marker() {
            let h = TestHarness::new("prepare_failure_flag", 5);
            let mut state = h.state;

            // Pre-create CF "1" to cause a collision
            {
                let db = state.db.db();
                db.create_cf("1", &rocksdb::Options::default()).unwrap();
            }

            let strict = vec![(Index::new(IndexType::HashMap, vec![1]), None)];
            let indices = state
                .prepare_indices_for_build(strict, vec![])
                .unwrap()
                .expect("Should have indices");

            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();
            ctx.build_indices(indices)
                .expect_err("build should fail due to CF collision");

            assert_clean_state_after_failure(&state);
            match state.lookup(&[0], &PointKey::Single(3.into())) {
                LookupResult::Some(RecordResult::Owned(rows)) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][1], "value_3".into());
                }
                _ => panic!("Existing data should remain accessible"),
            }
        }

        #[test]
        fn prepare_failure_allows_successful_retry() {
            let h = TestHarness::new("prepare_failure_retry", 5);
            let mut state = h.state;

            // Fail the first attempt
            {
                let db = state.db.db();
                db.create_cf("1", &rocksdb::Options::default()).unwrap();
            }
            let strict = vec![(Index::new(IndexType::HashMap, vec![1]), None)];
            let indices = state
                .prepare_indices_for_build(strict, vec![])
                .unwrap()
                .expect("Should have indices");
            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();
            ctx.build_indices(indices)
                .expect_err("build should fail due to CF collision");
            assert_clean_state_after_failure(&state);

            // Remove colliding CF and retry
            {
                let db = state.db.db();
                db.drop_cf("1").unwrap();
            }
            let strict = vec![(Index::new(IndexType::HashMap, vec![1]), None)];
            let indices = state
                .prepare_indices_for_build(strict, vec![])
                .unwrap()
                .expect("Should have indices");
            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();
            ctx.build_indices(indices).unwrap();

            assert_eq!(state.index_build_status(), IndexBuildStatus::Succeeded);
            match state.lookup(&[1], &PointKey::Single("value_3".into())) {
                LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 1),
                _ => panic!("Index should be usable after retry"),
            }
        }
    }

    // =========================================================================
    // prepare_indices_dedup
    // =========================================================================

    mod prepare_indices_dedup {
        use common::IndexType;
        use readyset_client::internal::Index;

        use super::setup_persistent;
        use crate::State;

        /// Tests all three deduplication scenarios in one test.
        #[test]
        fn deduplicates_across_all_scenarios() {
            // Scenario 1: Same index in both strict and weak
            {
                let mut state = setup_persistent("dedup_strict_weak", None);
                state.add_index(Index::new(IndexType::HashMap, vec![0]), None);

                let dup = Index::new(IndexType::HashMap, vec![1]);
                let result = state
                    .prepare_indices_for_build(vec![(dup.clone(), None)], vec![dup])
                    .unwrap();
                let indices = result.expect("should return Some");
                assert_eq!(indices.len(), 1, "strict+weak dup should be deduplicated");
            }

            // Scenario 2: Duplicate within strict
            {
                let mut state = setup_persistent("dedup_within_strict", None);
                state.add_index(Index::new(IndexType::HashMap, vec![0]), None);

                let dup = Index::new(IndexType::HashMap, vec![2]);
                let result = state
                    .prepare_indices_for_build(vec![(dup.clone(), None), (dup, None)], vec![])
                    .unwrap();
                let indices = result.expect("should return Some");
                assert_eq!(indices.len(), 1, "within-strict dup should be deduplicated");
            }

            // Scenario 3: Duplicate within weak
            {
                let mut state = setup_persistent("dedup_within_weak", None);
                state.add_index(Index::new(IndexType::HashMap, vec![0]), None);

                let dup = Index::new(IndexType::HashMap, vec![3]);
                let result = state
                    .prepare_indices_for_build(vec![], vec![dup.clone(), dup])
                    .unwrap();
                let indices = result.expect("should return Some");
                assert_eq!(indices.len(), 1, "within-weak dup should be deduplicated");
            }
        }

        #[test]
        fn prepare_indices_for_build_filters_existing() {
            let mut state = setup_persistent("prepare_filters_existing", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_index(Index::new(IndexType::HashMap, vec![1]), None);

            let strict = vec![
                (Index::new(IndexType::HashMap, vec![1]), None), // exists
                (Index::new(IndexType::HashMap, vec![2]), None), // new
            ];
            let result = state.prepare_indices_for_build(strict, vec![]).unwrap();
            let indices = result.expect("should have indices");
            assert_eq!(indices.len(), 1);
            assert_eq!(indices[0].0.columns, vec![2]);
        }

        #[test]
        fn prepare_indices_for_build_returns_none_when_all_exist() {
            let mut state = setup_persistent("prepare_all_exist", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_index(Index::new(IndexType::HashMap, vec![1]), None);

            let strict = vec![(Index::new(IndexType::HashMap, vec![1]), None)];
            let result = state.prepare_indices_for_build(strict, vec![]).unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn prepare_indices_for_build_creates_primary() {
            let mut state = setup_persistent("prepare_creates_primary", None);
            let strict = vec![(Index::new(IndexType::HashMap, vec![0]), None)];
            let result = state.prepare_indices_for_build(strict, vec![]).unwrap();
            assert!(result.is_none());
            assert_eq!(state.db.shared_state().indices.len(), 1);
        }
    }

    // =========================================================================
    // btreemap_wal_tests
    // =========================================================================

    mod btreemap_wal_tests {
        use common::IndexType;
        use readyset_client::internal::Index;
        use readyset_data::{Bound, DfValue};
        use vec1::vec1;

        use super::{insert, setup_persistent};
        use crate::{IndexBuildStatus, RangeKey, RangeLookupResult, State};

        #[test]
        fn btreemap_index_with_wal_catchup() {
            let mut state = setup_persistent("btreemap_wal", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            for i in 0..20i32 {
                insert(&mut state, vec![i.into(), (i * 10).into()]);
            }

            let strict = vec![(Index::new(IndexType::BTreeMap, vec![1]), None)];
            let indices = state
                .prepare_indices_for_build(strict, vec![])
                .unwrap()
                .expect("Should have indices");

            for i in 100..110i32 {
                insert(&mut state, vec![i.into(), (i * 10).into()]);
            }

            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();
            ctx.build_indices(indices).unwrap();

            assert_eq!(state.index_build_status(), IndexBuildStatus::Succeeded);

            // Range covering initial data: [50, 100] -> 6 rows
            let range = RangeKey::from(&(
                Bound::Included(vec1![DfValue::from(50)]),
                Bound::Included(vec1![DfValue::from(100)]),
            ));
            match state.lookup_range(&[1], &range) {
                RangeLookupResult::Some(rows) => assert_eq!(rows.len(), 6),
                _ => panic!("Range lookup should succeed"),
            }

            // Range covering WAL catch-up data: [1000, 1050] -> 6 rows
            let range = RangeKey::from(&(
                Bound::Included(vec1![DfValue::from(1000)]),
                Bound::Included(vec1![DfValue::from(1050)]),
            ));
            match state.lookup_range(&[1], &range) {
                RangeLookupResult::Some(rows) => assert_eq!(rows.len(), 6),
                _ => panic!("Range lookup on WAL data should succeed"),
            }

            // Range spanning both: [180, 1010] -> 4 rows
            let range = RangeKey::from(&(
                Bound::Included(vec1![DfValue::from(180)]),
                Bound::Included(vec1![DfValue::from(1010)]),
            ));
            match state.lookup_range(&[1], &range) {
                RangeLookupResult::Some(rows) => assert_eq!(rows.len(), 4),
                _ => panic!("Cross-boundary range should succeed"),
            }
        }
    }

    // =========================================================================
    // cleanup_pending_indices_tests
    // =========================================================================

    mod cleanup_pending_indices_tests {
        use common::IndexType;
        use readyset_client::internal::Index;

        use super::{get_pending_build, insert, setup_persistent, TestHarness};
        use crate::State;

        #[test]
        fn cleanup_pending_indices_drops_cfs_and_clears_marker() {
            let h = TestHarness::new("cleanup_drops_cfs", 5);

            let ctx = h.state.create_index_build_context();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let pending_build = ctx.scan_snapshot(&indices).expect("scan should succeed");

            {
                let db = h.state.db.db();
                assert!(get_pending_build(db).is_some());
                assert!(db
                    .cf_handle(&pending_build.pending_indices[0].column_family)
                    .is_some());
            }

            ctx.cleanup_pending_indices(&pending_build.pending_indices);

            {
                let db = h.state.db.db();
                assert!(get_pending_build(db).is_none());
                assert!(db
                    .cf_handle(&pending_build.pending_indices[0].column_family)
                    .is_none());
            }

            h.assert_lookup_count(0, 2.into(), 1);
        }

        #[test]
        fn prepare_failure_cleans_up_partial_cfs() {
            let mut state = setup_persistent("prepare_partial", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            for i in 0..3 {
                insert(
                    &mut state,
                    vec![
                        i.into(),
                        format!("a_{}", i).into(),
                        format!("b_{}", i).into(),
                    ],
                );
            }

            // Pre-create CF "2" to cause collision on second index
            {
                let db = state.db.db();
                db.create_cf("2", &rocksdb::Options::default()).unwrap();
            }

            let strict = vec![
                (Index::new(IndexType::HashMap, vec![1]), None),
                (Index::new(IndexType::HashMap, vec![2]), None),
            ];
            let indices = state
                .prepare_indices_for_build(strict, vec![])
                .unwrap()
                .expect("Should have indices");

            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();
            ctx.build_indices(indices)
                .expect_err("build should fail due to CF collision");

            // CF "1" (partially created then rolled back) should not exist
            assert!(state.db.db().cf_handle("1").is_none());
        }
    }

    // =========================================================================
    // shutdown_tests
    // =========================================================================

    mod shutdown_tests {
        use std::time::Duration;

        use common::IndexType;
        use readyset_client::internal::Index;

        use super::{insert, setup_persistent, TestHarness};
        use crate::{IndexBuildStatus, State};

        #[test]
        fn completion_channel_unblocks_on_drop() {
            let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);

            let handle = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                drop(tx);
            });

            match rx.recv_timeout(Duration::from_secs(2)) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    panic!("recv should unblock after sender is dropped");
                }
            }
            handle.join().unwrap();
        }

        #[test]
        fn shutdown_cancels_build_indices() {
            let h = TestHarness::new("shutdown_cancels", 1000);
            let state = h.state;

            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();

            // Request shutdown before build starts
            state
                .shutdown_requested
                .store(true, std::sync::atomic::Ordering::Release);

            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];
            let result = ctx.build_indices(indices);

            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("shutdown"),
                "error should mention shutdown: {err_msg}"
            );
            assert_eq!(state.index_build_status(), IndexBuildStatus::Failed);

            // Completion channel should be disconnected (guard dropped)
            let rx = state.build_completion_rx.lock().expect("poisoned").take();
            if let Some(rx) = rx {
                assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
            }
        }

        #[test]
        fn shutdown_during_background_build() {
            let mut state = setup_persistent("shutdown_bg_build", None);
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            for i in 0..5000 {
                insert(&mut state, vec![i.into(), format!("val_{i}").into()]);
            }

            state.mark_index_build_in_progress().unwrap();
            let ctx = state.create_index_build_context();
            let shutdown = state.shutdown_requested.clone();
            let completion_rx = state.build_completion_rx.clone();
            let indices = vec![(Index::new(IndexType::HashMap, vec![1]), false)];

            let handle = std::thread::spawn(move || ctx.build_indices(indices));

            std::thread::sleep(Duration::from_millis(10));
            shutdown.store(true, std::sync::atomic::Ordering::Release);

            // Wait for build to finish via completion channel
            if let Some(rx) = completion_rx.lock().expect("poisoned").take() {
                match rx.recv_timeout(Duration::from_secs(10)) {
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        panic!("build should complete within timeout");
                    }
                }
            }

            let result = handle.join().expect("thread should not panic");
            match result {
                Ok(()) => assert_eq!(state.index_build_status(), IndexBuildStatus::Succeeded),
                Err(e) => {
                    assert!(e.to_string().contains("shutdown"));
                    assert_eq!(state.index_build_status(), IndexBuildStatus::Failed);
                }
            }
        }
    }

    // =========================================================================
    // prop_tests -- property-based tests
    // =========================================================================

    mod prop_tests {
        use proptest::prelude::*;
        use rocksdb::WriteBatchIteratorCf;

        use crate::persistent_state::index_build::{WalOperation, WalOperationCollector};

        proptest! {
            /// WalOperationCollector filtering: for arbitrary sequences of
            /// (cf_id, op_type) operations, verify the collector keeps exactly
            /// target-CF ops in order.
            #[test]
            fn wal_collector_filtering(
                target_cf in 0u32..10,
                ops in prop::collection::vec((0u32..10, prop::bool::ANY), 0..100)
            ) {
                let mut collector = WalOperationCollector::new(target_cf);

                let mut expected_ops: Vec<(bool, Vec<u8>)> = Vec::new(); // (is_put, key)

                for (i, &(cf_id, is_put)) in ops.iter().enumerate() {
                    let key = format!("key_{}", i);
                    if is_put {
                        collector.put_cf(cf_id, key.as_bytes(), b"val");
                        if cf_id == target_cf {
                            expected_ops.push((true, key.into_bytes()));
                        }
                    } else {
                        collector.delete_cf(cf_id, key.as_bytes());
                        if cf_id == target_cf {
                            expected_ops.push((false, key.into_bytes()));
                        }
                    }
                }

                prop_assert_eq!(
                    collector.operations.len(),
                    expected_ops.len(),
                    "Collector should have exactly {} ops",
                    expected_ops.len()
                );

                for (i, (expected, actual)) in
                    expected_ops.iter().zip(collector.operations.iter()).enumerate()
                {
                    match (expected, actual) {
                        ((true, key), WalOperation::Put { key: actual_key, .. }) => {
                            prop_assert_eq!(
                                key, actual_key,
                                "Put key mismatch at index {}",
                                i
                            );
                        }
                        ((false, key), WalOperation::Delete { key: actual_key }) => {
                            prop_assert_eq!(
                                key, actual_key,
                                "Delete key mismatch at index {}",
                                i
                            );
                        }
                        _ => {
                            prop_assert!(false, "Op type mismatch at index {}", i);
                        }
                    }
                }
            }
        }
    }
}
