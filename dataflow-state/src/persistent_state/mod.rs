//! Node state that's persisted to disk
//!
//! The [`PersistedState`] struct is an implementation of [`State`] that stores rows (currently only
//! for base tables) in [RocksDB], an on-disk key-value store. The data is stored in
//! [indices](PersistentState::indices) - each lookup index stores the copies of all the rows in the
//! database.
//!
//! [RocksDB]: https://rocksdb.org/
//!
//! # Internals
//!
//! ## Metadata
//!
//! We serialize metadata information about the db, including the replication offset (see
//! "Replication Offsets" below) and which indices exist, as a single [`PersistentMeta`] data
//! structure, serialized to the [default column family] under the [`META_KEY`] key.
//!
//! [default column family]: DEFAULT_CF
//!
//! ## Indices
//!
//! Each lookup index is stored in a separate [`ColumnFamily`], which is configured based on the
//! parameters of that [`Index`], to optimize for the types of queries we want to support given the
//! [`IndexType`]:
//!
//! * For [`HashMap`] indices, we optimize for point lookups (for unique indices) and in-prefix
//!   scans (for non-unique indices), at the cost of not allowing cross-prefix range scans.
//! * For [`BTreeMap`] indices, we configure a custom key comparator to compare the keys via
//!   deserializing to `DfValue` rather than RocksDB's default lexicographic bytewise ordering,
//!   which allows us to do queries across ranges covering multiple keys. To avoid having to encode
//!   the length of the index (really the enum variant tag for [`PointKey`]) in the key itself, this
//!   custom key comparator is built dynamically based on the number of columns in the index, up to
//!   a maximum of 6 (since past that point we use [`PointKey::Multi`]). Note that since we
//!   currently don't have the ability to do zero-copy deserialization of [`DfValue`], deserializing
//!   for the custom comparator currently requires copying any string values just to compare them.
//!   If we are able to make [`DfValue`] enable zero-copy deserialization (by adding a lifetime
//!   parameter) this would likely speed up rather significantly.
//!
//! Since RocksDB requires that we always provide the same set of options (including the name of the
//! custom comparator, if any) when re-opening column families, we have to be careful to always
//! write information about new indices we're creating to the [`PersistentMeta`] *before* we
//! actually create the column family.
//!
//! For each key that we know to be unique we simply store the serialized representation of the key
//! as `(serialized_key_len || key)`, with the value stored being the serialized row.  For keys that
//! are not unique, we either append (epoch, seq) for the primary index, or the primary key itself
//! if the index is a secondary index and the primary key is unique.
//!
//! The data is only stored in the primary index (index 0), while all other indices only store the
//! primary key for each row, and require an additional lookup into the primary index.
//!
//! [`ColumnFamily`]: https://github.com/facebook/rocksdb/wiki/Column-Families
//! [`HashMap`]: IndexType::HashMap
//! [`BTreeMap`]: IndexType::BTreeMap
//!
//! # Replication Offsets
//!
//! When running in a read-replica configuration, where a thread is run as part of the controller
//! that reads the replication log from the underlying database, we need to persist the *offset* in
//! that replication log of the last record that we have successfully applied. To maintain
//! atomicity, these offsets are stored inside of rocksdb as part of the persisted
//! [`PersistentMeta`], and updated as part of every write.
mod handle;
mod metrics;
mod recorded;

use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::{self, Read};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::{fmt, fs, mem};

use bincode::Options;
use clap::{Args, ValueEnum};
use common::{IndexType, Record, Records, SizeOf, Tag};
pub use handle::PersistentStateHandle;
use handle::{PersistentStateReadGuard, PersistentStateWriteGuard};
use rand::Rng;
use readyset_alloc::thread::StdThreadBuildWrapper;
use readyset_client::debug::info::KeyCount;
use readyset_client::internal::Index;
use readyset_client::{KeyComparison, SqlIdentifier};
use readyset_data::{Bound, BoundedRange, DfValue};
use readyset_errors::{internal_err, invariant, ReadySetError, ReadySetResult};
use replication_offset::ReplicationOffset;
use rocksdb::{
    self, BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, CompactOptions, IteratorMode,
    SliceTransform, WriteBatch, DB,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::{Builder, TempDir};
use test_strategy::Arbitrary;
use thiserror::Error;
use tracing::{debug, error, info, info_span, instrument, trace, warn};

use crate::persistent_state::metrics::{MetricsReporter, MetricsReporterStop};
use crate::{
    EvictKeysResult, EvictRandomResult, LookupResult, PersistencePoint, PointKey, RangeKey,
    RangeLookupResult, RecordResult, State,
};

// Incremented on each PersistentState initialization so that IndexSeq
// can be used to create unique identifiers for rows.
type IndexEpoch = u64;

// Monotonically increasing sequence number since last IndexEpoch used to uniquely identify a row.
type IndexSeq = u64;

// RocksDB key used for storing meta information (like indices).
const META_KEY: &[u8] = b"meta";

// A default column family is always created, so we'll make use of that for meta information.
// The indices themselves are stored in a column family each, with their position in
// PersistentState::indices as name.
const DEFAULT_CF: &str = "default";

// The column family for the primary key. It is always zero, because it is always the first index.
const PK_CF: &str = "0";

// The subdirectory name where temporary, working files will be written.
const WORKING_DIR: &str = "readyset.tmp";

// Maximum rows per WriteBatch when building new indices for existing rows.
const INDEX_BATCH_SIZE: usize = 10_000;

// Constants for configurable RocksDB settings
const DEFAULT_WRITE_BUFFER_SIZE: usize = 32 * 1024 * 1024; // 32MB
const DEFAULT_DB_WRITE_BUFFER_SIZE: usize = 128 * 1024 * 1024; // 128MB
const DEFAULT_BLOCK_SIZE: usize = 32 * 1024; // 32KB

/// Configuration options for RocksDB instances
#[derive(Args, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RocksDbOptions {
    /// Write buffer size in RocksDB
    #[arg(
        long = "rocksdb-write-buffer-size",
        default_value_t = DEFAULT_WRITE_BUFFER_SIZE,
        env = "ROCKSDB_WRITE_BUFFER_SIZE",
        hide = true
    )]
    pub write_buffer_size: usize,

    /// DB write buffer size in RocksDB
    #[arg(
        long = "rocksdb-db-write-buffer-size",
        default_value_t = DEFAULT_DB_WRITE_BUFFER_SIZE,
        env = "ROCKSDB_DB_WRITE_BUFFER_SIZE",
        hide = true
    )]
    pub db_write_buffer_size: usize,

    /// Block size in RocksDB
    #[arg(
        long = "rocksdb-block-size",
        default_value_t = DEFAULT_BLOCK_SIZE,
        env = "ROCKSDB_BLOCK_SIZE",
        hide = true
    )]
    pub block_size: usize,
}

impl Default for RocksDbOptions {
    fn default() -> Self {
        Self {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            db_write_buffer_size: DEFAULT_DB_WRITE_BUFFER_SIZE,
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}

/// Delete any working/temp files from the last process run. Normally, those files
/// will be cleaned up on process exit, but if readyset crashes or fails, delete them
/// to prevent any further disk use leakage. This function should be executed once,
/// preferably at startup.
pub fn clean_working_dir(params: &PersistenceParameters) -> Result<()> {
    let working_dir = params.working_dir_base();
    if working_dir.is_dir() {
        debug!("deleting any prior working files from {:?}", &working_dir);
        fs::remove_dir_all(working_dir)?
    }

    Ok(())
}

/// Load the metadata from the database, stored in the `DEFAULT_CF` column family under the
/// `META_KEY`
fn get_meta(db: &DB) -> Result<PersistentMeta<'static>> {
    Ok(db
        .get_pinned(META_KEY)?
        .and_then(|data| {
            serde_json::from_slice(&data)
                .map_err(|error| {
                    error!(
                        %error,
                        "Failed to deserialize metadata from RocksDB, marking table as empty"
                    );
                })
                .ok()
        })
        .unwrap_or_default())
}

/// Abstraction over writing to different kinds of rocksdb dbs.
///
/// This trait is (consciously) incomplete - if necessary, a more complete version including
/// *put_cf* etc could be put inside a utility module somewhere
trait Put: Sized {
    /// Write a key/value pair
    ///
    /// This method is prefixed with "do" so that it doesn't conflict with the `put` method on both
    /// [`DB`] and [`rocksdb::WriteBatch`]
    fn do_put<K, V>(self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    fn save_meta(self, meta: &PersistentMeta) {
        self.do_put(META_KEY, serde_json::to_string(meta).unwrap());
    }
}

impl Put for &DB {
    fn do_put<K, V>(self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put(key, value).unwrap()
    }
}

impl Put for &mut rocksdb::WriteBatch {
    fn do_put<K, V>(self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put(key, value)
    }
}

/// Load the saved [`PersistentMeta`] from the database, increment its
/// [epoch](PersistentMeta::epoch) by one, and return it
fn increment_epoch(db: &DB) -> Result<PersistentMeta<'static>> {
    let mut meta = get_meta(db)?;
    meta.epoch += 1;
    db.save_meta(&meta);
    Ok(meta)
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum SnapshotMode {
    SnapshotModeEnabled,
    SnapshotModeDisabled,
}

impl SnapshotMode {
    pub fn is_enabled(&self) -> bool {
        matches!(self, SnapshotMode::SnapshotModeEnabled)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PersistenceType {
    BaseTable,
    FullMaterialization,
}

/// Indicates to what degree updates should be persisted.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, ValueEnum)]
pub enum DurabilityMode {
    /// Don't do any durability
    #[value(alias("memory"))]
    MemoryOnly,
    /// Delete any log files on exit. Useful mainly for tests.
    #[value(alias("ephemeral"))]
    DeleteOnExit,
    /// Persist updates to disk, and don't delete them later.
    #[value(alias("persistent"))]
    Permanent,
}

#[derive(Debug, Error)]
#[error("Invalid durability mode; expected one of persistent, ephemeral, or memory")]
pub struct InvalidDurabilityMode;

impl FromStr for DurabilityMode {
    type Err = InvalidDurabilityMode;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "persistent" => Ok(Self::Permanent),
            "ephemeral" => Ok(Self::DeleteOnExit),
            "memory" => Ok(Self::MemoryOnly),
            _ => Err(InvalidDurabilityMode),
        }
    }
}

/// Parameters to control the operation of GroupCommitQueue.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistenceParameters {
    /// Whether the output files should be deleted when the GroupCommitQueue is dropped.
    pub mode: DurabilityMode,
    /// Filename prefix for the RocksDB database folder
    pub db_filename_prefix: String,
    /// An optional path to a directory where to store the DB files, if None will be stored in the
    /// current working directory
    pub storage_dir: Option<PathBuf>,
    /// An optional path to a directory where the working/temporary DB files can be stored. If
    /// None, the files will be stored in a subdirectory under `storage_dir`.
    pub working_temp_dir: Option<PathBuf>,
    /// The interval on which the RocksDB WAL will be flushed and synced to disk. If this value is
    /// set to 0, the WAL will be flushed and synced to disk with every write
    #[serde(default)]
    pub wal_flush_interval_seconds: u64,
    #[serde(default)]
    pub rocksdb_options: RocksDbOptions,
}

impl Default for PersistenceParameters {
    fn default() -> Self {
        Self {
            mode: DurabilityMode::MemoryOnly,
            db_filename_prefix: String::from("readyset"),
            storage_dir: None,
            working_temp_dir: None,
            wal_flush_interval_seconds: 0,
            rocksdb_options: RocksDbOptions::default(),
        }
    }
}

impl PersistenceParameters {
    /// Parameters to control the persistence mode, and parameters related to persistence.
    ///
    /// Three modes are available:
    ///
    ///  1. `DurabilityMode::Permanent`: all writes to base nodes should be written to disk.
    ///  2. `DurabilityMode::DeleteOnExit`: all writes to base nodes are written to disk, but the
    ///     persistent files are deleted once the `ReadySetHandle` is dropped. Useful for tests.
    ///  3. `DurabilityMode::MemoryOnly`: no writes to disk, store all writes in memory. Useful for
    ///     baseline numbers.
    pub fn new(
        mode: DurabilityMode,
        db_filename_prefix: Option<String>,
        storage_dir: Option<PathBuf>,
        working_temp_dir: Option<PathBuf>,
        wal_flush_interval_seconds: u64,
        rocksdb_options: RocksDbOptions,
    ) -> Self {
        // NOTE(fran): DO NOT impose a particular format on `db_filename_prefix`. If you need to,
        // modify it before use, but do not make assertions on it. The reason being, we use
        // ReadySet's deployment name as db filename prefix (which makes sense), and we don't
        // want to impose any restriction on it (since sometimes we automate the deployments
        // and deployment name generation).
        let db_filename_prefix = db_filename_prefix.unwrap_or_else(|| String::from("readyset"));

        Self {
            mode,
            db_filename_prefix,
            storage_dir,
            working_temp_dir,
            wal_flush_interval_seconds,
            rocksdb_options,
        }
    }

    fn storage_dir(&self) -> PathBuf {
        self.storage_dir.clone().unwrap_or_else(|| ".".into())
    }

    fn working_dir_base(&self) -> PathBuf {
        let base_dir = match &self.working_temp_dir {
            Some(s) => s.clone(),
            None => self.storage_dir(),
        };

        base_dir.join(WORKING_DIR)
    }
}

/// Errors that can occur when creating a new persistent state or opening an existing one.
///
/// This is a distinct enum from [`ReadySetError`] so we can make it include [`rocksdb::Error`]
/// without the `readyset-errors` crate having to depend on `rocksdb`
#[derive(Debug, Error)]
pub enum Error {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("Invalid on-disk DB format")]
    BadDbFormat,

    #[error(
        "Persisted state at {} has serialization version {persisted_version}, which does not match \
         our serialization version {our_version}",
        path.display(),
    )]
    SerdeVersionMismatch {
        path: PathBuf,
        persisted_version: u8,
        our_version: u8,
    },

    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<&Error> for ReadySetError {
    fn from(err: &Error) -> Self {
        internal_err!("{err}")
    }
}

impl From<Error> for ReadySetError {
    fn from(err: Error) -> Self {
        ReadySetError::from(&err)
    }
}

impl Error {
    /// Returns `true` if this error is "permanent", meaning it is not likely to go away if we
    /// delete the DB file and try again
    pub fn is_permanent(&self) -> bool {
        match self {
            Error::RocksDb(e) => !matches!(e.kind(), rocksdb::ErrorKind::Corruption),
            // Could *maybe* try to slice up the IO errors here, but for now it's simpler to just
            // assume all IO errors are permanent
            Error::Io(_) => true,
            Error::BadDbFormat | Error::SerdeVersionMismatch { .. } => false,
        }
    }
}

/// Result type for persistent state
pub type Result<T> = std::result::Result<T, Error>;

/// Data structure used to persist metadata about the [`PersistentState`] to rocksdb
#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistentMeta<'a> {
    /// The version of serialization used to serialize data to this [`PersistentState`]. This is
    /// compared against [`DfValue::SERDE_VERSION`] at startup, and if it's unequal an error will
    /// be returned
    serde_version: u8,

    /// Index information is stored in RocksDB to avoid rebuilding indices on recovery
    indices: Vec<Index>,
    epoch: IndexEpoch,

    /// The latest replication offset that has been written to the base table backed by this
    /// [`PersistentState`]. Corresponds to [`PersistentState::replication_offset`]
    replication_offset: Option<Cow<'a, ReplicationOffset>>,
}

#[derive(Debug, Clone)]
struct PersistentIndex {
    column_family: String,
    index: Index,
    is_unique: bool,
    is_primary: bool,
}

/// Handle to a manual compaction running in the background
struct CompactionThreadHandle {
    handle: Option<JoinHandle<()>>,
}

impl CompactionThreadHandle {
    fn is_finished(&self) -> bool {
        self.handle.as_ref().map_or(true, |h| h.is_finished())
    }

    fn join(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

impl Drop for CompactionThreadHandle {
    fn drop(&mut self) {
        self.join();
    }
}

/// PersistentState stores data in RocksDB.
pub struct PersistentState {
    name: SqlIdentifier,
    default_options: rocksdb::Options,
    db: PersistentStateHandle,
    // The list of all the indices that are defined as unique in the schema for this table
    unique_keys: Vec<Box<[usize]>>,
    seq: IndexSeq,
    epoch: IndexEpoch,
    // With DurabilityMode::DeleteOnExit,
    // RocksDB files are stored in a temporary directory.
    _tmpdir: Option<TempDir>,
    /// When set to true [`SnapshotMode::SnapshotModeEnabled`] compaction will be disabled and
    /// writes will bypass WAL and fsync
    snapshot_mode: SnapshotMode,
    compaction_threads: Vec<CompactionThreadHandle>,
    wal_flush_thread_handle: Option<(mpsc::Sender<()>, JoinHandle<()>)>,

    persistence_type: PersistenceType,
    replay_done: bool,
    metrics_stop: Option<MetricsReporterStop>,
    persistence_parameters: PersistenceParameters,
}

/// Things that are shared between read handles and the state itself, that can be locked under a
/// single lock
struct SharedState {
    /// The latest replication offset that has been written to the base table backed by this
    /// [`PersistentState`]
    replication_offset: Option<ReplicationOffset>,
    /// The current state of the RocksDB WAL as it relates to flushing and persisting data to disk
    wal_state: WalState,
    /// The last error that occurred in the WAL flush thread
    last_wal_flush_error: Option<Error>,
    /// The lookup indices stored for this table. The first element is always considered the
    /// primary index
    indices: Vec<PersistentIndex>,
}

impl SharedState {
    /// Returns the PersistentIndex for the given index, panicking if it doesn't exist
    // TODO(aspen): This should actually be an error, since it can be triggered by bad requests
    fn index(&self, index_type: IndexType, columns: &[usize]) -> &PersistentIndex {
        self.indices
            .iter()
            .find(|index| index.index.index_type == index_type && index.index.columns == columns)
            .unwrap_or_else(|| {
                panic!(
                    "lookup on non-indexed column set {:?}({:?})",
                    index_type, columns
                )
            })
    }
}

#[derive(Debug, Clone)]
enum WalState {
    FlushedAndPersisted,
    FlushedAndUnpersisted {
        /// The replication offset up to which this state has been persisted to disk
        persisted_up_to: ReplicationOffset,
    },
    Unflushed {
        /// The replication offset up to which this state has been persisted to disk
        persisted_up_to: ReplicationOffset,
    },
}

struct WalFlusher {
    rx: mpsc::Receiver<()>,
    state_handle: PersistentStateHandle,
    table: SqlIdentifier,
    flush_interval: Duration,
}

impl WalFlusher {
    fn run(self) {
        // Sleep for a random number of seconds between 1 and 10 to introduce jitter to
        // stagger flushes across different base tables
        let jitter = Duration::from_secs_f64(
            rand::thread_rng().gen_range(0.0..self.flush_interval.as_secs_f64()),
        );

        // We use recv_timeout for interruptible sleep. If recv_timeout() returns `Ok`, it means
        // we've received the shutdown signal; if it returns `Err`, we haven't received a shutdown
        // signal in the given period, so we allow the background thread to persist
        if self.rx.recv_timeout(jitter).is_ok() {
            return;
        }

        loop {
            match self.rx.recv_timeout(self.flush_interval) {
                Err(RecvTimeoutError::Timeout) => {
                    let wal_state = self.state_handle.inner().shared_state.wal_state.clone();

                    // We don't need to check `last_wal_flush_error` here because we just want to
                    // keep retrying based on our current state. If there's further action to be
                    // taken, the controller will orchestrate it
                    match wal_state {
                        WalState::Unflushed { persisted_up_to } => {
                            if self.flush_wal(persisted_up_to) {
                                self.sync_wal();
                            }
                        }
                        WalState::FlushedAndUnpersisted { .. } => {
                            self.sync_wal();
                        }
                        WalState::FlushedAndPersisted => {}
                    }
                }
                Err(RecvTimeoutError::Disconnected) | Ok(()) => return,
            }
        }
    }

    /// Returns true if the flush succeeds and false otherwise. If the flush fails, the
    /// corresponding error is stored in `SharedState.last_wal_flush_error`.
    fn flush_wal(&self, persisted_up_to: ReplicationOffset) -> bool {
        trace!(%self.table, "flushing WAL");

        // Writes to persistent state don't require a write lock since they only need immutable
        // access to the DB handle; however, to keep things clean, we acquire a write lock to
        // prevent another thread from changing the WAL state or writing a new replication offset to
        // the shared state to ensure that we flush the WAL and update the WAL state atomically.
        let mut inner = self.state_handle.inner_mut();

        // Flushing the WAL blocks other writes to RocksDB, but this operation should be relatively
        // quick given that we aren't writing any bytes to disk. The bottleneck here is probably
        // the fwrite system call
        //
        // If a flush fails, it's possible that the low watermark of our unflushed data has
        // increased if *some* of the data was flushed. Regardless, we have no way of knowing what
        // data *was* successfully flushed, so we keep our state as-is
        if let Err(error) = inner.db.flush_wal(false) {
            // If we failed to flush, we set the error in `SharedState` so the replicator sees it
            // and waits till the next iteration of the loop to retry
            error!(%error, %self.table, "failed to flush WAL");
            inner.shared_state.last_wal_flush_error = Some(error.into());

            false
        } else {
            inner.shared_state.wal_state = WalState::FlushedAndUnpersisted { persisted_up_to };

            true
        }
    }

    fn sync_wal(&self) {
        trace!(%self.table, "syncing WAL");

        let res = self.state_handle.db().flush_wal(true);

        // If a sync fails, it's possible that *some* but not *all* of the flushed but unsynced data
        // has been synced to disk. Regardless, we have no way of knowing what data *was*
        // successfully synced, so we keep our state as-is. We'll know we're caught up when a future
        // sync succeeds
        if let Err(error) = res {
            // If we failed to sync, we set the error in `SharedState` so the replicator sees it and
            // wait till the next iteration of the loop to retry
            error!(%error, %self.table, "failed to sync WAL");

            self.state_handle
                .inner_mut()
                .shared_state
                .last_wal_flush_error = Some(error.into());
        } else {
            let mut inner = self.state_handle.inner_mut();

            match inner.shared_state.wal_state {
                // No data has been written to this state since the sync began, so we can change our
                // WAL state to `FlushedAndPersisted`
                WalState::FlushedAndUnpersisted { .. } => {
                    inner.shared_state.wal_state = WalState::FlushedAndPersisted;
                }
                // If our state changed to `Unflushed` while we were syncing, we don't want to do
                // anything, because there's new data that needs to be flushed
                WalState::Unflushed { .. } => {}
                // If we just synced, our previous state should not have been `FlushedAndPersisted`.
                // If it is, there's a bug somewhere
                WalState::FlushedAndPersisted => unreachable!(
                    "another thread should never transition the WAL state to `FlushedAndPersisted`"
                ),
            }
        }
    }
}

impl fmt::Debug for PersistentState {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("PersistentState")
            .field("name", &self.name)
            .field("unique_keys", &self.unique_keys)
            .field("seq", &self.seq)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

impl PersistentMeta<'_> {
    fn get_indices(&self, unique_keys: &[Box<[usize]>]) -> Vec<PersistentIndex> {
        self.indices
            .iter()
            .enumerate()
            .map(|(i, index)| PersistentIndex {
                is_unique: check_if_index_is_unique(unique_keys, &index.columns),
                column_family: i.to_string(),
                index: index.clone(),
                is_primary: i == 0,
            })
            .collect()
    }
}

impl State for PersistentState {
    fn process_records(
        &mut self,
        records: &mut Records,
        partial_tag: Option<Tag>,
        replication_offset: Option<ReplicationOffset>,
    ) -> ReadySetResult<()> {
        // `partial_tag` should only be present when writing to a non-base table
        // as writing to a base table isn't part of a replay path
        if self.persistence_type == PersistenceType::BaseTable {
            invariant!(partial_tag.is_none(), "PersistentState can't be partial");
        }

        // Streamline the records by eliminating pairs that would negate each other.
        records.remove_deleted();

        if records.len() == 0 && replication_offset.is_none() {
            return Ok(());
        }

        // Don't process records if the replication offset is less than our current.
        if let (Some(new), Some(current)) = (&replication_offset, &self.db.replication_offset) {
            if new <= current {
                warn!("Dropping writes we have already processed");
                return Ok(());
            }
        }

        let mut batch = WriteBatch::default();
        for r in records.iter() {
            match *r {
                Record::Positive(ref r) => {
                    self.insert(&mut batch, r)?;
                }
                Record::Negative(ref r) => {
                    self.remove(&mut batch, r)?;
                }
            }
        }
        self.write_to_db(batch, &replication_offset)?;
        Ok(())
    }

    fn replication_offset(&self) -> Option<&ReplicationOffset> {
        self.db.replication_offset.as_ref()
    }

    fn persisted_up_to(&self) -> ReadySetResult<PersistencePoint> {
        let mut inner = self.db.inner_mut();

        // We clear out the error here (if one exists) since we're reporting it upwards
        if let Some(error) = &inner.shared_state.last_wal_flush_error.take() {
            Err(error.into())
        } else {
            match &inner.shared_state.wal_state {
                WalState::FlushedAndPersisted => Ok(PersistencePoint::Persisted),
                WalState::FlushedAndUnpersisted { persisted_up_to }
                | WalState::Unflushed { persisted_up_to } => {
                    Ok(PersistencePoint::UpTo(persisted_up_to.clone()))
                }
            }
        }
    }

    fn lookup(&self, columns: &[usize], key: &PointKey) -> LookupResult {
        self.db.lookup(columns, key)
    }

    fn lookup_range<'a>(&'a self, columns: &[usize], key: &RangeKey) -> RangeLookupResult<'a> {
        self.db.lookup_range(columns, key)
    }

    fn as_persistent(&self) -> Option<&PersistentState> {
        Some(self)
    }

    fn as_persistent_mut(&mut self) -> Option<&mut PersistentState> {
        Some(self)
    }

    /// Add a new index to the table, the first index we add will contain the data
    /// each additional index we add, will contain pointers to the primary index
    /// Panics if partial is Some
    fn add_index(&mut self, index: Index, tags: Option<Vec<Tag>>) {
        self.add_index_multi(vec![(index, tags)], vec![]);
    }

    fn add_index_multi(&mut self, strict: Vec<(Index, Option<Vec<Tag>>)>, weak: Vec<Index>) {
        let mut indices = Vec::new();
        let mut is_unique = Vec::new();
        let mut inner = self.db.inner_mut();

        for (index, tags) in strict
            .into_iter()
            .chain(weak.into_iter().map(|x| (x, None)))
        {
            assert!(tags.is_none(), "Base tables can't be partial");
            let existing = inner
                .shared_state
                .indices
                .iter()
                .any(|pi| pi.index == index);
            if existing {
                continue;
            }

            let uniq = check_if_index_is_unique(&self.unique_keys, &index.columns);
            if inner.shared_state.indices.is_empty() {
                self.add_primary_index(&mut inner, &index.columns, uniq)
                    .unwrap();
                // Primary indices can only be HashMaps, so if this is our first index and it's
                // *not* a HashMap index, add another secondary index of the correct index type
                if index.index_type == IndexType::HashMap {
                    continue;
                }
            }
            indices.push(index);
            is_unique.push(uniq);
        }

        if indices.is_empty() {
            return;
        }

        let threads = self.add_secondary(inner, &indices, &is_unique);
        for t in threads {
            self.push_compaction_thread(t);
        }
    }

    fn all_records(&self) -> crate::AllRecords {
        self.read_handle().all_records()
    }

    /// Returns a *row* count estimate from RocksDB (not a key count as the function name would
    /// suggest), since getting a key count could be quite expensive, and we care less about the
    /// key count of persistent nodes anyway.
    fn key_count(&self) -> KeyCount {
        KeyCount::EstimatedRowCount(self.row_count())
    }

    /// Returns a row count estimate from RocksDB.
    fn row_count(&self) -> usize {
        self.db.row_count()
    }

    fn is_useful(&self) -> bool {
        self.db.is_useful()
    }

    fn is_partial(&self) -> bool {
        false
    }

    fn replay_done(&self) -> bool {
        match self.persistence_type {
            // Base tables by definition have always been "replayed to"
            PersistenceType::BaseTable => true,
            PersistenceType::FullMaterialization => self.replay_done,
        }
    }

    /// Panics if called
    #[allow(clippy::unreachable)] // this should never happen!
    fn mark_filled(&mut self, _: KeyComparison, _: Tag) {
        unreachable!("PersistentState can't be partial")
    }

    /// Panics if called
    #[allow(clippy::unreachable)] // this should never happen!
    fn mark_hole(&mut self, _: &KeyComparison, _: Tag) {
        unreachable!("PersistentState can't be partial")
    }

    /// Panics if called
    #[allow(clippy::unreachable)] // this should never happen!
    fn evict_bytes(&mut self, _: usize) -> Option<super::EvictBytesResult> {
        unreachable!("can't evict keys from PersistentState")
    }

    /// Panics if called
    #[allow(clippy::unreachable)] // this should never happen!
    fn evict_keys(&mut self, _: Tag, _: &[KeyComparison]) -> Option<EvictKeysResult> {
        unreachable!("can't evict keys from PersistentState")
    }

    /// Panics if called
    #[allow(clippy::unreachable)] // this should never happen!
    fn evict_random<R: rand::Rng>(&mut self, _: Tag, _: &mut R) -> Option<EvictRandomResult> {
        unreachable!("can't evict keys from PersistentState")
    }

    /// Panics if called
    #[allow(clippy::unreachable)] // this should never happen!
    fn clear(&mut self) {
        unreachable!("can't clear PersistentState")
    }

    fn add_weak_index(&mut self, index: Index) {
        self.add_index(index, None);
    }

    fn lookup_weak<'a>(&'a self, columns: &[usize], key: &PointKey) -> Option<RecordResult<'a>> {
        self.db.lookup_weak(columns, key)
    }

    fn shut_down(&mut self) -> ReadySetResult<()> {
        trace!("PersistentState received shutdown, stopping the WAL");
        self.shut_down_wal()

        // DurabilityMode::DeleteOnExit will delete all data when the TempFile instance
        // is dropped. Thus, we don't need to do anything special here for that data.
    }

    fn tear_down(mut self) -> ReadySetResult<()> {
        let _ = &self.shut_down_wal()?;

        let temp_dir = self._tmpdir.take();
        let full_path = self.db.inner().db.path().to_path_buf();

        // We have to make the drop here so that rocksdb gets closed and frees
        // the file descriptors, so that we can remove the directory.
        // We can't implement this logic by implementing the `Drop` trait, because
        // otherwise we would be dropping rocksdb twice, which will make the whole thing
        // panic.
        // DurabilityMode::DeleteOnExit will delete all data when the TempFile instance
        // is dropped. Thus, we don't need to do anything special here for that data.
        drop(self);
        match temp_dir {
            Some(_) => Ok(()),
            None => fs::remove_dir_all(full_path).map_err(|e| {
                ReadySetError::IOError(format!("Failed to remove rocksdb directory: {}", e))
            }),
        }
    }
}

impl Drop for PersistentState {
    fn drop(&mut self) {
        // We must stop the metrics thread before dropping the db to avoid a crash.
        self.shut_down_metrics_reporting();
    }
}

impl State for PersistentStateHandle {
    fn add_index(&mut self, _: Index, _: Option<Vec<Tag>>) {
        // Do nothing, as all keys are propagated via the [`PersistentState::add_index`]
    }

    fn add_weak_index(&mut self, _: Index) {
        // Add key does nothing, as all keys are propagated via the [`PersistentState::add_index`]
    }

    fn process_records(
        &mut self,
        _: &mut Records,
        _: Option<Tag>,
        replication_offset: Option<ReplicationOffset>,
    ) -> ReadySetResult<()> {
        // We ignore all the records, as record processing is handled by the [`PersistentState`], we
        // only read records. However we must know that we are up to date when reading from the base
        // table, and have to compare our replication offset to that of the table.
        if let Some(replication_offset) = replication_offset {
            self.replication_offset = Some(replication_offset);
        }

        Ok(())
    }

    fn is_useful(&self) -> bool {
        !self.inner().shared_state.indices.is_empty()
    }

    fn is_partial(&self) -> bool {
        false
    }

    fn replay_done(&self) -> bool {
        // Base tables by definition have always been "replayed to"
        true
    }

    fn replication_offset(&self) -> Option<&ReplicationOffset> {
        None
    }

    fn persisted_up_to(&self) -> ReadySetResult<PersistencePoint> {
        Ok(PersistencePoint::Persisted)
    }

    fn mark_filled(&mut self, _: KeyComparison, _: Tag) {}

    fn mark_hole(&mut self, _: &KeyComparison, _: Tag) {}

    fn lookup(&self, columns: &[usize], key: &PointKey) -> LookupResult {
        match self.do_lookup(columns, key) {
            Some(result) => LookupResult::Some(result.into()),
            None => LookupResult::Missing,
        }
    }

    fn lookup_range<'a>(&'a self, columns: &[usize], key: &RangeKey) -> RangeLookupResult<'a> {
        let inner = self.inner();
        if self.replication_offset < inner.shared_state.replication_offset {
            debug!("Consistency miss in PersistentStateHandle");
            // TODO(vlad): The read handle missed on binlog position, but that doesn't mean we want
            // to replay the entire range, all we want is for something to trigger a
            // replay and a repeat lookup
            return RangeLookupResult::Missing(vec![key.as_bounded_range()]);
        }

        let index = inner.shared_state.index(IndexType::BTreeMap, columns);
        let is_primary = index.is_primary;

        let cf = inner.db.cf_handle(&index.column_family).unwrap();

        let primary_cf = inner
            .db
            .cf_handle(PK_CF)
            .expect("Primary key column family not found");

        let (lower, upper) = serialize_range(key.clone());

        let mut opts = rocksdb::ReadOptions::default();
        let mut inclusive_end = None;

        match upper {
            Bound::Excluded(k) => opts.set_iterate_upper_bound(k),
            Bound::Included(k) => {
                // RocksDB's iterate_upper_bound is exclusive, so after we reach that, we still have
                // to lookup the inclusive bound
                inclusive_end = Some(k.clone());
                opts.set_iterate_upper_bound(k);
            }
        }

        let mut iterator = inner.db.raw_iterator_cf_opt(cf, opts);

        match lower {
            Bound::Included(k) => iterator.seek(k),
            Bound::Excluded(start_key) => {
                iterator.seek(&start_key);
                // The key in the exclusive bound might not actually exist in the db, in which case
                // `seek` brings us to the next key after that. We only want to skip forward as long
                // as the current key has the exact same prefix as our `start_key`.
                while let Some(cur_key) = iterator.key() {
                    if prefix_transform(cur_key) == start_key {
                        iterator.next();
                    } else {
                        break;
                    }
                }
            }
        }

        let mut rows = Vec::new();
        let mut keys: Vec<Box<[u8]>> = Vec::new();

        if is_primary {
            rows.reserve(32);
        } else {
            keys.reserve(32);
        }

        while let Some(value) = iterator.value() {
            if is_primary {
                // If this is the primary CF, the value is already the value we are looking for
                rows.push(deserialize_row(value));
            } else {
                // Otherwise this is the key to lookup the value in the primary CF
                keys.push(value.into());

                if keys.len() == 128 {
                    let primary_rows = inner.db.batched_multi_get_cf(primary_cf, &keys, false);
                    rows.extend(primary_rows.into_iter().map(|r| {
                        deserialize_row(r.expect("can't error on known primary key").unwrap())
                    }));
                    keys.clear();
                }
            }
            iterator.next();
        }

        // After the iterator is done, still have to fetch the rows for the inclusive upper bound
        if let Some(end_key) = inclusive_end {
            iterator = inner.db.raw_iterator_cf(cf);
            iterator.seek(&end_key);
            while let Some(cur_key) = iterator.key() {
                if prefix_transform(cur_key) != end_key {
                    break;
                }
                if is_primary {
                    rows.push(deserialize_row(iterator.value().unwrap()));
                } else {
                    keys.push(iterator.value().unwrap().into());
                }
                iterator.next();
            }
        }

        if !keys.is_empty() {
            let primary_rows = inner.db.batched_multi_get_cf(primary_cf, &keys, false);
            rows.extend(
                primary_rows.into_iter().map(|r| {
                    deserialize_row(r.expect("can't error on known primary key").unwrap())
                }),
            );
        }

        RangeLookupResult::Some(RecordResult::Owned(rows))
    }

    fn lookup_weak<'a>(&'a self, columns: &[usize], key: &PointKey) -> Option<RecordResult<'a>> {
        self.lookup(columns, key).records()
    }

    fn key_count(&self) -> KeyCount {
        KeyCount::ExternalMaterialization
    }

    /// Returns a row count estimate from RocksDB.
    fn row_count(&self) -> usize {
        let inner = &self.inner();
        let cf = inner.db.cf_handle(PK_CF).unwrap();
        inner
            .db
            .property_int_value_cf(cf, "rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap() as usize
    }

    fn all_records(&self) -> crate::AllRecords {
        crate::AllRecords::Persistent(AllRecords(self.clone()))
    }

    fn evict_bytes(&mut self, _: usize) -> Option<crate::EvictBytesResult> {
        None
    }

    fn evict_keys(&mut self, _: Tag, _: &[KeyComparison]) -> Option<EvictKeysResult> {
        None
    }

    fn evict_random<R: rand::Rng>(&mut self, _: Tag, _: &mut R) -> Option<EvictRandomResult> {
        None
    }

    fn clear(&mut self) {}

    fn shut_down(&mut self) -> ReadySetResult<()> {
        Ok(())
    }

    fn tear_down(self) -> ReadySetResult<()> {
        Ok(())
    }
}

fn build_key(row: &[DfValue], columns: &[usize]) -> PointKey {
    PointKey::from(columns.iter().map(|i| row[*i].clone()))
}

/// Our RocksDB keys come in three forms, and are encoded as follows:
///
/// * Unique Primary Keys (size, key), where size is the serialized byte size of `key` (used in
///   `prefix_transform`).
///
/// * Non-unique Primary Keys (size, key, epoch, seq), where epoch is incremented on each recover,
///   and seq is a monotonically increasing sequence number that starts at 0 for every new epoch.
///
/// * Secondary Index Keys (size, key, primary_key), where `primary_key` makes sure that each
///   secondary index row is unique.
///
/// `serialize_key` is responsible for serializing the underlying PointKey tuple
/// directly, plus any extra information as described above.
fn serialize_key<K: Serialize, E: Serialize>(k: K, extra: E) -> Vec<u8> {
    let size: u64 = bincode::options().serialized_size(&k).unwrap();
    bincode::options().serialize(&(size, k, extra)).unwrap()
}

fn serialize_range(range: RangeKey) -> BoundedRange<Vec<u8>> {
    let (lower, upper) = range.into_point_keys();
    (
        lower.map(|v| serialize_key(v, ())),
        upper.map(|v| serialize_key(v, ())),
    )
}

fn deserialize_row<T: AsRef<[u8]>>(bytes: T) -> Vec<DfValue> {
    bincode::options()
        .deserialize(bytes.as_ref())
        .expect("Deserializing from rocksdb")
}

/// Build the base set of rocksdb options for persistent state based on the given persistence
/// parameters.
///
/// This will construct the set of options that *all* column families should have regardless of
/// index type.
fn base_options(params: &PersistenceParameters) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_allow_concurrent_memtable_write(false);
    opts.set_enable_pipelined_write(true);

    if params.wal_flush_interval_seconds > 0 {
        opts.set_manual_wal_flush(true);
    }

    let cpus = num_cpus::get() as i32;
    opts.set_max_write_buffer_number(cpus);
    opts.set_max_background_jobs(cpus * 4); // only 1/4 of these write memtables

    opts.set_write_buffer_size(params.rocksdb_options.write_buffer_size);
    opts.set_db_write_buffer_size(params.rocksdb_options.db_write_buffer_size);

    let block_opts = block_based_options(false, params);
    opts.set_block_based_table_factory(&block_opts);

    opts
}

/// Creates a standard set of `BlockBasedOptions`.
fn block_based_options(set_filter: bool, params: &PersistenceParameters) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_size(params.rocksdb_options.block_size);
    block_opts.set_optimize_filters_for_memory(true);

    if set_filter {
        block_opts.set_ribbon_filter(9.9);
    }

    block_opts
}

/// Representation of the set of parameters for an index in persistent state
///
/// This type is constructed either via an [`Index`] (with the `From<&Index>`) impl, directly from
/// an [`IndexType`] and a number of columns with the [`new`] function, or via parsing from a string
/// with the [`FromStr`] impl. It can be used either to construct a set of rocksdb options, with the
/// [`make_rocksdb_options`] function, or to generate the name for a column family, with the
/// [`column_family_name`] function
///
/// [`new`]: IndexType::new
/// [`make_rocksdb_options`]: IndexType::make_rocksdb_options
/// [`column_family_name`]: IndexType::column_family_name
#[derive(Debug, Clone, Copy, PartialEq, Eq, Arbitrary)]
struct IndexParams {
    /// The index type for this index
    index_type: IndexType,
    /// The number of columns that we're indexing on, if that number is between 1 and 6, or None if
    /// the number is greater than 6
    ///
    /// The "6" limit corresponds to the upper limit on the variants of [`PointKey`] and
    /// [`RangeKey`]
    ///
    /// # Invariants
    ///
    /// * If `Some`, this field will never contain zero or a number greater than 6. Enforced at
    ///   construction via [`IndexParams::new`]
    #[strategy(proptest::option::of(1_usize..=6))]
    num_columns: Option<usize>,
}

impl From<&Index> for IndexParams {
    fn from(index: &Index) -> Self {
        Self::new(index.index_type, index.len())
    }
}

impl IndexParams {
    /// Construct a new `IndexParams` with the given index type and number of columns.
    fn new(index_type: IndexType, num_columns: usize) -> Self {
        Self {
            index_type,
            num_columns: Some(num_columns).filter(|n| *n <= 6),
        }
    }

    /// Construct a set of rocksdb Options for column families with this set of params, based on the
    /// given set of `base_options`.
    #[allow(clippy::unreachable)] // Checked at construction
    fn make_rocksdb_options(
        &self,
        base_options: &rocksdb::Options,
        params: &PersistenceParameters,
    ) -> rocksdb::Options {
        let mut opts = base_options.clone();
        match self.index_type {
            // For hash map indices, optimize for point queries and in-prefix range iteration, but
            // don't allow cross-prefix range iteration.
            IndexType::HashMap => {
                let block_opts = block_based_options(true, params);
                opts.set_block_based_table_factory(&block_opts);

                // We're either going to be doing direct point lookups, in the case of unique
                // indexes, or iterating within a range.
                let transform = SliceTransform::create("key", prefix_transform, Some(in_domain));
                opts.set_prefix_extractor(transform);

                // Use a hash linked list since we're doing prefix seeks.
                opts.set_allow_concurrent_memtable_write(false);
                opts.set_memtable_factory(rocksdb::MemtableFactory::HashLinkList {
                    bucket_count: 1_000_000,
                });
            }
            // For "btree" indices, allow full total-order range iteration using the native ordering
            // semantics of DfValue, by configuring a custom comparator based on the number of
            // columns in the index
            IndexType::BTreeMap => match self.num_columns {
                Some(0) => unreachable!("Can't create a column family with 0 columns"),
                Some(1) => opts.set_comparator("compare_keys_1", Box::new(compare_keys_1)),
                Some(2) => opts.set_comparator("compare_keys_2", Box::new(compare_keys_2)),
                Some(3) => opts.set_comparator("compare_keys_3", Box::new(compare_keys_3)),
                Some(4) => opts.set_comparator("compare_keys_4", Box::new(compare_keys_4)),
                Some(5) => opts.set_comparator("compare_keys_5", Box::new(compare_keys_5)),
                Some(6) => opts.set_comparator("compare_keys_6", Box::new(compare_keys_6)),
                _ => opts.set_comparator("compare_keys_multi", Box::new(compare_keys_multi)),
            },
        }

        opts
    }
}

// Getting the current compaction progress is as easy as getting the property value
// for `rocksdb.num-files-at-level<N>` NOT.
// Essentially we have to implement a huge hack here, since the only way I could find
// to get accurate progress stats is from reading the DB LOG directly. This is very
// fragile, as it depends on the LOG format not changing, and if it does the report
// will be less accurate or not work at all. This is however not critical.
fn compaction_progress_watcher(table_name: &str, db: &DB) -> anyhow::Result<impl notify::Watcher> {
    use std::fs::File;
    use std::io::{Seek, SeekFrom};

    use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};

    // We open the LOG file, skip to the end, and begin watching for change events
    // on it in order to get the latest log entries as they come
    let log_path = db.path().join("LOG");
    let (tx, rx) = std::sync::mpsc::channel();

    // note: the Config is ignored in `RecommendedWatcher` :meh:
    let mut log_watcher = RecommendedWatcher::new(tx, Config::default())?;
    let table = table_name.to_owned();
    // Row count, but without a lock
    let pk_cf = db.cf_handle(PK_CF).unwrap();
    let row_count = db
        .property_int_value_cf(pk_cf, "rocksdb.estimate-num-keys")
        .unwrap()
        .unwrap() as usize;
    let mut log_file = File::options().read(true).open(&log_path)?;
    log_file.seek(SeekFrom::End(0))?;

    log_watcher.watch(log_path.as_ref(), RecursiveMode::NonRecursive)?;

    let mut monitor = move || -> anyhow::Result<()> {
        const REPORT_INTERVAL: Duration = Duration::from_secs(1);
        let mut compaction_started = false;
        let mut buf = String::new();
        let mut first_stage_keys = 0;
        let mut second_stage_keys = 0;
        let mut last_report = Instant::now();

        // The thread will stop once the notifier drops
        while rx.recv().is_ok() {
            // When we get notified about changes to LOG, we read its latest contents
            log_file.read_to_string(&mut buf)?;
            for line in buf.lines() {
                if line.contains("compaction_started") && line.contains("ManualCompaction") {
                    compaction_started = true;
                }
                if !compaction_started {
                    continue;
                }
                // As far as I can tell compaction has four stages, first files are created for
                // the appropriate keys, then are indexed, then moved to the
                // correct level (zero cost in case of manual compaction),
                // finally old files are deleted. The final two stages are almost immediate so
                // we don't care about logging them. We only going to log
                // progress for the first two stages.

                // In the first stage we have log entries of the form `Generated table #53:
                // 3314046 keys, 268436084 bytes` we will be looking for the
                // number of keys in the table, it seems when we have all of the keys processed
                // is when first stage is done.
                if line.contains("Generated table") {
                    // Look for number of keys
                    let mut fields = line.split(' ').peekable();
                    while let Some(f) = fields.next() {
                        if fields.peek() == Some(&"keys,") {
                            first_stage_keys += f.parse().unwrap_or(0);
                            break;
                        }
                    }
                }
                // In the second stage we have log entries of the form
                // `Number of Keys per prefix Histogram: Count: 1313702 Average: 1.0000  StdDev:
                // 0.00` Here we are looking for the Count to figure out the
                // number of keys processed in this stage
                if line.contains("Number of Keys per prefix Histogram") {
                    // Look for number of keys
                    let mut fields = line.split(' ').peekable();
                    while let Some(f) = fields.next() {
                        if f == "Count:" {
                            let count_per_hist =
                                fields.next().and_then(|f| f.parse().ok()).unwrap_or(0);
                            let avg_per_hist =
                                fields.nth(1).and_then(|f| f.parse().ok()).unwrap_or(0f64);
                            second_stage_keys += (count_per_hist as f64 * avg_per_hist) as u64;
                            break;
                        }
                    }
                }

                if last_report.elapsed() > REPORT_INTERVAL {
                    let first_stage = format!(
                        "{:.2}%",
                        (first_stage_keys as f64 / row_count as f64) * 100.0
                    );
                    let second_stage = format!(
                        "{:.2}%",
                        (second_stage_keys as f64 / row_count as f64) * 100.0
                    );
                    info!(%table, %first_stage, %second_stage, "Compaction");
                    last_report = Instant::now();
                }
            }
            buf.clear();
        }

        Ok(())
    };

    let table = table_name.to_owned();

    let s = std::thread::Builder::new();
    s.name("Compaction Monitor".to_string())
        .spawn_wrapper(move || {
            if let Err(err) = monitor() {
                warn!(%err, %table, "Compaction monitor error");
            }
        })?;

    Ok(log_watcher)
}

fn compact_cf(table: &str, db: &DB, index: &PersistentIndex, opts: &CompactOptions) {
    let cf = match db.cf_handle(&index.column_family) {
        Some(cf) => cf,
        None => {
            warn!(cf = %index.column_family, "Column family not found");
            return;
        }
    };

    let _log_watcher = compaction_progress_watcher(table, db);
    if let Err(error) = &_log_watcher {
        warn!(%error, %table, "Could not start compaction monitor");
    }

    info!(%table, cf = %index.column_family, "Compaction starting");
    db.compact_range_cf_opt(cf, Option::<&[u8]>::None, Option::<&[u8]>::None, opts);
    info!(%table, cf = %index.column_family, "Compaction finished");

    // Reenable auto compactions when done
    if let Err(error) = db.set_options_cf(cf, &[("disable_auto_compactions", "false")]) {
        error!(%error, %table, "Error setting cf options");
    }
}

/// Handle to all the records in a persistent state.
///
/// This type exists as distinct from [`AllRecordsGuard`] to allow it to be sent between threads.
pub struct AllRecords(PersistentStateHandle);

/// RAII guard providing the ability to stream all the records out of a persistent state
pub struct AllRecordsGuard<'a>(PersistentStateReadGuard<'a>);

impl AllRecords {
    /// Construct an RAII guard providing the ability to stream all the records out of a persistent
    /// state
    pub fn read(&self) -> AllRecordsGuard<'_> {
        AllRecordsGuard(self.0.inner())
    }
}

impl<'a> AllRecordsGuard<'a> {
    /// Construct an iterator over all the records in a persistent state
    pub fn iter<'b>(&'a self) -> impl Iterator<Item = Vec<DfValue>> + 'b
    where
        'a: 'b,
    {
        let cf = self
            .0
            .db
            .cf_handle(&self.0.shared_state.indices[0].column_family)
            .expect("Column families always exist for all indices");
        self.0
            .db
            .full_iterator_cf(cf, IteratorMode::Start)
            .map(|res| deserialize_row(res.unwrap().1))
    }
}

struct IndexKeyValue {
    pk: Vec<u8>,
    row: Vec<DfValue>,
}

impl IndexKeyValue {
    fn new(pk: Vec<u8>, row: Vec<DfValue>) -> Self {
        Self { pk, row }
    }
}

impl PersistentState {
    #[instrument(name = "Creating persistent state", skip_all, fields(name))]
    pub fn new<C: AsRef<[usize]>, K: IntoIterator<Item = C>>(
        mut name: String,
        unique_keys: K,
        params: &PersistenceParameters,
        persistence_type: PersistenceType,
    ) -> Result<Self> {
        let mut params = params.clone();

        // For persisting full materializations, we only want the rocksdb
        // files to live as long as the process (don't retain across restarts).
        // Also, disable our background WAL flusher for this rocksdb instance as
        // we'll use the "natural" rocskdb mechanisms for flushing the WAL,
        // even though we won't write entries to the WAL (disabled via WriteOptions below).
        if persistence_type == PersistenceType::FullMaterialization {
            params.mode = DurabilityMode::DeleteOnExit;
            params.wal_flush_interval_seconds = 0;
        }

        let unique_keys: Vec<Box<[usize]>> =
            unique_keys.into_iter().map(|c| c.as_ref().into()).collect();

        if !name.ends_with(".db") {
            name.push_str(".db");
        }

        let (tmpdir, full_path) = match params.mode {
            DurabilityMode::Permanent => {
                let mut path = params.storage_dir();
                if !path.is_dir() {
                    fs::create_dir_all(&path)?;
                }
                path.push(&name);

                (None, path)
            }
            DurabilityMode::DeleteOnExit | DurabilityMode::MemoryOnly => {
                let working_dir = params.working_dir_base();
                if !working_dir.exists() {
                    fs::create_dir_all(&working_dir)?;
                }
                let canonical_dir = working_dir.canonicalize()?;

                // create the temp subdirectory specific to this node
                let tempdir = Builder::new().prefix(&name).tempdir_in(canonical_dir)?;
                let path = tempdir.path().to_path_buf();

                (Some(tempdir), path)
            }
        };

        trace!(
            "persistent directories, tempdir: {:?}, full_path: {:?}",
            &tmpdir,
            &full_path
        );

        let name = SqlIdentifier::from(name);

        match Self::new_inner(
            name.clone(),
            full_path.clone(),
            unique_keys.clone(),
            &params,
            persistence_type,
        ) {
            Ok(mut ps) => {
                ps._tmpdir = tmpdir;
                Ok(ps)
            }
            Err(e) if e.is_permanent() => Err(e),
            Err(error) => {
                warn!(
                    %error,
                    "Non-permanent error creating persistent state, deleting path and trying again"
                );
                if full_path.is_dir() {
                    fs::remove_dir_all(&full_path)?;
                }
                Self::new_inner(name, full_path, unique_keys, &params, persistence_type)
            }
        }
    }

    fn new_inner(
        name: SqlIdentifier,
        path: PathBuf,
        unique_keys: Vec<Box<[usize]>>,
        params: &PersistenceParameters,
        persistence_type: PersistenceType,
    ) -> Result<Self> {
        let default_options = base_options(params);
        // We use a column family for each index, and one for metadata.
        // When opening the DB the exact same column families needs to be used,
        // so we'll have to retrieve the existing ones first:
        let cf_names = match DB::list_cf(&default_options, &path) {
            Ok(cfs) => cfs,
            Err(_err) => vec![DEFAULT_CF.to_string()],
        };

        let meta = DB::open_for_read_only(&default_options, &path, false)
            .ok()
            .and_then(|db| get_meta(&db).ok());

        if let Some(meta) = &meta {
            if meta.serde_version != DfValue::SERDE_VERSION {
                return Err(Error::SerdeVersionMismatch {
                    path,
                    persisted_version: meta.serde_version,
                    our_version: DfValue::SERDE_VERSION,
                });
            }
        }

        let cf_index_params = meta
            .into_iter()
            .flat_map(|meta: PersistentMeta| {
                meta.indices
                    .into_iter()
                    .map(|index| IndexParams::from(&index))
            })
            .collect::<Vec<_>>();

        // ColumnFamilyDescriptor does not implement Clone, so we have to create a new Vec each time
        let make_cfs = || -> Result<Vec<ColumnFamilyDescriptor>> {
            cf_names
                .iter()
                .map(|cf_name| {
                    Ok(ColumnFamilyDescriptor::new(
                        cf_name,
                        if cf_name == DEFAULT_CF {
                            default_options.clone()
                        } else {
                            let cf_id: usize = cf_name.parse().map_err(|_| Error::BadDbFormat)?;
                            let index_params =
                                cf_index_params.get(cf_id).ok_or(Error::BadDbFormat)?;
                            index_params.make_rocksdb_options(&default_options, params)
                        },
                    ))
                })
                .collect()
        };

        let mut retry = 0;
        let mut db = loop {
            // TODO: why is this loop even needed?
            match DB::open_cf_descriptors(&default_options, &path, make_cfs()?) {
                Ok(db) => break db,
                _ if retry < 100 => {
                    retry += 1;
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => return Err(e.into()),
            }
        };

        let meta = increment_epoch(&db)?;
        let indices = meta.get_indices(&unique_keys);

        // If there are more column families than indices (+1 to account for the default column
        // family) we either crashed while trying to build the last index (in `Self::add_index`), or
        // something (like failed deserialization) caused us to reset the meta to the default
        // value.
        // Either way, we should drop all column families that are in the db but not in the
        // meta.
        if cf_names.len() > indices.len() + 1 {
            for cf_name in cf_names.iter().skip(indices.len() + 1) {
                db.drop_cf(cf_name)?;
            }
        }

        // If there are less column families than indices (+1 to account for the default column
        // family) we must have crashed while enabling the snapshot mode (after dropping a column
        // family, but before creating a new one). Create the missing cf now.
        if cf_names.len() < indices.len() + 1 {
            for index in &indices {
                if !cf_names.iter().any(|e| e.as_str() == index.column_family) {
                    // This column family was dropped, but index remains
                    db.create_cf(
                        &index.column_family,
                        &IndexParams::from(&index.index)
                            .make_rocksdb_options(&default_options, params),
                    )?;
                }
            }
        }

        let replication_offset = meta.replication_offset.map(|ro| ro.into_owned());
        let shared_state = SharedState {
            replication_offset: replication_offset.clone(),
            wal_state: WalState::FlushedAndPersisted,
            last_wal_flush_error: None,
            indices,
        };
        let read_handle = PersistentStateHandle::new(shared_state, db, replication_offset);

        let wal_flush_thread_handle = if params.wal_flush_interval_seconds == 0 {
            None
        } else {
            let (tx, rx) = mpsc::channel::<()>();
            let wal_flusher = WalFlusher {
                rx,
                state_handle: read_handle.clone(),
                table: name.clone(),
                flush_interval: Duration::from_secs(params.wal_flush_interval_seconds),
            };

            let jh = std::thread::Builder::new()
                .name("WAL Flusher".to_string())
                .spawn_wrapper(move || wal_flusher.run())?;

            Some((tx, jh))
        };

        let metrics = MetricsReporter::start(name.as_str().to_string(), read_handle.clone());

        let state = Self {
            name,
            default_options,
            seq: 0,
            unique_keys,
            epoch: meta.epoch,
            db: read_handle,
            _tmpdir: None,
            snapshot_mode: SnapshotMode::SnapshotModeDisabled,
            compaction_threads: vec![],
            wal_flush_thread_handle,
            persistence_type,
            replay_done: persistence_type == PersistenceType::BaseTable,
            metrics_stop: Some(metrics),
            persistence_parameters: params.clone(),
        };

        if let Some(pk) = state.unique_keys.first().cloned() {
            // This is the first time we're initializing this PersistentState,
            // so persist the primary key index right away.
            state.init_primary_index(&pk, true)?;
        }

        Ok(state)
    }

    /// Returns a new [`PersistentStateHandle`] that can be used to read directly from this
    /// [`PersistentState`] from other threads.
    pub fn read_handle(&self) -> PersistentStateHandle {
        // The cloning here clones an inner Arc reference not the database
        self.db.clone()
    }

    fn index_progress(&self, started: Instant, last: &mut Instant, rows: usize, estimated: usize) {
        const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
        if last.elapsed() < PROGRESS_INTERVAL {
            return;
        }
        *last += PROGRESS_INTERVAL;

        let progress = ((rows as f64) / (estimated.max(1) as f64)).clamp(0.0, 1.0);
        let running = started.elapsed().as_secs_f64();
        let total = running / progress;
        let left = (total - running) as u64;
        let hours = left / 60 / 60;
        let mins = left / 60 % 60;
        let secs = left % 60;

        let progress = format!("{:.1}%", progress * 100.0);
        let left = format!("{:02}:{:02}:{:02}", hours, mins, secs);

        info!(
            base = %self.name,
            estimated_rows = %estimated,
            indexed = %rows,
            %progress,
            estimated_remaining_time = %left,
            "Secondary index progress"
        );
    }

    /// Adds a new primary index, assuming there are none present
    fn add_primary_index(
        &self,
        inner: &mut PersistentStateWriteGuard<'_>,
        columns: &[usize],
        is_unique: bool,
    ) -> Result<()> {
        if !inner.shared_state.indices.is_empty() {
            return Ok(());
        }

        debug!(base = %self.name, index = ?columns, is_unique, "Base creating primary index");
        let index_params = IndexParams::new(IndexType::HashMap, columns.len());

        // add the index to the meta first so even if we fail before we fully reindex we still
        // have the information about the column family
        let persistent_index = PersistentIndex {
            column_family: PK_CF.to_string(),
            index: Index::hash_map(columns.to_vec()),
            is_unique,
            is_primary: true,
        };

        inner.shared_state.indices.push(persistent_index);
        let meta = self.meta(&inner.shared_state);
        inner.db.save_meta(&meta);
        inner.db.create_cf(
            PK_CF,
            &index_params.make_rocksdb_options(&self.default_options, &self.persistence_parameters),
        )?;

        Ok(())
    }

    fn init_primary_index(&self, columns: &[usize], is_unique: bool) -> Result<()> {
        self.add_primary_index(&mut self.db.inner_mut(), columns, is_unique)
    }

    fn create_secondary(
        &self,
        inner: &mut PersistentStateWriteGuard<'_>,
        indices: &[Index],
        is_unique: &[bool],
    ) -> Vec<PersistentIndex> {
        indices
            .iter()
            .zip(is_unique)
            .map(|(index, is_unique)| {
                info!(base = %self.name, ?index, is_unique, "Base creating secondary index");

                let index_params = IndexParams::from(index);
                let cf_name = inner.shared_state.indices.len().to_string();
                let persistent = PersistentIndex {
                    column_family: cf_name.clone(),
                    is_unique: *is_unique,
                    is_primary: false,
                    index: index.clone(),
                };

                inner.shared_state.indices.push(persistent.clone());

                // Add the index to the meta first so even if we fail before we fully reindex we
                // still have the information about the column family.
                inner.db.save_meta(&self.meta(&inner.shared_state));
                inner
                    .db
                    .create_cf(
                        &cf_name,
                        &index_params.make_rocksdb_options(
                            &self.default_options,
                            &self.persistence_parameters,
                        ),
                    )
                    .unwrap();

                persistent
            })
            .collect()
    }

    fn open_secondary_cf<'a>(
        inner: &'a PersistentStateReadGuard<'a>,
        new: &PersistentIndex,
    ) -> &'a ColumnFamily {
        let cf = inner.db.cf_handle(&new.column_family).unwrap();

        // Prevent autocompactions while we reindex the table
        if let Err(err) = inner
            .db
            .set_options_cf(cf, &[("disable_auto_compactions", "true")])
        {
            error!(%err, "Error setting cf options");
        }

        cf
    }

    #[allow(clippy::type_complexity)]
    fn make_channels<T>(num: usize) -> (Vec<Sender<Arc<T>>>, Vec<Receiver<Arc<T>>>) {
        let mut txs = Vec::new();
        let mut rxs = Vec::new();
        for _ in 0..num {
            let (tx, rx) = mpsc::channel();
            txs.push(tx);
            rxs.push(rx);
        }
        (txs, rxs)
    }

    fn write_secondary(
        inner: &PersistentStateReadGuard<'_>,
        index: &Index,
        is_unique: bool,
        new: &PersistentIndex,
        rx: Receiver<Arc<IndexKeyValue>>,
    ) {
        let cf = Self::open_secondary_cf(inner, new);

        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);

        'outer: loop {
            let mut batch = WriteBatch::default();

            for _ in 0..INDEX_BATCH_SIZE {
                let Ok(kv) = rx.recv() else {
                    inner.db.write_opt(batch, &opts).unwrap();
                    break 'outer;
                };

                let index_key = build_key(&kv.row, &index.columns);
                let key = if is_unique && !index_key.has_null() {
                    Self::serialize_prefix(&index_key)
                } else {
                    // TODO avoid storing pk as the value; already in the key
                    Self::serialize_secondary(&index_key, &kv.pk)
                };
                batch.put_cf(cf, &key, &kv.pk);
            }

            inner.db.write_opt(batch, &opts).unwrap();
        }

        // Flush just in case
        inner.db.flush_cf(cf).unwrap();
    }

    /// Adds new secondary indices.  Secondary indices point to the primary index
    /// and don't store values on their own.
    fn add_secondary(
        &self,
        mut inner: PersistentStateWriteGuard<'_>,
        indices: &[Index],
        is_unique: &[bool],
    ) -> Vec<CompactionThreadHandle> {
        let new = self.create_secondary(&mut inner, indices, is_unique);
        let inner = inner.downgrade();
        let (txs, rxs) = Self::make_channels(new.len());

        thread::scope(|scope| {
            for (index, is_unique, new, rx) in itertools::izip!(indices, is_unique, &new, rxs) {
                scope.spawn(|| Self::write_secondary(&inner, index, *is_unique, new, rx));
            }

            let mut opts = rocksdb::ReadOptions::default();
            opts.set_total_order_seek(true); // because not doing a prefix seek

            let pk_cf = inner.db.cf_handle(PK_CF).unwrap();
            let mut iter = inner.db.raw_iterator_cf_opt(pk_cf, opts);
            iter.seek_to_first();

            let started = Instant::now();
            let estimated = self.row_count();
            let mut last_progress = started;
            let mut indexed = 0;

            while let (Some(pk), Some(value)) = (iter.key(), iter.value()) {
                let row = deserialize_row(value);
                let kv = Arc::new(IndexKeyValue::new(pk.to_vec(), row));
                for tx in &txs {
                    tx.send(Arc::clone(&kv))
                        .expect("send to index writer failed");
                }

                indexed += 1;
                self.index_progress(started, &mut last_progress, indexed, estimated);
                iter.next();
            }

            if let Err(err) = iter.status() {
                // FIXME can't return error from here
                error!(%err, "Error creating index");
            }
            drop(txs);
        });

        // Compact the newly created column families in the background
        new.into_iter().map(|p| self.compact_index(p)).collect()
    }

    /// Builds a [`PersistentMeta`] from the in-memory metadata information stored in `self`,
    /// including:
    ///
    /// * The columns and index types of the indices
    /// * The epoch
    /// * The replication offset
    fn meta(&self, shared_state: &SharedState) -> PersistentMeta<'_> {
        PersistentMeta {
            serde_version: DfValue::SERDE_VERSION,
            indices: shared_state
                .indices
                .iter()
                .map(|pi| pi.index.clone())
                .collect(),
            epoch: self.epoch,
            replication_offset: self.replication_offset().map(Cow::Borrowed),
        }
    }

    /// Add an operation to the given [`WriteBatch`] to set the [replication
    /// offset](PersistentMeta::replication_offset) to the given value.
    fn set_replication_offset(&mut self, batch: &mut WriteBatch, offset: ReplicationOffset) {
        // It's ok to read and update meta in two steps here since each State can (currently) only
        // be modified by a single thread.
        self.db.replication_offset = Some(offset.clone());

        let mut inner = self.db.inner_mut();
        // TODO(ethan) do we want to be updating our in-memory replication offsets here before
        // the write succeeds?
        inner.shared_state.replication_offset = Some(offset.clone());

        match inner.shared_state.wal_state {
            // If snapshot mode is enabled, the WAL is disabled, and we don't have to worry
            // about setting flushed_up_to or persisted_up_to
            _ if self.snapshot_mode.is_enabled() => {}
            // All of the data in this state has been persisted and the batch is empty, which
            // means we are just updating the offset. We don't want to update either of
            // flushed_up_to or synced_up_to here because we're not adding any new data
            _ if batch.is_empty() => {}
            // If our data is flushed and persisted or our data is flushed but unpersisted,
            // we need to change our state to `WalState::Unflushed` since we're writing new
            // unflushed data
            WalState::FlushedAndPersisted | WalState::FlushedAndUnpersisted { .. } => {
                inner.shared_state.wal_state = WalState::Unflushed {
                    // The new offset marks the start of the unpersisted data in this state
                    persisted_up_to: offset,
                };
            }
            // If there is already unflushed data, we don't have to transition the WAL state,
            // since adding new unflushed data doesn't change the low watermark of all of our
            // unflushed data
            WalState::Unflushed { .. } => {}
        }

        batch.save_meta(&self.meta(&inner.shared_state));
    }

    /// Enables or disables the snapshot mode. In snapshot mode auto compactions are
    /// disabled and writes don't go to WAL first. When set to false manual compaction
    /// will be triggered, which may block for some time.
    /// In addition all column families will be dropped prior to entering this mode.
    pub fn set_snapshot_mode(&mut self, snapshot: SnapshotMode) {
        self.snapshot_mode = snapshot;

        if snapshot.is_enabled() {
            self.enable_snapshot_mode();
        } else {
            self.compact_all_indices();
        }
    }

    pub fn compaction_finished(&mut self) -> bool {
        self.scrub_compaction_threads();
        self.compaction_threads.is_empty()
    }

    pub fn wait_for_compaction(&mut self) {
        for thread in &mut self.compaction_threads {
            thread.join();
        }
        self.scrub_compaction_threads();
    }

    fn push_compaction_thread(&mut self, thread: CompactionThreadHandle) {
        self.scrub_compaction_threads();
        self.compaction_threads.push(thread);
    }

    fn scrub_compaction_threads(&mut self) {
        self.compaction_threads.retain(|thr| !thr.is_finished());
    }

    fn enable_snapshot_mode(&mut self) {
        // Remove any replication offset first (although it should be None already)
        self.db.replication_offset = None;
        let mut inner = self.db.inner_mut();
        let meta = self.meta(&inner.shared_state);
        inner.db.save_meta(&meta);

        // Clear the data by dropping each column family and creating it anew
        for index in inner.shared_state.indices.iter() {
            let cf_name = index.column_family.as_str();
            inner.db.drop_cf(cf_name).unwrap();

            inner
                .db
                .create_cf(
                    cf_name,
                    &IndexParams::from(&index.index)
                        .make_rocksdb_options(&self.default_options, &self.persistence_parameters),
                )
                .unwrap();

            let cf = inner.db.cf_handle(cf_name).expect("just created this cf");

            if let Err(err) = inner
                .db
                .set_options_cf(cf, &[("disable_auto_compactions", "true")])
            {
                error!(%err, "Error setting cf options");
            }
        }
    }

    fn compact_worker(
        table: SqlIdentifier,
        index: PersistentIndex,
        read: PersistentStateHandle,
        opts: Arc<CompactOptions>,
    ) {
        let span = info_span!("Compacting index", %table, column_family = %index.column_family);
        let _guard = span.enter();
        compact_cf(&table, &read.inner().db, &index, &opts);
    }

    /// Perform a manual compaction for a single column family.
    fn compact_index(&self, index: PersistentIndex) -> CompactionThreadHandle {
        let mut opts = CompactOptions::default();
        opts.set_exclusive_manual_compaction(false);
        let opts = Arc::new(opts);

        let table = self.name.clone();
        let read = self.read_handle();
        let opts = Arc::clone(&opts);
        let name = format!(
            "Compacting index table={}, cf={}",
            table, index.column_family
        );
        let compaction_thread = std::thread::Builder::new()
            .name(name)
            .spawn_wrapper(move || Self::compact_worker(table, index, read, opts))
            .expect("spawn_wrapper failed");

        CompactionThreadHandle {
            handle: Some(compaction_thread),
        }
    }

    /// Perform a manual compaction for each column family.
    fn compact_all_indices(&mut self) {
        let indices = self.db.inner().shared_state.indices.to_vec();
        for index in indices {
            let thr = self.compact_index(index);
            self.push_compaction_thread(thr);
        }
        self.wait_for_compaction();
    }

    pub fn set_replay_done(&mut self, replay_done: bool) {
        // only change the replay_done value for non-base tables,
        // as base tables are always considered "replayed"
        if self.persistence_type == PersistenceType::FullMaterialization {
            self.replay_done = replay_done;
        }
    }

    fn serialize_prefix(key: &PointKey) -> Vec<u8> {
        serialize_key(key, ())
    }

    fn serialize_secondary(key: &PointKey, raw_primary: &[u8]) -> Vec<u8> {
        let mut bytes = serialize_key(key, ());
        bytes.extend_from_slice(raw_primary);
        bytes
    }

    /// Inserts the row into the database by replicating it across all of the column
    /// families. The insert is performed in a context of a [`rocksdb::WriteBatch`]
    /// operation and is therefore guaranteed to be atomic.
    fn insert(&mut self, batch: &mut WriteBatch, r: &[DfValue]) -> ReadySetResult<()> {
        let inner = self.db.inner();
        let primary_index = inner
            .shared_state
            .indices
            .first()
            .ok_or_else(|| internal_err!("Insert on un-indexed state"))?;
        let primary_key = build_key(r, &primary_index.index.columns);
        let primary_cf = inner.db.cf_handle(&primary_index.column_family).unwrap();

        // Generate a new primary key by extracting the key columns from the provided row
        // using the primary index and serialize it as RocksDB prefix.
        let serialized_pk = if primary_index.is_unique && !primary_key.has_null() {
            Self::serialize_prefix(&primary_key)
        } else {
            // The primary index may not be unique so we append a monotonically incremented
            // counter to make sure the key is unique (prefixes will be shared for non unique keys)
            self.seq += 1;
            serialize_key(&primary_key, (self.epoch, self.seq))
        };

        let serialized_row = bincode::options().serialize(r)?;

        // First store the row for the primary index:
        batch.put_cf(primary_cf, &serialized_pk, &serialized_row);

        // Then insert the value for all the secondary indices:
        for index in inner.shared_state.indices[1..].iter() {
            // Construct a key with the index values, and serialize it with bincode:
            let cf = inner.db.cf_handle(&index.column_family).unwrap();
            let key = build_key(r, &index.index.columns);

            if index.is_unique && !key.has_null() {
                let serialized_key = Self::serialize_prefix(&key);
                batch.put_cf(cf, &serialized_key, &serialized_pk);
            } else {
                let serialized_key = Self::serialize_secondary(&key, &serialized_pk);
                // TODO: Since the primary key is already serialized in here, no reason to store it
                // as value again
                batch.put_cf(cf, &serialized_key, &serialized_pk);
            };
        }

        Ok(())
    }

    fn remove(&self, batch: &mut WriteBatch, r: &[DfValue]) -> ReadySetResult<()> {
        let inner = self.db.inner();

        let primary_index = inner
            .shared_state
            .indices
            .first()
            .ok_or_else(|| internal_err!("Delete on un-indexed state"))?;
        let primary_key = build_key(r, &primary_index.index.columns);
        let primary_cf = inner.db.cf_handle(&primary_index.column_family).unwrap();

        let prefix = Self::serialize_prefix(&primary_key);

        let serialized_pk = if primary_index.is_unique && !primary_key.has_null() {
            // This key is unique, so we can delete it as is
            prefix
        } else {
            // This is key is not unique, therefore we have to iterate over the
            // the values, looking for the first one that matches the full row
            // and then return the (full length) unique primary key associated with it
            let mut iter = inner.db.raw_iterator_cf(primary_cf);
            iter.seek(&prefix); // Find the first key

            loop {
                let key = iter
                    .key()
                    .filter(|k| k.starts_with(&prefix))
                    .ok_or_else(|| internal_err!("tried removing non-existent row"))?;
                let val = deserialize_row(iter.value().unwrap());
                if val == r {
                    break key.to_vec();
                }
                iter.next();
            }
        };

        // First delete the row for the primary index:
        batch.delete_cf(primary_cf, &serialized_pk);

        // Then delete the value for all the secondary indices
        for index in inner.shared_state.indices[1..].iter() {
            // Construct a key with the index values, and serialize it with bincode:
            let key = build_key(r, &index.index.columns);
            let serialized_key = if index.is_unique && !key.has_null() {
                Self::serialize_prefix(&key)
            } else {
                // For non unique keys, we use the primary key to make sure we delete
                // the *exact* same row from each family
                Self::serialize_secondary(&key, &serialized_pk)
            };
            let cf = inner.db.cf_handle(&index.column_family).unwrap();
            batch.delete_cf(cf, &serialized_key);
        }

        Ok(())
    }

    pub fn is_snapshotting(&self) -> bool {
        self.snapshot_mode.is_enabled()
    }

    /// Get the persistent state's snapshot mode.
    pub fn snapshot_mode(&self) -> SnapshotMode {
        self.snapshot_mode
    }

    /// Perform a lookup for multiple equal keys at once, the results are returned in order of the
    /// original keys
    pub fn lookup_multi<'a>(
        &'a self,
        columns: &[usize],
        keys: &[PointKey],
    ) -> Vec<RecordResult<'a>> {
        self.db.lookup_multi(columns, keys)
    }

    /// Takes the provided batch and optionally a replication offset and writes to the RocksDB
    /// database.
    fn write_to_db(
        &mut self,
        batch: WriteBatch,
        replication_offset: &Option<ReplicationOffset>,
    ) -> ReadySetResult<()> {
        let mut batch = batch;
        let mut write_options = rocksdb::WriteOptions::default();
        if self.snapshot_mode.is_enabled()
            // if we're setting the replication offset, that means we've snapshot the full table, so
            // set sync to true there even if snapshot_mode is enabled, to make sure that makes it
            // onto disk (not doing this *will* cause the write to get lost if the server restarts!)
            && replication_offset.is_none()
        {
            write_options.disable_wal(true);
        } else if self.persistence_type == PersistenceType::FullMaterialization {
            // don't write full mat entries to a WAL, we don't care about recovery
            // as we'll rebuild the full mat node on restart.
            write_options.disable_wal(true);
            write_options.set_sync(false);
        } else {
            let inner = &self.db.inner();
            if self.snapshot_mode.is_enabled() && replication_offset.is_some() {
                // We are setting the replication offset, which is great, but all of our previous
                // writes are not guaranteed to flush to disk even if the next write is synced. We
                // therefore perform a flush before handling the next write.
                //
                // See: https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
                // Q: After a write following option.disableWAL=true, I write another record with
                // options.sync=true,    will it persist the previous write too?
                // A: No. After the program crashes, writes with option.disableWAL=true will be
                // lost, if they are not flushed to SST files.
                for index in inner.shared_state.indices.iter() {
                    inner
                        .db
                        .flush_cf(inner.db.cf_handle(&index.column_family).unwrap())
                        .map_err(|e| internal_err!("Flush to disk failed: {e}"))?;
                }

                inner
                    .db
                    .flush()
                    .map_err(|e| internal_err!("Flush to disk failed: {e}"))?;
            }

            if self.wal_flush_thread_handle.is_none() {
                write_options.set_sync(true);
            }
        }

        if let Some(offset) = replication_offset {
            self.set_replication_offset(&mut batch, offset.clone());
        }

        self.db
            .inner()
            .db
            .write_opt(batch, &write_options)
            .map_err(|e| internal_err!("Write failed: {e}"))?;

        Ok(())
    }

    fn shut_down_metrics_reporting(&mut self) {
        if let Some(x) = self.metrics_stop.take() {
            x.stop();
        }
    }

    fn shut_down_wal(&mut self) -> ReadySetResult<()> {
        if let Some((tx, jh)) = self.wal_flush_thread_handle.take() {
            // Stop the thread that periodically flushes the WAL
            tx.send(()).unwrap();

            jh.join().map_err(|_| {
                ReadySetError::Internal(format!(
                    "could not join WAL flush thread for table {}",
                    self.name
                ))
            })?;
        }
        Ok(())
    }
}

/// Checks if the given index is unique for this base table.
/// An index is unique if any of its subkeys or permutations is unique.
/// i.e.: if the key [0,2] is unique, [2,0] is also unique, as well as [2,3,0]
/// This check is not asymptotically efficient, but it doesn't matter as long
/// as we only use it during add_index.
fn check_if_index_is_unique(unique_indices: &[Box<[usize]>], columns: &[usize]) -> bool {
    // We go over all of the unique indices for the table and check if the
    // provided index contains all of its columns. If so, the index is also
    // unique.
    unique_indices
        .iter()
        .any(|ui| ui.iter().all(|col| columns.contains(col)))
}

// SliceTransforms are used to create prefixes of all inserted keys, which can then be used for
// both bloom filters and hash structure lookups.
//
// Selects a prefix of `key` without the epoch or sequence number.
//
// The RocksDB docs state the following:
// > If non-nullptr, use the specified function to determine the
// > prefixes for keys.  These prefixes will be placed in the filter.
// > Depending on the workload, this can reduce the number of read-IOP
// > cost for scans when a prefix is passed via ReadOptions to
// > db.NewIterator(). For prefix filtering to work properly,
// > "prefix_extractor" and "comparator" must be such that the following
// > properties hold:
//
// > 1) key.starts_with(prefix(key))
// > 2) Compare(prefix(key), key) <= 0.
// > 3) If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
// > 4) prefix(prefix(key)) == prefix(key)
//
fn prefix_transform(key: &[u8]) -> &[u8] {
    // We'll have to make sure this isn't the META_KEY even when we're filtering it out
    // in Self::in_domain_fn, as the SliceTransform is used to make hashed keys for our
    // HashLinkedList memtable factory.
    if key == META_KEY {
        return key;
    }

    let key_size: u64 = bincode::options()
        .allow_trailing_bytes()
        .deserialize(key)
        .unwrap();
    let size_offset = bincode::options().serialized_size(&key_size).unwrap();
    let prefix_len = (size_offset + key_size) as usize;
    // Strip away the key suffix if we haven't already done so:
    &key[..prefix_len]
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OwnedKey {
    Single(DfValue),
    Double((DfValue, DfValue)),
    Tri((DfValue, DfValue, DfValue)),
    Quad((DfValue, DfValue, DfValue, DfValue)),
    Quin((DfValue, DfValue, DfValue, DfValue, DfValue)),
    Sex((DfValue, DfValue, DfValue, DfValue, DfValue, DfValue)),
    Multi(Vec<DfValue>),
}

fn deserialize_key<D: DeserializeOwned>(inp: &[u8]) -> (u64, D) {
    bincode::options()
        .allow_trailing_bytes()
        .deserialize(inp)
        .unwrap()
}

macro_rules! make_compare_keys {
    ($name: ident($key_variant: ident)) => {
        fn $name(k1: &[u8], k2: &[u8]) -> Ordering {
            let deserialize_key_type = |inp| {
                let (len, k) = deserialize_key(inp);
                (len as usize, OwnedKey::$key_variant(k))
            };
            let (k1_len, k1_de) = deserialize_key_type(k1);
            let (k2_len, k2_de) = deserialize_key_type(k2);

            // First compare the deserialized keys...
            k1_de
                .cmp(&k2_de)
                // ... then, if they're equal, compare the suffixes, which contain either primary
                // keys or sequence numbers to distinguish between rows with equal keys in
                // non-unique indices. These don't need to be deserialized since we just care about
                // whether they're equal or not, the semantics of less or greater are irrelevant.
                .then_with(|| k1[k1_len..].cmp(&k2[k2_len..]))
        }
    };
}

make_compare_keys!(compare_keys_1(Single));
make_compare_keys!(compare_keys_2(Double));
make_compare_keys!(compare_keys_3(Tri));
make_compare_keys!(compare_keys_4(Quad));
make_compare_keys!(compare_keys_5(Quin));
make_compare_keys!(compare_keys_6(Sex));
make_compare_keys!(compare_keys_multi(Multi));

// Decides which keys the prefix transform should apply to.
fn in_domain(key: &[u8]) -> bool {
    key != META_KEY
}

impl SizeOf for PersistentStateHandle {
    fn deep_size_of(&self) -> u64 {
        0
    }

    fn size_of(&self) -> u64 {
        mem::size_of::<Self>() as u64
    }

    fn is_empty(&self) -> bool {
        self.inner()
            .db
            .property_int_value("rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap()
            == 0
    }
}

impl SizeOf for PersistentState {
    fn size_of(&self) -> u64 {
        mem::size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        let inner = self.db.inner();
        inner
            .shared_state
            .indices
            .iter()
            .map(|idx| {
                let cf = inner
                    .db
                    .cf_handle(&idx.column_family)
                    .unwrap_or_else(|| panic!("Column family not found: {}", idx.column_family));
                let sstable_size = inner
                    .db
                    .property_int_value_cf(cf, "rocksdb.estimate-live-data-size")
                    .unwrap()
                    .unwrap();
                let memtable_size = inner
                    .db
                    .property_int_value_cf(cf, "rocksdb.size-all-mem-tables")
                    .unwrap()
                    .unwrap();
                sstable_size + memtable_size
            })
            .sum()
    }

    fn is_empty(&self) -> bool {
        self.db.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unreachable)]
mod tests {
    use std::fmt::Debug;
    use std::path::PathBuf;

    use pretty_assertions::assert_eq;
    use readyset_data::Bound::*;
    use readyset_data::Collation;
    use replication_offset::mysql::MySqlPosition;
    use replication_offset::postgres::PostgresPosition;
    use rust_decimal::Decimal;
    use test_strategy::proptest;

    use super::*;

    fn insert<S: State>(state: &mut S, row: Vec<DfValue>) {
        let record: Record = row.into();
        state
            .process_records(&mut record.into(), None, None)
            .unwrap();
    }

    fn get_tmp_path() -> (TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("readyset");
        (dir, path.to_string_lossy().into())
    }

    fn setup_persistent<'a, K: IntoIterator<Item = &'a [usize]>>(
        prefix: &str,
        unique_keys: K,
    ) -> PersistentState {
        PersistentState::new(
            String::from(prefix),
            unique_keys,
            &PersistenceParameters::default(),
            PersistenceType::BaseTable,
        )
        .unwrap()
    }

    fn setup_single_key(name: &str) -> PersistentState {
        let mut state = setup_persistent(name, None);
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        state
    }

    #[proptest]
    fn point_key_serialize_round_trip(key: PointKey) {
        let serialized = PersistentState::serialize_prefix(&key);

        fn check<D>(serialized: &[u8], v: D)
        where
            D: DeserializeOwned + Debug + PartialEq,
        {
            assert_eq!(deserialize_key::<D>(serialized).1, v);
        }

        match key {
            PointKey::Empty => check(&serialized, ()),
            PointKey::Single(x) => check(&serialized, x),
            PointKey::Double(x) => check(&serialized, x),
            PointKey::Tri(x) => check(&serialized, x),
            PointKey::Quad(x) => check(&serialized, x),
            PointKey::Quin(x) => check(&serialized, x),
            PointKey::Sex(x) => check(&serialized, x),
            PointKey::Multi(x) => check(&serialized, x),
        }
    }

    #[test]
    fn persistent_state_is_partial() {
        let state = setup_persistent("persistent_state_is_partial", None);
        assert!(!state.is_partial());
    }

    #[test]
    fn persistent_state_single_key() {
        let mut state = setup_single_key("persistent_state_single_key");
        let row: Vec<DfValue> = vec![10.into(), "Cat".into()];
        insert(&mut state, row);

        match state.lookup(&[0], &PointKey::Single(5.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        match state.lookup(&[0], &PointKey::Single(10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows[0][0], 10.into());
                assert_eq!(rows[0][1], "Cat".into());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_multi_key() {
        let mut state = setup_persistent("persistent_state_multi_key", None);
        let cols = vec![0, 2];
        let index = Index::new(IndexType::HashMap, cols.clone());
        let row: Vec<DfValue> = vec![10.into(), "Cat".into(), 20.into()];
        state.add_index(index, None);
        insert(&mut state, row.clone());

        match state.lookup(&cols, &PointKey::Double((1.into(), 2.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        match state.lookup(&cols, &PointKey::Double((10.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows[0], row);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_multiple_indices() {
        let mut state = setup_persistent("persistent_state_multiple_indices", None);
        let first: Vec<DfValue> = vec![10.into(), "Cat".into(), 1.into()];
        let second: Vec<DfValue> = vec![20.into(), "Cat".into(), 1.into()];
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_index(Index::new(IndexType::HashMap, vec![1, 2]), None);
        state
            .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
            .unwrap();

        match state.lookup(&[0], &PointKey::Single(10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0], first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1, 2], &PointKey::Double(("Cat".into(), 1.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&rows[0], &first);
                assert_eq!(&rows[1], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_add_remove_same_record() {
        let mut state = setup_persistent("persistent_state_multiple_indices", None);
        let first: Vec<DfValue> = vec![10.into(), "Cat".into(), 1.into()];
        let second: Vec<DfValue> = vec![10.into(), "Cat".into(), 1.into()];
        let mut records: Records = Default::default();
        records.push(Record::Positive(first));
        records.push(Record::Negative(second));

        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_index(Index::new(IndexType::HashMap, vec![1, 2]), None);
        state.process_records(&mut records, None, None).unwrap();
    }

    #[test]
    fn empty_column_set() {
        let mut state = setup_persistent("empty_column_set", None);
        state.add_index(Index::hash_map(vec![]), None);

        let mut rows = vec![
            vec![1.into()],
            vec![2.into()],
            vec![3.into()],
            vec![4.into()],
            vec![5.into()],
            vec![6.into()],
        ];

        state
            .process_records(&mut rows.clone().into(), None, None)
            .unwrap();
        let mut res = state
            .lookup(&[], &PointKey::Empty)
            .unwrap()
            .into_iter()
            .map(|v| v.into_owned())
            .collect::<Vec<_>>();
        res.sort();
        assert_eq!(res, rows);

        state
            .process_records(
                &mut vec![(vec![DfValue::from(6)], false)].into(),
                None,
                None,
            )
            .unwrap();
        rows.pop();
        let mut res = state
            .lookup(&[], &PointKey::Empty)
            .unwrap()
            .into_iter()
            .map(|v| v.into_owned())
            .collect::<Vec<_>>();
        res.sort();
        assert_eq!(res, rows);
        //state.tear_down();
    }

    #[test]
    fn lookup_citext() {
        let mut state = setup_persistent("lookup_citext", None);
        state.add_index(Index::hash_map(vec![0]), None);

        let abc = vec![
            vec![
                DfValue::from_str_and_collation("abc", Collation::Citext),
                1.into(),
            ],
            vec![
                DfValue::from_str_and_collation("AbC", Collation::Citext),
                2.into(),
            ],
        ];

        state
            .process_records(&mut abc.clone().into(), None, None)
            .unwrap();

        let res = state
            .lookup(
                &[0],
                &PointKey::Single(DfValue::from_str_and_collation("abc", Collation::Citext)),
            )
            .unwrap();

        assert_eq!(res, abc.into())
    }

    #[test]
    fn lookup_numeric_with_different_precision() {
        let mut state = setup_persistent("lookup_numeric_with_different_precision", None);
        state.add_index(Index::btree_map(vec![0]), None);

        let records = vec![vec![DfValue::from(Decimal::from_str_exact("4.0").unwrap())]];

        state
            .process_records(&mut records.clone().into(), None, None)
            .unwrap();

        let res = state
            .lookup(
                &[0],
                &PointKey::Single(DfValue::from(Decimal::from_str_exact("4").unwrap())),
            )
            .unwrap();

        assert_eq!(res, records.into());
        let val = Decimal::try_from(&res.into_iter().next().unwrap()[0]).unwrap();
        assert_eq!(val.scale(), 1);
    }

    #[test]
    fn persistent_state_lookup_multi() {
        for primary in [None, Some(&[0usize][..])] {
            let mut state = setup_persistent("persistent_state_lookup_multi", primary);
            let first: Vec<DfValue> = vec![10.into(), "Cat".into(), 1.into()];
            let second: Vec<DfValue> = vec![20.into(), "Cat".into(), 1.into()];
            let third: Vec<DfValue> = vec![30.into(), "Dog".into(), 1.into()];
            let fourth: Vec<DfValue> = vec![40.into(), "Dog".into(), 1.into()];
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_index(Index::new(IndexType::HashMap, vec![1, 2]), None);
            state
                .process_records(
                    &mut vec![first.clone(), second.clone(), third.clone(), fourth.clone()].into(),
                    None,
                    None,
                )
                .unwrap();

            match state
                .lookup_multi(
                    &[0],
                    &[
                        PointKey::Single(10.into()),
                        PointKey::Single(20.into()),
                        PointKey::Single(30.into()),
                        PointKey::Single(10.into()),
                        PointKey::Single(40.into()),
                    ],
                )
                .as_slice()
            {
                &[RecordResult::Owned(ref r0), RecordResult::Owned(ref r1), RecordResult::Owned(ref r2), RecordResult::Owned(ref r3), RecordResult::Owned(ref r4)] =>
                {
                    assert_eq!(r0.len(), 1);
                    assert_eq!(r0[0], first);
                    assert_eq!(r1.len(), 1);
                    assert_eq!(r1[0], second);
                    assert_eq!(r2.len(), 1);
                    assert_eq!(r2[0], third);
                    assert_eq!(r3.len(), 1);
                    assert_eq!(r3[0], first);
                    assert_eq!(r4.len(), 1);
                    assert_eq!(r4[0], fourth);
                }
                _ => unreachable!(),
            }

            match state
                .lookup_multi(
                    &[1, 2],
                    &[
                        PointKey::Double(("Dog".into(), 1.into())),
                        PointKey::Double(("Cat".into(), 1.into())),
                        PointKey::Double(("Dog".into(), 1.into())),
                        PointKey::Double(("Cat".into(), 1.into())),
                    ],
                )
                .as_slice()
            {
                &[RecordResult::Owned(ref r0), RecordResult::Owned(ref r1), RecordResult::Owned(ref r2), RecordResult::Owned(ref r3)] =>
                {
                    assert_eq!(r0.len(), 2);
                    assert_eq!(r0[0], third);
                    assert_eq!(r0[1], fourth);
                    assert_eq!(r1.len(), 2);
                    assert_eq!(r1[0], first);
                    assert_eq!(r1[1], second);
                    assert_eq!(r2.len(), 2);
                    assert_eq!(r2[0], third);
                    assert_eq!(r2[1], fourth);
                    assert_eq!(r3.len(), 2);
                    assert_eq!(r3[0], first);
                    assert_eq!(r3[1], second);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn persistent_state_primary_key() {
        let pk_cols = vec![0, 1];
        let pk = Index::new(IndexType::HashMap, pk_cols.clone());
        let mut state = PersistentState::new(
            String::from("persistent_state_primary_key"),
            Some(&pk_cols),
            &PersistenceParameters::default(),
            PersistenceType::BaseTable,
        )
        .unwrap();
        let first: Vec<DfValue> = vec![1.into(), 2.into(), "Cat".into()];
        let second: Vec<DfValue> = vec![10.into(), 20.into(), "Cat".into()];
        state.add_index(pk, None);
        state.add_index(Index::new(IndexType::HashMap, vec![2]), None);
        state
            .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
            .unwrap();

        match state.lookup(&pk_cols, &PointKey::Double((1.into(), 2.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&pk_cols, &PointKey::Double((10.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }

        match state.lookup(&pk_cols, &PointKey::Double((1.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 0);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[2], &PointKey::Single("Cat".into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&rows[0], &first);
                assert_eq!(&rows[1], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_primary_key_delete() {
        let pk = Index::new(IndexType::HashMap, vec![0]);
        let mut state = PersistentState::new(
            String::from("persistent_state_primary_key_delete"),
            Some(&pk.columns),
            &PersistenceParameters::default(),
            PersistenceType::BaseTable,
        )
        .unwrap();
        let first: Vec<DfValue> = vec![1.into(), 2.into()];
        let second: Vec<DfValue> = vec![10.into(), 20.into()];
        state.add_index(pk, None);
        state
            .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
            .unwrap();
        match state.lookup(&[0], &PointKey::Single(1.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        state
            .process_records(&mut vec![(first, false)].into(), None, None)
            .unwrap();
        match state.lookup(&[0], &PointKey::Single(1.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 0);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[0], &PointKey::Single(10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_not_unique_primary() {
        let mut state = setup_persistent("persistent_state_multiple_indices", None);
        let first: Vec<DfValue> = vec![0.into(), 0.into()];
        let second: Vec<DfValue> = vec![0.into(), 1.into()];
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
        state
            .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
            .unwrap();

        match state.lookup(&[0], &PointKey::Single(0.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&rows[0], &first);
                assert_eq!(&rows[1], &second);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &PointKey::Single(0.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_different_indices() {
        let mut state = setup_persistent("persistent_state_different_indices", None);
        let first: Vec<DfValue> = vec![10.into(), "Cat".into()];
        let second: Vec<DfValue> = vec![20.into(), "Bob".into()];
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
        state
            .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
            .unwrap();

        match state.lookup(&[0], &PointKey::Single(10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &PointKey::Single("Bob".into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_recover() {
        let (_dir, name) = get_tmp_path();
        let params = PersistenceParameters {
            mode: DurabilityMode::Permanent,
            ..Default::default()
        };
        let first: Vec<DfValue> = vec![10.into(), "Cat".into()];
        let second: Vec<DfValue> = vec![20.into(), "Bob".into()];
        {
            let mut state = PersistentState::new(
                name.clone(),
                Vec::<Box<[usize]>>::new(),
                &params,
                PersistenceType::BaseTable,
            )
            .unwrap();
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
            state
                .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
                .unwrap();
        }

        let state = PersistentState::new(
            name,
            Vec::<Box<[usize]>>::new(),
            &params,
            PersistenceType::BaseTable,
        )
        .unwrap();
        match state.lookup(&[0], &PointKey::Single(10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &PointKey::Single("Bob".into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_recover_unique_key() {
        let (_dir, name) = get_tmp_path();
        let params = PersistenceParameters {
            mode: DurabilityMode::Permanent,
            ..Default::default()
        };
        let first: Vec<DfValue> = vec![10.into(), "Cat".into()];
        let second: Vec<DfValue> = vec![20.into(), "Bob".into()];
        {
            let mut state = PersistentState::new(
                name.clone(),
                Some(&[0]),
                &params,
                PersistenceType::BaseTable,
            )
            .unwrap();
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
            state
                .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
                .unwrap();
        }

        let state =
            PersistentState::new(name, Some(&[0]), &params, PersistenceType::BaseTable).unwrap();
        match state.lookup(&[0], &PointKey::Single(10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &PointKey::Single("Bob".into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_remove() {
        let mut state = setup_persistent("persistent_state_remove", None);
        let first: Vec<DfValue> = vec![10.into(), "Cat".into()];
        let duplicate: Vec<DfValue> = vec![10.into(), "Other Cat".into()];
        let second: Vec<DfValue> = vec![20.into(), "Cat".into()];
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
        state
            .process_records(
                &mut vec![first.clone(), duplicate.clone(), second.clone()].into(),
                None,
                None,
            )
            .unwrap();
        state
            .process_records(
                &mut vec![(first.clone(), false), (first.clone(), false)].into(),
                None,
                None,
            )
            .unwrap();

        // We only want to remove rows that match exactly, not all rows that match the key
        match state.lookup(&[0], &PointKey::Single(first[0].clone())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &duplicate);
            }
            _ => unreachable!(),
        };

        // Also should have removed the secondary CF
        match state.lookup(&[1], &PointKey::Single(first[1].clone())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        };

        // Also shouldn't have removed other keys:
        match state.lookup(&[0], &PointKey::Single(second[0].clone())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }

        // Make sure we didn't remove secondary keys pointing to different rows:
        match state.lookup(&[1], &PointKey::Single(second[1].clone())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_remove_with_unique_secondary() {
        let mut state = setup_persistent("persistent_state_remove_unique", Some(&[2usize][..]));
        let first: Vec<DfValue> = vec![10.into(), "Cat".into(), DfValue::None];
        let duplicate: Vec<DfValue> = vec![10.into(), "Other Cat".into(), DfValue::None];
        let second: Vec<DfValue> = vec![20.into(), "Cat".into(), DfValue::None];
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
        state.add_index(Index::new(IndexType::HashMap, vec![2]), None);
        state
            .process_records(
                &mut vec![first.clone(), duplicate.clone(), second.clone()].into(),
                None,
                None,
            )
            .unwrap();
        state
            .process_records(
                &mut vec![(first.clone(), false), (first.clone(), false)].into(),
                None,
                None,
            )
            .unwrap();

        for i in 0..3usize {
            // Make sure we removed the row for every CF
            match state.lookup(&[i], &PointKey::Single(first[i].clone())) {
                LookupResult::Some(RecordResult::Owned(rows)) => {
                    assert!(!rows.is_empty());
                    assert!(rows.iter().all(|row| row[i] == first[i] && row != &first));
                }
                _ => unreachable!(),
            };
        }

        // Make sure we have all of our unique nulls intact
        match state.lookup(&[2], &PointKey::Single(DfValue::None)) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&rows[0], &duplicate);
                assert_eq!(&rows[1], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_is_useful() {
        let mut state = setup_persistent("persistent_state_is_useful", None);
        assert!(!state.is_useful());
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        assert!(state.is_useful());
    }

    #[test]
    fn persistent_state_rows() {
        let mut state = setup_persistent("persistent_state_rows", None);
        let mut rows = vec![];
        for i in 0..30 {
            let row = vec![DfValue::from(i); 30];
            rows.push(row);
            state.add_index(Index::new(IndexType::HashMap, vec![i]), None);
        }

        for row in rows.iter().cloned() {
            insert(&mut state, row);
        }

        let count = state.row_count();
        // rows() is estimated, but we want to make sure we at least don't return
        // self.indices.len() * rows.len() here.
        assert!(count > 0 && count < rows.len() * 2);
    }

    mod all_records {
        use pretty_assertions::assert_eq;

        use super::*;

        #[test]
        fn simple_case() {
            let mut state = setup_persistent("persistent_state_cloned_records", None);
            let first: Vec<DfValue> = vec![10.into(), "Cat".into()];
            let second: Vec<DfValue> = vec![20.into(), "Cat".into()];
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
            state
                .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
                .unwrap();

            let mut all_records = state.all_records();
            assert_eq!(
                all_records.read().iter().collect::<Vec<_>>(),
                vec![first, second]
            );
        }

        #[test]
        fn wonky_drop_order() {
            let mut state = setup_persistent("persistent_state_cloned_records", None);
            let first: Vec<DfValue> = vec![10.into(), "Cat".into()];
            let second: Vec<DfValue> = vec![20.into(), "Cat".into()];
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
            state
                .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
                .unwrap();
            let mut all_records = state.all_records();
            drop(state);

            assert_eq!(
                all_records.read().iter().collect::<Vec<_>>(),
                vec![first, second]
            );
        }

        #[test]
        fn writes_during_iter() {
            let mut state = setup_persistent("persistent_state_cloned_records", None);
            let first: Vec<DfValue> = vec![10.into(), "Cat".into()];
            let second: Vec<DfValue> = vec![20.into(), "Cat".into()];
            state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_index(Index::new(IndexType::HashMap, vec![1]), None);
            state
                .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
                .unwrap();
            let mut all_records = state.all_records();
            let mut guard = all_records.read();
            let iter = guard.iter();
            state
                .process_records(&mut vec![first.clone(), second.clone()].into(), None, None)
                .unwrap();
            drop(state);

            assert_eq!(iter.collect::<Vec<_>>(), vec![first, second]);
        }
    }

    #[test]
    #[cfg(not(windows))]
    fn persistent_state_drop() {
        let path = {
            let state = PersistentState::new(
                String::from(".s-o_u#p."),
                Vec::<Box<[usize]>>::new(),
                &PersistenceParameters::default(),
                PersistenceType::BaseTable,
            )
            .unwrap();
            let path = state._tmpdir.as_ref().unwrap().path();
            assert!(path.exists());
            String::from(path.to_str().unwrap())
        };

        assert!(!PathBuf::from(path).exists());
    }

    #[test]
    fn persistent_state_old_records_new_index() {
        let mut state = setup_persistent("persistent_state_old_records_new_index", None);
        let row: Vec<DfValue> = vec![10.into(), "Cat".into()];
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        insert(&mut state, row.clone());
        state.add_index(Index::new(IndexType::HashMap, vec![1]), None);

        match state.lookup(&[1], &PointKey::Single(row[1].clone())) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(&rows[0], &row),
            _ => unreachable!(),
        };
    }

    #[test]
    fn persistent_state_process_records() {
        let mut state = setup_persistent("persistent_state_process_records", None);
        let records: Records = vec![
            (vec![1.into(), "A".into()], true),
            (vec![2.into(), "B".into()], true),
            (vec![3.into(), "C".into()], true),
            (vec![1.into(), "A".into()], false),
        ]
        .into();

        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        state
            .process_records(&mut Vec::from(&records[..3]).into(), None, None)
            .unwrap();
        state
            .process_records(&mut records[3].clone().into(), None, None)
            .unwrap();

        // Make sure the first record has been deleted:
        match state.lookup(&[0], &PointKey::Single(records[0][0].clone())) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        // Then check that the rest exist:
        for record in &records[1..3] {
            match state.lookup(&[0], &PointKey::Single(record[0].clone())) {
                LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows[0], **record),
                _ => unreachable!(),
            };
        }
    }

    #[test]
    fn replication_offset_roundtrip_mysql() {
        let mut state = setup_persistent("replication_offset_roundtrip_mysql", None);
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        let mut records: Records = vec![(vec![1.into(), "A".into()], true)].into();
        let replication_offset = ReplicationOffset::MySql(
            MySqlPosition::from_file_name_and_position("binlog.00001".to_owned(), 12).unwrap(),
        );
        state
            .process_records(&mut records, None, Some(replication_offset.clone()))
            .unwrap();
        let result = state.replication_offset();
        assert_eq!(result, Some(&replication_offset));
    }

    #[test]
    fn replication_offset_roundtrip_postgres() {
        let mut state = setup_persistent("replication_offset_roundtrip_postgres", None);
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        let mut records: Records = vec![(vec![1.into(), "A".into()], true)].into();
        let replication_offset = ReplicationOffset::Postgres(PostgresPosition {
            commit_lsn: 12.into(),
            lsn: 0.into(),
        });
        state
            .process_records(&mut records, None, Some(replication_offset.clone()))
            .unwrap();
        let result = state.replication_offset();
        assert_eq!(result, Some(&replication_offset));
    }

    #[test]
    #[allow(clippy::op_ref)]
    fn persistent_state_prefix_transform() {
        let mut state = setup_persistent("persistent_state_prefix_transform", None);
        state.add_index(Index::new(IndexType::HashMap, vec![0]), None);
        let data = (DfValue::from(1), DfValue::from(10));
        let r = PointKey::Double(data.clone());
        let k = PersistentState::serialize_prefix(&r);
        let prefix = prefix_transform(&k);
        let size: u64 = bincode::options()
            .allow_trailing_bytes()
            .deserialize(prefix)
            .unwrap();
        assert_eq!(size, bincode::options().serialized_size(&data).unwrap());

        // prefix_extractor requirements:
        // 1) key.starts_with(prefix(key))
        assert!(k.starts_with(prefix));

        // 2) Compare(prefix(key), key) <= 0.
        assert!(prefix <= &k[..]);

        // 3) If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
        let other_k = PersistentState::serialize_prefix(&r);
        let other_prefix = prefix_transform(&other_k);
        assert!(k <= other_k);
        assert!(prefix <= other_prefix);

        // 4) prefix(prefix(key)) == prefix(key)
        assert_eq!(prefix, prefix_transform(prefix));
    }

    #[test]
    fn reindex_btree_with_nulls() {
        let mut state = setup_persistent("reindex_with_nulls", None);
        state.add_index(Index::hash_map(vec![0]), None);
        insert(&mut state, vec![1.into()]);
        insert(&mut state, vec![DfValue::None]);
        state.add_index(Index::btree_map(vec![0]), None);
    }

    #[test]
    /// Test that a read handle will miss on lookups unless it was informed of the same binlog
    /// position as the parent handle, this is important to avoid accidental reorder of upqueries
    /// and forward processing in nodes that would use the read handle for upqueries.
    fn read_handle_misses_on_binlog() {
        let mut state = setup_persistent("read_handle_misses_on_binlog", None);
        state.add_index(Index::hash_map(vec![0]), None);

        state
            .process_records(
                &mut (0..10)
                    .map(|n| Record::from(vec![n.into()]))
                    .collect::<Records>(),
                None,
                Some(ReplicationOffset::Postgres(PostgresPosition {
                    commit_lsn: 1.into(),
                    lsn: 0.into(),
                })),
            )
            .unwrap();

        let mut rh = state.read_handle();
        // When we first create the rh, it is up to date
        assert!(rh.do_lookup(&[0], &PointKey::Single(0.into())).is_some());

        // Process more records ...
        state
            .process_records(
                &mut (0..10)
                    .map(|n| Record::from(vec![n.into()]))
                    .collect::<Records>(),
                None,
                Some(ReplicationOffset::Postgres(PostgresPosition {
                    commit_lsn: 2.into(),
                    lsn: 0.into(),
                })),
            )
            .unwrap();

        // Now read handle is behind, since it didn't get the forward processing yet
        assert!(rh.do_lookup(&[0], &PointKey::Single(0.into())).is_none());

        rh.process_records(
            &mut Records::from(Vec::<Record>::new()),
            None,
            Some(ReplicationOffset::Postgres(PostgresPosition {
                commit_lsn: 2.into(),
                lsn: 0.into(),
            })),
        )
        .unwrap();

        // Read handle is up to date now
        assert!(rh.do_lookup(&[0], &PointKey::Single(0.into())).is_some());
    }

    mod lookup_range {
        use std::iter;

        use pretty_assertions::assert_eq;
        use readyset_data::IntoBoundedRange;
        use vec1::vec1;

        use super::*;

        fn setup() -> PersistentState {
            let mut state = setup_persistent("persistent_state_single_key", None);
            state.add_index(Index::btree_map(vec![0]), None);
            state
                .process_records(
                    &mut (0..10)
                        .map(|n| Record::from(vec![n.into()]))
                        .collect::<Records>(),
                    None,
                    None,
                )
                .unwrap();
            state
        }

        #[test]
        fn missing() {
            let state = setup();
            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&(vec1![DfValue::from(11)]..vec1![DfValue::from(20)]))
                ),
                RangeLookupResult::Some(vec![].into())
            );
        }

        #[test]
        fn inclusive_exclusive() {
            let state = setup();
            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&(vec1![DfValue::from(3)]..vec1![DfValue::from(7)]))
                ),
                RangeLookupResult::Some((3..7).map(|n| vec![n.into()]).collect::<Vec<_>>().into())
            );
        }

        #[test]
        fn inclusive_inclusive() {
            let state = setup();
            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&(vec1![DfValue::from(3)]..=vec1![DfValue::from(7)]))
                ),
                RangeLookupResult::Some((3..=7).map(|n| vec![n.into()]).collect::<Vec<_>>().into())
            );
        }

        #[test]
        fn exclusive_exclusive() {
            let state = setup();
            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&(
                        Bound::Excluded(vec1![DfValue::from(3)]),
                        Bound::Excluded(vec1![DfValue::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (3..7)
                        .skip(1)
                        .map(|n| vec![n.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn exclusive_exclusive_skip_all() {
            let mut state = setup();
            // ENG-1559: If state has more than one key for the exclusive start bound, it has to
            // skip them all
            state
                .process_records(&mut vec![Record::from(vec![3.into()])].into(), None, None)
                .unwrap();
            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&(
                        Bound::Excluded(vec1![DfValue::from(3)]),
                        Bound::Excluded(vec1![DfValue::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (3..7)
                        .skip(1)
                        .map(|n| vec![n.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn exclusive_inclusive() {
            let state = setup();
            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&(
                        Bound::Excluded(vec1![DfValue::from(3)]),
                        Bound::Included(vec1![DfValue::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (3..=7)
                        .skip(1)
                        .map(|n| vec![n.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn exclusive_inclusive_missing() {
            let mut state = setup();
            // ENG-1560: When the upper included bound is not actually in the map, shouldn't read
            // past it anyway
            state
                .process_records(
                    &mut vec![Record::from((vec![7.into()], false))].into(),
                    None,
                    None,
                )
                .unwrap();

            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&(
                        Bound::Excluded(vec1![DfValue::from(3)]),
                        Bound::Included(vec1![DfValue::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (3..7)
                        .skip(1)
                        .map(|n| vec![n.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn inclusive_unbounded() {
            let state = setup();
            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&vec1![DfValue::from(3)].range_from_inclusive())
                ),
                RangeLookupResult::Some((3..10).map(|n| vec![n.into()]).collect::<Vec<_>>().into())
            );
        }

        #[test]
        fn unbounded_inclusive_multiple_rows_in_upper_bound() {
            let mut state = setup();
            state
                .process_records(&mut vec![vec![DfValue::from(3)]].into(), None, None)
                .unwrap();

            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&vec1![DfValue::from(3)].range_to_inclusive())
                ),
                RangeLookupResult::Some(
                    vec![
                        vec![DfValue::from(0)],
                        vec![DfValue::from(1)],
                        vec![DfValue::from(2)],
                        vec![DfValue::from(3)],
                        vec![DfValue::from(3)],
                    ]
                    .into()
                )
            )
        }

        #[test]
        fn non_unique_then_reindex() {
            let mut state = setup_persistent("persistent_state_single_key", Some(&[1][..]));
            state
                .process_records(
                    &mut [0, 0, 1, 1, 2, 2, 3, 3]
                        .iter()
                        .enumerate()
                        .map(|(i, n)| Record::from(vec![(*n).into(), i.into()]))
                        .collect::<Records>(),
                    None,
                    None,
                )
                .unwrap();
            state.add_index(Index::btree_map(vec![0]), None);

            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&vec1![DfValue::from(2)].range_from_inclusive())
                ),
                RangeLookupResult::Some(
                    [(2, 4), (2, 5), (3, 6), (3, 7)]
                        .iter()
                        .map(|&(n, i)| vec![n.into(), i.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn unbounded_inclusive() {
            let state = setup();
            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&vec1![DfValue::from(3)].range_to_inclusive())
                ),
                RangeLookupResult::Some((0..=3).map(|n| vec![n.into()]).collect::<Vec<_>>().into())
            );
        }

        #[test]
        fn unbounded_exclusive() {
            let state = setup();
            assert_eq!(
                state.lookup_range(&[0], &RangeKey::from(&vec1![DfValue::from(3)].range_to())),
                RangeLookupResult::Some((0..3).map(|n| vec![n.into()]).collect::<Vec<_>>().into())
            );
        }

        fn setup_secondary() -> PersistentState {
            let mut state = setup_persistent("reindexed", Some(&[0usize][..]));
            state
                .process_records(
                    &mut (-10..10)
                        .map(|i| Record::from(row_for_secondary_key(i)))
                        .collect::<Records>(),
                    None,
                    None,
                )
                .unwrap();
            state.add_index(Index::btree_map(vec![0]), None);
            state.add_index(Index::btree_map(vec![1]), None);
            state
        }

        fn row_for_secondary_key(k: i64) -> Vec<DfValue> {
            vec![(k - 1).into(), k.into(), (k + 1).into()]
        }

        #[test]
        fn inclusive_unbounded_secondary() {
            let state = setup_secondary();
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&vec1![DfValue::from(3)].range_from_inclusive())
                ),
                RangeLookupResult::Some(
                    (3..10)
                        .map(row_for_secondary_key)
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn exclusive_unbounded_secondary_big_values() {
            let mut state =
                setup_persistent("exclusive_unbounded_secondary_2", Some(&[0usize][..]));
            state
                .process_records(
                    &mut [
                        (0, 1221662829),
                        (1, -1708946381),
                        (2, -1499655272),
                        (3, -2116759780),
                        (4, -156921416),
                        (5, -2088438952),
                        (6, -567360636),
                        (7, -2025118595),
                        (8, 555671065),
                        (9, 925768521),
                    ]
                    .iter()
                    .copied()
                    .map(|(n1, n2)| Record::from(vec![n1.into(), n2.into()]))
                    .collect::<Records>(),
                    None,
                    None,
                )
                .unwrap();
            state.add_index(Index::btree_map(vec![1]), None);
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&vec1![DfValue::from(10)].range_from())
                ),
                RangeLookupResult::Some(
                    [(8, 555671065), (9, 925768521), (0, 1221662829)]
                        .iter()
                        .copied()
                        .map(|(n1, n2)| vec![n1.into(), n2.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn exclusive_inclusive_secondary() {
            let state = setup_secondary();
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&(
                        Excluded(vec1![DfValue::from(3)]),
                        Included(vec1![DfValue::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (4..=7)
                        .map(row_for_secondary_key)
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn exclusive_exclusive_secondary() {
            let state = setup_secondary();
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&(
                        Excluded(vec1![DfValue::from(3)]),
                        Excluded(vec1![DfValue::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (4..7).map(row_for_secondary_key).collect::<Vec<_>>().into()
                )
            );
        }

        #[test]
        fn inclusive_exclusive_secondary() {
            let state = setup_secondary();
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&(
                        Included(vec1![DfValue::from(3)]),
                        Excluded(vec1![DfValue::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (3..7).map(row_for_secondary_key).collect::<Vec<_>>().into()
                )
            );
        }

        #[test]
        fn inclusive_inclusive_secondary() {
            let state = setup_secondary();
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&(
                        Excluded(vec1![DfValue::from(3)]),
                        Included(vec1![DfValue::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (4..=7)
                        .map(row_for_secondary_key)
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn unbounded_inclusive_secondary() {
            let state = setup_secondary();
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&vec1![DfValue::from(7)].range_to_inclusive())
                ),
                RangeLookupResult::Some(
                    (-10..=7)
                        .map(row_for_secondary_key)
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn unbounded_exclusive_secondary() {
            let state = setup_secondary();
            assert_eq!(
                state.lookup_range(&[1], &RangeKey::from(&vec1![DfValue::from(7)].range_to())),
                RangeLookupResult::Some(
                    (-10..7)
                        .map(row_for_secondary_key)
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn inclusive_unbounded_secondary_compound() {
            let mut state = setup_secondary();
            state.add_index(Index::btree_map(vec![0, 1]), None);
            assert_eq!(
                state.lookup_range(
                    &[0, 1],
                    &RangeKey::from(
                        &vec1![DfValue::from(2), DfValue::from(3)].range_from_inclusive()
                    )
                ),
                RangeLookupResult::Some(
                    (3..10)
                        .map(row_for_secondary_key)
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn inclusive_unbounded_secondary_non_unique() {
            let mut state = setup_secondary();
            let extra_row_beginning = vec![DfValue::from(11), DfValue::from(3), DfValue::from(3)];
            let extra_row_end = vec![DfValue::from(12), DfValue::from(9), DfValue::from(9)];

            state
                .process_records(
                    &mut vec![extra_row_beginning.clone(), extra_row_end.clone()].into(),
                    None,
                    None,
                )
                .unwrap();

            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&vec1![DfValue::from(3)].range_from_inclusive())
                ),
                RangeLookupResult::Some(
                    vec![vec![2.into(), 3.into(), 4.into()], extra_row_beginning]
                        .into_iter()
                        .chain((4..10).map(row_for_secondary_key))
                        .chain(iter::once(extra_row_end))
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn citext() {
            let mut state = setup();
            state.add_index(Index::btree_map(vec![0]), None);
            state
                .process_records(
                    &mut vec![
                        vec![DfValue::from_str_and_collation("a", Collation::Citext)],
                        vec![DfValue::from_str_and_collation("B", Collation::Citext)],
                        vec![DfValue::from_str_and_collation("c", Collation::Citext)],
                        vec![DfValue::from_str_and_collation("D", Collation::Citext)],
                    ]
                    .into(),
                    None,
                    None,
                )
                .unwrap();

            let result = state
                .lookup_range(
                    &[0],
                    &RangeKey::from(&(
                        Included(vec1![DfValue::from_str_and_collation(
                            "b",
                            Collation::Citext
                        )]),
                        Included(vec1![DfValue::from_str_and_collation(
                            "c",
                            Collation::Citext
                        )]),
                    )),
                )
                .unwrap();

            assert_eq!(
                result,
                vec![
                    vec![DfValue::from_str_and_collation("B", Collation::Citext)],
                    vec![DfValue::from_str_and_collation("c", Collation::Citext)],
                ]
                .into()
            )
        }
    }
}
