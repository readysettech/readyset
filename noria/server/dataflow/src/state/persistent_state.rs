//! Node state that's persisted to disk
//!
//! The [`PersistedState`] struct is an implementation of [`State`] that stores rows (currently
//! only for base tables) in [RocksDB][0], an on-disk key-value store. The data is stored in
//! [indices](PersistentState::indices) - each lookup index stores the copies of all the rows in
//! the database.
//!
//! # Internals
//!
//! Each lookup index is stored in a separate [`ColumnFamily`](https://github.com/facebook/rocksdb/wiki/Column-Families).
//! The name of the ColumnFamily is the index it occupies in the `indices` vector so we can find
//! the CF for each lookup index.
//! Internally RocksDB stores the keys as Sorted Strings, and looks them up using binary search. Lookups
//! are performed either according to the full key value, or possibly by using a prefix. Therefore for
//! each key that we know to be unique we simply store the serialized representation of the key as
//! `(serialized_key_len || key)`, with the value stored being the serialized row.
//! For keys that are not unique, we either append (epoch, seq) for the primary index, or the primary key
//! itself if it is a secondary index.
//!
//! # Replication Offsets
//!
//! When running in a read-replica configuration, where a thread is run as part of the controller
//! that reads the replication log from the underlying database, we need to persist the *offset* in
//! that replication log of the last record that we have successfully applied. To maintain
//! atomicity, these offsets are stored inside of rocksdb as part of the persisted
//! [`PersistentMeta`], and updated as part of every write.
//!
//! [0]: https://rocksdb.org/

use crate::node::special::base::SnapshotMode;
use crate::prelude::*;
use crate::state::{RangeLookupResult, RecordResult, State};
use bincode::Options;
use common::SizeOf;
use noria::replication::ReplicationOffset;
use noria::KeyComparison;
use rocksdb::{
    self, Direction, IteratorMode, PlainTableFactoryOptions, SliceTransform, WriteBatch,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::ops::Bound;
use tempfile::{tempdir, TempDir};
use tracing::error;

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

// Maximum rows per WriteBatch when building new indices for existing rows.
const INDEX_BATCH_SIZE: usize = 10_000;

/// Load the metadata from the database, stored in the `DEFAULT_CF` column family under the `META_KEY`
fn get_meta(db: &rocksdb::DB) -> PersistentMeta<'static> {
    db.get_pinned(META_KEY)
        .unwrap()
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
        .unwrap_or_default()
}

/// Abstraction over writing to different kinds of rocksdb dbs.
///
/// This trait is (consciously) incomplete - if necessary, a more complete version including
/// *put_cf* etc could be put inside a utility module somewhere
trait Put: Sized {
    /// Write a key/value pair
    ///
    /// This method is prefixed with "do" so that it doesn't conflict with the `put` method on both
    /// [`rocksdb::DB`] and [`rocksdb::WriteBatch`]
    fn do_put<K, V>(self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    fn save_meta(self, meta: &PersistentMeta) {
        self.do_put(META_KEY, serde_json::to_string(meta).unwrap());
    }
}

impl Put for &rocksdb::DB {
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
fn increment_epoch(db: &rocksdb::DB) -> PersistentMeta<'static> {
    let mut meta = get_meta(db);
    meta.epoch += 1;
    db.save_meta(&meta);
    meta
}

/// Data structure used to persist metadata about the [`PersistentState`] to rocksdb
#[derive(Default, Serialize, Deserialize)]
struct PersistentMeta<'a> {
    /// Index information is stored in RocksDB to avoid rebuilding indices on recovery
    indices: Vec<Box<[usize]>>,
    epoch: IndexEpoch,

    /// The latest replication offset that has been written to the base table backed by this
    /// [`PersistentState`]. Corresponds to [`PersistentState::replication_offset`]
    replication_offset: Option<Cow<'a, ReplicationOffset>>,
}

#[derive(Clone)]
struct PersistentIndex {
    column_family: String,
    columns: Box<[usize]>,
    is_unique: bool,
}

/// PersistentState stores data in RocksDB.
pub struct PersistentState {
    db_opts: rocksdb::Options,
    db: rocksdb::DB,
    // The lookup indices stored for this table. The first element is always considered the primary index
    indices: Vec<PersistentIndex>,
    // The list of all the indices that are defined as unique in the schema for this table
    unique_keys: Vec<Box<[usize]>>,
    seq: IndexSeq,
    epoch: IndexEpoch,
    /// The latest replication offset that has been written to the base table backed by this
    /// [`PersistentState`]
    replication_offset: Option<ReplicationOffset>,
    // With DurabilityMode::DeleteOnExit,
    // RocksDB files are stored in a temporary directory.
    _directory: Option<TempDir>,
    /// When set to true [`SnapshotMode::SnapshotModeEnabled`] compaction will be disabled and writes will
    /// bypass WAL and fsync
    pub(crate) snapshot_mode: SnapshotMode,
}

impl<'a> PersistentMeta<'a> {
    fn get_indices(&self, unique_keys: &[Box<[usize]>]) -> Vec<PersistentIndex> {
        self.indices
            .iter()
            .enumerate()
            .map(|(i, columns)| PersistentIndex {
                is_unique: check_if_index_is_unique(unique_keys, columns),
                column_family: i.to_string(),
                columns: columns.clone(),
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
    ) {
        assert!(partial_tag.is_none(), "PersistentState can't be partial");
        if records.len() == 0 && replication_offset.is_none() {
            return;
        }

        let mut batch = WriteBatch::default();
        for r in records.iter() {
            match *r {
                Record::Positive(ref r) => {
                    self.insert(&mut batch, r);
                }
                Record::Negative(ref r) => {
                    self.remove(&mut batch, r);
                }
            }
        }

        let mut opts = rocksdb::WriteOptions::default();
        if self.snapshot_mode.is_enabled()
            // if we're setting the replication offset, that means we've snapshot the full table, so
            // set sync to true there even if snapshot_mode is enabled, to make sure that makes it
            // onto disk (not doing this *will* cause the write to get lost if the server restarts!)
            && replication_offset.is_none()
        {
            opts.disable_wal(true);
        } else {
            if self.snapshot_mode.is_enabled() && replication_offset.is_some() {
                // We are setting the replication offset, which is great, but
                // all of our previous rights are not guranteed to flush to disk even
                // if the next write is synced. We therefore perform a flush before
                // handling the next write.
                // See: https://github.com/facebook/rocksdb/wiki/RocksDB-FAQhttps://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
                // Q: After a write following option.disableWAL=true, I write another record with options.sync=true,
                //    will it persist the previous write too?
                // A: No. After the program crashes, writes with option.disableWAL=true will be lost, if they are not flushed
                //    to SST files.
                self.db
                    .flush_cf(self.db.cf_handle(PK_CF).unwrap())
                    .expect("Flush to disk failed");
            }
            opts.set_sync(true);
        }

        if let Some(offset) = replication_offset {
            self.set_replication_offset(&mut batch, offset);
        }

        tokio::task::block_in_place(|| self.db.write_opt(batch, &opts)).unwrap();
    }

    fn replication_offset(&self) -> Option<&ReplicationOffset> {
        self.replication_offset.as_ref()
    }

    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let index = self.index(columns);

        tokio::task::block_in_place(|| {
            let cf = self.db.cf_handle(&index.column_family).unwrap();
            let prefix = Self::serialize_prefix(key);
            let data = if index.is_unique && !key.has_null() {
                // This is a unique key, so we know there's only one row to retrieve
                // (no need to use prefix_iterator).
                let raw_row = self.db.get_pinned_cf(cf, &prefix).unwrap();
                if let Some(raw) = raw_row {
                    vec![bincode::options().deserialize(&raw).unwrap()]
                } else {
                    vec![]
                }
            } else {
                // This could correspond to more than one value, so we'll use a prefix_iterator:
                let mut rows = Vec::new();
                let mut iter = self.db.raw_iterator_cf(cf);
                iter.seek(&prefix); // Find the first key

                while iter.key().map(|k| k.starts_with(&prefix)).unwrap_or(false) {
                    let val = deserialize(iter.value().unwrap());
                    rows.push(val);
                    iter.next(); // Find the next row for this key
                }

                rows
            };

            LookupResult::Some(RecordResult::Owned(data))
        })
    }

    fn lookup_range<'a>(&'a self, columns: &[usize], key: &RangeKey) -> RangeLookupResult<'a> {
        let db = &self.db;
        let index = self.index(columns);

        tokio::task::block_in_place(|| {
            let cf = db.cf_handle(&index.column_family).unwrap();
            let (lower, upper) = Self::serialize_range(key, ((), ()));

            let mut opts = rocksdb::ReadOptions::default();
            // Necessary to override prefix extraction - see
            // https://github.com/facebook/rocksdb/wiki/Prefix-Seek#how-to-ignore-prefix-bloom-filters-in-read
            opts.set_total_order_seek(true);

            match upper {
                Bound::Excluded(k) => {
                    opts.set_iterate_upper_bound(k);
                }
                Bound::Included(mut k) => {
                    // Rocksdb's iterate_upper_bound is exclusive, so add 1 to the last byte of the
                    // end value so we get our inclusive last key
                    if let Some(byte) = k.last_mut() {
                        *byte += 1;
                    }
                    opts.set_iterate_upper_bound(k);
                }
                _ => {}
            }

            let start = match &lower {
                Bound::Unbounded => None,
                Bound::Included(k) | Bound::Excluded(k) => Some(k),
            };

            let mut iterator = db.iterator_cf_opt(
                cf,
                opts,
                match start.as_ref() {
                    Some(k) => IteratorMode::From(k, Direction::Forward),
                    None => IteratorMode::Start,
                },
            );

            if matches!(lower, Bound::Excluded(_)) {
                iterator.next();
            }

            RangeLookupResult::Some(RecordResult::Owned(
                iterator
                    .map(|(_, value)| {
                        bincode::options()
                            .deserialize(&*value)
                            .expect("Error deserializing data from rocksdb")
                    })
                    .collect(),
            ))
        })
    }

    fn as_persistent(&self) -> Option<&PersistentState> {
        Some(self)
    }

    fn as_persistent_mut(&mut self) -> Option<&mut PersistentState> {
        Some(self)
    }

    /// Add a new index to the table, each index contains a copy of the data
    /// Panics if partial is Some
    fn add_key(&mut self, index: Index, partial: Option<Vec<Tag>>) {
        #[allow(clippy::panic)] // This should definitely never happen!
        {
            assert!(partial.is_none(), "Bases can't be partial");
        }
        let columns = &index.columns;
        let existing = self
            .indices
            .iter()
            .any(|index| &index.columns[..] == columns);

        if existing {
            return;
        }

        let columns: Box<[usize]> = columns.clone().into();

        // We'll store all the values for this index in its own column family:
        let index_id = self.indices.len().to_string();

        tokio::task::block_in_place(|| {
            self.create_cf(&index_id);

            let db = &mut self.db;
            let cf = db.cf_handle(&index_id).unwrap();
            let is_unique = check_if_index_is_unique(&self.unique_keys, &columns);

            // Prevent autocompactions while we reindex the whole table
            if let Err(err) = db.set_options_cf(cf, &[("disable_auto_compactions", "true")]) {
                error!(%err, "Error setting cf options");
            }

            let mut opts = rocksdb::WriteOptions::default();
            opts.disable_wal(true);

            if let Some(primary_index) = self.indices.first() {
                // This is not the first index that we add to this state, and we may already
                // have some data stored in the table. We use the first index to iterate
                // over the data and copy it to the newly created column family.
                let primary_cf = db.cf_handle(&primary_index.column_family).unwrap();

                let mut read_opts = rocksdb::ReadOptions::default();
                read_opts.set_total_order_seek(true);
                let mut iter = db.raw_iterator_cf_opt(primary_cf, read_opts);

                iter.seek_to_first();

                while iter.valid() {
                    let mut batch = WriteBatch::default();

                    while let (Some(pk), Some(value)) = (iter.key(), iter.value()) {
                        if batch.len() == INDEX_BATCH_SIZE {
                            break;
                        }

                        let row: Vec<DataType> = deserialize(value);
                        let index_key = Self::build_key(&row, &columns);
                        let key = if is_unique && !index_key.has_null() {
                            // We know this key to be unique, so we just use it as is
                            Self::serialize_prefix(&index_key)
                        } else {
                            Self::serialize_secondary(&index_key, pk)
                        };
                        batch.put_cf(cf, &key, value);

                        iter.next();
                    }

                    db.write_opt(batch, &opts).unwrap();
                }
            }
            // Manually compact the newly created column family
            db.compact_range_cf(cf, Option::<&[u8]>::None, Option::<&[u8]>::None);
            // Flush just in case
            db.flush_cf(cf).unwrap();
            // Reenable auto compactions when done
            if let Err(err) = db.set_options_cf(cf, &[("disable_auto_compactions", "false")]) {
                error!(%err, "Error setting cf options");
            }

            self.indices.push(PersistentIndex {
                columns,
                column_family: index_id,
                is_unique,
            });

            self.persist_meta();
        });
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        self.all_rows()
            .map(|(_, ref value)| deserialize(value))
            .collect()
    }

    /// Returns a row count estimate from RocksDB.
    fn rows(&self) -> usize {
        tokio::task::block_in_place(|| {
            let db = &self.db;
            let cf = db.cf_handle(PK_CF).unwrap();
            let total_keys = db
                .property_int_value_cf(cf, "rocksdb.estimate-num-keys")
                .unwrap()
                .unwrap() as usize;

            total_keys / self.indices.len()
        })
    }

    fn is_useful(&self) -> bool {
        !self.indices.is_empty()
    }

    fn is_partial(&self) -> bool {
        false
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
    fn evict_bytes(&mut self, _: usize) -> Option<super::StateEvicted> {
        unreachable!("can't evict keys from PersistentState")
    }

    /// Panics if called
    #[allow(clippy::unreachable)] // this should never happen!
    fn evict_keys(&mut self, _: Tag, _: &[KeyComparison]) -> Option<(&[usize], u64)> {
        unreachable!("can't evict keys from PersistentState")
    }

    /// Panics if called
    #[allow(clippy::unreachable)] // this should never happen!
    fn clear(&mut self) {
        unreachable!("can't clear PersistentState")
    }

    fn add_weak_key(&mut self, index: Index) {
        self.add_key(index, None);
    }

    fn lookup_weak<'a>(&'a self, columns: &[usize], key: &KeyType) -> Option<RecordResult<'a>> {
        self.lookup(columns, key).records()
    }
}

fn serialize<K: serde::Serialize, E: serde::Serialize>(k: K, extra: E) -> Vec<u8> {
    let size: u64 = bincode::options().serialized_size(&k).unwrap();
    bincode::options().serialize(&(size, k, extra)).unwrap()
}

fn deserialize<'a, T: serde::Deserialize<'a>>(bytes: &'a [u8]) -> T {
    bincode::options()
        .deserialize(bytes)
        .expect("Deserializing from rocksdb")
}

impl PersistentState {
    pub fn new<C: AsRef<[usize]>, K: IntoIterator<Item = C>>(
        name: String,
        unique_keys: K,
        params: &PersistenceParameters,
    ) -> Self {
        let unique_keys: Vec<Box<[usize]>> =
            unique_keys.into_iter().map(|c| c.as_ref().into()).collect();

        tokio::task::block_in_place(|| {
            use rocksdb::{ColumnFamilyDescriptor, DB};
            let (directory, full_name) = match params.mode {
                DurabilityMode::Permanent => (None, format!("{}.db", name)),
                _ => {
                    let dir = tempdir().unwrap();
                    let path = dir.path().join(name.clone());
                    let full_name = format!("{}.db", path.to_str().unwrap());
                    (Some(dir), full_name)
                }
            };

            let opts = Self::build_options(&name, params);
            // We use a column family for each index, and one for metadata.
            // When opening the DB the exact same column families needs to be used,
            // so we'll have to retrieve the existing ones first:
            let cf_names = match DB::list_cf(&opts, &full_name) {
                Ok(cfs) => cfs,
                Err(_err) => vec![DEFAULT_CF.to_string()],
            };

            // ColumnFamilyDescriptor does not implement Clone, so we have to create a new Vec each time
            let make_cfs = || -> Vec<ColumnFamilyDescriptor> {
                cf_names
                    .iter()
                    .map(|cf| ColumnFamilyDescriptor::new(cf, opts.clone()))
                    .collect()
            };

            let mut retry = 0;
            let mut db = loop {
                // TODO: why is this loop even needed?
                match DB::open_cf_descriptors(&opts, &full_name, make_cfs()) {
                    Ok(db) => break db,
                    _ if retry < 100 => {
                        retry += 1;
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                    err => break err.expect("Unable to open RocksDB"),
                }
            };

            let meta = increment_epoch(&db);
            let indices = meta.get_indices(&unique_keys);

            // If there are more column families than indices (+1 to account for the default column
            // family) we either crashed while trying to build the last index (in Self::add_key), or
            // something (like failed deserialization) caused us to reset the meta to the default
            // value.
            // Either way, we should drop all column families that are in the db but not in the
            // meta.
            if cf_names.len() > indices.len() + 1 {
                for cf_name in cf_names.iter().skip(indices.len() + 1) {
                    db.drop_cf(cf_name).unwrap();
                }
            }

            let mut state = Self {
                seq: 0,
                indices,
                unique_keys,
                epoch: meta.epoch,
                replication_offset: meta.replication_offset.map(|ro| ro.into_owned()),
                db_opts: opts,
                db,
                _directory: directory,
                snapshot_mode: SnapshotMode::SnapshotModeDisabled,
            };

            if let Some(pk) = state.unique_keys.first().cloned() {
                if state.indices.is_empty() {
                    // This is the first time we're initializing this PersistentState,
                    // so persist the primary key index right away.
                    state.create_cf(PK_CF);

                    let persistent_index = PersistentIndex {
                        column_family: PK_CF.to_string(),
                        columns: pk,
                        is_unique: true,
                    };

                    state.indices.push(persistent_index);
                    state.persist_meta();
                }
            }

            state
        })
    }

    /// Create a new column family
    fn create_cf(&mut self, cf: &str) {
        // for the unwrap here, see the note on Self::db
        self.db.create_cf(cf, &self.db_opts).unwrap();
    }

    fn build_options(name: &str, params: &PersistenceParameters) -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let user_key_length = 0; // variable key length
        let bloom_bits_per_key = 10;
        let hash_table_ratio = 0.75;
        let index_sparseness = 16;
        opts.set_plain_table_factory(&PlainTableFactoryOptions {
            user_key_length,
            bloom_bits_per_key,
            hash_table_ratio,
            index_sparseness,
        });

        if let Some(ref path) = params.log_dir {
            // Append the db name to the WAL path to ensure
            // that we create a directory for each base shard:
            opts.set_wal_dir(path.join(&name));
        }

        // Create prefixes using `prefix_transform` on all new inserted keys:
        let transform = SliceTransform::create("key", prefix_transform, Some(in_domain));
        opts.set_prefix_extractor(transform);

        // Assigns the number of threads for compactions and flushes in RocksDB.
        // Optimally we'd like to use env->SetBackgroundThreads(n, Env::HIGH)
        // and env->SetBackgroundThreads(n, Env::LOW) here, but that would force us to create our
        // own env instead of relying on the default one that's shared across RocksDB instances
        // (which isn't supported by rust-rocksdb yet either).
        if params.persistence_threads > 1 {
            opts.set_max_background_jobs(params.persistence_threads);
        }

        // Increase a few default limits:
        opts.set_max_bytes_for_level_base(1024 * 1024 * 1024);
        opts.set_target_file_size_base(256 * 1024 * 1024);

        // Keep up to 4 parallel memtables:
        opts.set_max_write_buffer_number(4);

        // Use a hash linked list since we're doing prefix seeks.
        opts.set_allow_concurrent_memtable_write(false);
        opts.set_memtable_factory(rocksdb::MemtableFactory::HashLinkList {
            bucket_count: 1_000_000,
        });

        opts
    }

    fn build_key<'a>(row: &'a [DataType], columns: &[usize]) -> KeyType<'a> {
        KeyType::from(columns.iter().map(|i| &row[*i]))
    }

    /// Builds a [`PersistentMeta`] from the in-memory metadata information stored in `self`,
    /// including:
    ///
    /// * The columns of the indices
    /// * The epoch
    /// * The replication offset
    fn meta(&self) -> PersistentMeta<'_> {
        PersistentMeta {
            indices: self.indices.iter().map(|i| i.columns.clone()).collect(),
            epoch: self.epoch,
            replication_offset: self.replication_offset().map(Cow::Borrowed),
        }
    }

    /// Save metadata about this [`PersistentState`] to the db.
    ///
    /// See [Self::meta] for more information about what is saved to the db
    fn persist_meta(&mut self) {
        self.db.save_meta(&self.meta());
    }

    /// Add an operation to the given [`WriteBatch`] to set the [replication
    /// offset](PersistentMeta::replication_offset) to the given value.
    fn set_replication_offset(&mut self, batch: &mut WriteBatch, offset: ReplicationOffset) {
        // It's ok to read and update meta in two steps here since each State can (currently) only
        // be modified by a single thread.
        self.replication_offset = Some(offset);
        batch.save_meta(&self.meta());
    }

    /// Enables or disables the snapshot mode. In snapshot mode auto compactions are
    /// disabled and writes don't go to WAL first. When set to false manual compaction
    /// will be triggered, which may block for some time.
    /// In addition all column families will be dropped prior to entering this mode.
    pub(crate) fn set_snapshot_mode(&mut self, snapshot: SnapshotMode) {
        self.snapshot_mode = snapshot;

        if snapshot.is_enabled() {
            let main_index = self.indices.first().cloned();

            // Clear the data
            while let Some(index_to_drop) = self.indices.pop() {
                self.persist_meta();
                self.db.drop_cf(&index_to_drop.column_family).unwrap();
            }

            // Recreate the original primary index
            if let Some(main_index) = main_index {
                self.create_cf(PK_CF);
                self.indices.push(main_index);
                self.persist_meta();
            }

            self.replication_offset = None; // Remove any replication offset
            self.persist_meta();
        }

        let opts = &[(
            "disable_auto_compactions",
            if snapshot.is_enabled() {
                "true"
            } else {
                "false"
            },
        )];

        self.indices.first().and_then(|pi| {
            self.db.cf_handle(&pi.column_family).map(|cf| {
                // If done snapshotting, perform a manual compaction
                if !snapshot.is_enabled() {
                    tokio::task::block_in_place(|| {
                        self.db
                            .compact_range_cf(cf, Option::<&[u8]>::None, Option::<&[u8]>::None);
                    });
                }
                // Enable/disable auto compaction
                if let Err(err) = self.db.set_options_cf(cf, opts) {
                    error!(%err, "Error setting cf options");
                }
            })
        });
    }

    // Our RocksDB keys come in three forms, and are encoded as follows:
    //
    // * Unique Primary Keys
    // (size, key), where size is the serialized byte size of `key`
    // (used in `prefix_transform`).
    //
    // * Non-unique Primary Keys
    // (size, key, epoch, seq), where epoch is incremented on each recover, and seq is a
    // monotonically increasing sequence number that starts at 0 for every new epoch.
    //
    // * Secondary Index Keys
    // (size, key, primary_key), where `primary_key` makes sure that each secondary index row is
    // unique.
    //
    // Self::serialize_raw_key is responsible for serializing the underlying KeyType tuple directly
    // (without the enum variant), plus any extra information as described above.
    fn serialize_raw_key<S: serde::Serialize>(key: &KeyType, extra: S) -> Vec<u8> {
        match key {
            KeyType::Single(k) => serialize(k, extra),
            KeyType::Double(k) => serialize(k, extra),
            KeyType::Tri(k) => serialize(k, extra),
            KeyType::Quad(k) => serialize(k, extra),
            KeyType::Quin(k) => serialize(k, extra),
            KeyType::Sex(k) => serialize(k, extra),
            KeyType::Multi(k) => serialize(k, extra),
        }
    }

    fn serialize_prefix(key: &KeyType) -> Vec<u8> {
        Self::serialize_raw_key(key, ())
    }

    fn serialize_secondary(key: &KeyType, raw_primary: &[u8]) -> Vec<u8> {
        let mut bytes = Self::serialize_raw_key(key, ());
        bytes.extend_from_slice(raw_primary);
        bytes
    }

    fn serialize_range<S, T>(key: &RangeKey, extra: (S, T)) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)
    where
        S: serde::Serialize,
        T: serde::Serialize,
    {
        use Bound::*;
        fn do_serialize_range<K, S, T>(
            range: (Bound<K>, Bound<K>),
            extra: (S, T),
        ) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)
        where
            K: serde::Serialize,
            S: serde::Serialize,
            T: serde::Serialize,
        {
            (
                range.0.map(|k| serialize(k, &extra.0)),
                range.1.map(|k| serialize(k, &extra.1)),
            )
        }
        match key {
            RangeKey::Unbounded => (Unbounded, Unbounded),
            RangeKey::Single(range) => do_serialize_range(*range, extra),
            RangeKey::Double(range) => do_serialize_range(*range, extra),
            RangeKey::Tri(range) => do_serialize_range(*range, extra),
            RangeKey::Quad(range) => do_serialize_range(*range, extra),
            RangeKey::Quin(range) => do_serialize_range(*range, extra),
            RangeKey::Sex(range) => do_serialize_range(*range, extra),
            RangeKey::Multi(range) => do_serialize_range(*range, extra),
        }
    }

    // Filters out secondary indices to return an iterator for the actual key-value pairs.
    fn all_rows(&self) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> + '_ {
        let cf = self.db.cf_handle(&self.indices[0].column_family).unwrap();
        self.db.full_iterator_cf(cf, rocksdb::IteratorMode::Start)
    }

    /// Inserts the row into the database by replicating it across all of the column
    /// families. The insert is performed in a context of a [`rocksdb::WriteBatch`]
    /// operation and is therefore guaranteed to be atomic.
    fn insert(&mut self, batch: &mut WriteBatch, r: &[DataType]) {
        tokio::task::block_in_place(|| {
            let primary_index = self.indices.first().expect("Insert on un-indexed state");
            let primary_key = Self::build_key(r, &primary_index.columns);
            let primary_cf = self.db.cf_handle(&primary_index.column_family).unwrap();

            // Generate a new primary key by extracting the key columns from the provided row
            // using the primary index and serialize it as RocksDB prefix.
            let serialized_pk = if primary_index.is_unique && !primary_key.has_null() {
                Self::serialize_prefix(&primary_key)
            } else {
                // The primary index may not be unique so we append a monotonically incremented
                // counter to make sure the key is unique (prefixes will be shared for non unique keys)
                self.seq += 1;
                Self::serialize_raw_key(&primary_key, (self.epoch, self.seq))
            };

            let serialized_row = bincode::options().serialize(r).unwrap();

            // First store the row for the primary index:
            batch.put_cf(primary_cf, &serialized_pk, &serialized_row);

            // Then insert the value for all the secondary indices:
            for index in self.indices[1..].iter() {
                // Construct a key with the index values, and serialize it with bincode:
                let key = Self::build_key(r, &index.columns);
                let serialized_key = if index.is_unique && !key.has_null() {
                    Self::serialize_prefix(&key)
                } else {
                    Self::serialize_secondary(&key, &serialized_pk)
                };
                let cf = self.db.cf_handle(&index.column_family).unwrap();
                batch.put_cf(cf, &serialized_key, &serialized_row);
            }
        })
    }

    fn remove(&self, batch: &mut WriteBatch, r: &[DataType]) {
        tokio::task::block_in_place(|| {
            let primary_index = self.indices.first().expect("Delete on un-indexed state");
            let primary_key = Self::build_key(r, &primary_index.columns);
            let primary_cf = self.db.cf_handle(&primary_index.column_family).unwrap();

            let prefix = Self::serialize_prefix(&primary_key);

            let serialized_pk = if primary_index.is_unique && !primary_key.has_null() {
                // This key is unique, so we can delete it as is
                prefix
            } else {
                // This is key is not unique, therefore we have to iterate over the
                // the values, looking for the first one that matches the full row
                // and then return the (full length) unique primary key associated with it
                let mut iter = self.db.raw_iterator_cf(primary_cf);
                iter.seek(&prefix); // Find the first key

                loop {
                    let key = iter
                        .key()
                        .filter(|k| k.starts_with(&prefix))
                        .expect("tried removing non-existant row");
                    let val: Vec<DataType> = deserialize(iter.value().unwrap());
                    if val == r {
                        break key.to_vec();
                    }
                    iter.next();
                }
            };

            // First delete the row for the primary index:
            batch.delete_cf(primary_cf, &serialized_pk);

            // Then delete the value for all the secondary indices
            for index in self.indices[1..].iter() {
                // Construct a key with the index values, and serialize it with bincode:
                let key = Self::build_key(r, &index.columns);
                let serialized_key = if index.is_unique && !key.has_null() {
                    Self::serialize_prefix(&key)
                } else {
                    // For non unique keys, we use the primary key to make sure we delete
                    // the *exact* same row from each family
                    Self::serialize_secondary(&key, &serialized_pk)
                };
                let cf = self.db.cf_handle(&index.column_family).unwrap();
                batch.delete_cf(cf, &serialized_key);
            }
        })
    }

    /// Returns the PersistentIndex for the given colums, panicking if it doesn't exist
    fn index(&self, columns: &[usize]) -> &PersistentIndex {
        self.indices
            .iter()
            .find(|index| &index.columns[..] == columns)
            .expect("lookup on non-indexed column set")
    }

    /// Perform a lookup for multiple equal keys at once, the results are returned in order of the
    /// original keys
    pub(crate) fn lookup_multi<'a>(
        &'a self,
        columns: &[usize],
        keys: &[KeyType],
    ) -> Vec<RecordResult<'a>> {
        let index = self.index(columns);
        tokio::task::block_in_place(|| {
            let cf = self.db.cf_handle(&index.column_family).unwrap();
            // Create an iterator once, reuse it for each key
            let mut iter = self.db.raw_iterator_cf(cf);

            keys.iter()
                .map(|k| {
                    let prefix = Self::serialize_prefix(k);
                    let mut rows = Vec::new();

                    let is_unique = index.is_unique && !k.has_null();

                    iter.seek(&prefix); // Find the next key

                    while iter.key().map(|k| k.starts_with(&prefix)).unwrap_or(false) {
                        let val = deserialize(iter.value().unwrap());
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
        })
    }

    pub fn is_snapshotting(&self) -> bool {
        self.snapshot_mode.is_enabled()
    }
}

/// Checks if the given index is unique for this base table.
/// An index is unique if any of its subkeys or permutations is unique.
/// i.e.: if the key [0,2] is unique, [2,0] is also unique, as well as [2,3,0]
/// This check is not asymptotically efficient, but it doesn't matter as long
/// as we only use it during add_key.
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

// Decides which keys the prefix transform should apply to.
fn in_domain(key: &[u8]) -> bool {
    key != META_KEY
}

impl SizeOf for PersistentState {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        if cfg!(debug_assertions) {
            // In test mode flush to disk to make sure size is not zero
            let _ = self.db.flush();
        }
        self.db
            .property_int_value("rocksdb.estimate-live-data-size")
            .unwrap()
            .unwrap()
    }

    fn is_empty(&self) -> bool {
        self.db
            .property_int_value("rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap()
            == 0
    }
}

#[cfg(debug_assertions)]
impl Drop for PersistentState {
    // Sometimes during tests we can't reopen the same db because
    // some background tasks weren't finished so it lingers in the
    // background. This makes sure everything is stopped before we
    // drop the inner db
    fn drop(&mut self) {
        self.db.cancel_all_background_work(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::path::PathBuf;

    fn insert<S: State>(state: &mut S, row: Vec<DataType>) {
        let record: Record = row.into();
        state.process_records(&mut record.into(), None, None);
    }

    fn get_tmp_path() -> (TempDir, String) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("soup");
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
        )
    }

    pub(self) fn setup_single_key(name: &str) -> PersistentState {
        let mut state = setup_persistent(name, None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        state
    }

    #[test]
    fn persistent_state_is_partial() {
        let state = setup_persistent("persistent_state_is_partial", None);
        assert!(!state.is_partial());
    }

    #[test]
    fn persistent_state_single_key() {
        let mut state = setup_single_key("persistent_state_single_key");
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        insert(&mut state, row);

        match state.lookup(&[0], &KeyType::Single(&5.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
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
        let index = Index::new(IndexType::BTreeMap, cols.clone());
        let row: Vec<DataType> = vec![10.into(), "Cat".into(), 20.into()];
        state.add_key(index, None);
        insert(&mut state, row.clone());

        match state.lookup(&cols, &KeyType::Double((1.into(), 2.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        match state.lookup(&cols, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows[0], row);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_multiple_indices() {
        let mut state = setup_persistent("persistent_state_multiple_indices", None);
        let first: Vec<DataType> = vec![10.into(), "Cat".into(), 1.into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".into(), 1.into()];
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![1, 2]), None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0], first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1, 2], &KeyType::Double(("Cat".into(), 1.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&rows[0], &first);
                assert_eq!(&rows[1], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_lookup_multi() {
        for primary in [None, Some(&[0usize][..])] {
            let mut state = setup_persistent("persistent_state_lookup_multi", primary);
            let first: Vec<DataType> = vec![10.into(), "Cat".into(), 1.into()];
            let second: Vec<DataType> = vec![20.into(), "Cat".into(), 1.into()];
            let third: Vec<DataType> = vec![30.into(), "Dog".into(), 1.into()];
            let fourth: Vec<DataType> = vec![40.into(), "Dog".into(), 1.into()];
            state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
            state.add_key(Index::new(IndexType::BTreeMap, vec![1, 2]), None);
            state.process_records(
                &mut vec![first.clone(), second.clone(), third.clone(), fourth.clone()].into(),
                None,
                None,
            );

            match state
                .lookup_multi(
                    &[0],
                    &[
                        KeyType::Single(&10.into()),
                        KeyType::Single(&20.into()),
                        KeyType::Single(&30.into()),
                        KeyType::Single(&10.into()),
                        KeyType::Single(&40.into()),
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
                        KeyType::Double(("Dog".into(), 1.into())),
                        KeyType::Double(("Cat".into(), 1.into())),
                        KeyType::Double(("Dog".into(), 1.into())),
                        KeyType::Double(("Cat".into(), 1.into())),
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
        let pk = Index::new(IndexType::BTreeMap, pk_cols.clone());
        let mut state = PersistentState::new(
            String::from("persistent_state_primary_key"),
            Some(&pk_cols),
            &PersistenceParameters::default(),
        );
        let first: Vec<DataType> = vec![1.into(), 2.into(), "Cat".into()];
        let second: Vec<DataType> = vec![10.into(), 20.into(), "Cat".into()];
        state.add_key(pk, None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![2]), None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);

        match state.lookup(&pk_cols, &KeyType::Double((1.into(), 2.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&pk_cols, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }

        match state.lookup(&pk_cols, &KeyType::Double((1.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 0);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[2], &KeyType::Single(&"Cat".into())) {
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
        let pk = Index::new(IndexType::BTreeMap, vec![0]);
        let mut state = PersistentState::new(
            String::from("persistent_state_primary_key_delete"),
            Some(&pk.columns),
            &PersistenceParameters::default(),
        );
        let first: Vec<DataType> = vec![1.into(), 2.into()];
        let second: Vec<DataType> = vec![10.into(), 20.into()];
        state.add_key(pk, None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);
        match state.lookup(&[0], &KeyType::Single(&1.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        state.process_records(&mut vec![(first, false)].into(), None, None);
        match state.lookup(&[0], &KeyType::Single(&1.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 0);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
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
        let first: Vec<DataType> = vec![0.into(), 0.into()];
        let second: Vec<DataType> = vec![0.into(), 1.into()];
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);

        match state.lookup(&[0], &KeyType::Single(&0.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&rows[0], &first);
                assert_eq!(&rows[1], &second);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &KeyType::Single(&0.into())) {
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
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &KeyType::Single(&"Bob".into())) {
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
        let mut params = PersistenceParameters::default();
        params.mode = DurabilityMode::Permanent;
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        {
            let mut state = PersistentState::new(name.clone(), Vec::<Box<[usize]>>::new(), &params);
            state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
            state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);
            state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);
        }

        let state = PersistentState::new(name, Vec::<Box<[usize]>>::new(), &params);
        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &KeyType::Single(&"Bob".into())) {
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
        let mut params = PersistenceParameters::default();
        params.mode = DurabilityMode::Permanent;
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        {
            let mut state = PersistentState::new(name.clone(), Some(&[0]), &params);
            state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
            state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);
            state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);
        }

        let state = PersistentState::new(name, Some(&[0]), &params);
        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &KeyType::Single(&"Bob".into())) {
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
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let duplicate: Vec<DataType> = vec![10.into(), "Other Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".into()];
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);
        state.process_records(
            &mut vec![first.clone(), duplicate.clone(), second.clone()].into(),
            None,
            None,
        );
        state.process_records(
            &mut vec![(first.clone(), false), (first.clone(), false)].into(),
            None,
            None,
        );

        // We only want to remove rows that match exactly, not all rows that match the key
        match state.lookup(&[0], &KeyType::Single(&first[0])) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &duplicate);
            }
            _ => unreachable!(),
        };

        // Also should have removed the secondary CF
        match state.lookup(&[1], &KeyType::Single(&first[1])) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        };

        // Also shouldn't have removed other keys:
        match state.lookup(&[0], &KeyType::Single(&second[0])) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }

        // Make sure we didn't remove secondary keys pointing to different rows:
        match state.lookup(&[1], &KeyType::Single(&second[1])) {
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
        let first: Vec<DataType> = vec![10.into(), "Cat".into(), DataType::None];
        let duplicate: Vec<DataType> = vec![10.into(), "Other Cat".into(), DataType::None];
        let second: Vec<DataType> = vec![20.into(), "Cat".into(), DataType::None];
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![2]), None);
        state.process_records(
            &mut vec![first.clone(), duplicate.clone(), second.clone()].into(),
            None,
            None,
        );
        state.process_records(
            &mut vec![(first.clone(), false), (first.clone(), false)].into(),
            None,
            None,
        );

        for i in 0..3usize {
            // Make sure we removed the row for every CF
            match state.lookup(&[i], &KeyType::Single(&first[i])) {
                LookupResult::Some(RecordResult::Owned(rows)) => {
                    assert!(rows.len() >= 1);
                    assert!(rows.iter().all(|row| row[i] == first[i] && row != &first));
                }
                _ => unreachable!(),
            };
        }

        // Make sure we have all of our unique nulls intact
        match state.lookup(&[2], &KeyType::Single(&DataType::None)) {
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
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        assert!(state.is_useful());
    }

    #[test]
    fn persistent_state_rows() {
        let mut state = setup_persistent("persistent_state_rows", None);
        let mut rows = vec![];
        for i in 0..30 {
            let row = vec![DataType::from(i); 30];
            rows.push(row);
            state.add_key(Index::new(IndexType::BTreeMap, vec![i]), None);
        }

        for row in rows.iter().cloned() {
            insert(&mut state, row);
        }

        let count = state.rows();
        // rows() is estimated, but we want to make sure we at least don't return
        // self.indices.len() * rows.len() here.
        assert!(count > 0 && count < rows.len() * 2);
    }

    #[test]
    fn persistent_state_dangling_indices() {
        let (_dir, name) = get_tmp_path();
        let mut rows = vec![];
        for i in 0..10 {
            let row = vec![DataType::from(i); 10];
            rows.push(row);
        }

        let mut params = PersistenceParameters::default();
        params.mode = DurabilityMode::Permanent;

        {
            let mut state = PersistentState::new(name.clone(), Vec::<Box<[usize]>>::new(), &params);
            state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
            state.process_records(&mut rows.clone().into(), None, None);
            // Add a second index that we'll have to build in add_key:
            state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);
            // Make sure we actually built the index:
            match state.lookup(&[1], &KeyType::Single(&0.into())) {
                LookupResult::Some(RecordResult::Owned(rs)) => {
                    assert_eq!(rs.len(), 1);
                    assert_eq!(&rs[0], &rows[0]);
                }
                _ => unreachable!(),
            };

            // Pretend we crashed right before calling self.persist_meta in self.add_key by
            // removing the last index from indices:
            state.indices.truncate(1);
            state.persist_meta();
        }

        // During recovery we should now remove all the rows for the second index,
        // since it won't exist in PersistentMeta.indices:
        let mut state = PersistentState::new(name, Vec::<Box<[usize]>>::new(), &params);
        assert_eq!(state.indices.len(), 1);
        // Now, re-add the second index which should trigger an index build:
        state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);
        // And finally, make sure we actually pruned the index
        // (otherwise we'd get two rows from this .lookup):
        match state.lookup(&[1], &KeyType::Single(&0.into())) {
            LookupResult::Some(RecordResult::Owned(rs)) => {
                assert_eq!(rs.len(), 1);
                assert_eq!(&rs[0], &rows[0]);
            }
            _ => unreachable!(),
        };
    }

    #[test]
    fn persistent_state_all_rows() {
        let mut state = setup_persistent("persistent_state_all_rows", None);
        let mut rows = vec![];
        for i in 0..10 {
            let row = vec![DataType::from(i); 10];
            rows.push(row);
            // Add a bunch of indices to make sure the sorting in all_rows()
            // correctly filters out non-primary indices:
            state.add_key(Index::new(IndexType::BTreeMap, vec![i]), None);
        }

        for row in rows.iter().cloned() {
            insert(&mut state, row);
        }

        let actual_rows: Vec<Vec<DataType>> = state
            .all_rows()
            .map(|(_key, value)| deserialize(&value))
            .collect();

        assert_eq!(actual_rows, rows);
    }

    #[test]
    fn persistent_state_cloned_records() {
        let mut state = setup_persistent("persistent_state_cloned_records", None);
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".into()];
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);

        assert_eq!(state.cloned_records(), vec![first, second]);
    }

    #[test]
    #[cfg(not(windows))]
    fn persistent_state_drop() {
        let path = {
            let state = PersistentState::new(
                String::from(".s-o_u#p."),
                Vec::<Box<[usize]>>::new(),
                &PersistenceParameters::default(),
            );
            let path = state._directory.as_ref().unwrap().path().clone();
            assert!(path.exists());
            String::from(path.to_str().unwrap())
        };

        assert!(!PathBuf::from(path).exists());
    }

    #[test]
    fn persistent_state_old_records_new_index() {
        let mut state = setup_persistent("persistent_state_old_records_new_index", None);
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        insert(&mut state, row.clone());
        state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
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

        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        state.process_records(&mut Vec::from(&records[..3]).into(), None, None);
        state.process_records(&mut records[3].clone().into(), None, None);

        // Make sure the first record has been deleted:
        match state.lookup(&[0], &KeyType::Single(&records[0][0])) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        // Then check that the rest exist:
        for record in &records[1..3] {
            match state.lookup(&[0], &KeyType::Single(&record[0])) {
                LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows[0], **record),
                _ => unreachable!(),
            };
        }
    }

    #[test]
    fn replication_offset_roundtrip() {
        let mut state = setup_persistent("replication_offset_roundtrip", None);
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        let mut records: Records = vec![(vec![1.into(), "A".into()], true)].into();
        let replication_offset = ReplicationOffset {
            offset: 12,
            replication_log_name: "binlog".to_owned(),
        };
        state.process_records(&mut records, None, Some(replication_offset.clone()));
        let result = state.replication_offset();
        assert_eq!(result, Some(&replication_offset));
    }

    #[test]
    #[allow(clippy::op_ref)]
    fn persistent_state_prefix_transform() {
        let mut state = setup_persistent("persistent_state_prefix_transform", None);
        state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
        let data = (DataType::from(1), DataType::from(10));
        let r = KeyType::Double(data.clone());
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

    mod lookup_range {
        use super::*;
        use pretty_assertions::assert_eq;
        use vec1::vec1;

        fn setup() -> PersistentState {
            let mut state = setup_single_key("persistent_state_single_key");
            state.process_records(
                &mut (0..10)
                    .map(|n| Record::from(vec![n.into()]))
                    .collect::<Records>(),
                None,
                None,
            );
            state
        }

        #[test]
        fn missing() {
            let state = setup();
            assert_eq!(
                state.lookup_range(
                    &[0],
                    &RangeKey::from(&(vec1![DataType::from(11)]..vec1![DataType::from(20)]))
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
                    &RangeKey::from(&(vec1![DataType::from(3)]..vec1![DataType::from(7)]))
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
                    &RangeKey::from(&(vec1![DataType::from(3)]..=vec1![DataType::from(7)]))
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
                        Bound::Excluded(vec1![DataType::from(3)]),
                        Bound::Excluded(vec1![DataType::from(7)])
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
                        Bound::Excluded(vec1![DataType::from(3)]),
                        Bound::Included(vec1![DataType::from(7)])
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
        fn inclusive_unbounded() {
            let state = setup();
            assert_eq!(
                state.lookup_range(&[0], &RangeKey::from(&(vec1![DataType::from(3)]..))),
                RangeLookupResult::Some((3..10).map(|n| vec![n.into()]).collect::<Vec<_>>().into())
            );
        }

        #[test]
        fn unbounded_inclusive() {
            let state = setup();
            assert_eq!(
                state.lookup_range(&[0], &RangeKey::from(&(..=vec1![DataType::from(3)]))),
                RangeLookupResult::Some((0..=3).map(|n| vec![n.into()]).collect::<Vec<_>>().into())
            );
        }

        #[test]
        fn unbounded_exclusive() {
            let state = setup();
            assert_eq!(
                state.lookup_range(&[0], &RangeKey::from(&(..vec1![DataType::from(3)]))),
                RangeLookupResult::Some((0..3).map(|n| vec![n.into()]).collect::<Vec<_>>().into())
            );
        }
    }
}

#[cfg(feature = "bench")]
pub mod bench {
    use super::*;

    const UNIQUE_ENTIRES: usize = 100000;

    lazy_static::lazy_static! {
        static ref STATE: PersistentState = {
            let mut state = PersistentState::new(
                String::from("bench"),
                vec![&[0usize][..], &[3][..]],
                &PersistenceParameters::default(),
            );

            state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
            state.add_key(Index::new(IndexType::BTreeMap, vec![1, 2]), None);
            state.add_key(Index::new(IndexType::BTreeMap, vec![3]), None);

            let animals = ["Cat", "Dog", "Bat"];

            for i in 0..UNIQUE_ENTIRES {
                let rec: Vec<DataType> = vec![
                    i.into(),
                    animals[i % 3].into(),
                    (i % 99).into(),
                    i.into(),
                ];
                state.process_records(&mut vec![rec].into(), None, None);
            }

            state
        };
    }

    pub fn rocksdb_get_primary_key(c: &mut criterion::Criterion) {
        let state = &*STATE;

        let mut group = c.benchmark_group("RockDB get primary key");
        group.bench_function("lookup_multi", |b| {
            let mut iter = 0usize;
            b.iter(|| {
                criterion::black_box(state.lookup_multi(
                    &[0],
                    &[
                        KeyType::Single(&iter.into()),
                        KeyType::Single(&(iter + 100).into()),
                        KeyType::Single(&(iter + 200).into()),
                        KeyType::Single(&(iter + 300).into()),
                        KeyType::Single(&(iter + 400).into()),
                        KeyType::Single(&(iter + 500).into()),
                        KeyType::Single(&(iter + 600).into()),
                        KeyType::Single(&(iter + 700).into()),
                        KeyType::Single(&(iter + 800).into()),
                        KeyType::Single(&(iter + 900).into()),
                    ],
                ));
                iter = (iter + 1) % (UNIQUE_ENTIRES - 1000);
            })
        });

        group.bench_function("lookup", |b| {
            let mut iter = 0usize;
            b.iter(|| {
                criterion::black_box({
                    state.lookup(&[0], &KeyType::Single(&iter.into()));
                    state.lookup(&[0], &KeyType::Single(&(iter + 100).into()));
                    state.lookup(&[0], &KeyType::Single(&(iter + 200).into()));
                    state.lookup(&[0], &KeyType::Single(&(iter + 300).into()));
                    state.lookup(&[0], &KeyType::Single(&(iter + 400).into()));
                    state.lookup(&[0], &KeyType::Single(&(iter + 500).into()));
                    state.lookup(&[0], &KeyType::Single(&(iter + 600).into()));
                    state.lookup(&[0], &KeyType::Single(&(iter + 700).into()));
                    state.lookup(&[0], &KeyType::Single(&(iter + 800).into()));
                    state.lookup(&[0], &KeyType::Single(&(iter + 900).into()));
                });
                iter = (iter + 1) % (UNIQUE_ENTIRES - 1000);
            })
        });

        group.finish();
    }

    pub fn rocksdb_get_secondary_key(c: &mut criterion::Criterion) {
        let state = &*STATE;

        let mut group = c.benchmark_group("RockDB get secondary key");
        group.bench_function("lookup_multi", |b| {
            b.iter(|| {
                criterion::black_box(state.lookup_multi(
                    &[1, 2],
                    &[
                        KeyType::Double(("Dog".into(), 1.into())),
                        KeyType::Double(("Cat".into(), 2.into())),
                    ],
                ));
            })
        });

        group.bench_function("lookup", |b| {
            b.iter(|| {
                criterion::black_box({
                    state.lookup(&[1, 2], &KeyType::Double(("Dog".into(), 1.into())));
                    state.lookup(&[1, 2], &KeyType::Double(("Cat".into(), 2.into())));
                })
            })
        });

        group.finish();
    }

    pub fn rocksdb_get_secondary_unique_key(c: &mut criterion::Criterion) {
        let state = &*STATE;

        let mut group = c.benchmark_group("RockDB get secondary unique key");
        group.bench_function("lookup_multi", |b| {
            let mut iter = 0usize;
            b.iter(|| {
                criterion::black_box(state.lookup_multi(
                    &[3],
                    &[
                        KeyType::Single(&iter.into()),
                        KeyType::Single(&(iter + 100).into()),
                        KeyType::Single(&(iter + 200).into()),
                        KeyType::Single(&(iter + 300).into()),
                        KeyType::Single(&(iter + 400).into()),
                        KeyType::Single(&(iter + 500).into()),
                        KeyType::Single(&(iter + 600).into()),
                        KeyType::Single(&(iter + 700).into()),
                        KeyType::Single(&(iter + 800).into()),
                        KeyType::Single(&(iter + 900).into()),
                    ],
                ));
                iter = (iter + 1) % (UNIQUE_ENTIRES - 1000);
            })
        });

        group.bench_function("lookup", |b| {
            let mut iter = 0usize;
            b.iter(|| {
                criterion::black_box({
                    state.lookup(&[3], &KeyType::Single(&iter.into()));
                    state.lookup(&[3], &KeyType::Single(&(iter + 100).into()));
                    state.lookup(&[3], &KeyType::Single(&(iter + 200).into()));
                    state.lookup(&[3], &KeyType::Single(&(iter + 300).into()));
                    state.lookup(&[3], &KeyType::Single(&(iter + 400).into()));
                    state.lookup(&[3], &KeyType::Single(&(iter + 500).into()));
                    state.lookup(&[3], &KeyType::Single(&(iter + 600).into()));
                    state.lookup(&[3], &KeyType::Single(&(iter + 700).into()));
                    state.lookup(&[3], &KeyType::Single(&(iter + 800).into()));
                    state.lookup(&[3], &KeyType::Single(&(iter + 900).into()));
                });
                iter = (iter + 1) % (UNIQUE_ENTIRES - 1000);
            })
        });

        group.finish();
    }
}
