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
//!   deserializing to DataTypes rather than RocksDB's default lexicographic bytewise ordering,
//!   which allows us to do queries across ranges covering multiple keys. To avoid having to encode
//!   the length of the index (really the enum variant tag for [`KeyType`]) in the key itself, this
//!   custom key comparator is built dynamically based on the number of columns in the index, up to
//!   a maximum of 6 (since past that point we use [`KeyType::Multi`]).
//!   Note that since we currently don't have the ability to do zero-copy deserialization of
//!   [`DataType`], deserializing for the custom comparator currently requires copying any string
//!   values just to compare them. If we are able to make [`DataType`] enable zero-copy
//!   deserialization (by adding a lifetime parameter) this would likely speed up rather
//!   significantly.
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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::Read;
use std::ops::Bound;
use std::time::{Duration, Instant};

use bincode::Options;
use common::SizeOf;
use noria::replication::ReplicationOffset;
use noria::KeyComparison;
use rocksdb::{self, PlainTableFactoryOptions, SliceTransform, WriteBatch};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::{tempdir, TempDir};
use test_strategy::Arbitrary;
use tracing::{error, info, warn};

use crate::node::special::base::SnapshotMode;
use crate::prelude::*;
use crate::state::{RangeLookupResult, RecordResult, State};

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
#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistentMeta<'a> {
    /// Index information is stored in RocksDB to avoid rebuilding indices on recovery
    indices: Vec<Index>,
    epoch: IndexEpoch,

    /// The latest replication offset that has been written to the base table backed by this
    /// [`PersistentState`]. Corresponds to [`PersistentState::replication_offset`]
    replication_offset: Option<Cow<'a, ReplicationOffset>>,
}

#[derive(Clone)]
struct PersistentIndex {
    column_family: String,
    index: Index,
    is_unique: bool,
    is_primary: bool,
}

/// PersistentState stores data in RocksDB.
pub struct PersistentState {
    name: String,
    default_options: rocksdb::Options,
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
    _tmpdir: Option<TempDir>,
    /// When set to true [`SnapshotMode::SnapshotModeEnabled`] compaction will be disabled and writes will
    /// bypass WAL and fsync
    pub(crate) snapshot_mode: SnapshotMode,
}

impl<'a> PersistentMeta<'a> {
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
    ) {
        assert!(partial_tag.is_none(), "PersistentState can't be partial");
        if records.len() == 0 && replication_offset.is_none() {
            return;
        }

        // Don't process records if the replication offset is less than our current.
        if let (Some(new), Some(current)) = (&replication_offset, &self.replication_offset) {
            if new <= current {
                warn!("Dropping writes we have already processed");
                return;
            }
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
                // We are setting the replication offset, which is great, but all of our previous
                // writes are not guranteed to flush to disk even if the next write is synced. We
                // therefore perform a flush before handling the next write.
                //
                // See: https://github.com/facebook/rocksdb/wiki/RocksDB-FAQhttps://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
                // Q: After a write following option.disableWAL=true, I write another record with options.sync=true,
                //    will it persist the previous write too?
                // A: No. After the program crashes, writes with option.disableWAL=true will be lost, if they are not flushed
                //    to SST files.
                self.db
                    .flush_cf(self.db.cf_handle(PK_CF).unwrap())
                    .expect("Flush to disk failed");
                self.db.flush().expect("Flush to disk failed");
            }
            opts.set_sync(true);
        }

        if let Some(offset) = replication_offset {
            self.set_replication_offset(&mut batch, offset);
        }

        self.db.write_opt(batch, &opts).unwrap();
    }

    fn replication_offset(&self) -> Option<&ReplicationOffset> {
        self.replication_offset.as_ref()
    }

    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let index = self.index(IndexType::HashMap, columns);
        LookupResult::Some(self.do_lookup(index, key).into())
    }

    fn lookup_range<'a>(&'a self, columns: &[usize], key: &RangeKey) -> RangeLookupResult<'a> {
        let db = &self.db;
        let index = self.index(IndexType::BTreeMap, columns);

        let cf = db.cf_handle(&index.column_family).unwrap();
        let primary_cf = if !index.is_primary {
            Some(self.db.cf_handle(PK_CF).unwrap())
        } else {
            None
        };

        let (lower, upper) = Self::serialize_range(key, ((), ()));

        let mut opts = rocksdb::ReadOptions::default();
        let mut inclusive_end = None;
        match upper {
            Bound::Excluded(k) => {
                opts.set_iterate_upper_bound(k);
            }
            Bound::Included(k) => {
                // RocksDB's iterate_upper_bound is exclusive, so we can't use that - instead just
                // save the upper bound and stop iterating once we hit it
                inclusive_end = Some(k);
            }
            _ => {}
        }

        let mut iterator = db.raw_iterator_cf_opt(cf, opts);

        match lower {
            Bound::Included(k) => iterator.seek(k),
            Bound::Excluded(start_key) => {
                iterator.seek(&start_key);
                // The key in the exclusive bound might not actually exist in the db, in which case
                // `seek` brings us to the next key after that. We only want to skip forward if that
                // didn't happen, so first check to see if the iterator is pointing at the exclusive
                // bound
                if iterator.valid() {
                    let curr_key = iterator.key().unwrap();
                    if curr_key.len() >= start_key.len() && start_key == curr_key[..start_key.len()]
                    {
                        // If the iterator *is* pointing at the exclusive bound, skip forward one.
                        iterator.next();
                    }
                }
            }
            Bound::Unbounded => iterator.seek_to_first(),
        }

        let mut rows = vec![];
        // If we have a primary index, then the values of this index contain primary keys, which
        // are keys in that primary index.
        let get_value = |val: &[u8]| match primary_cf {
            Some(primary_cf) => {
                deserialize_row(db.get_pinned_cf(primary_cf, val).unwrap().unwrap())
            }
            None => deserialize_row(val),
        };

        let mut hit_end = false;
        while iterator.valid() {
            // Are we currently pointing at a key that is equal to our inclusive upper bound? (Note
            // that there may be *multiple* rows where this is the case, and we want to collect them
            // all)
            let at_end = inclusive_end.iter().any(|end| {
                let key = iterator.key().unwrap();
                key.len() >= end.len() && end == &key[..end.len()]
            });

            // If we previously hit the inclusive upper bound, but we're not *currently* at that
            // bound, it means we went off the top of our range, so break out of the iteration
            // before we add the value to the result set
            if !at_end && hit_end {
                break;
            }

            rows.push(get_value(iterator.value().unwrap()));

            // Remember if we've already hit the upper bound during iteration
            hit_end = at_end;

            iterator.next();
        }

        RangeLookupResult::Some(RecordResult::Owned(rows))
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
    fn add_key(&mut self, index: Index, partial: Option<Vec<Tag>>) {
        #[allow(clippy::panic)] // This should definitely never happen!
        {
            assert!(partial.is_none(), "Bases can't be partial");
        }
        let columns = &index.columns;
        let existing = self.indices.iter().any(|pi| pi.index == index);

        if existing {
            return;
        }

        let is_unique = check_if_index_is_unique(&self.unique_keys, columns);
        if self.indices.is_empty() {
            self.add_primary_index(&index.columns, is_unique);
            if index.index_type != IndexType::HashMap {
                // Primary indices can only be HashMaps, so if this is our first index and it's
                // *not* a HashMap index, add another secondary index of the correct index type
                self.add_secondary_index(&index, is_unique);
            }
        } else {
            self.add_secondary_index(&index, is_unique)
        }
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        self.all_rows()
            .map(|(_, ref value)| deserialize_row(value))
            .collect()
    }

    /// Returns a row count estimate from RocksDB.
    fn rows(&self) -> usize {
        let db = &self.db;
        let cf = db.cf_handle(PK_CF).unwrap();
        db.property_int_value_cf(cf, "rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap() as usize
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
    fn evict_keys(&mut self, _: Tag, _: &[KeyComparison]) -> Option<(&Index, u64)> {
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

fn serialize_key<K: serde::Serialize, E: serde::Serialize>(k: K, extra: E) -> Vec<u8> {
    let size: u64 = bincode::options().serialized_size(&k).unwrap();
    bincode::options().serialize(&(size, k, extra)).unwrap()
}

fn deserialize_row<T: AsRef<[u8]>>(bytes: T) -> Vec<DataType> {
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

    opts
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
    /// The "6" limit corresponds to the upper limit on the variants of [`KeyType`] and [`RangeKey`]
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
    fn make_rocksdb_options(&self, base_options: &rocksdb::Options) -> rocksdb::Options {
        let mut opts = base_options.clone();
        match self.index_type {
            // For hash map indices, optimize for point queries and in-prefix range iteration, but
            // don't allow cross-prefix range iteration.
            IndexType::HashMap => {
                opts.set_plain_table_factory(&PlainTableFactoryOptions {
                    user_key_length: 0, // variable key length
                    bloom_bits_per_key: 10,
                    hash_table_ratio: 0.75,
                    index_sparseness: 16,
                });

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
            // semantics of DataType, by configuring a custom comparator based on the number of
            // columns in the index
            IndexType::BTreeMap => match self.num_columns {
                Some(0) => unreachable!("Can't create a column family with 0 columns"),
                Some(1) => opts.set_comparator("compare_keys_1", compare_keys_1),
                Some(2) => opts.set_comparator("compare_keys_2", compare_keys_2),
                Some(3) => opts.set_comparator("compare_keys_3", compare_keys_3),
                Some(4) => opts.set_comparator("compare_keys_4", compare_keys_4),
                Some(5) => opts.set_comparator("compare_keys_5", compare_keys_5),
                Some(6) => opts.set_comparator("compare_keys_6", compare_keys_6),
                _ => opts.set_comparator("compare_keys_multi", compare_keys_multi),
            },
        }

        opts
    }
}

impl PersistentState {
    pub fn new<C: AsRef<[usize]>, K: IntoIterator<Item = C>>(
        name: String,
        unique_keys: K,
        params: &PersistenceParameters,
    ) -> Self {
        let unique_keys: Vec<Box<[usize]>> =
            unique_keys.into_iter().map(|c| c.as_ref().into()).collect();

        use rocksdb::{ColumnFamilyDescriptor, DB};
        let (tmpdir, full_path) = match params.mode {
            DurabilityMode::Permanent => {
                let mut path = params.db_dir.clone().unwrap_or_else(|| ".".into());
                if !path.is_dir() {
                    std::fs::create_dir_all(&path).expect("Could not create DB directory");
                }
                path.push(&name);
                path.set_extension("db");

                (None, path)
            }
            _ => {
                let dir = tempdir().unwrap();
                let mut path = dir.path().join(&name);
                path.set_extension("db");
                (Some(dir), path)
            }
        };

        let default_options = base_options(params);
        // We use a column family for each index, and one for metadata.
        // When opening the DB the exact same column families needs to be used,
        // so we'll have to retrieve the existing ones first:
        let cf_names = match DB::list_cf(&default_options, &full_path) {
            Ok(cfs) => cfs,
            Err(_err) => vec![DEFAULT_CF.to_string()],
        };

        let cf_index_params = DB::open_for_read_only(&default_options, &full_path, false)
            .ok()
            .map(|db| get_meta(&db))
            .into_iter()
            .flat_map(|meta: PersistentMeta| {
                meta.indices
                    .into_iter()
                    .map(|index| IndexParams::from(&index))
            })
            .collect::<Vec<_>>();

        // ColumnFamilyDescriptor does not implement Clone, so we have to create a new Vec each time
        let make_cfs = || -> Vec<ColumnFamilyDescriptor> {
            cf_names
                .iter()
                .map(|cf_name| {
                    ColumnFamilyDescriptor::new(
                        cf_name,
                        if cf_name == DEFAULT_CF {
                            default_options.clone()
                        } else {
                            let cf_id: usize = cf_name.parse().expect("Invalid column family ID");
                            let index_params =
                                cf_index_params.get(cf_id).expect("Unknown column family");
                            index_params.make_rocksdb_options(&default_options)
                        },
                    )
                })
                .collect()
        };

        let mut retry = 0;
        let mut db = loop {
            // TODO: why is this loop even needed?
            match DB::open_cf_descriptors(&default_options, &full_path, make_cfs()) {
                Ok(db) => break db,
                _ if retry < 100 => {
                    retry += 1;
                    std::thread::sleep(Duration::from_millis(50));
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
            name,
            default_options,
            seq: 0,
            indices,
            unique_keys,
            epoch: meta.epoch,
            replication_offset: meta.replication_offset.map(|ro| ro.into_owned()),
            db,
            _tmpdir: tmpdir,
            snapshot_mode: SnapshotMode::SnapshotModeDisabled,
        };

        if let Some(pk) = state.unique_keys.first().cloned() {
            // This is the first time we're initializing this PersistentState,
            // so persist the primary key index right away.
            state.add_primary_index(&pk, true);
        }

        state
    }

    /// Adds a new primary index, assuming there are none present
    fn add_primary_index(&mut self, columns: &[usize], is_unique: bool) {
        if self.indices.is_empty() {
            info!(base = %self.name, index = ?columns, is_unique, "Base creating primary index");

            let index_params = IndexParams::new(IndexType::HashMap, columns.len());

            // add the index to the meta first so even if we fail before we fully reindex we still have
            // the information about the column family
            let persistent_index = PersistentIndex {
                column_family: PK_CF.to_string(),
                index: Index::hash_map(columns.to_vec()),
                is_unique,
                is_primary: true,
            };

            self.indices.push(persistent_index);
            self.persist_meta();

            self.db
                .create_cf(
                    PK_CF,
                    &index_params.make_rocksdb_options(&self.default_options),
                )
                .unwrap();
        }
    }

    /// Adds a new secondary index, secondary indices point to the primary index
    /// and don't store values on their own
    fn add_secondary_index(&mut self, index: &Index, is_unique: bool) {
        info!(base = %self.name, ?index, is_unique, "Base creating secondary index");

        // We'll store all the values for this index in its own column family:
        let index_params = IndexParams::from(index);
        let cf_name = self.indices.len().to_string();

        // add the index to the meta first so even if we fail before we fully reindex we still have
        // the information about the column family
        self.indices.push(PersistentIndex {
            column_family: cf_name.clone(),
            is_unique,
            is_primary: false,
            index: index.clone(),
        });

        self.persist_meta();

        self.db
            .create_cf(
                &cf_name,
                &index_params.make_rocksdb_options(&self.default_options),
            )
            .unwrap();

        let db = &self.db;
        let cf = db.cf_handle(&cf_name).unwrap();

        // Prevent autocompactions while we reindex the table
        if let Err(err) = db.set_options_cf(cf, &[("disable_auto_compactions", "true")]) {
            error!(%err, "Error setting cf options");
        }

        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);

        // We know a primary index exists, which is why unwrap is fine
        let primary_cf = db.cf_handle(PK_CF).unwrap();
        // Because we aren't doing a prefix seek, we must set total order first
        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_total_order_seek(true);

        let mut iter = db.raw_iterator_cf_opt(primary_cf, read_opts);
        iter.seek_to_first();
        // We operate in batches to improve performance
        while iter.valid() {
            let mut batch = WriteBatch::default();

            while let (Some(pk), Some(value)) = (iter.key(), iter.value()) {
                if batch.len() == INDEX_BATCH_SIZE {
                    break;
                }

                let row = deserialize_row(value);
                let index_key = Self::build_key(&row, &index.columns);
                if is_unique && !index_key.has_null() {
                    // We know this key to be unique, so we just use it as is
                    let key = Self::serialize_prefix(&index_key);
                    batch.put_cf(cf, &key, pk);
                } else {
                    let key = Self::serialize_secondary(&index_key, pk);
                    // TODO: avoid storing pk as the value, since it is already serialized in
                    // the key, seems wasteful
                    batch.put_cf(cf, &key, pk);
                };

                iter.next();
            }

            db.write_opt(batch, &opts).unwrap();
        }

        info!("Base compacting secondary index");

        // Flush just in case
        db.flush_cf(cf).unwrap();
        // Manually compact the newly created column family
        self.compact_cf(self.indices.last().unwrap());
        // Reenable auto compactions when done
        if let Err(err) = db.set_options_cf(cf, &[("disable_auto_compactions", "false")]) {
            error!(%err, "Error setting cf options");
        }

        info!("Base finished compacting secondary index");
    }

    /// Looks up rows in an index
    /// If the index is the primary index, the lookup get the rows from the primary index directly.
    /// If the index is a secondary index, we will first lookup the primary index keys from that
    /// secondary index, then perform a lookup into the primary index
    fn do_lookup(&self, index: &PersistentIndex, key: &KeyType) -> Vec<Vec<DataType>> {
        let db = &self.db;
        let cf = db.cf_handle(&index.column_family).unwrap();
        let primary_cf = if !index.is_primary {
            Some(self.db.cf_handle(PK_CF).unwrap())
        } else {
            None
        };

        let prefix = Self::serialize_prefix(key);

        if index.is_unique && !key.has_null() {
            // This is a unique key, so we know there's only one row to retrieve
            let value = db.get_pinned_cf(cf, &prefix).unwrap();
            match (value, primary_cf) {
                (None, _) => vec![],
                (Some(value), None) => vec![deserialize_row(value)],
                (Some(pk), Some(primary_cf)) => vec![deserialize_row(
                    db.get_pinned_cf(primary_cf, pk)
                        .unwrap()
                        .expect("Existing primary key"),
                )],
            }
        } else {
            // This could correspond to more than one value, so we'll use a prefix_iterator,
            // for each row
            let mut rows = Vec::new();
            let mut opts = rocksdb::ReadOptions::default();
            opts.set_prefix_same_as_start(true);

            let mut iter = db.raw_iterator_cf_opt(cf, opts);
            let mut iter_primary = primary_cf.map(|pcf| db.raw_iterator_cf(pcf));

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

            rows
        }
    }

    fn build_key<'a>(row: &'a [DataType], columns: &[usize]) -> KeyType<'a> {
        KeyType::from(columns.iter().map(|i| &row[*i]))
    }

    /// Builds a [`PersistentMeta`] from the in-memory metadata information stored in `self`,
    /// including:
    ///
    /// * The columns and index types of the indices
    /// * The epoch
    /// * The replication offset
    fn meta(&self) -> PersistentMeta<'_> {
        PersistentMeta {
            indices: self.indices.iter().map(|pi| pi.index.clone()).collect(),
            epoch: self.epoch,
            replication_offset: self.replication_offset().map(Cow::Borrowed),
        }
    }

    /// Save metadata about this [`PersistentState`] to the db.
    ///
    /// See [Self::meta] for more information about what is saved to the db
    fn persist_meta(&self) {
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
            self.enable_snapshot_mode();
        } else {
            self.disable_snapshot_mode();
        }
    }

    fn enable_snapshot_mode(&mut self) {
        let main_index = self.indices.first().cloned();
        // Clear the data
        while let Some(index_to_drop) = self.indices.pop() {
            self.persist_meta();
            self.db.drop_cf(&index_to_drop.column_family).unwrap();
        }

        // Recreate the original primary index
        if let Some(main_index) = main_index {
            self.db
                .create_cf(
                    PK_CF,
                    &IndexParams::from(&main_index.index)
                        .make_rocksdb_options(&self.default_options),
                )
                .unwrap();
            self.indices.push(main_index);
        }

        // Disable auto compactions for the primary index
        self.indices.first().and_then(|pi| {
            self.db.cf_handle(&pi.column_family).map(|cf| {
                if let Err(err) = self
                    .db
                    .set_options_cf(cf, &[("disable_auto_compactions", "true")])
                {
                    error!(%err, "Error setting cf options");
                }
            })
        });

        self.replication_offset = None; // Remove any replication offset
        self.persist_meta();
    }

    fn disable_snapshot_mode(&mut self) {
        let pi = match self.indices.first() {
            Some(pi) => pi.clone(),
            None => return,
        };

        let db = &self.db;
        let cf = match db.cf_handle(&pi.column_family) {
            Some(cf) => cf,
            None => return,
        };

        // Perform a manual compaction first
        self.compact_cf(&pi);
        // Enable auto compactions
        if let Err(err) = db.set_options_cf(cf, &[("disable_auto_compactions", "false")]) {
            error!(%err, "Error setting cf options");
        }
    }

    // Getting the current compaction progress is as easy as getting the property value
    // for `rocksdb.num-files-at-level<N>` NOT.
    // Essentially we have to implement a huge hack here, since the only way I could find
    // to get accurate progress stats is from reading the DB LOG directly. This is very
    // fragile, as it depends on the LOG format not changing, and if it does the report
    // will be less accurate or not work at all. This is however not critical.
    fn compaction_progress_watcher(&self) -> anyhow::Result<impl notify::Watcher> {
        use notify::{raw_watcher, RecursiveMode, Watcher};
        use std::fs::File;
        use std::io::{Seek, SeekFrom};

        // We open the LOG file, skip to the end, and begin watching for change events
        // on it in order to get the latest log entries as they come
        let log_path = self.db.path().join("LOG");
        let (tx, rx) = std::sync::mpsc::channel();
        let mut log_watcher = raw_watcher(tx)?;
        let table = self.name.clone();
        let rows = self.rows();
        let mut log_file = File::options().read(true).open(&log_path)?;
        log_file.seek(SeekFrom::End(0))?;

        log_watcher.watch(log_path, RecursiveMode::NonRecursive)?;

        let mut monitor = move || -> anyhow::Result<()> {
            const REPORT_INTERVAL: Duration = Duration::from_secs(120);
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
                    if line.contains("Manual compaction starting") {
                        compaction_started = true;
                    }
                    if !compaction_started {
                        continue;
                    }
                    // As far as I can tell compaction has four stages, first files are created for the appropriate keys,
                    // then are indexed, then moved to the correct level (zero cost in case of manual compaction),
                    // finally old files are deleted. The final two stages are almost immediate so we don't care about logging
                    // them. We only going to log progress for the first two stages.

                    // In the first stage we have log entries of the form `Generated table #53: 3314046 keys, 268436084 bytes`
                    // we will be looking for the number of keys in the table, it seems when we have all of the keys proccessed
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
                    // `Number of Keys per prefix Histogram: Count: 1313702 Average: 1.0000  StdDev: 0.00`
                    // Here we are looking for the Count to figure out the number of keys processed in this
                    // stage
                    if line.contains("Number of Keys per prefix Histogram") {
                        // Look for number of keys
                        let mut fields = line.split(' ').peekable();
                        while let Some(f) = fields.next() {
                            if f == "Count:" {
                                second_stage_keys +=
                                    fields.peek().and_then(|f| f.parse().ok()).unwrap_or(0);
                                break;
                            }
                        }
                    }

                    if last_report.elapsed() > REPORT_INTERVAL {
                        let first_stage =
                            format!("{:.2}%", (first_stage_keys as f64 / rows as f64) * 100.0);
                        let second_stage =
                            format!("{:.2}%", (second_stage_keys as f64 / rows as f64) * 100.0);
                        info!(%table, %first_stage, %second_stage, "Compaction");
                        last_report = Instant::now();
                    }
                }
                buf.clear();
            }

            Ok(())
        };

        let table = self.name.clone();

        std::thread::spawn(move || {
            if let Err(err) = monitor() {
                warn!(%err, %table, "Compaction monitor error");
            }
        });

        Ok(log_watcher)
    }

    fn compact_cf(&self, index: &PersistentIndex) {
        let db = &self.db;
        let cf = match db.cf_handle(&index.column_family) {
            Some(cf) => cf,
            None => {
                warn!(table = %self.name, cf = %index.column_family, "Column family not found");
                return;
            }
        };

        let _log_watcher = self.compaction_progress_watcher();
        if let Err(err) = &_log_watcher {
            warn!(%err, table = %self.name, "Could not start compaction monitor");
        }

        let mut opts = rocksdb::CompactOptions::default();
        // We don't want to block other compactions happening in parallel
        opts.set_exclusive_manual_compaction(false);
        db.compact_range_cf_opt(cf, Option::<&[u8]>::None, Option::<&[u8]>::None, &opts);

        info!(table = %self.name, cf = %index.column_family, "Compaction finished");
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
            KeyType::Single(k) => serialize_key(k, extra),
            KeyType::Double(k) => serialize_key(k, extra),
            KeyType::Tri(k) => serialize_key(k, extra),
            KeyType::Quad(k) => serialize_key(k, extra),
            KeyType::Quin(k) => serialize_key(k, extra),
            KeyType::Sex(k) => serialize_key(k, extra),
            KeyType::Multi(k) => serialize_key(k, extra),
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
                range.0.map(|k| serialize_key(k, &extra.0)),
                range.1.map(|k| serialize_key(k, &extra.1)),
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
        let primary_index = self.indices.first().expect("Insert on un-indexed state");
        let primary_key = Self::build_key(r, &primary_index.index.columns);
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
            let cf = self.db.cf_handle(&index.column_family).unwrap();

            let key = Self::build_key(r, &index.index.columns);
            if index.is_unique && !key.has_null() {
                let serialized_key = Self::serialize_prefix(&key);
                batch.put_cf(cf, &serialized_key, &serialized_pk);
            } else {
                let serialized_key = Self::serialize_secondary(&key, &serialized_pk);
                // TODO: Since the primary key is already serialized in here, no reason to store it as value again
                batch.put_cf(cf, &serialized_key, &serialized_pk);
            };
        }
    }

    fn remove(&self, batch: &mut WriteBatch, r: &[DataType]) {
        let primary_index = self.indices.first().expect("Delete on un-indexed state");
        let primary_key = Self::build_key(r, &primary_index.index.columns);
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
        for index in self.indices[1..].iter() {
            // Construct a key with the index values, and serialize it with bincode:
            let key = Self::build_key(r, &index.index.columns);
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
    }

    /// Returns the PersistentIndex for the given index, panicking if it doesn't exist
    // TODO(grfn): This should actually be an error, since it can be triggered by bad requests
    #[allow(clippy::panic)]
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

    /// Perform a lookup for multiple equal keys at once, the results are returned in order of the
    /// original keys
    pub(crate) fn lookup_multi<'a>(
        &'a self,
        columns: &[usize],
        keys: &[KeyType],
    ) -> Vec<RecordResult<'a>> {
        if keys.is_empty() {
            return vec![];
        }

        let index = self.index(IndexType::HashMap, columns);
        let is_primary = index.is_primary;

        let cf = self.db.cf_handle(&index.column_family).unwrap();
        // Create an iterator once, reuse it for each key
        let mut iter = self.db.raw_iterator_cf(cf);
        let mut iter_primary = if !is_primary {
            Some(
                self.db.raw_iterator_cf(
                    self.db
                        .cf_handle(PK_CF)
                        .expect("Primary key column family not found"),
                ),
            )
        } else {
            None
        };

        keys.iter()
            .map(|k| {
                let prefix = Self::serialize_prefix(k);
                let mut rows = Vec::new();

                let is_unique = index.is_unique && !k.has_null();

                iter.seek(&prefix); // Find the next key

                while iter.key().map(|k| k.starts_with(&prefix)).unwrap_or(false) {
                    let val = match &mut iter_primary {
                        Some(iter_primary) => {
                            // If we have a primary iterator, it means this is a secondary index and we need
                            // to lookup by the primary key next
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OwnedKey {
    Single(DataType),
    Double((DataType, DataType)),
    Tri((DataType, DataType, DataType)),
    Quad((DataType, DataType, DataType, DataType)),
    Quin((DataType, DataType, DataType, DataType, DataType)),
    Sex((DataType, DataType, DataType, DataType, DataType, DataType)),
    Multi(Vec<DataType>),
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

impl SizeOf for PersistentState {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    #[allow(clippy::panic)] // Can't return a result, panicking is the best we can do
    fn deep_size_of(&self) -> u64 {
        self.indices
            .iter()
            .map(|idx| {
                let cf = self
                    .db
                    .cf_handle(&idx.column_family)
                    .unwrap_or_else(|| panic!("Column family not found: {}", idx.column_family));

                self.db
                    .property_int_value_cf(cf, "rocksdb.estimate-live-data-size")
                    .unwrap()
                    .unwrap()
            })
            .sum()
    }

    fn is_empty(&self) -> bool {
        self.db
            .property_int_value("rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap()
            == 0
    }
}

#[cfg(test)]
#[allow(clippy::unreachable)]
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
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
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
        let index = Index::new(IndexType::HashMap, cols.clone());
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
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![1, 2]), None);
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
            state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![1, 2]), None);
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
        let pk = Index::new(IndexType::HashMap, pk_cols.clone());
        let mut state = PersistentState::new(
            String::from("persistent_state_primary_key"),
            Some(&pk_cols),
            &PersistenceParameters::default(),
        );
        let first: Vec<DataType> = vec![1.into(), 2.into(), "Cat".into()];
        let second: Vec<DataType> = vec![10.into(), 20.into(), "Cat".into()];
        state.add_key(pk, None);
        state.add_key(Index::new(IndexType::HashMap, vec![2]), None);
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
        let pk = Index::new(IndexType::HashMap, vec![0]);
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
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
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
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
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
            state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
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
            state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
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
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
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
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![2]), None);
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
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        assert!(state.is_useful());
    }

    #[test]
    fn persistent_state_rows() {
        let mut state = setup_persistent("persistent_state_rows", None);
        let mut rows = vec![];
        for i in 0..30 {
            let row = vec![DataType::from(i); 30];
            rows.push(row);
            state.add_key(Index::new(IndexType::HashMap, vec![i]), None);
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
    fn persistent_state_all_rows() {
        let mut state = setup_persistent("persistent_state_all_rows", None);
        let mut rows = vec![];
        for i in 0..10 {
            let row = vec![DataType::from(i); 10];
            rows.push(row);
            // Add a bunch of indices to make sure the sorting in all_rows()
            // correctly filters out non-primary indices:
            state.add_key(Index::new(IndexType::HashMap, vec![i]), None);
        }

        for row in rows.iter().cloned() {
            insert(&mut state, row);
        }

        let actual_rows: Vec<Vec<DataType>> = state
            .all_rows()
            .map(|(_key, value)| deserialize_row(&value))
            .collect();

        assert_eq!(actual_rows, rows);
    }

    #[test]
    fn persistent_state_cloned_records() {
        let mut state = setup_persistent("persistent_state_cloned_records", None);
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".into()];
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
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
            let path = state._tmpdir.as_ref().unwrap().path();
            assert!(path.exists());
            String::from(path.to_str().unwrap())
        };

        assert!(!PathBuf::from(path).exists());
    }

    #[test]
    fn persistent_state_old_records_new_index() {
        let mut state = setup_persistent("persistent_state_old_records_new_index", None);
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        insert(&mut state, row.clone());
        state.add_key(Index::new(IndexType::HashMap, vec![1]), None);

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

        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
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
        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
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

    #[test]
    fn reindex_btree_with_nulls() {
        let mut state = setup_persistent("reindex_with_nulls", None);
        state.add_key(Index::hash_map(vec![0]), None);
        insert(&mut state, vec![1.into()]);
        insert(&mut state, vec![DataType::None]);
        state.add_key(Index::btree_map(vec![0]), None);
    }

    mod lookup_range {
        use super::*;
        use pretty_assertions::assert_eq;
        use std::{iter, ops::Bound::*};
        use vec1::vec1;

        fn setup() -> PersistentState {
            let mut state = setup_persistent("persistent_state_single_key", None);
            state.add_key(Index::btree_map(vec![0]), None);
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
        fn unbounded_inclusive_multiple_rows_in_upper_bound() {
            let mut state = setup();
            state.process_records(&mut vec![vec![DataType::from(3)]].into(), None, None);

            assert_eq!(
                state.lookup_range(&[0], &RangeKey::from(&(..=vec1![DataType::from(3)]))),
                RangeLookupResult::Some(
                    vec![
                        vec![DataType::from(0)],
                        vec![DataType::from(1)],
                        vec![DataType::from(2)],
                        vec![DataType::from(3)],
                        vec![DataType::from(3)],
                    ]
                    .into()
                )
            )
        }

        #[test]
        fn non_unique_then_reindex() {
            let mut state = setup_persistent("persistent_state_single_key", Some(&[1][..]));
            state.process_records(
                &mut [0, 0, 1, 1, 2, 2, 3, 3]
                    .iter()
                    .enumerate()
                    .map(|(i, n)| Record::from(vec![(*n).into(), i.into()]))
                    .collect::<Records>(),
                None,
                None,
            );
            state.add_key(Index::btree_map(vec![0]), None);

            assert_eq!(
                state.lookup_range(&[0], &RangeKey::from(&(vec1![DataType::from(2)]..))),
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

        fn setup_secondary() -> PersistentState {
            let mut state = setup_persistent("reindexed", Some(&[0usize][..]));
            state.process_records(
                &mut (-10..10)
                    .map(|n| Record::from(vec![n.into(), n.into(), n.into()]))
                    .collect::<Records>(),
                None,
                None,
            );
            state.add_key(Index::btree_map(vec![1]), None);
            state
        }

        #[test]
        fn inclusive_unbounded_secondary() {
            let state = setup_secondary();
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&(Included(vec1![DataType::from(3)]), Unbounded))
                ),
                RangeLookupResult::Some(
                    (3..10)
                        .map(|n| vec![n.into(), n.into(), n.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn exclusive_unbounded_secondary_big_values() {
            let mut state =
                setup_persistent("exclusive_unbounded_secondary_2", Some(&[0usize][..]));
            state.process_records(
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
            );
            state.add_key(Index::btree_map(vec![1]), None);
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&(Excluded(vec1![DataType::from(10)]), Unbounded))
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
                        Excluded(vec1![DataType::from(3)]),
                        Included(vec1![DataType::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (4..=7)
                        .map(|n| vec![n.into(), n.into(), n.into()])
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
                        Excluded(vec1![DataType::from(3)]),
                        Excluded(vec1![DataType::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (4..7)
                        .map(|n| vec![n.into(), n.into(), n.into()])
                        .collect::<Vec<_>>()
                        .into()
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
                        Included(vec1![DataType::from(3)]),
                        Excluded(vec1![DataType::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (3..7)
                        .map(|n| vec![n.into(), n.into(), n.into()])
                        .collect::<Vec<_>>()
                        .into()
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
                        Excluded(vec1![DataType::from(3)]),
                        Included(vec1![DataType::from(7)])
                    ))
                ),
                RangeLookupResult::Some(
                    (4..=7)
                        .map(|n| vec![n.into(), n.into(), n.into()])
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
                    &RangeKey::from(&(Unbounded, Included(vec1![DataType::from(7)])))
                ),
                RangeLookupResult::Some(
                    (-10..=7)
                        .map(|n| vec![n.into(), n.into(), n.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn unbounded_exclusive_secondary() {
            let state = setup_secondary();
            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&(Unbounded, Excluded(vec1![DataType::from(7)])))
                ),
                RangeLookupResult::Some(
                    (-10..7)
                        .map(|n| vec![n.into(), n.into(), n.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn inclusive_unbounded_secondary_compound() {
            let mut state = setup_secondary();
            state.add_key(Index::btree_map(vec![0, 1]), None);
            assert_eq!(
                state.lookup_range(
                    &[0, 1],
                    &RangeKey::from(&(
                        Included(vec1![DataType::from(3), DataType::from(3)]),
                        Unbounded
                    ))
                ),
                RangeLookupResult::Some(
                    (3..10)
                        .map(|n| vec![n.into(), n.into(), n.into()])
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }

        #[test]
        fn inclusive_unbounded_secondary_non_unique() {
            let mut state = setup_secondary();
            let extra_row_beginning =
                vec![DataType::from(11), DataType::from(3), DataType::from(3)];
            let extra_row_end = vec![DataType::from(12), DataType::from(9), DataType::from(9)];

            state.process_records(
                &mut vec![extra_row_beginning.clone(), extra_row_end.clone()].into(),
                None,
                None,
            );

            assert_eq!(
                state.lookup_range(
                    &[1],
                    &RangeKey::from(&(Included(vec1![DataType::from(3)]), Unbounded))
                ),
                RangeLookupResult::Some(
                    vec![vec![3.into(), 3.into(), 3.into()], extra_row_beginning]
                        .into_iter()
                        .chain((4..10).map(|n| vec![n.into(), n.into(), n.into()]))
                        .chain(iter::once(extra_row_end))
                        .collect::<Vec<_>>()
                        .into()
                )
            );
        }
    }
}

#[cfg(feature = "bench")]
pub mod bench {
    use super::*;
    use itertools::Itertools;

    const UNIQUE_ENTIRES: usize = 100000;

    lazy_static::lazy_static! {
        static ref STATE: PersistentState = {
            let mut state = PersistentState::new(
                String::from("bench"),
                vec![&[0usize][..], &[3][..]],
                &PersistenceParameters::default(),
            );

            state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![1, 2]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![3]), None);

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

        static ref LARGE_STRINGS: Vec<String> = ["a", "b", "c"].iter().map(|s| {
            std::iter::once(s).cycle().take(10000).join("")
        }).collect::<Vec<_>>();

        static ref STATE_LARGE_STRINGS: PersistentState = {
            let mut state = PersistentState::new(
                String::from("bench"),
                vec![&[0usize][..], &[3][..]],
                &PersistenceParameters::default(),
            );

            state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![3]), None);

            for i in 0..UNIQUE_ENTIRES {
                let rec: Vec<DataType> = vec![
                    i.into(),
                    LARGE_STRINGS[i % 3].clone().into(),
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

    pub fn rocksdb_range_lookup_large_strings(c: &mut criterion::Criterion) {
        let state = &*STATE_LARGE_STRINGS;
        let key = DataType::from(LARGE_STRINGS[0].clone());

        let mut group = c.benchmark_group("RocksDB with large strings");
        group.bench_function("lookup_range", |b| {
            b.iter(|| {
                criterion::black_box(state.lookup_range(
                    &[1],
                    &RangeKey::Single((Bound::Included(&key), Bound::Unbounded)),
                ));
            })
        });
        group.finish();
    }
}
