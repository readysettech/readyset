//! Node state that's persisted to disk
//!
//! The [`PersistedState`] struct is an implementation of [`State`] that stores rows (currently
//! only for base tables) in [RocksDB][0], an on-disk key-value store. The data is stored in
//! [indices](PersistentState::indices) - one primary index which stores the actual rows, and a
//! number of secondary indices which maintain pointers to the data in the primary index.
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

use itertools::Itertools;
use noria::{KeyComparison, ReplicationOffset};
use rocksdb::{
    self, Direction, IteratorMode, PlainTableFactoryOptions, SliceTransform, WriteBatch,
};
use std::borrow::Cow;
use std::ops::Bound;
use tempfile::{tempdir, TempDir};

use crate::prelude::*;
use crate::state::{RangeLookupResult, RecordResult, State};
use common::SizeOf;

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

// Maximum rows per WriteBatch when building new indices for existing rows.
const INDEX_BATCH_SIZE: usize = 100_000;

fn get_meta(db: &rocksdb::DB) -> PersistentMeta<'static> {
    db.get_pinned(META_KEY)
        .unwrap()
        .map(|data| bincode::deserialize(data.as_ref()).unwrap())
        .unwrap_or_default()
}

/// Abstraction over writing to different kinds of rocksdb dbs.
///
/// This trait is (consciously) incomplete - if necessary, a more complete version including
/// *put_cf* etc could be put inside a utility module somewhere
trait Put {
    /// Write a key/value pair
    ///
    /// This method is prefixed with "do" so that it doesn't conflict with the `put` method on both
    /// [`rocksdb::DB`] and [`rocksdb::WriteBatch`]
    fn do_put<K, V>(self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;
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

fn save_meta<DB>(db: DB, meta: &PersistentMeta)
where
    DB: Put,
{
    db.do_put(META_KEY, bincode::serialize(meta).unwrap());
}

/// Load the saved [`PersistentMeta`] from the database, increment its
/// [epoch](PersistentMeta::epoch) by one, and return it
fn increment_epoch(db: &rocksdb::DB) -> PersistentMeta<'static> {
    let mut meta = get_meta(db);
    meta.epoch += 1;
    save_meta(db, &meta);
    meta
}

/// Data structure used to persist metadata about the [`PersistentState`] to rocksdb
#[derive(Default, Serialize, Deserialize)]
struct PersistentMeta<'a> {
    /// Index information is stored in RocksDB to avoid rebuilding indices on recovery
    indices: Vec<Vec<usize>>,
    epoch: IndexEpoch,

    /// The latest replication offset that has been written to the base table backed by this
    /// [`PersistentState`]. Corresponds to [`PersistentState::replication_offset`]
    replication_offset: Option<Cow<'a, ReplicationOffset>>,
}

#[derive(Clone)]
struct PersistentIndex {
    column_family: String,
    columns: Vec<usize>,
}

/// PersistentState stores data in RocksDB.
pub struct PersistentState {
    db_opts: rocksdb::Options,
    // We don't really want DB to be an option, but doing so lets us drop it manually in
    // PersistenState's Drop by setting `self.db = None` - after which we can then discard the
    // persisted files if we want to.
    db: Option<rocksdb::DB>,
    // The first element is always considered the primary index, where the actual data is stored.
    // Subsequent indices maintain pointers to the data in the first index, and cause an additional
    // read during lookups. When `self.has_unique_index` is true the first index is a primary key,
    // and all its keys are considered unique.
    indices: Vec<PersistentIndex>,
    seq: IndexSeq,
    epoch: IndexEpoch,
    /// The latest replication offset that has been written to the base table backed by this
    /// [`PersistentState`]
    replication_offset: Option<ReplicationOffset>,
    has_unique_index: bool,
    // With DurabilityMode::DeleteOnExit,
    // RocksDB files are stored in a temporary directory.
    _directory: Option<TempDir>,
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

        if let Some(offset) = replication_offset {
            self.set_replication_offset(&mut batch, offset);
        }

        // Sync the writes to RocksDB's WAL:
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(true);
        tokio::task::block_in_place(|| self.db().write_opt(batch, &opts)).unwrap();
    }

    fn replication_offset(&self) -> Option<&ReplicationOffset> {
        self.replication_offset.as_ref()
    }

    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let db = self.db();
        let index_id = self.index_id(columns);
        tokio::task::block_in_place(|| {
            let cf = db.cf_handle(&self.indices[index_id].column_family).unwrap();
            let prefix = Self::serialize_prefix(key);
            let data = if index_id == 0 && self.has_unique_index {
                // This is a primary key, so we know there's only one row to retrieve
                // (no need to use prefix_iterator).
                let raw_row = db.get_cf(cf, &prefix).unwrap();
                if let Some(raw) = raw_row {
                    let row = bincode::deserialize(&*raw).unwrap();
                    vec![row]
                } else {
                    vec![]
                }
            } else {
                // This could correspond to more than one value, so we'll use a prefix_iterator:
                db.prefix_iterator_cf(cf, &prefix)
                    .map(|(_key, value)| {
                        bincode::deserialize(&*value)
                            .expect("Error deserializing data from rocksdb")
                    })
                    .collect()
            };

            LookupResult::Some(RecordResult::Owned(data))
        })
    }

    fn lookup_range<'a>(&'a self, columns: &[usize], key: &RangeKey) -> RangeLookupResult<'a> {
        let db = self.db();
        tokio::task::block_in_place(|| {
            let cf = self.index_cf(columns);
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
                        bincode::deserialize(&*value)
                            .expect("Error deserializing data from rocksdb")
                    })
                    .collect(),
            ))
        })
    }

    /// Panics if partial is Some
    fn add_key(&mut self, index: &Index, partial: Option<Vec<Tag>>) {
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

        // We'll store all the pointers (or values if this is index 0) for
        // this index in its own column family:
        let index_id = self.indices.len().to_string();

        tokio::task::block_in_place(|| {
            let db = self.db.as_mut().unwrap();
            db.create_cf(&index_id, &self.db_opts).unwrap();

            // Build the new index for existing values:
            if !self.indices.is_empty() {
                let first_cf = db.cf_handle(&self.indices[0].column_family).unwrap();
                let iter = db.full_iterator_cf(first_cf, rocksdb::IteratorMode::Start);
                for chunk in iter.chunks(INDEX_BATCH_SIZE).into_iter() {
                    let mut batch = WriteBatch::default();
                    for (ref pk, ref value) in chunk {
                        let row: Vec<DataType> = bincode::deserialize(value).unwrap();
                        let index_key = Self::build_key(&row, columns);
                        let key = Self::serialize_secondary(&index_key, pk);
                        let cf = db.cf_handle(&index_id).unwrap();
                        batch.put_cf(cf, &key, value);
                    }

                    db.write(batch).unwrap();
                }
            }

            self.indices.push(PersistentIndex {
                columns: columns.clone(),
                column_family: index_id.to_string(),
            });

            self.persist_meta();
        });
    }

    fn keys(&self) -> Vec<Vec<usize>> {
        self.indices
            .iter()
            .map(|index| index.columns.clone())
            .collect()
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        self.all_rows()
            .map(|(_, ref value)| bincode::deserialize(value).unwrap())
            .collect()
    }

    // Returns a row count estimate from RocksDB.
    fn rows(&self) -> usize {
        tokio::task::block_in_place(|| {
            let db = self.db();
            let cf = db.cf_handle("0").unwrap();
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
    fn evict_random_keys(&mut self, _: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
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

    fn add_weak_key(&mut self, index: &Index) {
        self.add_key(index, None);
    }

    fn lookup_weak<'a>(&'a self, columns: &[usize], key: &KeyType) -> Option<RecordResult<'a>> {
        self.lookup(columns, key).records()
    }
}

fn serialize<K: serde::Serialize, E: serde::Serialize>(k: K, extra: E) -> Vec<u8> {
    let size: u64 = bincode::serialized_size(&k).unwrap();
    bincode::serialize(&(size, k, extra)).unwrap()
}

impl PersistentState {
    pub fn new(
        name: String,
        primary_key: Option<&[usize]>,
        params: &PersistenceParameters,
    ) -> Self {
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
            let column_families = match DB::list_cf(&opts, &full_name) {
                Ok(cfs) => cfs,
                Err(_err) => vec![DEFAULT_CF.to_string()],
            };

            let make_cfs = || -> Vec<ColumnFamilyDescriptor> {
                column_families
                    .iter()
                    .map(|cf| {
                        ColumnFamilyDescriptor::new(cf.clone(), Self::build_options(&name, params))
                    })
                    .collect()
            };

            let mut db = DB::open_cf_descriptors(&opts, &full_name, make_cfs());
            for _ in 0..100 {
                if db.is_ok() {
                    break;
                }
                ::std::thread::sleep(::std::time::Duration::from_millis(50));
                db = DB::open_cf_descriptors(&opts, &full_name, make_cfs());
            }
            let mut db = db.unwrap();
            let meta = increment_epoch(&db);
            let indices: Vec<PersistentIndex> = meta
                .indices
                .into_iter()
                .enumerate()
                .map(|(i, columns)| PersistentIndex {
                    column_family: i.to_string(),
                    columns,
                })
                .collect();

            // If there are more column families than indices (-1 to account for the default column
            // family) we probably crashed while trying to build the last index (in Self::add_key), so
            // we'll throw away our progress and try re-building it again later:
            if column_families.len() - 1 > indices.len() {
                db.drop_cf(&indices.len().to_string()).unwrap();
            }

            let mut state = Self {
                seq: 0,
                indices,
                has_unique_index: primary_key.is_some(),
                epoch: meta.epoch,
                replication_offset: meta.replication_offset.map(|ro| ro.into_owned()),
                db_opts: opts,
                db: Some(db),
                _directory: directory,
            };

            if let Some(pk) = primary_key {
                if state.indices.is_empty() {
                    // This is the first time we're initializing this PersistentState,
                    // so persist the primary key index right away.
                    state
                        .db
                        .as_mut()
                        .unwrap()
                        .create_cf("0", &state.db_opts)
                        .unwrap();

                    let persistent_index = PersistentIndex {
                        column_family: "0".to_string(),
                        columns: pk.to_vec(),
                    };

                    state.indices.push(persistent_index);
                    state.persist_meta();
                }
            }

            state
        })
    }

    /// Return a reference to the underlying [`rocksdb::DB`] for this [`PersistentState`]
    fn db(&self) -> &rocksdb::DB {
        // for the unwrap here, see the note on Self::db
        self.db.as_ref().unwrap()
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
        opts.set_max_bytes_for_level_base(2048 * 1024 * 1024);
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
        save_meta(self.db(), &self.meta());
    }

    /// Add an operation to the given [`WriteBatch`] to set the [replication
    /// offset](PersistentMeta::replication_offset) to the given value.
    fn set_replication_offset(&mut self, batch: &mut WriteBatch, offset: ReplicationOffset) {
        // It's ok to read and update meta in two steps here since each State can (currently) only
        // be modified by a single thread.
        self.replication_offset = Some(offset);
        save_meta(batch, &self.meta());
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
        let db = self.db();
        let cf = db.cf_handle(&self.indices[0].column_family).unwrap();
        db.full_iterator_cf(cf, rocksdb::IteratorMode::Start)
    }

    // Puts by primary key first, then retrieves the existing value for each index and appends the
    // newly created primary key value.
    // TODO(ekmartin): This will put exactly the values that are given, and can only be retrieved
    // with exactly those values. I think the regular state implementation supports inserting
    // something like an Int and retrieving with a BigInt.
    fn insert(&mut self, batch: &mut WriteBatch, r: &[DataType]) {
        let serialized_pk = {
            let pk = Self::build_key(r, &self.indices[0].columns);
            if self.has_unique_index {
                Self::serialize_prefix(&pk)
            } else {
                // For bases without primary keys we store the actual row values keyed by the index
                // that was added first. This means that we can't consider the keys unique though, so
                // we'll append a sequence number.
                self.seq += 1;
                Self::serialize_raw_key(&pk, (self.epoch, self.seq))
            }
        };

        // First insert the actual value for our primary index:
        let serialized_row = bincode::serialize(&r).unwrap();

        tokio::task::block_in_place(|| {
            let db = self.db();
            let value_cf = db.cf_handle(&self.indices[0].column_family).unwrap();
            batch.put_cf(value_cf, &serialized_pk, &serialized_row);

            // Then insert primary key pointers for all the secondary indices:
            for index in self.indices[1..].iter() {
                // Construct a key with the index values, and serialize it with bincode:
                let key = Self::build_key(r, &index.columns);
                let serialized_key = Self::serialize_secondary(&key, &serialized_pk);
                let cf = db.cf_handle(&index.column_family).unwrap();
                batch.put_cf(cf, &serialized_key, &serialized_row);
            }
        })
    }

    fn remove(&self, batch: &mut WriteBatch, r: &[DataType]) {
        tokio::task::block_in_place(|| {
            let db = self.db();
            let pk_index = &self.indices[0];
            let value_cf = db.cf_handle(&pk_index.column_family).unwrap();
            let mut do_remove = move |primary_key: &[u8]| {
                // Delete the value row first (primary index):
                batch.delete_cf(value_cf, &primary_key);

                // Then delete any references that point _exactly_ to that row:
                for index in self.indices[1..].iter() {
                    let key = Self::build_key(r, &index.columns);
                    let serialized_key = Self::serialize_secondary(&key, primary_key);
                    let cf = db.cf_handle(&index.column_family).unwrap();
                    batch.delete_cf(cf, &serialized_key);
                }
            };

            let pk = Self::build_key(r, &pk_index.columns);
            let prefix = Self::serialize_prefix(&pk);
            if self.has_unique_index {
                if cfg!(debug_assertions) {
                    // This would imply that we're trying to delete a different row than the one we
                    // found when we resolved the DeleteRequest in Base. This really shouldn't happen,
                    // but we'll leave a check here in debug mode for now.
                    let raw = db
                        .get_cf(value_cf, &prefix)
                        .unwrap()
                        .expect("tried removing non-existant primary key row");
                    let value: Vec<DataType> = bincode::deserialize(&*raw).unwrap();
                    assert_eq!(r, &value[..], "tried removing non-matching primary key row");
                }

                do_remove(&prefix[..]);
            } else {
                let (key, _value) = db
                    .prefix_iterator_cf(value_cf, &prefix)
                    .find(|(_, raw_value)| {
                        let value: Vec<DataType> = bincode::deserialize(&*raw_value).unwrap();
                        r == &value[..]
                    })
                    .expect("tried removing non-existant row");
                do_remove(&key[..]);
            };
        })
    }

    /// Returns the id of the index for the given coluns, panicking if it doesn't exist
    fn index_id(&self, columns: &[usize]) -> usize {
        self.indices
            .iter()
            .position(|index| &index.columns[..] == columns)
            .expect("lookup on non-indexed column set")
    }

    /// Returns a column family handle for the index for the given columns, panicking if it doesn't
    /// exist
    fn index_cf<'a>(&'a self, columns: &[usize]) -> &'a rocksdb::ColumnFamily {
        self.db
            .as_ref()
            .unwrap()
            .cf_handle(&self.indices[self.index_id(columns)].column_family)
            .unwrap()
    }
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
// NOTE(ekmartin): Encoding the key size in the key increases the total size with 8 bytes.
// If we really wanted to avoid this while still maintaining the same serialization scheme
// we could do so by figuring out how many bytes our bincode serialized KeyType takes
// up here in transform_fn. Example:
// Double((DataType::Int(1), DataType::BigInt(10))) would be serialized as:
// 1u32 (enum type), 0u32 (enum variant), 1i32 (value), 1u32 (enum variant), 1i64 (value)
// By stepping through the serialized bytes and checking each enum variant we would know
// when we reached the end, and could then with certainty say whether we'd already
// prefix transformed this key before or not
// (without including the byte size of Vec<DataType>).
fn prefix_transform(key: &[u8]) -> &[u8] {
    // We'll have to make sure this isn't the META_KEY even when we're filtering it out
    // in Self::in_domain_fn, as the SliceTransform is used to make hashed keys for our
    // HashLinkedList memtable factory.
    if key == META_KEY {
        return key;
    }

    // We encoded the size of the key itself with a u64, which bincode uses 8 bytes to encode:
    let size_offset = 8;
    let key_size: u64 = bincode::deserialize(&key[..size_offset]).unwrap();
    let prefix_len = size_offset + key_size as usize;
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
        self.db()
            .property_int_value("rocksdb.estimate-live-data-size")
            .unwrap()
            .unwrap()
    }

    fn is_empty(&self) -> bool {
        self.db()
            .property_int_value("rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap()
            == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::convert::TryInto;
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

    fn setup_persistent(prefix: &str) -> PersistentState {
        PersistentState::new(
            String::from(prefix),
            None,
            &PersistenceParameters::default(),
        )
    }

    pub(self) fn setup_single_key(name: &str) -> PersistentState {
        let mut state = setup_persistent(name);
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        state
    }

    #[test]
    fn persistent_state_is_partial() {
        let state = setup_persistent("persistent_state_is_partial");
        assert!(!state.is_partial());
    }

    #[test]
    fn persistent_state_single_key() {
        let mut state = setup_single_key("persistent_state_single_key");
        let row: Vec<DataType> = vec![10.into(), "Cat".try_into().unwrap()];
        insert(&mut state, row);

        match state.lookup(&[0], &KeyType::Single(&5.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows[0][0], 10.into());
                assert_eq!(rows[0][1], "Cat".try_into().unwrap());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_multi_key() {
        let mut state = setup_persistent("persistent_state_multi_key");
        let index = Index::new(IndexType::BTreeMap, vec![0, 2]);
        let row: Vec<DataType> = vec![10.into(), "Cat".try_into().unwrap(), 20.into()];
        state.add_key(&index, None);
        insert(&mut state, row.clone());

        match state.lookup(&index.columns, &KeyType::Double((1.into(), 2.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        match state.lookup(&index.columns, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows[0], row);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_multiple_indices() {
        let mut state = setup_persistent("persistent_state_multiple_indices");
        let first: Vec<DataType> = vec![10.into(), "Cat".try_into().unwrap(), 1.into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".try_into().unwrap(), 1.into()];
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(&Index::new(IndexType::BTreeMap, vec![1, 2]), None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0], first);
            }
            _ => unreachable!(),
        }

        match state.lookup(
            &[1, 2],
            &KeyType::Double(("Cat".try_into().unwrap(), 1.into())),
        ) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&rows[0], &first);
                assert_eq!(&rows[1], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_primary_key() {
        let pk = Index::new(IndexType::BTreeMap, vec![0, 1]);
        let mut state = PersistentState::new(
            String::from("persistent_state_primary_key"),
            Some(&pk.columns),
            &PersistenceParameters::default(),
        );
        let first: Vec<DataType> = vec![1.into(), 2.into(), "Cat".try_into().unwrap()];
        let second: Vec<DataType> = vec![10.into(), 20.into(), "Cat".try_into().unwrap()];
        state.add_key(&pk, None);
        state.add_key(&Index::new(IndexType::BTreeMap, vec![2]), None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);

        match state.lookup(&pk.columns, &KeyType::Double((1.into(), 2.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&pk.columns, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }

        match state.lookup(&pk.columns, &KeyType::Double((1.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 0);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[2], &KeyType::Single(&"Cat".try_into().unwrap())) {
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
        state.add_key(&pk, None);
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
        let mut state = setup_persistent("persistent_state_multiple_indices");
        let first: Vec<DataType> = vec![0.into(), 0.into()];
        let second: Vec<DataType> = vec![0.into(), 1.into()];
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);
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
        let mut state = setup_persistent("persistent_state_different_indices");
        let first: Vec<DataType> = vec![10.into(), "Cat".try_into().unwrap()];
        let second: Vec<DataType> = vec![20.into(), "Bob".try_into().unwrap()];
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &KeyType::Single(&"Bob".try_into().unwrap())) {
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
        let first: Vec<DataType> = vec![10.into(), "Cat".try_into().unwrap()];
        let second: Vec<DataType> = vec![20.into(), "Bob".try_into().unwrap()];
        {
            let mut state = PersistentState::new(name.clone(), None, &params);
            state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
            state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);
            state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);
        }

        let state = PersistentState::new(name, None, &params);
        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &KeyType::Single(&"Bob".try_into().unwrap())) {
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
        let first: Vec<DataType> = vec![10.into(), "Cat".try_into().unwrap()];
        let second: Vec<DataType> = vec![20.into(), "Bob".try_into().unwrap()];
        {
            let mut state = PersistentState::new(name.clone(), Some(&[0]), &params);
            state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
            state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);
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

        match state.lookup(&[1], &KeyType::Single(&"Bob".try_into().unwrap())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_remove() {
        let mut state = setup_persistent("persistent_state_remove");
        let first: Vec<DataType> = vec![10.into(), "Cat".try_into().unwrap()];
        let duplicate: Vec<DataType> = vec![10.into(), "Other Cat".try_into().unwrap()];
        let second: Vec<DataType> = vec![20.into(), "Cat".try_into().unwrap()];
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);
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

        // We only want to remove rows that match exactly, not all rows that match the key:
        match state.lookup(&[0], &KeyType::Single(&first[0])) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &duplicate);
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
    fn persistent_state_is_useful() {
        let mut state = setup_persistent("persistent_state_is_useful");
        assert!(!state.is_useful());
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        assert!(state.is_useful());
    }

    #[test]
    fn persistent_state_rows() {
        let mut state = setup_persistent("persistent_state_rows");
        let mut rows = vec![];
        for i in 0..30 {
            let row = vec![DataType::from(i); 30];
            rows.push(row);
            state.add_key(&Index::new(IndexType::BTreeMap, vec![i]), None);
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
    fn persistent_state_deep_size_of() {
        let state = setup_persistent("persistent_state_deep_size_of");
        let size = state.deep_size_of();
        assert_eq!(size, 0);
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
            let mut state = PersistentState::new(name.clone(), None, &params);
            state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
            state.process_records(&mut rows.clone().into(), None, None);
            // Add a second index that we'll have to build in add_key:
            state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);
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
        let mut state = PersistentState::new(name, None, &params);
        assert_eq!(state.indices.len(), 1);
        // Now, re-add the second index which should trigger an index build:
        state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);
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
        let mut state = setup_persistent("persistent_state_all_rows");
        let mut rows = vec![];
        for i in 0..10 {
            let row = vec![DataType::from(i); 10];
            rows.push(row);
            // Add a bunch of indices to make sure the sorting in all_rows()
            // correctly filters out non-primary indices:
            state.add_key(&Index::new(IndexType::BTreeMap, vec![i]), None);
        }

        for row in rows.iter().cloned() {
            insert(&mut state, row);
        }

        let actual_rows: Vec<Vec<DataType>> = state
            .all_rows()
            .map(|(_key, value)| bincode::deserialize(&value).unwrap())
            .collect();

        assert_eq!(actual_rows, rows);
    }

    #[test]
    fn persistent_state_cloned_records() {
        let mut state = setup_persistent("persistent_state_cloned_records");
        let first: Vec<DataType> = vec![10.into(), "Cat".try_into().unwrap()];
        let second: Vec<DataType> = vec![20.into(), "Cat".try_into().unwrap()];
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None, None);

        assert_eq!(state.cloned_records(), vec![first, second]);
    }

    #[test]
    #[cfg(not(windows))]
    fn persistent_state_drop() {
        let path = {
            let state = PersistentState::new(
                String::from(".s-o_u#p."),
                None,
                &PersistenceParameters::default(),
            );
            let dir = state._directory.unwrap();
            let path = dir.path();
            assert!(path.exists());
            String::from(path.to_str().unwrap())
        };

        assert!(!PathBuf::from(path).exists());
    }

    #[test]
    fn persistent_state_old_records_new_index() {
        let mut state = setup_persistent("persistent_state_old_records_new_index");
        let row: Vec<DataType> = vec![10.into(), "Cat".try_into().unwrap()];
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        insert(&mut state, row.clone());
        state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(&rows[0], &row),
            _ => unreachable!(),
        };
    }

    #[test]
    fn persistent_state_process_records() {
        let mut state = setup_persistent("persistent_state_process_records");
        let records: Records = vec![
            (vec![1.into(), "A".try_into().unwrap()], true),
            (vec![2.into(), "B".try_into().unwrap()], true),
            (vec![3.into(), "C".try_into().unwrap()], true),
            (vec![1.into(), "A".try_into().unwrap()], false),
        ]
        .into();

        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
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
        let mut state = setup_persistent("replication_offset_roundtrip");
        state.add_key(&Index::new(IndexType::HashMap, vec![0]), None);
        let mut records: Records = vec![(vec![1.into(), "A".try_into().unwrap()], true)].into();
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
        let mut state = setup_persistent("persistent_state_prefix_transform");
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        let data = (DataType::from(1), DataType::from(10));
        let r = KeyType::Double(data.clone());
        let k = PersistentState::serialize_prefix(&r);
        let prefix = prefix_transform(&k);
        let size: u64 = bincode::deserialize(prefix).unwrap();
        assert_eq!(size, bincode::serialized_size(&data).unwrap());

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
