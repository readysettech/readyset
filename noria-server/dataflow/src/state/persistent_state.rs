use bincode;
use itertools::Itertools;
use rocksdb::{self, ColumnFamily, SliceTransform, SliceTransformFns, WriteBatch};
use serde;
use tempfile::{tempdir, TempDir};

use common::SizeOf;
use prelude::*;
use state::{RecordResult, State};

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

// Store index information in RocksDB to avoid rebuilding indices on recovery.
#[derive(Default, Serialize, Deserialize)]
struct PersistentMeta {
    indices: Vec<Vec<usize>>,
    epoch: IndexEpoch,
}

#[derive(Clone)]
struct PersistentIndex {
    column_family: ColumnFamily,
    columns: Vec<usize>,
}

impl PersistentIndex {
    fn new(column_family: ColumnFamily, columns: Vec<usize>) -> Self {
        Self {
            column_family,
            columns,
        }
    }
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
    has_unique_index: bool,
    // With DurabilityMode::DeleteOnExit,
    // RocksDB files are stored in a temporary directory.
    _directory: Option<TempDir>,
}

struct PrefixTransform;

// SliceTransforms are used to create prefixes of all inserted keys, which can then be used for
// both bloom filters and hash structure lookups.
impl SliceTransformFns for PrefixTransform {
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
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
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
    fn in_domain(&mut self, key: &[u8]) -> bool {
        key != META_KEY
    }
}

impl SizeOf for PersistentState {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        self.db
            .as_ref()
            .unwrap()
            .property_int_value("rocksdb.estimate-live-data-size")
            .unwrap()
    }
}

impl State for PersistentState {
    fn process_records(&mut self, records: &mut Records, partial_tag: Option<Tag>) {
        assert!(partial_tag.is_none(), "PersistentState can't be partial");
        if records.len() == 0 {
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

        // Sync the writes to RocksDB's WAL:
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(true);
        self.db.as_ref().unwrap().write_opt(batch, &opts).unwrap();
    }

    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let db = self.db.as_ref().unwrap();
        let index_id = self
            .indices
            .iter()
            .position(|index| &index.columns[..] == columns)
            .expect("lookup on non-indexed column set");
        let cf = self.indices[index_id].column_family;
        let prefix = Self::serialize_prefix(&key);
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
                .unwrap()
                .map(|(_key, value)| bincode::deserialize(&*value).unwrap())
                .collect()
        };

        LookupResult::Some(RecordResult::Owned(data))
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        assert!(partial.is_none(), "Bases can't be partial");
        let existing = self
            .indices
            .iter()
            .any(|index| &index.columns[..] == columns);

        if existing {
            return;
        }

        let cols = Vec::from(columns);
        // We'll store all the pointers (or values if this is index 0) for
        // this index in its own column family:
        let index_id = self.indices.len().to_string();
        let column_family = self
            .db
            .as_mut()
            .unwrap()
            .create_cf(&index_id, &self.db_opts)
            .unwrap();

        // Build the new index for existing values:
        if !self.indices.is_empty() {
            for chunk in self.all_rows().chunks(INDEX_BATCH_SIZE).into_iter() {
                let mut batch = WriteBatch::default();
                for (ref pk, ref value) in chunk {
                    let row: Vec<DataType> = bincode::deserialize(&value).unwrap();
                    let index_key = Self::build_key(&row, columns);
                    let key = Self::serialize_secondary(&index_key, pk);
                    batch.put_cf(column_family, &key, value).unwrap();
                }

                self.db.as_ref().unwrap().write(batch).unwrap();
            }
        }

        self.indices.push(PersistentIndex {
            columns: cols,
            column_family,
        });

        self.persist_meta();
    }

    fn keys(&self) -> Vec<Vec<usize>> {
        self.indices
            .iter()
            .map(|index| index.columns.clone())
            .collect()
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        self.all_rows()
            .map(|(_, ref value)| bincode::deserialize(&value).unwrap())
            .collect()
    }

    // Returns a row count estimate from RocksDB.
    fn rows(&self) -> usize {
        let db = self.db.as_ref().unwrap();
        let cf = db.cf_handle("0").unwrap();
        let total_keys = db
            .property_int_value_cf(cf, "rocksdb.estimate-num-keys")
            .unwrap() as usize;

        (total_keys / self.indices.len())
    }

    fn is_useful(&self) -> bool {
        !self.indices.is_empty()
    }

    fn is_partial(&self) -> bool {
        false
    }

    fn mark_filled(&mut self, _: Vec<DataType>, _: Tag) {
        unreachable!("PersistentState can't be partial")
    }

    fn mark_hole(&mut self, _: &[DataType], _: Tag) {
        unreachable!("PersistentState can't be partial")
    }

    fn evict_random_keys(&mut self, _: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        unreachable!("can't evict keys from PersistentState")
    }

    fn evict_keys(&mut self, _: Tag, _: &[Vec<DataType>]) -> Option<(&[usize], u64)> {
        unreachable!("can't evict keys from PersistentState")
    }
}

impl PersistentState {
    pub fn new(
        name: String,
        primary_key: Option<&[usize]>,
        params: &PersistenceParameters,
    ) -> Self {
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
        // We use a column for each index, and one for meta information.
        // When opening the DB the exact same column families needs to be used,
        // so we'll have to retrieve the existing ones first:
        let column_family_names = match DB::list_cf(&opts, &full_name) {
            Ok(cfs) => cfs,
            Err(_err) => vec![DEFAULT_CF.to_string()],
        };

        let make_cfs = || {
            column_family_names
                .iter()
                .map(|cf| {
                    ColumnFamilyDescriptor::new(cf.clone(), Self::build_options(&name, &params))
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
        let meta = Self::retrieve_and_update_meta(&db);
        let indices: Vec<PersistentIndex> = meta
            .indices
            .into_iter()
            .enumerate()
            .map(|(i, columns)| {
                let cf = db.cf_handle(&i.to_string()).unwrap();
                PersistentIndex::new(cf, columns)
            })
            .collect();

        // If there are more column families than indices (-1 to account for the default column
        // family) we probably crashed while trying to build the last index (in Self::add_key), so
        // we'll throw away our progress and try re-building it again later:
        if column_family_names.len() - 1 > indices.len() {
            db.drop_cf(&indices.len().to_string()).unwrap();
        }

        let mut state = Self {
            seq: 0,
            indices,
            has_unique_index: primary_key.is_some(),
            epoch: meta.epoch,
            db_opts: opts,
            db: Some(db),
            _directory: directory,
        };

        if primary_key.is_some() && state.indices.is_empty() {
            // This is the first time we're initializing this PersistentState,
            // so persist the primary key index right away.
            let cf = state
                .db
                .as_mut()
                .unwrap()
                .create_cf("0", &state.db_opts)
                .unwrap();
            state
                .indices
                .push(PersistentIndex::new(cf, primary_key.unwrap().to_vec()));
            state.persist_meta();
        }

        state
    }

    fn build_options(name: &str, params: &PersistenceParameters) -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let key_len = 0; // variable key length
        let bloom_bits_per_key = 10;
        let hash_table_ratio = 0.75;
        let index_sparseness = 16;
        opts.set_plain_table_factory(
            key_len,
            bloom_bits_per_key,
            hash_table_ratio,
            index_sparseness,
        );

        if let Some(ref path) = params.log_dir {
            // Append the db name to the WAL path to ensure
            // that we create a directory for each base shard:
            opts.set_wal_dir(path.join(&name));
        }

        // Create prefixes with PrefixTransform on all new inserted keys:
        let transform = SliceTransform::create("key", Box::new(PrefixTransform));
        opts.set_prefix_extractor(transform);

        // Assigns the number of threads for compactions and flushes in RocksDB.
        // Optimally we'd like to use env->SetBackgroundThreads(n, Env::HIGH)
        // and env->SetBackgroundThreads(n, Env::LOW) here, but that would force us to create our
        // own env instead of relying on the default one that's shared across RocksDB instances
        // (which isn't supported by rust-rocksdb yet either).
        //
        // Using opts.increase_parallelism here would only change the thread count in
        // the low priority pool, so we'll rather use the deprecated max_background_compactions
        // and max_background_flushes for now.
        if params.persistence_threads > 1 {
            // Split the threads between compactions and flushes,
            // but round up for compactions and down for flushes:
            opts.set_max_background_compactions((params.persistence_threads + 1) / 2);
            opts.set_max_background_flushes(params.persistence_threads / 2);
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

    fn retrieve_and_update_meta(db: &rocksdb::DB) -> PersistentMeta {
        let indices = db.get(META_KEY).unwrap();
        let mut meta = match indices {
            Some(data) => bincode::deserialize(&*data).unwrap(),
            None => PersistentMeta::default(),
        };

        meta.epoch += 1;
        let data = bincode::serialize(&meta).unwrap();
        db.put(META_KEY, &data).unwrap();
        meta
    }

    fn persist_meta(&mut self) {
        let db = self.db.as_ref().unwrap();
        // Stores the columns of self.indices in RocksDB so that we don't rebuild indices on recovery.
        let columns = self.indices.iter().map(|i| i.columns.clone()).collect();
        let meta = PersistentMeta {
            indices: columns,
            epoch: self.epoch,
        };

        let data = bincode::serialize(&meta).unwrap();
        db.put(META_KEY, &data).unwrap();
    }

    // Our RocksDB keys come in three forms, and are encoded as follows:
    //
    // * Unique Primary Keys
    // (size, key), where size is the serialized byte size of `key`
    // (used in PrefixTransform::transform).
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
        fn serialize<K: serde::Serialize, E: serde::Serialize>(k: K, extra: E) -> Vec<u8> {
            let size: u64 = bincode::serialized_size(&k).unwrap();
            bincode::serialize(&(size, k, extra)).unwrap()
        }

        match key {
            KeyType::Single(k) => serialize(k, extra),
            KeyType::Double(k) => serialize(k, extra),
            KeyType::Tri(k) => serialize(k, extra),
            KeyType::Quad(k) => serialize(k, extra),
            KeyType::Quin(k) => serialize(k, extra),
            KeyType::Sex(k) => serialize(k, extra),
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

    // Filters out secondary indices to return an iterator for the actual key-value pairs.
    fn all_rows(&self) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> {
        let db = self.db.as_ref().unwrap();
        let cf = self.indices[0].column_family;
        db.full_iterator_cf(cf, rocksdb::IteratorMode::Start)
            .unwrap()
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
        let value_cf = self.indices[0].column_family;
        batch
            .put_cf(value_cf, &serialized_pk, &serialized_row)
            .unwrap();

        // Then insert primary key pointers for all the secondary indices:
        for index in self.indices[1..].iter() {
            // Construct a key with the index values, and serialize it with bincode:
            let key = Self::build_key(&r, &index.columns);
            let serialized_key = Self::serialize_secondary(&key, &serialized_pk);
            batch
                .put_cf(index.column_family, &serialized_key, &serialized_row)
                .unwrap();
        }
    }

    fn remove(&self, batch: &mut WriteBatch, r: &[DataType]) {
        let db = self.db.as_ref().unwrap();
        let pk_index = &self.indices[0];
        let value_cf = pk_index.column_family;
        let mut do_remove = move |primary_key: &[u8]| {
            // Delete the value row first (primary index):
            batch.delete_cf(value_cf, &primary_key).unwrap();

            // Then delete any references that point _exactly_ to that row:
            for index in self.indices[1..].iter() {
                let key = Self::build_key(&r, &index.columns);
                let serialized_key = Self::serialize_secondary(&key, primary_key);
                batch
                    .delete_cf(index.column_family, &serialized_key)
                    .unwrap();
            }
        };

        let pk = Self::build_key(&r, &pk_index.columns);
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
                .unwrap()
                .find(|(_, raw_value)| {
                    let value: Vec<DataType> = bincode::deserialize(&*raw_value).unwrap();
                    r == &value[..]
                })
                .expect("tried removing non-existant row");
            do_remove(&key[..]);
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;
    use std::path::PathBuf;

    fn insert<S: State>(state: &mut S, row: Vec<DataType>) {
        let record: Record = row.into();
        state.process_records(&mut record.into(), None);
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

    #[test]
    fn persistent_state_is_partial() {
        let state = setup_persistent("persistent_state_is_partial");
        assert!(!state.is_partial());
    }

    #[test]
    fn persistent_state_single_key() {
        let mut state = setup_persistent("persistent_state_single_key");
        let columns = &[0];
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(columns, None);
        insert(&mut state, row);

        match state.lookup(columns, &KeyType::Single(&5.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        match state.lookup(columns, &KeyType::Single(&10.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows[0][0], 10.into());
                assert_eq!(rows[0][1], "Cat".into());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_multi_key() {
        let mut state = setup_persistent("persistent_state_multi_key");
        let columns = &[0, 2];
        let row: Vec<DataType> = vec![10.into(), "Cat".into(), 20.into()];
        state.add_key(columns, None);
        insert(&mut state, row.clone());

        match state.lookup(columns, &KeyType::Double((1.into(), 2.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        match state.lookup(columns, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows[0], row);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_multiple_indices() {
        let mut state = setup_persistent("persistent_state_multiple_indices");
        let first: Vec<DataType> = vec![10.into(), "Cat".into(), 1.into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".into(), 1.into()];
        state.add_key(&[0], None);
        state.add_key(&[1, 2], None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);

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
    fn persistent_state_primary_key() {
        let pk = &[0, 1];
        let mut state = PersistentState::new(
            String::from("persistent_state_primary_key"),
            Some(pk),
            &PersistenceParameters::default(),
        );
        let first: Vec<DataType> = vec![1.into(), 2.into(), "Cat".into()];
        let second: Vec<DataType> = vec![10.into(), 20.into(), "Cat".into()];
        state.add_key(pk, None);
        state.add_key(&[2], None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);

        match state.lookup(pk, &KeyType::Double((1.into(), 2.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(pk, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &second);
            }
            _ => unreachable!(),
        }

        match state.lookup(pk, &KeyType::Double((1.into(), 20.into()))) {
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
        let pk = &[0];
        let mut state = PersistentState::new(
            String::from("persistent_state_primary_key_delete"),
            Some(pk),
            &PersistenceParameters::default(),
        );
        let first: Vec<DataType> = vec![1.into(), 2.into()];
        let second: Vec<DataType> = vec![10.into(), 20.into()];
        state.add_key(pk, None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);
        match state.lookup(&[0], &KeyType::Single(&1.into())) {
            LookupResult::Some(RecordResult::Owned(rows)) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&rows[0], &first);
            }
            _ => unreachable!(),
        }

        state.process_records(&mut vec![(first.clone(), false)].into(), None);
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
        state.add_key(&[0], None);
        state.add_key(&[1], None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);

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
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(&[0], None);
        state.add_key(&[1], None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);

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
            let mut state = PersistentState::new(name.clone(), None, &params);
            state.add_key(&[0], None);
            state.add_key(&[1], None);
            state.process_records(&mut vec![first.clone(), second.clone()].into(), None);
        }

        let state = PersistentState::new(name, None, &params);
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
            state.add_key(&[0], None);
            state.add_key(&[1], None);
            state.process_records(&mut vec![first.clone(), second.clone()].into(), None);
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
        let mut state = setup_persistent("persistent_state_remove");
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let duplicate: Vec<DataType> = vec![10.into(), "Other Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".into()];
        state.add_key(&[0], None);
        state.add_key(&[1], None);
        state.process_records(
            &mut vec![first.clone(), duplicate.clone(), second.clone()].into(),
            None,
        );
        state.process_records(
            &mut vec![(first.clone(), false), (first.clone(), false)].into(),
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
        let columns = &[0];
        assert!(!state.is_useful());
        state.add_key(columns, None);
        assert!(state.is_useful());
    }

    #[test]
    fn persistent_state_rows() {
        let mut state = setup_persistent("persistent_state_rows");
        let mut rows = vec![];
        for i in 0..30 {
            let row = vec![DataType::from(i); 30];
            rows.push(row);
            state.add_key(&[i], None);
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
            state.add_key(&[0], None);
            state.process_records(&mut rows.clone().into(), None);
            // Add a second index that we'll have to build in add_key:
            state.add_key(&[1], None);
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
        state.add_key(&[1], None);
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
            state.add_key(&[i], None);
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
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".into()];
        state.add_key(&[0], None);
        state.add_key(&[1], None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);

        assert_eq!(state.cloned_records(), vec![first, second]);
    }

    #[test]
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
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(&[0], None);
        insert(&mut state, row.clone());
        state.add_key(&[1], None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(RecordResult::Owned(rows)) => assert_eq!(&rows[0], &row),
            _ => unreachable!(),
        };
    }

    #[test]
    fn persistent_state_process_records() {
        let mut state = setup_persistent("persistent_state_process_records");
        let mut records: Records = vec![
            (vec![1.into(), "A".into()], true),
            (vec![2.into(), "B".into()], true),
            (vec![3.into(), "C".into()], true),
            (vec![1.into(), "A".into()], false),
        ]
        .into();

        state.add_key(&[0], None);
        state.process_records(&mut Vec::from(&records[..3]).into(), None);
        state.process_records(&mut records[3].clone().into(), None);

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
    #[allow(clippy::op_ref)]
    fn persistent_state_prefix_transform() {
        let mut state = setup_persistent("persistent_state_prefix_transform");
        state.add_key(&[0], None);
        let data = (DataType::from(1), DataType::from(10));
        let r = KeyType::Double(data.clone());
        let k = PersistentState::serialize_prefix(&r);
        let mut transform_fns = PrefixTransform;
        let prefix = transform_fns.transform(&k);
        let size: u64 = bincode::deserialize(&prefix).unwrap();
        assert_eq!(size, bincode::serialized_size(&data).unwrap());

        // prefix_extractor requirements:
        // 1) key.starts_with(prefix(key))
        assert!(k.starts_with(&prefix));

        // 2) Compare(prefix(key), key) <= 0.
        assert!(prefix <= &k[..]);

        // 3) If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
        let other_k = PersistentState::serialize_prefix(&r);
        let other_prefix = transform_fns.transform(&other_k);
        assert!(k <= other_k);
        assert!(prefix <= other_prefix);

        // 4) prefix(prefix(key)) == prefix(key)
        assert_eq!(prefix, transform_fns.transform(&prefix));
    }
}
