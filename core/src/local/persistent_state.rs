use std::borrow::Cow;
use std::rc::Rc;

use bincode;
use itertools::Itertools;
use rocksdb::{self, SliceTransform, WriteBatch};

use ::*;
use data::SizeOf;

// Uniquely identifies an index for each base (the numeral index in PersistentState.indices).
type IndexID = u32;

// Incremented on each PersistentState initialization so that IndexSeq
// can be used to create unique identifiers for rows.
type IndexEpoch = u64;

// Monotonically increasing sequence number since last IndexEpoch used to uniquely identify a row.
type IndexSeq = u64;

// RocksDB key used for storing meta information (like indices).
const META_KEY: &'static [u8] = b"meta";

// Maximum rows per WriteBatch when building new indices for existing rows.
const INDEX_BATCH_SIZE: usize = 512;

struct PersistentIndex {
    columns: Vec<usize>,
    seq: IndexSeq,
}

// Store index information in RocksDB to avoid rebuilding indices on recovery.
#[derive(Default, Serialize, Deserialize)]
struct PersistentMeta {
    indices: Vec<Vec<usize>>,
    epoch: IndexEpoch,
}

impl PersistentIndex {
    fn new(columns: Vec<usize>) -> Self {
        Self { columns, seq: 0 }
    }
}

/// PersistentState stores data in RocksDB.
pub struct PersistentState {
    name: String,
    db_opts: rocksdb::Options,
    // We don't really want DB to be an option, but doing so lets us drop it manually in
    // PersistenState's Drop by setting `self.db = None` - after which we can then discard the
    // persisted files if we want to.
    db: Option<rocksdb::DB>,
    durability_mode: DurabilityMode,
    // The first element is always considered the primary index, where the actual data is stored.
    // Subsequent indices maintain pointers to the data in the first index, and cause an additional
    // read during lookups. When `self.has_unique_index` is true the first index is a primary key,
    // and all its keys are considered unique.
    indices: Vec<PersistentIndex>,
    epoch: IndexEpoch,
    has_unique_index: bool,
}

impl SizeOf for PersistentState {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        self.size_of()
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
                Record::BaseOperation(..) => unreachable!(),
            }
        }

        // Sync the writes to RocksDB's WAL:
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(true);
        self.db.as_ref().unwrap().write_opt(batch, &opts).unwrap();
    }

    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let db = self.db.as_ref().unwrap();
        let index_id = self.indices
            .iter()
            .position(|index| &index.columns[..] == columns)
            .expect("lookup on non-indexed column set") as u32;

        let prefix = Self::serialize_prefix(index_id, &key);
        let data = if index_id == 0 && self.has_unique_index {
            // This is a primary key, so we know there's only one row to retrieve
            // (no need to use prefix_iterator).
            let raw_row = db.get(&prefix).unwrap();
            if let Some(raw) = raw_row {
                let row: Vec<DataType> = bincode::deserialize(&*raw).unwrap();
                vec![Row(Rc::new(row))]
            } else {
                vec![]
            }
        } else if index_id == 0 {
            // The first index isn't unique, so we'll have to use a prefix_iterator.
            db.prefix_iterator(&prefix)
                .map(|(_key, value)| {
                    let row: Vec<DataType> = bincode::deserialize(&*value).unwrap();
                    Row(Rc::new(row))
                })
                .collect()
        } else {
            // For non-primary indices we need to first retrieve the primary index key
            // through our secondary indices, and then call `.get` again on that.
            db.prefix_iterator(&prefix)
                .map(|(_key, value)| {
                    let raw_row = db.get(&value)
                        .unwrap()
                        .expect("secondary index pointed to missing primary key value");
                    let row: Vec<DataType> = bincode::deserialize(&*raw_row).unwrap();
                    Row(Rc::new(row))
                })
                .collect()
        };

        LookupResult::Some(Cow::Owned(data))
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        assert!(partial.is_none(), "Bases can't be partial");
        let existing = self.indices
            .iter()
            .any(|index| &index.columns[..] == columns);

        if existing {
            return;
        }

        let cols = Vec::from(columns);
        let index_id = self.indices.len() as u32;
        let mut seq = 0;

        // Build the new index for existing values:
        for chunk in self.all_rows().chunks(INDEX_BATCH_SIZE).into_iter() {
            let mut batch = WriteBatch::default();
            for (ref pk, ref value) in chunk {
                seq += 1;
                let row: Vec<DataType> = bincode::deserialize(&value).unwrap();
                let index_key = KeyType::from(columns.iter().map(|i| &row[*i]));
                let key = self.serialize_key(index_id, &index_key, seq);
                batch.put(&key, &pk).unwrap();
            }

            self.db.as_ref().unwrap().write(batch).unwrap();
        }

        self.indices.push(PersistentIndex { columns: cols, seq });
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

    fn rows(&self) -> usize {
        // TODO(ekmartin): Use estimated property or return 0?
        self.all_rows().count()
    }

    fn is_useful(&self) -> bool {
        self.indices.len() > 0
    }

    fn is_partial(&self) -> bool {
        false
    }

    fn mark_filled(&mut self, _: Vec<DataType>, _: &Tag) {
        unreachable!("PersistentState can't be partial")
    }

    fn mark_hole(&mut self, _: &[DataType], _: &Tag) {
        unreachable!("PersistentState can't be partial")
    }

    fn evict_random_keys(&mut self, _: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        unreachable!("can't evict keys from PersistentState")
    }

    fn evict_keys(&mut self, _: &Tag, _: &[Vec<DataType>]) -> (&[usize], u64) {
        unreachable!("can't evict keys from PersistentState")
    }
}

impl PersistentState {
    pub fn new(
        name: String,
        primary_key: Option<&[usize]>,
        params: &PersistenceParameters,
    ) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_bloom_filter(10, true);
        opts.set_block_based_table_factory(&block_opts);

        if let Some(ref path) = params.log_dir {
            // Append the db name to the WAL path to ensure
            // that we create a directory for each base shard:
            opts.set_wal_dir(path.join(&name));
        }

        // Create prefixes by Self::transform_fn on all new inserted keys:
        let transform = SliceTransform::create("key", Self::transform_fn, Some(Self::in_domain_fn));
        opts.set_prefix_extractor(transform);

        // Assigns the number of threads for RocksDB's low priority background pool:
        opts.increase_parallelism(params.persistence_threads);

        // Use a hash linked list since we're doing prefix seeks.
        opts.set_allow_concurrent_memtable_write(false);
        opts.set_memtable_factory(rocksdb::MemtableFactory::HashLinkList {
            bucket_count: 1_000_000,
        });

        let full_name = format!("{}.db", name);
        let db = rocksdb::DB::open(&opts, &full_name).unwrap();
        let meta = Self::retrieve_and_update_meta(&db);
        let mut indices: Vec<PersistentIndex> = meta.indices
            .into_iter()
            .map(|columns| PersistentIndex::new(columns))
            .collect();

        if let Some(pk_cols) = primary_key {
            indices.insert(0, PersistentIndex::new(pk_cols.to_vec()));
        }

        Self {
            indices,
            has_unique_index: primary_key.is_some(),
            epoch: meta.epoch,
            db_opts: opts,
            db: Some(db),
            durability_mode: params.mode.clone(),
            name: full_name,
        }
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
        // Stores the columns of self.indices in RocksDB so that we don't rebuild indices on recovery.
        let start_at = if self.has_unique_index {
            // Skip the first index if it's a primary key, since we'll add it in Self::new anyway.
            1
        } else {
            0
        };

        let columns = self.indices[start_at..]
            .iter()
            .map(|i| i.columns.clone())
            .collect();
        let meta = PersistentMeta {
            indices: columns,
            epoch: self.epoch,
        };

        let data = bincode::serialize(&meta).unwrap();
        let db = self.db.as_ref().unwrap();
        db.put(META_KEY, &data).unwrap();
    }

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
    fn transform_fn(key: &[u8]) -> Vec<u8> {
        // We'll have to make sure this isn't the META_KEY even when we're filtering it out
        // in Self::in_domain_fn, as the SliceTransform is used to make hashed keys for our
        // HashLinkedList memtable factory.
        if key == META_KEY {
            return Vec::from(key);
        }

        // IndexID is a u32, so bincode uses 4 bytes to serialize it (which we'll skip past):
        let start = 4;
        // We encoded the size of the key itself with a u64, which bincode uses 8 bytes to encode:
        let size_offset = start + 8;
        let key_size: u64 = bincode::deserialize(&key[start..size_offset]).unwrap();
        let prefix_len = size_offset + key_size as usize;
        // Strip away the IndexEpoch and IndexSeq if we haven't already done so:
        Vec::from(&key[..prefix_len])
    }

    // Decides which keys the prefix transform should apply to.
    fn in_domain_fn(key: &[u8]) -> bool {
        key != META_KEY
    }

    // A key is built up of five components:
    //  * `index_id`
    //     Uniquely identifies the index in self.indices.
    //  * `key_size`
    //      The byte size of `key` when serialized with bincode.
    //  * `key`
    //      The actual index values.
    //  * `epoch`
    //      Incremented on each PersistentState initialization.
    //  * `seq`
    //      Sequence number since last epoch.
    fn serialize_key(&self, index_id: IndexID, key: &KeyType, seq: IndexSeq) -> Vec<u8> {
        let size: u64 = bincode::serialized_size(&key).unwrap();
        bincode::serialize(&(index_id, size, key, self.epoch, seq)).unwrap()
    }

    // Used with DB::prefix_iterator to go through all the rows for a given key.
    fn serialize_prefix(index: IndexID, key: &KeyType) -> Vec<u8> {
        let size: u64 = bincode::serialized_size(&key).unwrap();
        bincode::serialize(&(index, size, key)).unwrap()
    }

    // Filters out secondary indices to return an iterator for the actual key-value pairs.
    fn all_rows(&self) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> {
        self.db
            .as_ref()
            .unwrap()
            .full_iterator(rocksdb::IteratorMode::Start)
            .filter(|&(ref key, _)| {
                // Filter out non-pk indices:
                let i: IndexID = bincode::deserialize(&key).unwrap();
                i == 0
            })
    }

    // Puts by primary key first, then retrieves the existing value for each index and appends the
    // newly created primary key value.
    // TODO(ekmartin): This will put exactly the values that are given, and can only be retrieved
    // with exactly those values. I think the regular state implementation supports inserting
    // something like an Int and retrieving with a BigInt.
    fn insert(&mut self, batch: &mut WriteBatch, r: &[DataType]) {
        for index in self.indices[1..].iter_mut() {
            index.seq += 1;
        }

        let serialized_pk = {
            let pk = KeyType::from(self.indices[0].columns.iter().map(|i| &r[*i]));
            if self.has_unique_index {
                Self::serialize_prefix(0, &pk)
            } else {
                // For bases without primary keys we store the actual row values keyed by the index
                // that was added first. This means that we can't consider the keys unique though, so
                // we'll append a sequence number.
                self.indices[0].seq += 1;
                self.serialize_key(0, &pk, self.indices[0].seq)
            }
        };

        // First insert the actual value for our primary index:
        let pk_value = bincode::serialize(&r).unwrap();
        batch.put(&serialized_pk, &pk_value).unwrap();

        // Then insert primary key pointers for all the secondary indices:
        for (i, index) in self.indices[1..].iter().enumerate() {
            // Construct a key with the index values, and serialize it with bincode:
            let key = KeyType::from(index.columns.iter().map(|i| &r[*i]));
            // Add 1 to i since we're slicing self.indices by 1..:
            let serialized_key = self.serialize_key((i + 1) as u32, &key, index.seq);
            batch.put(&serialized_key, &serialized_pk).unwrap();
        }
    }

    fn remove(&self, batch: &mut WriteBatch, r: &[DataType]) {
        let db = self.db.as_ref().unwrap();
        let mut do_remove = move |indices: &[PersistentIndex], primary_key: &[u8]| {
            // First delete the actual primary key row:
            batch.delete(&primary_key).unwrap();

            // Then delete any references that point _exactly_ to
            // that row (i.e. not all matching rows):
            for (i, index) in indices.iter().enumerate() {
                // Construct a key with the index values, and serialize it with bincode:
                let key = KeyType::from(index.columns.iter().map(|i| &r[*i]));
                // Add 1 to i since we're slicing self.indices by 1..:
                let prefix = Self::serialize_prefix((i + 1) as u32, &key);
                for (raw_key, raw_value) in db.prefix_iterator(&prefix) {
                    if &*raw_value == &primary_key[..] {
                        batch.delete(&raw_key).unwrap();
                    }
                }
            }
        };

        // First retrieve the actual stored row that matches r:
        let pk_index = &self.indices[0];
        let pk = KeyType::from(pk_index.columns.iter().map(|i| &r[*i]));
        let prefix = Self::serialize_prefix(0, &pk);
        if self.has_unique_index {
            let raw_value = db.get(&prefix).unwrap();
            if let Some(raw) = raw_value {
                let value: Vec<DataType> = bincode::deserialize(&*raw).unwrap();
                if r != &value[..] {
                    return;
                }
            } else {
                return;
            }

            do_remove(&self.indices[..], &prefix[..]);
        } else {
            let key = db.prefix_iterator(&prefix).find(|(_, raw_value)| {
                let value: Vec<DataType> = bincode::deserialize(&*raw_value).unwrap();
                r == &value[..]
            });

            if key.is_none() {
                return;
            }

            let (raw_key, _) = key.unwrap();
            do_remove(&self.indices[1..], &raw_key[..]);
        };
    }
}

impl Drop for PersistentState {
    fn drop(&mut self) {
        if self.durability_mode != DurabilityMode::Permanent {
            self.db = None;
            rocksdb::DB::destroy(&self.db_opts, &self.name).unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use bincode;

    fn insert<S: State>(state: &mut S, row: Vec<DataType>) {
        let record: Record = row.into();
        state.process_records(&mut record.into(), None);
    }

    fn get_name(prefix: &str) -> String {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        format!(
            "{}.{}.{}",
            prefix,
            current_time.as_secs(),
            current_time.subsec_nanos()
        )
    }

    fn setup_persistent(prefix: &str) -> PersistentState {
        PersistentState::new(get_name(prefix), None, &PersistenceParameters::default())
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
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            LookupResult::Missing => panic!("PersistentStates can't be materialized"),
        };

        match state.lookup(columns, &KeyType::Single(&10.into())) {
            LookupResult::Some(rows) => {
                let data = &*rows[0];
                assert_eq!(data[0], 10.into());
                assert_eq!(data[1], "Cat".into());
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
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            LookupResult::Missing => panic!("PersistentStates can't be materialized"),
        };

        match state.lookup(columns, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(&*rows[0], &row);
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
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1, 2], &KeyType::Double(("Cat".into(), 1.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&*rows[0], &first);
                assert_eq!(&*rows[1], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_primary_key() {
        let pk = &[0, 1];
        let name = get_name("persistent_state_primary_key");
        let mut state = PersistentState::new(name, Some(pk), &PersistenceParameters::default());
        let first: Vec<DataType> = vec![1.into(), 2.into(), "Cat".into()];
        let second: Vec<DataType> = vec![10.into(), 20.into(), "Cat".into()];
        state.add_key(pk, None);
        state.add_key(&[2], None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);

        match state.lookup(pk, &KeyType::Double((1.into(), 2.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(pk, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &second);
            }
            _ => unreachable!(),
        }

        match state.lookup(pk, &KeyType::Double((1.into(), 20.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 0);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[2], &KeyType::Single(&"Cat".into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&*rows[0], &first);
                assert_eq!(&*rows[1], &second);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn persistent_state_primary_key_delete() {
        let pk = &[0];
        let name = get_name("persistent_state_primary_key_delete");
        let mut state = PersistentState::new(name, Some(pk), &PersistenceParameters::default());
        let first: Vec<DataType> = vec![1.into(), 2.into()];
        let second: Vec<DataType> = vec![10.into(), 20.into()];
        state.add_key(pk, None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);
        match state.lookup(&[0], &KeyType::Single(&1.into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
            }
            _ => unreachable!(),
        }

        state.process_records(&mut vec![(first.clone(), false)].into(), None);
        match state.lookup(&[0], &KeyType::Single(&1.into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 0);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &second);
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
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&*rows[0], &first);
                assert_eq!(&*rows[1], &second);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &KeyType::Single(&0.into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
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
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
            }
            _ => unreachable!(),
        }

        match state.lookup(&[1], &KeyType::Single(&"Bob".into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &second);
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
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &duplicate);
            }
            LookupResult::Missing => panic!("PersistentStates can't be materialized"),
        };

        // Also shouldn't have removed other keys:
        match state.lookup(&[0], &KeyType::Single(&second[0])) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &second);
            }
            _ => unreachable!(),
        }

        // Make sure we didn't remove secondary keys pointing to different rows:
        match state.lookup(&[1], &KeyType::Single(&second[1])) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &second);
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
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(&[0], None);
        state.add_key(&[1], None);
        assert_eq!(state.rows(), 0);
        insert(&mut state, first.clone());
        assert_eq!(state.rows(), 1);
        insert(&mut state, second.clone());
        assert_eq!(state.rows(), 2);
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
        let name = ".s-o_u#p.";
        let db_name = format!("{}.db", name);
        let path = Path::new(&db_name);
        {
            let _state =
                PersistentState::new(String::from(name), None, &PersistenceParameters::default());
            assert!(path.exists());
        }

        assert!(!path.exists());
    }

    #[test]
    fn persistent_state_old_records_new_index() {
        let mut state = setup_persistent("persistent_state_old_records_new_index");
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(&[0], None);
        insert(&mut state, row.clone());
        state.add_key(&[1], None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(rows) => assert_eq!(&*rows[0], &row),
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
        ].into();

        state.add_key(&[0], None);
        state.process_records(&mut Vec::from(&records[..3]).into(), None);
        state.process_records(&mut records[3].clone().into(), None);

        // Make sure the first record has been deleted:
        match state.lookup(&[0], &KeyType::Single(&records[0][0])) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        // Then check that the rest exist:
        for i in 1..3 {
            let record = &records[i];
            match state.lookup(&[0], &KeyType::Single(&record[0])) {
                LookupResult::Some(rows) => assert_eq!(&*rows[0], &**record),
                _ => unreachable!(),
            };
        }
    }

    #[test]
    fn persistent_state_transform_fn() {
        let state = setup_persistent("persistent_state_transform_fn");
        let index = PersistentIndex {
            seq: 10,
            columns: Default::default(),
        };

        let r = KeyType::Double((1.into(), 10.into()));
        let i = 5;
        let k = state.serialize_key(i, &r, index.seq);
        let prefix = PersistentState::transform_fn(&k);
        let (key_out, size): (IndexID, u64) = bincode::deserialize(&prefix).unwrap();
        assert_eq!(i, key_out);
        assert_eq!(size, bincode::serialized_size(&r).unwrap());

        // prefix_extractor requirements:
        // 1) key.starts_with(prefix(key))
        assert!(k.starts_with(&prefix));

        // 2) Compare(prefix(key), key) <= 0.
        assert!(&prefix <= &k);

        // 3) If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
        let other_k = state.serialize_key(i * 2, &r, index.seq);
        let other_prefix = PersistentState::transform_fn(&other_k);
        assert!(k <= other_k);
        assert!(prefix <= other_prefix);

        // 4) prefix(prefix(key)) == prefix(key)
        assert_eq!(prefix, PersistentState::transform_fn(&prefix));
    }
}
