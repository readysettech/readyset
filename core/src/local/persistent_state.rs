use std::borrow::Cow;
use std::rc::Rc;

use bincode;
use rocksdb::{self, SliceTransform, WriteBatch, DB};

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

/// PersistentState stores data in SQlite.
pub struct PersistentState {
    name: String,
    db_opts: rocksdb::Options,
    // We don't really want DB to be an option, but doing so lets us drop it manually in
    // PersistenState's Drop by setting `self.db = None` - after which we can then discard the
    // persisted files if we want to.
    db: Option<DB>,
    durability_mode: DurabilityMode,
    indices: Vec<PersistentIndex>,
    epoch: IndexEpoch,
    primary_key: Option<Vec<usize>>,
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
                    self.insert(&mut batch, r.clone());
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
        let key_values = match *key {
            KeyType::Single(a) => vec![a],
            KeyType::Double(ref r) => vec![&r.0, &r.1],
            KeyType::Tri(ref r) => vec![&r.0, &r.1, &r.2],
            KeyType::Quad(ref r) => vec![&r.0, &r.1, &r.2, &r.3],
            KeyType::Quin(ref r) => vec![&r.0, &r.1, &r.2, &r.3, &r.4],
            KeyType::Sex(ref r) => vec![&r.0, &r.1, &r.2, &r.3, &r.4, &r.5],
        };

        let db = self.db.as_ref().unwrap();
        let data = match self.primary_key {
            Some(ref pk) if &pk[..] == columns => {
                let prefix = Self::serialize_prefix(0, &key_values);
                // This is a primary key, so we know there's only one row to retrieve
                // (no need to use prefix_iterator).
                let raw_row = db.get(&prefix).unwrap();
                if let Some(raw) = raw_row {
                    let row: Vec<DataType> = bincode::deserialize(&*raw).unwrap();
                    vec![Row(Rc::new(row))]
                } else {
                    vec![]
                }
            }
            _ => {
                let index_id = self.get_index_id(columns)
                    .expect("lookup on non-indexed column set");
                let prefix = Self::serialize_prefix(index_id, &key_values);
                let values = db.prefix_iterator(&prefix);
                if index_id > 0 {
                    // For non-primary indices we need to first retrieve the primary index key
                    // through our secondary indices, and then call `.get` again on that.
                    values
                        .map(|(_key, value)| {
                            let raw_row = db.get(&value)
                                .unwrap()
                                .expect("secondary index pointed to missing primary key value");
                            let row: Vec<DataType> = bincode::deserialize(&*raw_row).unwrap();
                            Row(Rc::new(row))
                        })
                        .collect()
                } else {
                    values
                        .map(|(_key, value)| {
                            let row: Vec<DataType> = bincode::deserialize(&*value).unwrap();
                            Row(Rc::new(row))
                        })
                        .collect()
                }
            }
        };

        LookupResult::Some(Cow::Owned(data))
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        assert!(partial.is_none(), "Bases can't be partial");
        let existing = self.indices
            .iter()
            .any(|index| &index.columns[..] == columns);
        let is_pk = match self.primary_key {
            Some(ref pk) => &pk[..] == columns,
            _ => false,
        };

        if existing || is_pk {
            return;
        }

        let cols = Vec::from(columns);
        let index_id = self.indices.len() as u32;
        let mut seq = 0;
        let mut batch = None;

        // Build the new index for existing values:
        self.all_rows().for_each(|(ref pk, ref value)| {
            seq += 1;
            let row: Vec<DataType> = bincode::deserialize(&value).unwrap();
            let index_key = columns.iter().map(|i| &row[*i]).collect::<Vec<_>>();
            let key = self.serialize_key(index_id, &index_key, seq);
            let b = batch.get_or_insert_with(|| WriteBatch::default());
            b.put(&key, &pk).unwrap();
        });

        if let Some(b) = batch {
            self.db.as_ref().unwrap().write(b).unwrap();
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
        self.all_rows().count()
    }

    fn clear(&mut self) {
        // Would potentially be faster to just drop self.db and call DB::Destroy:
        let db = self.db.as_ref().unwrap();
        for (key, _) in db.full_iterator(rocksdb::IteratorMode::Start) {
            db.delete(&key).unwrap();
        }
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
        let db = DB::open(&opts, &full_name).unwrap();
        let meta = Self::retrieve_and_update_meta(&db);
        let indices = meta.indices
            .into_iter()
            .map(|columns| PersistentIndex::new(columns))
            .collect();

        Self {
            indices,
            primary_key: primary_key.and_then(|cols| Some(Vec::from(cols))),
            epoch: meta.epoch,
            db_opts: opts,
            db: Some(db),
            durability_mode: params.mode.clone(),
            name: full_name,
        }
    }

    fn retrieve_and_update_meta(db: &DB) -> PersistentMeta {
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
        let indices: Vec<Vec<usize>> = self.indices.iter().map(|i| i.columns.clone()).collect();
        let meta = PersistentMeta {
            indices,
            epoch: self.epoch,
        };

        let data = bincode::serialize(&meta).unwrap();
        let db = self.db.as_ref().unwrap();
        db.put(META_KEY, &data).unwrap();
    }

    fn get_index_id(&self, columns: &[usize]) -> Option<IndexID> {
        self.indices
            .iter()
            .position(|index| &index.columns[..] == columns)
            .and_then(|id| {
                let id = id as IndexID;
                // Increment the index ID with one if we have a primary key,
                // as that'll always be index 0:
                Some(match self.primary_key {
                    Some(..) => id + 1,
                    None => id,
                })
            })
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
    // NOTE(ekmartin): Encoding the key size in the key increases the total size with 4 bytes.
    // If we really wanted to avoid this while still maintaining the same serialization scheme
    // we could do so by figuring out how many bytes our bincode serialized Vec<DataType> takes
    // up here in transform_fn. Example:
    // [DataType::Int(1), DataType::BigInt(10)] would be serialized as:
    // 2u64 (vec length), 0u32 (enum variant), 1i32 (value), 1u32 (enum variant), 1i64 (value)
    // By stepping through the serialized bytes and checking each enum variant we would know
    // when we reached the end, and could then with certainty say whether we'd already
    // prefix transformed this key before or not
    // (without including the byte size of Vec<DataType>).
    fn transform_fn(key: &[u8]) -> Vec<u8> {
        if key == META_KEY {
            return Vec::from(key);
        }

        // IndexID is a u32, so bincode uses 4 bytes to serialize it (which we'll skip past):
        let start = 4;
        // We encoded the size of the key itself with a u64, which bincode uses 8 bytes to encode:
        let size_offset = start + 8;
        let key_size: u64 = bincode::deserialize(&key[start..size_offset]).unwrap();
        let prefix_len = size_offset + key_size as usize;
        let mut bytes = Vec::from(key);

        // Strip away the IndexEpoch and IndexSeq if we haven't already done so:
        if key.len() > prefix_len {
            bytes.truncate(prefix_len);
        }

        bytes
    }

    // Decides which keys the prefix transform should apply to.
    fn in_domain_fn(key: &[u8]) -> bool {
        key != META_KEY
    }

    // A key is built up of five components:
    //  * `index_id`
    //     Uniquely identifies the index in self.indices.
    //  * `row_size`
    //      The byte size of `row` when serialized with binode.
    //  * `row`
    //      The actual index values.
    //  * `epoch`
    //      Incremented on each PersistentState initialization.
    //  * `seq`
    //      Sequence number since last epoch.
    fn serialize_key(&self, index_id: IndexID, row: &Vec<&DataType>, seq: IndexSeq) -> Vec<u8> {
        let size: u64 = bincode::serialized_size(&row).unwrap();
        bincode::serialize(&(index_id, size, row, self.epoch, seq)).unwrap()
    }

    // Used with DB::prefix_iterator to go through all the rows for a given key.
    fn serialize_prefix(index: IndexID, key: &Vec<&DataType>) -> Vec<u8> {
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
    fn insert(&mut self, batch: &mut WriteBatch, r: Vec<DataType>) {
        for index in self.indices.iter_mut() {
            index.seq += 1;
        }

        let (secondary_indices, serialized_pk) = if let Some(ref pk_cols) = self.primary_key {
            let pk = pk_cols.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            (&self.indices[..], Self::serialize_prefix(0, &pk))
        } else {
            // For bases without primary keys we store the actual row values keyed by the index
            // that was added first. This means that we can't consider the keys unique though, so
            // we'll append a sequence number.
            let pk_index = &self.indices[0];
            let pk = pk_index.columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            (&self.indices[1..], self.serialize_key(0, &pk, pk_index.seq))
        };

        // First insert the actual value for our primary index:
        let pk_value = bincode::serialize(&r).unwrap();
        batch.put(&serialized_pk, &pk_value).unwrap();

        // Then insert primary key pointers for all the secondary indices:
        for (i, index) in secondary_indices.iter().enumerate() {
            // Construct a key with the index values, and serialize it with bincode:
            let key = index.columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            // Add 1 to i since we're slicing self.indices by 1..:
            let serialized_key = self.serialize_key((i + 1) as u32, &key, index.seq);
            batch.put(&serialized_key, &serialized_pk).unwrap();
        }
    }

    fn remove(&self, batch: &mut WriteBatch, r: &[DataType]) {
        let db = self.db.as_ref().unwrap();
        let mut delete_cols: Vec<_> = self.indices
            .iter()
            .enumerate()
            .map(|(i, index)| {
                if self.primary_key.is_some() {
                    (i + 1, &index.columns)
                } else {
                    (i, &index.columns)
                }
            })
            .collect();

        if let Some(ref pk_cols) = self.primary_key {
            delete_cols.push((0, pk_cols));
        }

        for (i, columns) in delete_cols.into_iter() {
            let index_row = columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            let serialized_key = Self::serialize_prefix(i as u32, &index_row);
            for (key, _value) in db.prefix_iterator(&serialized_key) {
                batch.delete(&key).unwrap();
            }
        }
    }
}

impl Drop for PersistentState {
    fn drop(&mut self) {
        if self.durability_mode != DurabilityMode::Permanent {
            self.db = None;
            DB::destroy(&self.db_opts, &self.name).unwrap()
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
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);
        state.process_records(
            &mut vec![(first.clone(), false), (first.clone(), false)].into(),
            None,
        );

        match state.lookup(columns, &KeyType::Single(&first[0])) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            LookupResult::Missing => panic!("PersistentStates can't be materialized"),
        };

        match state.lookup(columns, &KeyType::Single(&second[0])) {
            LookupResult::Some(rows) => {
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
    fn persistent_state_clear() {
        let mut state = setup_persistent("persistent_state_clear");
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        state.process_records(&mut vec![first.clone(), second.clone()].into(), None);

        state.clear();
        assert_eq!(state.rows(), 0);
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

        let row: Vec<DataType> = vec![1.into(), 10.into()];
        let i = 5;
        let r = row.iter().collect();
        let k = state.serialize_key(i, &r, index.seq);
        let prefix = PersistentState::transform_fn(&k);
        let (key_out, size, row_out): (IndexID, u64, Vec<DataType>) =
            bincode::deserialize(&prefix).unwrap();
        assert_eq!(i, key_out);
        assert_eq!(size, bincode::serialized_size(&row_out).unwrap());
        assert_eq!(row[0], row_out[0]);
        assert_eq!(row[1], row_out[1]);

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
