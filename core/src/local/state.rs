use std::borrow::Cow;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use ::*;
use data::SizeOf;
use local::single_state::SingleState;

use bincode;
use rand::{self, Rng};
use rocksdb::{self, WriteBatch, DB};

pub trait State: SizeOf + Send {
    /// Add an index keyed by the given columns and replayed to by the given partial tags.
    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>);

    /// Returns whether this state is currently keyed on anything. If not, then it cannot store any
    /// infromation and is thus "not useful".
    fn is_useful(&self) -> bool;

    fn is_partial(&self) -> bool;

    fn process_records(&mut self, records: &mut Records, partial_tag: Option<Tag>);

    fn mark_hole(&mut self, key: &[DataType], tag: &Tag);

    fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag);

    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a>;

    fn rows(&self) -> usize;

    fn keys(&self) -> Vec<Vec<usize>>;

    /// Return a copy of all records. Panics if the state is only partially materialized.
    fn cloned_records(&self) -> Vec<Vec<DataType>>;

    fn clear(&mut self);

    /// Evict `count` randomly selected keys, returning key colunms of the index chosen to evict
    /// from along with the keys evicted and the number of bytes evicted.
    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64);

    /// Evict the listed keys from the materialization targeted by `tag`, returning the key columns
    /// of the index that was evicted from and the number of bytes evicted.
    fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) -> (&[usize], u64);
}

// Uniquely identifies an index for each base (the numeral index in PersistentState.indices).
type IndexID = u32;

// Used by bases without a primary key to uniquely identify a row.
// The first component is a UNIX timestamp in seconds, and the second
// is a monotonically increasing value reset every new second.
type IndexTimestamp = (u64, u32);

struct PersistentIndex {
    columns: Vec<usize>,
    last_insert: IndexTimestamp,
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

        // TODO(ekmartin): .remove won't work for inserts in the same batch, since it needs to seek
        // for the values it's deleting before doing so, and values we're inserting in this batch
        // only become available _after_ we've written it.
        //
        // Can solve this by going through and removing unnecessary records, but should make sure
        // that's not being done somewhere else already, prior to this.
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

        let index = self.indices
            .iter()
            .position(|index| &index.columns[..] == columns)
            .expect("could not find index for columns");
        let prefix = Self::serialize_prefix(index as u32, &key_values);
        let db = self.db.as_ref().unwrap();
        let values = db.prefix_iterator(&prefix);

        let data = if index > 0 {
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
        };

        LookupResult::Some(Cow::Owned(data))
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        assert!(partial.is_none(), "Bases can't be partial");
        let existing = self.indices
            .iter()
            .any(|index| &index.columns[..] == columns);
        if !existing {
            self.indices.push(PersistentIndex {
                columns: Vec::from(columns),
                last_insert: (0, 0),
            });
        }

        if self.rows() > 0 {
            let rows = self.cloned_records();
            // We wouldn't have to clear everything here, could just retrieve all the primary key
            // rows and then insert them with the new index:
            self.clear();
            let mut batch = WriteBatch::default();
            for row in rows {
                self.insert(&mut batch, row);
            }

            self.db.as_ref().unwrap().write(batch).unwrap();
        }
    }

    fn keys(&self) -> Vec<Vec<usize>> {
        self.indices
            .iter()
            .map(|index| index.columns.clone())
            .collect()
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        self.db
            .as_ref()
            .unwrap()
            .full_iterator(rocksdb::IteratorMode::Start)
            .filter(|&(ref key, _)| {
                // Filter out non-pk indices:
                let i: IndexID = bincode::deserialize(&key).unwrap();
                i == 0
            })
            .map(|(_, ref value)| bincode::deserialize(&value).unwrap())
            .collect()
    }

    fn rows(&self) -> usize {
        self.db
            .as_ref()
            .unwrap()
            .full_iterator(rocksdb::IteratorMode::Start)
            .filter(|&(ref key, _)| {
                // Filter out non-pk indices:
                let i: IndexID = bincode::deserialize(&key).unwrap();
                i == 0
            })
            .count()
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
        unreachable!("PersistentBase can't be partial")
    }

    fn mark_hole(&mut self, _: &[DataType], _: &Tag) {
        unreachable!("PersistentBase can't be partial")
    }

    fn evict_random_keys(&mut self, _: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        unreachable!("can't evict keys from PersistentBase")
    }

    fn evict_keys(&mut self, _: &Tag, _: &[Vec<DataType>]) -> (&[usize], u64) {
        unreachable!("can't evict keys from PersistentBase")
    }
}

impl PersistentState {
    pub fn new(name: String, params: &PersistenceParameters) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.create_if_missing(true);

        if let Some(ref path) = params.log_dir {
            // Append the db name to the WAL path to ensure
            // that we create a directory for each base shard:
            opts.set_wal_dir(path.join(&name));
        }

        // Create prefixes by Self::transform_fn on all new inserted keys:
        let transform = rocksdb::SliceTransform::create("key", Self::transform_fn, None);
        opts.set_prefix_extractor(transform);

        // Assigns the number of threads for RocksDB's low priority background pool:
        opts.increase_parallelism(params.persistence_threads);

        // Use a hash linked list since we're doing prefix seeks.
        opts.set_allow_concurrent_memtable_write(false);
        opts.set_memtable_factory(rocksdb::MemtableFactory::HashLinkList {
            bucket_count: 1_000_000,
        });

        let full_name = format!("{}.db", name);
        let db = Some(DB::open(&opts, &full_name).unwrap());

        Self {
            db,
            db_opts: opts,
            durability_mode: params.mode.clone(),
            name: full_name,
            indices: Default::default(),
        }
    }

    // Selects a prefix of `key` without the sequence number.
    // Keys are on the form (IndexID, KeySize, Key, IndexTimestamp), with the types
    // (u64, u64, Vec<DataType>, (u64, u32)).
    //
    // We'd like to retrieve the prefix (IndexID, KeySize, Key).
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
    // To make sure that both 1 and 4 hold, we use the KeySize we've encoded earlier to
    // trim away the IndexTimestamp.
    fn transform_fn(key: &[u8]) -> Vec<u8> {
        // IndexID is a u32, so bincode uses 4 bytes to serialize it:
        let start = 4;
        // We encoded the size of the key itself with a u64, which bincode uses 8 bytes to encode:
        let size_offset = start + 8;
        let key_size: u64 = bincode::deserialize(&key[start..size_offset]).unwrap();
        let prefix_len = size_offset + key_size as usize;
        let mut bytes = Vec::from(key);

        // Strip away the IndexTimestamp if we haven't already done so:
        if key.len() > prefix_len {
            bytes.truncate(prefix_len);
        }

        bytes
    }

    // A key is built up of three components:
    //   * `index`
    //      This lets us lookup equally sized and typed indices correctly.
    //      As an example, imagine if we have a table with the columns (Int, Int, Int).
    //      How would we differ between an index on [0, 1] and [0, 2]?
    //      We'd either have to include placeholders for all the columns, or, as we have done,
    //      prefix each row with an identifier for that index.
    //  * `row_size`
    //      The size of `row`.
    //      This lets us create prefixes with (index, row_size, row) in Self::transform_fn.
    //  * `row`
    //      The actual index values.
    //  * `ts`
    //      Maintained for each index in `self.indices` and incremented on inserts. This lets us
    //      store multiple values for a single key.
    fn serialize_key(index: IndexID, row: &Vec<&DataType>, ts: IndexTimestamp) -> Vec<u8> {
        let size: u64 = bincode::serialized_size(&row).unwrap();
        bincode::serialize(&(index, size, row, ts)).unwrap()
    }

    // When reading keys we ignore the sequence number, but for the logic in Self::transform_fn to
    // work correctly we need a placeholder value.
    fn serialize_prefix(index: IndexID, row: &Vec<&DataType>) -> Vec<u8> {
        Self::serialize_key(index, row, (0, 0))
    }

    // Increments the sequence number portion of IndexTimestamp if we're still within
    // a second of the last timestamp. Resets the sequence counter to 0 otherwise.
    fn increment_sequence(last_insert: IndexTimestamp) -> IndexTimestamp {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let (last_secs, seq) = last_insert;
        if last_secs < secs {
            (secs, 0)
        } else {
            // We've inserted at least one row already this second,
            // so increment the sequence number instead:
            (secs, seq + 1)
        }
    }

    // Puts by primary key first, then retrieves the existing value for each index and appends the
    // newly created primary key value.
    // TODO(ekmartin): This will put exactly the values that are given, and can only be retrieved
    // with exactly those values. I think the regular state implementation supports inserting
    // something like an Int and retrieving with a BigInt.
    fn insert(&mut self, batch: &mut WriteBatch, r: Vec<DataType>) {
        let serialized_pk = {
            let pk_index = &mut self.indices[0];
            pk_index.last_insert = Self::increment_sequence(pk_index.last_insert);
            let pk = pk_index.columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            Self::serialize_key(0, &pk, pk_index.last_insert)
        };

        // First insert the actual value for our primary index:
        let pk_value = bincode::serialize(&r).unwrap();
        batch.put(&serialized_pk, &pk_value).unwrap();

        // Then insert primary key pointers for all the secondary indices:
        for (i, index) in self.indices[1..].iter_mut().enumerate() {
            index.last_insert = Self::increment_sequence(index.last_insert);
            // Construct a key with the index values, and serialize it with bincode:
            let key = index.columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            // Add 1 to i since we're slicing self.indices by 1..:
            let serialized_key = Self::serialize_key((i + 1) as u32, &key, index.last_insert);
            batch.put(&serialized_key, &serialized_pk).unwrap();
        }
    }

    fn remove(&self, batch: &mut WriteBatch, r: &[DataType]) {
        let db = self.db.as_ref().unwrap();
        for (i, index) in self.indices.iter().enumerate() {
            let index_row = index.columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
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

#[derive(Default)]
pub struct MemoryState {
    state: Vec<SingleState>,
    by_tag: HashMap<Tag, usize>,
    mem_size: u64,
}

impl SizeOf for MemoryState {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        self.mem_size
    }
}

impl State for MemoryState {
    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        let (i, exists) = if let Some(i) = self.state_for(columns) {
            // already keyed by this key; just adding tags
            (i, true)
        } else {
            // will eventually be assigned
            (self.state.len(), false)
        };

        if let Some(ref p) = partial {
            for &tag in p {
                self.by_tag.insert(tag, i);
            }
        }

        if exists {
            return;
        }

        self.state
            .push(SingleState::new(columns, partial.is_some()));

        if !self.state.is_empty() && partial.is_none() {
            // we need to *construct* the index!
            let (new, old) = self.state.split_last_mut().unwrap();

            if !old.is_empty() {
                assert!(!old[0].partial());
                for rs in old[0].values() {
                    for r in rs {
                        new.insert_row(Row(r.0.clone()));
                    }
                }
            }
        }
    }

    fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    fn is_partial(&self) -> bool {
        self.state.iter().any(|s| s.partial())
    }

    fn process_records(&mut self, records: &mut Records, partial_tag: Option<Tag>) {
        if self.is_partial() {
            records.retain(|r| {
                // we need to check that we're not erroneously filling any holes
                // there are two cases here:
                //
                //  - if the incoming record is a partial replay (i.e., partial.is_some()), then we
                //    *know* that we are the target of the replay, and therefore we *know* that the
                //    materialization must already have marked the given key as "not a hole".
                //  - if the incoming record is a normal message (i.e., partial.is_none()), then we
                //    need to be careful. since this materialization is partial, it may be that we
                //    haven't yet replayed this `r`'s key, in which case we shouldn't forward that
                //    record! if all of our indices have holes for this record, there's no need for us
                //    to forward it. it would just be wasted work.
                //
                //    XXX: we could potentially save come computation here in joins by not forcing
                //    `right` to backfill the lookup key only to then throw the record away
                match *r {
                    Record::Positive(ref r) => self.insert(r.clone(), partial_tag),
                    Record::Negative(ref r) => self.remove(r),
                    Record::BaseOperation(..) => unreachable!(),
                }
            });
        } else {
            for r in records.iter() {
                match *r {
                    Record::Positive(ref r) => {
                        let hit = self.insert(r.clone(), None);
                        debug_assert!(hit);
                    }
                    Record::Negative(ref r) => {
                        let hit = self.remove(r);
                        debug_assert!(hit);
                    }
                    Record::BaseOperation(..) => unreachable!(),
                }
            }
        }
    }

    fn rows(&self) -> usize {
        self.state.iter().map(|s| s.rows()).sum()
    }

    fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[tag];
        self.state[index].mark_filled(key);
    }

    fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[tag];
        self.state[index].mark_hole(key);
    }

    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        debug_assert!(!self.state.is_empty(), "lookup on uninitialized index");
        let index = self.state_for(columns)
            .expect("lookup on non-indexed column set");
        self.state[index].lookup(key)
    }

    fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| s.key().to_vec()).collect()
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        fn fix<'a>(rs: &'a Vec<Row>) -> impl Iterator<Item = Vec<DataType>> + 'a {
            rs.iter().map(|r| Vec::clone(&**r))
        }

        assert!(!self.state[0].partial());
        self.state[0].values().flat_map(fix).collect()
    }

    fn clear(&mut self) {
        for s in &mut self.state {
            s.clear();
        }
        self.mem_size = 0;
    }

    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0, self.state.len());
        let (bytes_freed, keys) = self.state[index].evict_random_keys(count, &mut rng);
        self.mem_size = self.mem_size.saturating_sub(bytes_freed);
        (self.state[index].key(), keys, bytes_freed)
    }

    fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) -> (&[usize], u64) {
        let index = self.by_tag[tag];
        let bytes = self.state[index].evict_keys(keys);
        self.mem_size = self.mem_size.saturating_sub(bytes);
        (self.state[index].key(), bytes)
    }
}

impl MemoryState {
    /// Returns the index in `self.state` of the index keyed on `cols`, or None if no such index
    /// exists.
    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| s.key() == cols)
    }

    fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        let r = Rc::new(r);

        if let Some(tag) = partial_tag {
            let i = match self.by_tag.get(&tag) {
                Some(i) => *i,
                None => {
                    // got tagged insert for unknown tag. this will happen if a node on an old
                    // replay path is now materialized. must return true to avoid any records
                    // (which are destined for a downstream materialization) from being pruned.
                    return true;
                }
            };
            self.mem_size += r.deep_size_of();
            self.state[i].insert_row(Row(r))
        } else {
            let mut hit_any = false;
            for i in 0..self.state.len() {
                hit_any |= self.state[i].insert_row(Row(r.clone()));
            }
            if hit_any {
                self.mem_size += r.deep_size_of();
            }
            hit_any
        }
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        let mut hit = false;
        for s in &mut self.state {
            if let Some(row) = s.remove_row(r, &mut hit) {
                if Rc::strong_count(&row.0) == 1 {
                    self.mem_size = self.mem_size.checked_sub(row.deep_size_of()).unwrap();
                }
            }
        }

        hit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn insert<S: State>(state: &mut S, row: Vec<DataType>) {
        let record: Record = row.into();
        state.process_records(&mut record.into(), None);
    }

    fn setup_persistent(prefix: &str) -> PersistentState {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let name = format!(
            "{}.{}.{}",
            prefix,
            current_time.as_secs(),
            current_time.subsec_nanos()
        );

        PersistentState::new(name, &PersistenceParameters::default())
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
                PersistentState::new(String::from(name), &PersistenceParameters::default());
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

    fn test_process_records<S: State>(state: &mut S) {
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
    fn persistent_state_process_records() {
        let mut state = setup_persistent("persistent_state_process_records");
        test_process_records(&mut state);
    }

    #[test]
    fn persistent_state_transform_fn() {
        let index = PersistentIndex {
            last_insert: (0, 0),
            columns: Default::default(),
        };

        let row: Vec<DataType> = vec![1.into(), 10.into()];
        let i = 5;
        let r = row.iter().collect();
        let k = PersistentState::serialize_key(i, &r, index.last_insert);
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
        let other_k = PersistentState::serialize_key(i * 2, &r, index.last_insert);
        let other_prefix = PersistentState::transform_fn(&other_k);
        assert!(k <= other_k);
        assert!(prefix <= other_prefix);

        // 4) prefix(prefix(key)) == prefix(key)
        assert_eq!(prefix, PersistentState::transform_fn(&prefix));
    }

    #[test]
    fn memory_state_process_records() {
        let mut state = MemoryState::default();
        test_process_records(&mut state);
    }

    #[test]
    fn memory_state_old_records_new_index() {
        let mut state = MemoryState::default();
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(&[0], None);
        insert(&mut state, row.clone());
        state.add_key(&[1], None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(rows) => assert_eq!(&*rows[0], &row),
            _ => unreachable!(),
        };
    }
}
