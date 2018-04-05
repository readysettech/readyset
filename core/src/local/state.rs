use std::borrow::Cow;
use std::collections::HashMap;
use std::rc::Rc;

use ::*;
use data::SizeOf;
use local::single_state::SingleState;

use bincode;
use rand::{self, Rng};
use rocksdb::{self, SliceTransform, DB};

pub enum State {
    InMemory(MemoryState),
    Persistent(PersistentState),
}

impl State {
    pub fn default() -> Self {
        State::InMemory(MemoryState::default())
    }

    pub fn base(name: String, threads: i32, durability_mode: DurabilityMode) -> Self {
        let persistent = PersistentState::initialize(name, threads, durability_mode);
        State::Persistent(persistent)
    }

    /// Add an index keyed by the given columns and replayed to by the given partial tags.
    pub fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        match *self {
            State::InMemory(ref mut s) => s.add_key(columns, partial),
            State::Persistent(ref mut s) => s.add_key(columns, partial),
        }
    }

    pub fn is_useful(&self) -> bool {
        match *self {
            State::InMemory(ref s) => s.is_useful(),
            State::Persistent(ref s) => s.indices.len() > 0,
        }
    }

    pub fn is_partial(&self) -> bool {
        match *self {
            State::InMemory(ref s) => s.is_partial(),
            // PersistentStates can't be partial:
            State::Persistent(..) => false,
        }
    }

    pub fn process_records(&mut self, records: &Records) {
        if records.len() == 0 {
            return;
        }

        match *self {
            State::InMemory(ref mut s) => s.process_records(records),
            State::Persistent(ref mut s) => s.process_records(records),
        }
    }

    pub fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        match *self {
            State::InMemory(ref mut s) => s.insert(r, partial_tag),
            State::Persistent(ref mut s) => {
                assert!(partial_tag.is_none(), "Bases can't be partial");
                s.insert(r)
            }
        }
    }

    pub fn remove(&mut self, r: &[DataType]) -> bool {
        match *self {
            State::InMemory(ref mut s) => s.remove(r),
            State::Persistent(ref mut s) => s.remove(r),
        }
    }

    pub fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        match *self {
            State::InMemory(ref mut s) => s.mark_hole(key, tag),
            // PersistentStates can't be partial:
            _ => unreachable!(),
        }
    }

    pub fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        match *self {
            State::InMemory(ref mut s) => s.mark_filled(key, tag),
            // PersistentStates can't be partial:
            _ => unreachable!(),
        }
    }

    pub fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        match *self {
            State::InMemory(ref s) => s.lookup(columns, key),
            State::Persistent(ref s) => s.lookup(columns, key),
        }
    }

    pub fn rows(&self) -> usize {
        match *self {
            State::InMemory(ref s) => s.rows(),
            State::Persistent(ref s) => s.rows(),
        }
    }

    pub fn keys(&self) -> Vec<Vec<usize>> {
        match *self {
            State::InMemory(ref s) => s.keys(),
            _ => unimplemented!(),
        }
    }

    pub fn cloned_records(&self) -> Vec<Vec<DataType>> {
        match *self {
            State::InMemory(ref s) => s.cloned_records(),
            State::Persistent(ref s) => s.cloned_records(),
        }
    }

    pub fn clear(&mut self) {
        match *self {
            State::InMemory(ref mut s) => s.clear(),
            State::Persistent(ref mut s) => s.clear(),
        }
    }

    pub fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        match *self {
            State::InMemory(ref mut s) => s.evict_random_keys(count),
            _ => unreachable!(),
        }
    }

    /// Evict the listed keys from the materialization targeted by `tag`.
    pub fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) -> (&[usize], u64) {
        match *self {
            State::InMemory(ref mut s) => s.evict_keys(tag, keys),
            _ => unreachable!(),
        }
    }
}

// Index in PersistentState.indices
type KeyIndex = u64;

// Sequence number in PersistentIndex
type KeySeq = u64;

struct PersistentIndex {
    columns: Vec<usize>,
    // TODO(ekmartin): seq is incremented each time a row is inserted for this index, which is
    // probably not optimal. Should probably look into something like the MyRocks record format:
    // https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format
    seq: KeySeq,
}

/// PersistentState stores data in SQlite.
pub struct PersistentState {
    name: String,
    // We don't really want DB to be an option, but doing so lets us drop it manually in
    // PersistenState's Drop by setting `self.db = None` - after which we can then discard the
    // persisted files if we want to.
    db: Option<DB>,
    durability_mode: DurabilityMode,
    indices: Vec<PersistentIndex>,
}

impl PersistentState {
    fn initialize(name: String, threads: i32, durability_mode: DurabilityMode) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let transform = SliceTransform::create("key", Self::transform_fn, None);
        opts.set_prefix_extractor(transform);

        // Assigns the number of threads for RocksDB's low priority background pool:
        opts.increase_parallelism(threads);

        // opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        // opts.optimize_for_point_lookup(cache_size); ?

        let full_name = format!("{}.db", name);
        let db = Some(DB::open(&opts, &full_name).unwrap());

        Self {
            db,
            durability_mode,
            name: full_name,
            indices: Default::default(),
        }
    }

    // Slices out the prefix from `key`.
    // Keys are on the form (KeyIndex, Key, Sequence), with the types (u64, Vec<DataType>, u64).
    // We'd like to retrieve the prefix (KeyIndex, Key), so we'll effectively slice away the
    // sequence number.
    fn transform_fn(key: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::from(key);
        bytes.truncate(key.len() - 8);
        bytes
    }

    // A key is built up of three components:
    //   * `index`
    //      This lets us lookup equally sized and typed indices correctly.
    //      As an example, imagine if we have a table with the columns (Int, Int, Int).
    //      How would we differ between an index on [0, 1] and [0, 2]?
    //      We'd either have to include placeholders for all the columns, or, as we have done,
    //      prefix each row with an identifier for that index.
    //  * `row`
    //      The actual index values
    //  * `seq`
    //      Maintained for each index in `self.indices` and incremented on inserts. This lets us
    //      store multiple values for a single key.
    fn serialize_key(index: KeyIndex, row: &Vec<&DataType>, seq: KeySeq) -> Vec<u8> {
        bincode::serialize(&(index, row, seq)).unwrap()
    }

    // When reading keys we ignore the sequence number, but for the logic in Self::transform_fn to
    // work correctly we need a placeholder value, which is 0.
    fn serialize_prefix(index: KeyIndex, row: &Vec<&DataType>) -> Vec<u8> {
        Self::serialize_key(index, row, 0)
    }

    // Puts by primary key first, then retrieves the existing value for each index and appends the
    // newly created primary key value.
    fn insert(&mut self, r: Vec<DataType>) -> bool {
        let (pk_seq, pk) = {
            let pk_index = &mut self.indices[0];
            pk_index.seq += 1;
            let pk = pk_index.columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            (pk_index.seq, pk)
        };

        let serialized_pk = Self::serialize_key(0, &pk, pk_seq);
        let db = self.db.as_ref().unwrap();
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(false);
        write_opts.disable_wal(true);
        let pk_value = bincode::serialize(&r).unwrap();
        db.put_opt(&serialized_pk, &pk_value, &write_opts).unwrap();
        let serialized_pk_only = bincode::serialize(&pk).unwrap();
        for (i, index) in self.indices[1..].iter_mut().enumerate() {
            index.seq += 1;
            // Construct a key with the index values, and serialize it with bincode:
            let key = index.columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            // Add 1 to i since we're slicing indices by 1..:
            let serialized_key = Self::serialize_key((i + 1) as u64, &key, index.seq);
            db.put_opt(&serialized_key, &serialized_pk_only, &write_opts)
                .unwrap();
        }

        true
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        let db = self.db.as_ref().unwrap();
        for (i, index) in self.indices.iter().enumerate() {
            let index_row = index.columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            let serialized_key = Self::serialize_key(i as u64, &index_row, 0u64);
            for (key, _value) in db.prefix_iterator(&serialized_key) {
                db.delete(&key).unwrap();
            }
        }

        true
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        assert!(partial.is_none(), "Bases can't be partial");
        let existing = self.indices
            .iter()
            .any(|index| &index.columns[..] == columns);
        if !existing {
            self.indices.push(PersistentIndex {
                columns: Vec::from(columns),
                seq: 0,
            });
        }

        if self.rows() > 0 {
            let rows = self.cloned_records();
            // We wouldn't have to clear everything here, could just retrieve all the primary key
            // rows and then insert them with the new index:
            self.clear();
            for row in rows {
                self.insert(row);
            }
        }
    }

    fn process_records(&mut self, records: &Records) {
        for r in records.iter() {
            match *r {
                Record::Positive(ref r) => {
                    self.insert(r.clone());
                }
                Record::Negative(ref r) => {
                    self.remove(r);
                }
                Record::BaseOperation(..) => unreachable!(),
            }
        }
    }

    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let values = match *key {
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
        let prefix = Self::serialize_prefix(index as u64, &values);
        let db = self.db.as_ref().unwrap();
        let data = if index > 0 {
            let mut rows = vec![];
            for (_key, value) in db.prefix_iterator(&prefix) {
                let row: Vec<DataType> = bincode::deserialize(&*value).unwrap();
                let row_key = KeyType::from(&row[..]);
                match self.lookup(&self.indices[0].columns, &row_key) {
                    LookupResult::Some(Cow::Owned(ref mut rs)) => rows.append(rs),
                    _ => unreachable!(),
                };
            }
            rows
        } else {
            let mut rows = vec![];
            for (_key, value) in db.prefix_iterator(&prefix) {
                let row: Vec<DataType> = bincode::deserialize(&*value).unwrap();
                rows.push(row);
            }

            rows.into_iter()
                .map(|row| Row(Rc::new(row)))
                .collect::<Vec<_>>()
        };

        LookupResult::Some(Cow::Owned(data))
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        self.db
            .as_ref()
            .unwrap()
            .iterator(rocksdb::IteratorMode::Start)
            .filter(|&(ref key, _)| {
                // Filter out non-pk indices:
                let i: KeyIndex = bincode::deserialize(&key).unwrap();
                i == 0
            })
            .map(|(_, ref value)| bincode::deserialize(&value).unwrap())
            .collect()
    }

    fn rows(&self) -> usize {
        self.db
            .as_ref()
            .unwrap()
            .iterator(rocksdb::IteratorMode::Start)
            .filter(|&(ref key, _)| {
                // Filter out non-pk indices:
                let i: KeyIndex = bincode::deserialize(&key).unwrap();
                i == 0
            })
            .count()
    }

    fn clear(&self) {
        // Would potentially be faster to just drop self.db and call DB::Destroy:
        let db = self.db.as_ref().unwrap();
        for (key, _) in db.iterator(rocksdb::IteratorMode::Start) {
            db.delete(&key).unwrap();
        }
    }
}

impl Drop for PersistentState {
    fn drop(&mut self) {
        if self.durability_mode != DurabilityMode::Permanent {
            self.db = None;
            DB::destroy(&rocksdb::Options::default(), &self.name).unwrap()
        }
    }
}

#[derive(Default)]
pub struct MemoryState {
    state: Vec<SingleState>,
    by_tag: HashMap<Tag, usize>,
    mem_size: u64,
}

impl MemoryState {
    /// Returns the index in `self.state` of the index keyed on `cols`, or None if no such index
    /// exists.
    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| s.key() == cols)
    }

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

    /// Returns a Vec of all keys that currently exist on this materialization.
    fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| s.key().to_vec()).collect()
    }

    /// Returns whether this state is currently keyed on anything. If not, then it cannot store any
    /// infromation and is thus "not useful".
    fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    fn is_partial(&self) -> bool {
        self.state.iter().any(|s| s.partial())
    }

    fn process_records(&mut self, records: &Records) {
        // Might consider using a write batch if it helps here (might not help when we're not
        // waiting for fsyncs).
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

    /// Return a copy of all records. Panics if the state is only partially materialized.
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

    /// Evict `count` randomly selected keys, returning key colunms of the index chosen to evict
    /// from along with the keys evicted and the number of bytes evicted.
    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0, self.state.len());
        let (bytes_freed, keys) = self.state[index].evict_random_keys(count, &mut rng);
        self.mem_size = self.mem_size.saturating_sub(bytes_freed);
        (self.state[index].key(), keys, bytes_freed)
    }

    /// Evict the listed keys from the materialization targeted by `tag`, returning the key columns
    /// of the index that was evicted from and the number of bytes evicted.
    fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) -> (&[usize], u64) {
        let index = self.by_tag[tag];
        let bytes = self.state[index].evict_keys(keys);
        self.mem_size = self.mem_size.saturating_sub(bytes);
        (self.state[index].key(), bytes)
    }
}

impl SizeOf for State {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        match *self {
            State::InMemory(ref s) => s.mem_size,
            State::Persistent(..) => self.size_of(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn setup_persistent(prefix: &str) -> State {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let name = format!(
            "{}.{}.{}",
            prefix,
            current_time.as_secs(),
            current_time.subsec_nanos()
        );

        State::base(name, 1, DurabilityMode::MemoryOnly)
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
        state.insert(row, None);

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
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_multi_key() {
        let mut state = setup_persistent("persistent_state_multi_key");
        let columns = &[0, 2];
        let row: Vec<DataType> = vec![10.into(), "Cat".into(), 20.into()];
        state.add_key(columns, None);
        assert!(state.insert(row.clone(), None));

        match state.lookup(columns, &KeyType::Double((1.into(), 2.into()))) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            LookupResult::Missing => panic!("PersistentStates can't be materialized"),
        };

        match state.lookup(columns, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(&*rows[0], &row);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_multiple_indices() {
        let mut state = setup_persistent("persistent_state_multiple_indices");
        let first: Vec<DataType> = vec![10.into(), "Cat".into(), 1.into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".into(), 1.into()];
        state.add_key(&[0], None);
        state.add_key(&[1, 2], None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
            }
            _ => panic!(),
        }

        match state.lookup(&[1, 2], &KeyType::Double(("Cat".into(), 1.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&*rows[0], &first);
                assert_eq!(&*rows[1], &second);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_different_indices() {
        let mut state = setup_persistent("persistent_state_different_indices");
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(&[0], None);
        state.add_key(&[1], None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
            }
            _ => panic!(),
        }

        match state.lookup(&[1], &KeyType::Single(&"Bob".into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &second);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_remove() {
        let mut state = setup_persistent("persistent_state_remove");
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));
        assert!(state.remove(&first));

        match state.lookup(columns, &KeyType::Single(&first[0])) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            LookupResult::Missing => panic!("PersistentStates can't be materialized"),
        };

        match state.lookup(columns, &KeyType::Single(&second[0])) {
            LookupResult::Some(rows) => {
                assert_eq!(&*rows[0], &second);
            }
            _ => panic!(),
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
        assert!(state.insert(first.clone(), None));
        assert_eq!(state.rows(), 1);
        assert!(state.insert(second.clone(), None));
        assert_eq!(state.rows(), 2);
    }

    #[test]
    fn persistent_state_cloned_records() {
        let mut state = setup_persistent("persistent_state_cloned_records");
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(&[0], None);
        state.add_key(&[0, 1], None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));
        assert_eq!(state.cloned_records(), vec![first, second]);
    }

    #[test]
    fn persistent_state_clear() {
        let mut state = setup_persistent("persistent_state_clear");
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));

        state.clear();
        assert_eq!(state.rows(), 0);
    }

    #[test]
    fn persistent_state_drop() {
        let name = ".s-o_u#p.";
        let db_name = format!("{}.db", name);
        let path = Path::new(&db_name);
        {
            let _state = State::base(String::from(name), 1, DurabilityMode::DeleteOnExit);
            assert!(path.exists());
        }

        assert!(!path.exists());
    }

    #[test]
    fn persistent_state_old_records_new_index() {
        let mut state = setup_persistent("persistent_state_old_records_new_index");
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(&[0], None);
        assert!(state.insert(row.clone(), None));
        state.add_key(&[1], None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(rows) => assert_eq!(&*rows[0], &row),
            _ => unreachable!(),
        };
    }

    fn process_records(mut state: State) {
        let records: Records = vec![
            (vec![1.into(), "A".into()], true),
            (vec![2.into(), "B".into()], true),
            (vec![3.into(), "C".into()], true),
            (vec![1.into(), "A".into()], false),
        ].into();

        state.add_key(&[0], None);
        state.process_records(&records);

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
        let state = setup_persistent("persistent_state_process_records");
        process_records(state);
    }

    #[test]
    fn persistent_state_transform_fn() {
        let index = PersistentIndex {
            seq: 10,
            columns: Default::default(),
        };

        let row: Vec<DataType> = vec![1.into(), 10.into()];
        let r = row.iter().collect();
        let k = PersistentState::serialize_key(0, &r, index.seq);
        let t = PersistentState::transform_fn(&k);
        let (key_out, row_out): (KeyIndex, Vec<DataType>) = bincode::deserialize(&t).unwrap();
        assert_eq!(0, key_out);
        assert_eq!(row[0], row_out[0]);
        assert_eq!(row[1], row_out[1]);

        // Make sure we're actually slicing away the sequence number:
        let full_key: Result<(KeyIndex, Vec<DataType>, KeySeq), _> = bincode::deserialize(&t);
        assert!(full_key.is_err());
    }

    #[test]
    fn memory_state_process_records() {
        let state = State::default();
        process_records(state);
    }

    #[test]
    fn memory_state_old_records_new_index() {
        let mut state = State::default();
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(&[0], None);
        assert!(state.insert(row.clone(), None));
        state.add_key(&[1], None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(rows) => assert_eq!(&*rows[0], &row),
            _ => unreachable!(),
        };
    }
}
