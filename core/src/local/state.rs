use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::rc::Rc;

use ::*;
use data::SizeOf;
use local::single_state::SingleState;

use bincode;
use rand::{self, Rng};
use rusqlite::{self, Connection};
use rusqlite::types::{ToSql, ToSqlOutput};

pub enum State {
    InMemory(MemoryState),
    Persistent(PersistentState),
}

impl State {
    pub fn default() -> Self {
        State::InMemory(MemoryState::default())
    }

    pub fn base(name: String, durability_mode: DurabilityMode) -> Self {
        let persistent = PersistentState::initialize(name, durability_mode);
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

    pub fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        match *self {
            State::InMemory(ref mut s) => s.insert(r, partial_tag),
            State::Persistent(ref mut s) => s.insert(r, partial_tag),
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

/// PersistentState stores data in SQlite.
pub struct PersistentState {
    name: String,
    connection: Connection,
    durability_mode: DurabilityMode,
    indices: HashSet<usize>,
}

impl ToSql for DataType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        Ok(match *self {
            DataType::None => unreachable!(),
            DataType::Int(n) => ToSqlOutput::from(n),
            DataType::BigInt(n) => ToSqlOutput::from(n),
            DataType::Real(i, f) => {
                let value = (i as f64) + (f as f64) * 1.0e-9;
                ToSqlOutput::from(value)
            }
            DataType::Text(..) | DataType::TinyText(..) => ToSqlOutput::from(self.to_string()),
            DataType::Timestamp(ts) => ToSqlOutput::from(ts.format("%+").to_string()),
        })
    }
}

impl PersistentState {
    fn initialize(name: String, durability_mode: DurabilityMode) -> Self {
        let full_name = format!("{}.db", name);
        let connection = match durability_mode {
            DurabilityMode::MemoryOnly => Connection::open_in_memory().unwrap(),
            _ => Connection::open(&full_name).unwrap(),
        };

        connection
            .execute("CREATE TABLE IF NOT EXISTS store (row BLOB)", &[])
            .unwrap();

        Self {
            connection,
            durability_mode,
            name: full_name,
            indices: Default::default(),
        }
    }

    // Joins together a SQL clause on the form of
    // index_0 = columns[0], index_1 = columns[1]...
    fn build_clause<'a, I>(columns: I) -> String
    where
        I: Iterator<Item = &'a usize>,
    {
        columns
            .enumerate()
            .map(|(i, column)| format!("index_{} = ?{}", column, i + 1))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    // Used with statement.query_map to deserialize the rows returned from SQlite
    fn map_rows(result: &rusqlite::Row) -> Vec<DataType> {
        let row: Vec<u8> = result.get(0);
        bincode::deserialize(&row).unwrap()
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        assert!(partial.is_none(), "Bases can't be partial");
        // Add each of the individual index columns (index_0, index_1...):
        for index in columns.iter() {
            if self.indices.contains(index) {
                continue;
            }

            self.indices.insert(*index);
            let query = format!("ALTER TABLE store ADD COLUMN index_{} TEXT", index);
            match self.connection.execute(&query, &[]) {
                Ok(..) => (),
                // Ignore existing column errors:
                Err(rusqlite::Error::SqliteFailure(_, Some(ref message)))
                    if message == &format!("duplicate column name: index_{}", index) =>
                {
                    ()
                }
                Err(e) => panic!(e),
            };
        }

        // Then create the combined index on the given columns:
        let index_clause = columns
            .into_iter()
            .map(|column| format!("index_{}", column))
            .collect::<Vec<_>>()
            .join(", ");

        let index_query = format!(
            "CREATE INDEX IF NOT EXISTS \"{}\" ON store ({})",
            index_clause, index_clause
        );

        // Make sure that existing rows contain the new index column too,
        // if there are any. This could be faster with UPDATE statements
        // if needed in the future.
        if self.rows() > 0 {
            let rows = self.cloned_records();
            self.clear();
            for row in rows {
                self.insert(row, None);
            }
        }

        self.connection.execute(&index_query, &[]).unwrap();
    }

    // Builds up an INSERT query on the form of:
    // `INSERT INTO store (index_0, index_1, row) VALUES (...)`
    // where row is a serialized representation of r.
    fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        assert!(partial_tag.is_none(), "Bases can't be partial");
        let columns = format!(
            "row, {}",
            self.indices
                .iter()
                .map(|index| format!("index_{}", index))
                .collect::<Vec<String>>()
                .join(", ")
        );

        let placeholders = (1..(self.indices.len() + 2))
            .map(|placeholder| format!("?{}", placeholder))
            .collect::<Vec<String>>()
            .join(", ");

        let mut statement = self.connection
            .prepare_cached(&format!(
                "INSERT INTO store ({}) VALUES ({})",
                columns, placeholders
            ))
            .unwrap();

        let row = bincode::serialize(&r).unwrap();
        let mut values: Vec<&ToSql> = vec![&row];
        let mut index_values = self.indices
            .iter()
            .map(|index| &r[*index] as &ToSql)
            .collect::<Vec<&ToSql>>();

        values.append(&mut index_values);
        statement.execute(&values[..]).unwrap();
        true
    }

    // Retrieves rows from SQlite by building up a SELECT query on the form of
    // `SELECT row FROM store WHERE index_0 = value AND index_1 = VALUE`
    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let clauses = Self::build_clause(columns.iter());
        let query = format!("SELECT row FROM store WHERE {}", clauses);
        let mut statement = self.connection.prepare_cached(&query).unwrap();

        let rows = match *key {
            KeyType::Single(a) => statement.query_map(&[a], Self::map_rows),
            KeyType::Double(ref r) => statement.query_map(&[&r.0, &r.1], Self::map_rows),
            KeyType::Tri(ref r) => statement.query_map(&[&r.0, &r.1, &r.2], Self::map_rows),
            KeyType::Quad(ref r) => statement.query_map(&[&r.0, &r.1, &r.2, &r.3], Self::map_rows),
            KeyType::Quin(ref r) => {
                statement.query_map(&[&r.0, &r.1, &r.2, &r.3, &r.4], Self::map_rows)
            }
            KeyType::Sex(ref r) => {
                statement.query_map(&[&r.0, &r.1, &r.2, &r.3, &r.4, &r.5], Self::map_rows)
            }
        };

        let data = rows.unwrap()
            .map(|row| Row(Rc::new(row.unwrap())))
            .collect::<Vec<_>>();

        LookupResult::Some(Cow::Owned(data))
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        let clauses = Self::build_clause(self.indices.iter());
        let index_values = self.indices
            .iter()
            .map(|index| &r[*index] as &ToSql)
            .collect::<Vec<&ToSql>>();

        let query = format!("DELETE FROM store WHERE {}", clauses);
        let mut statement = self.connection.prepare_cached(&query).unwrap();
        statement.execute(&index_values[..]).unwrap() > 0
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        let mut statement = self.connection
            .prepare_cached("SELECT row FROM store")
            .unwrap();

        let rows = statement
            .query_map(&[], Self::map_rows)
            .unwrap()
            .map(|row| row.unwrap())
            .collect::<Vec<_>>();

        rows
    }

    fn rows(&self) -> usize {
        let mut statement = self.connection
            .prepare_cached("SELECT COUNT(row) FROM store")
            .unwrap();

        let count: i64 = statement.query_row(&[], |row| row.get(0)).unwrap();
        count as usize
    }

    fn clear(&self) {
        let mut statement = self.connection.prepare_cached("DELETE FROM store").unwrap();

        statement.execute(&[]).unwrap();
    }
}

impl Drop for PersistentState {
    fn drop(&mut self) {
        // TODO(ekmartin): We don't support recovering persistent base node indices yet,
        // so drop the tables at the end for all modes except InMemory:
        if self.durability_mode == DurabilityMode::MemoryOnly {
            return;
        }

        fs::remove_file(&self.name).unwrap();
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

    fn setup_persistent() -> State {
        State::base(String::from("soup"), DurabilityMode::MemoryOnly)
    }

    #[test]
    fn persistent_state_is_partial() {
        let state = setup_persistent();
        assert!(!state.is_partial());
    }

    #[test]
    fn persistent_state_single_key() {
        let mut state = setup_persistent();
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
        let mut state = setup_persistent();
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
        let mut state = setup_persistent();
        let first: Vec<DataType> = vec![10.into(), "Cat".into(), 20.into()];
        let second: Vec<DataType> = vec![10.into(), "Bob".into(), 30.into()];
        state.add_key(&[0], None);
        state.add_key(&[0, 2], None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&*rows[0], &first);
                assert_eq!(&*rows[1], &second);
            }
            _ => panic!(),
        }

        match state.lookup(&[0, 2], &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_different_indices() {
        let mut state = setup_persistent();
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
        let mut state = setup_persistent();
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
    fn persistent_state_is_empty() {
        let mut state = setup_persistent();
        let columns = &[0];
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(columns, None);
        assert!(state.is_empty());
        assert!(state.insert(row.clone(), None));
        assert!(!state.is_empty());
    }

    #[test]
    fn persistent_state_is_useful() {
        let mut state = setup_persistent();
        let columns = &[0];
        assert!(!state.is_useful());
        state.add_key(columns, None);
        assert!(state.is_useful());
    }

    #[test]
    fn persistent_state_len() {
        let mut state = setup_persistent();
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        assert_eq!(state.len(), 0);
        assert!(state.insert(first.clone(), None));
        assert_eq!(state.len(), 1);
        assert!(state.insert(second.clone(), None));
        assert_eq!(state.len(), 2);
    }

    #[test]
    fn persistent_state_cloned_records() {
        let mut state = setup_persistent();
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));
        assert_eq!(state.cloned_records(), vec![first, second]);
    }

    #[test]
    fn persistent_state_clear() {
        let mut state = setup_persistent();
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));

        state.clear();
        assert!(state.is_empty());
    }

    #[test]
    fn persistent_state_drop() {
        let name = ".s-o_u#p.";
        let db_name = format!("{}.db", name);
        let path = Path::new(&db_name);
        {
            let _state = State::base(String::from(name), DurabilityMode::DeleteOnExit);
            assert!(path.exists());
        }

        assert!(!path.exists());
    }

    #[test]
    fn persistent_state_old_records_new_index() {
        let mut state = setup_persistent();
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(&[0], None);
        assert!(state.insert(row.clone(), None));
        state.add_key(&[1], None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(rows) => assert_eq!(&*rows[0], &row),
            _ => unreachable!(),
        };
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
