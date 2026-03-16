//! Generation state: the long-lived state that persists across multiple
//! query generations.

use data_generator::ColumnGenerationSpec;
use indexmap::IndexMap;
use readyset_sql::Dialect;
use readyset_sql::ast::{SqlIdentifier, SqlType};

use crate::entropy::Entropy;

/// Metadata for a single column in a table.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub sql_type: SqlType,
    pub gen_spec: ColumnGenerationSpec,
}

/// Schema information for a single table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: SqlIdentifier,
    pub columns: IndexMap<SqlIdentifier, ColumnMeta>,
    column_counter: usize,
    pub primary_key: Option<SqlIdentifier>,
}

impl TableSchema {
    /// Create a new empty table schema.
    pub fn new(name: SqlIdentifier) -> Self {
        Self {
            name,
            columns: IndexMap::new(),
            column_counter: 0,
            primary_key: None,
        }
    }

    /// Add a column to this table.
    ///
    /// Names must be unique within a table. The resolver guarantees this via
    /// `fresh_column_name` — no two synthesis paths ever produce the same
    /// name on the same `TableSchema`. Hits of an existing name with a
    /// *different* `ColumnMeta` would silently clobber the prior meta and
    /// cause `into_ddl_steps` to ship the wrong `gen_spec`, so we panic in
    /// debug to surface the invariant violation immediately.
    pub fn add_column(&mut self, name: SqlIdentifier, meta: ColumnMeta) {
        debug_assert!(
            !self.columns.contains_key(&name),
            "add_column collision on `{}.{}` — fresh_column_name should prevent this",
            self.name,
            name
        );
        self.columns.insert(name, meta);
    }

    /// Generate a fresh column name for this table (c0, c1, ...).
    pub fn fresh_column_name(&mut self) -> SqlIdentifier {
        let name = SqlIdentifier::from(format!("c{}", self.column_counter));
        self.column_counter += 1;
        name
    }
}

/// Configuration for the query generator.
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// 0.0 = always create new tables/columns; 1.0 = always reuse existing
    pub reuse_preference: f64,
    /// Maximum subquery/CTE nesting depth
    pub max_subquery_depth: usize,
    /// Default ColumnGenerationSpec for new non-PK columns
    pub default_gen_spec: ColumnGenerationSpec,
    /// Prefix for generated table names (e.g. "i0_" produces "i0_t0", "i0_t1", ...).
    /// Empty string means no prefix (default).
    pub table_prefix: String,
    /// When true, add extra compatibility rules that reject patterns Readyset
    /// cannot deep-cache. This maximizes dataflow/reader-map coverage by avoiding
    /// queries that would fall back to shallow caching or upstream proxying.
    pub readyset_compatible: bool,
    /// Maximum number of tables to retain in state. When exceeded, the oldest
    /// tables are evicted on insertion. 0 means unlimited (default).
    pub max_tables: usize,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            reuse_preference: 0.9,
            max_subquery_depth: 2,
            default_gen_spec: ColumnGenerationSpec::Random,
            table_prefix: String::new(),
            readyset_compatible: false,
            max_tables: 0,
        }
    }
}

/// A snapshot of GenerationState for checkpoint/restore (backtracking).
#[derive(Debug, Clone)]
pub(crate) struct StateCheckpoint {
    tables: IndexMap<SqlIdentifier, TableSchema>,
    table_counter: usize,
    alias_counter: usize,
    subquery_alias_counter: usize,
    cte_alias_counter: usize,
}

/// The long-lived state that persists across multiple query generations.
#[derive(Debug)]
pub struct GenerationState {
    dialect: Dialect,
    tables: IndexMap<SqlIdentifier, TableSchema>,
    table_counter: usize,
    alias_counter: usize,
    subquery_alias_counter: usize,
    cte_alias_counter: usize,
    config: GeneratorConfig,
}

impl GenerationState {
    /// Create a new empty generation state.
    pub fn new(dialect: Dialect, config: GeneratorConfig) -> Self {
        Self {
            dialect,
            tables: IndexMap::new(),
            table_counter: 0,
            alias_counter: 0,
            subquery_alias_counter: 0,
            cte_alias_counter: 0,
            config,
        }
    }

    /// Returns the SQL dialect.
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// Returns the generator config.
    pub fn config(&self) -> &GeneratorConfig {
        &self.config
    }

    /// Add a table schema to the state. If `max_tables` is set and the limit
    /// would be exceeded, the oldest table is evicted first. Replacing an
    /// existing entry by name does not count as an addition, so it does not
    /// trigger eviction (silent schema corruption when reuse_preference is
    /// high).
    pub fn add_table(&mut self, schema: TableSchema) {
        let max = self.config.max_tables;
        let already_present = self.tables.contains_key(&schema.name);
        if max > 0 && !already_present && self.tables.len() >= max {
            self.tables.shift_remove_index(0);
        }
        self.tables.insert(schema.name.clone(), schema);
    }

    /// Look up a table by name.
    pub fn table(&self, name: &SqlIdentifier) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    /// Look up a table by name, mutably.
    pub fn table_mut(&mut self, name: &SqlIdentifier) -> Option<&mut TableSchema> {
        self.tables.get_mut(name)
    }

    /// Returns all tables.
    pub fn tables(&self) -> &IndexMap<SqlIdentifier, TableSchema> {
        &self.tables
    }

    /// Pick a random table from the state.
    pub fn pick_random_table<'a>(&'a self, entropy: &mut Entropy<'_>) -> Option<&'a TableSchema> {
        if self.tables.is_empty() {
            return None;
        }
        let idx = entropy.range(0..self.tables.len());
        self.tables.get_index(idx).map(|(_, schema)| schema)
    }

    /// Pick a random column from a specific table.
    pub fn pick_random_column<'a>(
        &'a self,
        table_name: &SqlIdentifier,
        entropy: &mut Entropy<'_>,
    ) -> Option<(&'a SqlIdentifier, &'a ColumnMeta)> {
        let table = self.tables.get(table_name)?;
        if table.columns.is_empty() {
            return None;
        }
        let idx = entropy.range(0..table.columns.len());
        table.columns.get_index(idx)
    }

    /// Generate a fresh table name (t0, t1, ...) with optional prefix.
    pub fn fresh_table_name(&mut self) -> SqlIdentifier {
        let name = SqlIdentifier::from(format!(
            "{}t{}",
            self.config.table_prefix, self.table_counter
        ));
        self.table_counter += 1;
        name
    }

    /// Generate a fresh table alias (a0, a1, ...).
    pub fn fresh_alias(&mut self) -> SqlIdentifier {
        let name = SqlIdentifier::from(format!("a{}", self.alias_counter));
        self.alias_counter += 1;
        name
    }

    /// Generate a fresh subquery alias (sq0, sq1, ...).
    pub fn fresh_subquery_alias(&mut self) -> SqlIdentifier {
        let name = SqlIdentifier::from(format!("sq{}", self.subquery_alias_counter));
        self.subquery_alias_counter += 1;
        name
    }

    /// Generate a fresh CTE alias (cte0, cte1, ...).
    pub fn fresh_cte_alias(&mut self) -> SqlIdentifier {
        let name = SqlIdentifier::from(format!("cte{}", self.cte_alias_counter));
        self.cte_alias_counter += 1;
        name
    }

    /// Capture a checkpoint of the current state for backtracking.
    pub(crate) fn checkpoint(&self) -> StateCheckpoint {
        StateCheckpoint {
            tables: self.tables.clone(),
            table_counter: self.table_counter,
            alias_counter: self.alias_counter,
            subquery_alias_counter: self.subquery_alias_counter,
            cte_alias_counter: self.cte_alias_counter,
        }
    }

    /// Restore state from a previously captured checkpoint.
    pub(crate) fn restore(&mut self, cp: StateCheckpoint) {
        self.tables = cp.tables;
        self.table_counter = cp.table_counter;
        self.alias_counter = cp.alias_counter;
        self.subquery_alias_counter = cp.subquery_alias_counter;
        self.cte_alias_counter = cp.cte_alias_counter;
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    fn test_state() -> GenerationState {
        GenerationState::new(Dialect::MySQL, GeneratorConfig::default())
    }

    fn make_table(name: &str) -> TableSchema {
        let mut ts = TableSchema::new(SqlIdentifier::from(name));
        ts.add_column(
            SqlIdentifier::from("id"),
            ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Unique,
            },
        );
        ts.primary_key = Some(SqlIdentifier::from("id"));
        ts
    }

    #[test]
    fn add_and_retrieve_table() {
        let mut state = test_state();
        let table = make_table("users");
        state.add_table(table);

        let retrieved = state.table(&SqlIdentifier::from("users"));
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.expect("table exists").name,
            SqlIdentifier::from("users")
        );
    }

    #[test]
    fn table_not_found_returns_none() {
        let state = test_state();
        assert!(state.table(&SqlIdentifier::from("nonexistent")).is_none());
    }

    #[test]
    fn fresh_table_names_are_sequential() {
        let mut state = test_state();
        assert_eq!(state.fresh_table_name(), SqlIdentifier::from("t0"));
        assert_eq!(state.fresh_table_name(), SqlIdentifier::from("t1"));
        assert_eq!(state.fresh_table_name(), SqlIdentifier::from("t2"));
    }

    #[test]
    fn fresh_aliases_are_sequential() {
        let mut state = test_state();
        assert_eq!(state.fresh_alias(), SqlIdentifier::from("a0"));
        assert_eq!(state.fresh_alias(), SqlIdentifier::from("a1"));
    }

    #[test]
    fn alias_namespaces_are_separate() {
        let mut state = test_state();
        let alias = state.fresh_alias();
        let sq_alias = state.fresh_subquery_alias();
        let cte_alias = state.fresh_cte_alias();

        assert_eq!(alias, SqlIdentifier::from("a0"));
        assert_eq!(sq_alias, SqlIdentifier::from("sq0"));
        assert_eq!(cte_alias, SqlIdentifier::from("cte0"));

        // Each counter increments independently
        assert_eq!(state.fresh_alias(), SqlIdentifier::from("a1"));
        assert_eq!(state.fresh_subquery_alias(), SqlIdentifier::from("sq1"));
        assert_eq!(state.fresh_cte_alias(), SqlIdentifier::from("cte1"));
    }

    #[test]
    fn table_schema_fresh_column_names() {
        let mut ts = TableSchema::new(SqlIdentifier::from("t"));
        assert_eq!(ts.fresh_column_name(), SqlIdentifier::from("c0"));
        assert_eq!(ts.fresh_column_name(), SqlIdentifier::from("c1"));
        assert_eq!(ts.fresh_column_name(), SqlIdentifier::from("c2"));
    }

    #[test]
    fn checkpoint_restore_round_trips() {
        let mut state = test_state();
        state.add_table(make_table("users"));
        let _ = state.fresh_table_name(); // t0
        let _ = state.fresh_alias(); // a0
        let _ = state.fresh_subquery_alias(); // sq0

        let cp = state.checkpoint();

        // Modify state after checkpoint
        state.add_table(make_table("orders"));
        let _ = state.fresh_table_name(); // t1
        let _ = state.fresh_alias(); // a1

        assert!(state.table(&SqlIdentifier::from("orders")).is_some());
        assert_eq!(state.fresh_table_name(), SqlIdentifier::from("t2"));

        // Restore
        state.restore(cp);

        assert!(state.table(&SqlIdentifier::from("users")).is_some());
        assert!(state.table(&SqlIdentifier::from("orders")).is_none());
        assert_eq!(state.fresh_table_name(), SqlIdentifier::from("t1"));
        assert_eq!(state.fresh_alias(), SqlIdentifier::from("a1"));
        assert_eq!(state.fresh_subquery_alias(), SqlIdentifier::from("sq1"));
    }

    #[test]
    fn pick_random_table_empty_returns_none() {
        let state = test_state();
        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);
        assert!(state.pick_random_table(&mut entropy).is_none());
    }

    #[test]
    fn pick_random_table_returns_valid_table() {
        let mut state = test_state();
        state.add_table(make_table("users"));
        state.add_table(make_table("orders"));

        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        for _ in 0..20 {
            let table = state
                .pick_random_table(&mut entropy)
                .expect("should find a table");
            assert!(table.name == "users" || table.name == "orders");
        }
    }

    #[test]
    fn pick_random_column_returns_valid_column() {
        let mut state = test_state();
        let mut table = make_table("users");
        table.add_column(
            SqlIdentifier::from("name"),
            ColumnMeta {
                sql_type: SqlType::VarChar(Some(255)),
                gen_spec: ColumnGenerationSpec::Random,
            },
        );
        state.add_table(table);

        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        for _ in 0..20 {
            let (col_name, _) = state
                .pick_random_column(&SqlIdentifier::from("users"), &mut entropy)
                .expect("should find a column");
            assert!(*col_name == "id" || *col_name == "name");
        }
    }

    #[test]
    fn generator_config_defaults() {
        let config = GeneratorConfig::default();
        assert!((config.reuse_preference - 0.9).abs() < f64::EPSILON);
        assert_eq!(config.max_subquery_depth, 2);
        assert_eq!(config.max_tables, 0);
    }

    #[test]
    fn max_tables_evicts_oldest() {
        let config = GeneratorConfig {
            max_tables: 3,
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);

        state.add_table(make_table("t0"));
        state.add_table(make_table("t1"));
        state.add_table(make_table("t2"));
        assert_eq!(state.tables().len(), 3);

        // Adding a 4th should evict the oldest (t0)
        state.add_table(make_table("t3"));
        assert_eq!(state.tables().len(), 3);
        assert!(state.table(&SqlIdentifier::from("t0")).is_none());
        assert!(state.table(&SqlIdentifier::from("t1")).is_some());
        assert!(state.table(&SqlIdentifier::from("t2")).is_some());
        assert!(state.table(&SqlIdentifier::from("t3")).is_some());
    }

    #[test]
    fn max_tables_zero_means_unlimited() {
        let config = GeneratorConfig {
            max_tables: 0,
            ..Default::default()
        };
        let mut state = GenerationState::new(Dialect::MySQL, config);

        for i in 0..50 {
            state.add_table(make_table(&format!("t{i}")));
        }
        assert_eq!(state.tables().len(), 50);
    }
}
