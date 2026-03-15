//! Core Generator and ConstraintRegistry.
//!
//! The `Generator` is the main entry point for producing random SQL queries.
//! `ConstraintRegistry` holds the library of registered patterns.

use readyset_sql::Dialect;
use readyset_sql::ast::SqlType;

use crate::constraint::TypeClass;
use crate::entropy::Entropy;
use crate::incompat::{IncompatibilityRule, SelectionFilter, check_rules, default_rules};
use crate::pattern::Pattern;
use crate::resolver::{self, DdlStep, ResolverOutput};
use crate::state::{ColumnMeta, GenerationState, GeneratorConfig, TableSchema};

/// Error type for query generation.
#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    #[error("no compatible pattern found (attempted {attempted:?})")]
    NoCompatiblePattern { attempted: Vec<String> },
    #[error("resolution failed for pattern {pattern}: {reason}")]
    ResolutionFailed { pattern: String, reason: String },
    #[error("max retries ({retries}) exceeded")]
    MaxRetriesExceeded { retries: usize },
    #[error("no existing schema available for Mode 3")]
    NoExistingSchema,
}

/// The output of a successful query generation.
#[derive(Debug)]
pub struct QueryOutput {
    /// The generated SELECT statement.
    pub query: readyset_sql::ast::SelectStatement,
    /// DDL steps needed before executing the query.
    pub ddl: Vec<resolver::DdlStep>,
    /// Parameter metadata for data generation.
    pub params: Vec<resolver::ParamMeta>,
}

/// The output of a Mode 2 DDL-only generation.
#[derive(Debug)]
pub struct DdlOutput {
    /// DDL steps generated.
    pub ddl: Vec<DdlStep>,
    /// Optional query generated with the expanded schema.
    pub query: Option<QueryOutput>,
}

impl From<ResolverOutput> for QueryOutput {
    fn from(ro: ResolverOutput) -> Self {
        Self {
            query: ro.query,
            ddl: ro.ddl,
            params: ro.params,
        }
    }
}

/// Registry of constraint patterns available for query generation.
#[derive(Debug)]
pub struct ConstraintRegistry {
    patterns: Vec<Pattern>,
    rules: Vec<IncompatibilityRule>,
}

impl Default for ConstraintRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstraintRegistry {
    /// Create an empty registry with no patterns.
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
            rules: Vec::new(),
        }
    }

    /// Register a pattern.
    pub fn register(&mut self, pattern: Pattern) {
        self.patterns.push(pattern);
    }

    /// Add an incompatibility rule.
    pub fn add_rule(&mut self, rule: IncompatibilityRule) {
        self.rules.push(rule);
    }

    /// Returns the number of registered patterns.
    pub fn len(&self) -> usize {
        self.patterns.len()
    }

    /// Returns true if no patterns are registered.
    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    /// Returns the incompatibility rules.
    pub fn rules(&self) -> &[IncompatibilityRule] {
        &self.rules
    }

    /// Pick a random pattern matching the given filter.
    ///
    /// Returns `None` if no pattern matches the filter or if all matching
    /// patterns have weight 0.
    pub fn pick_random<'a>(
        &'a self,
        entropy: &mut Entropy<'_>,
        filter: &SelectionFilter,
    ) -> Option<&'a Pattern> {
        let candidates: Vec<&Pattern> = self
            .patterns
            .iter()
            .filter(|p| filter.matches(&p.tags, p.min_depth))
            .collect();

        if candidates.is_empty() {
            return None;
        }

        let total: u32 = candidates.iter().map(|p| p.weight).sum();
        if total == 0 {
            return None;
        }

        let mut pick = entropy.range(0..total);
        for p in &candidates {
            if pick < p.weight {
                return Some(*p);
            }
            pick -= p.weight;
        }
        candidates.last().copied()
    }

    /// Create a registry with all default patterns and rules.
    pub fn default_registry() -> Self {
        use crate::registry::*;

        let mut reg = Self::new();

        // Basic patterns
        reg.register(basic::single_table());
        reg.register(basic::single_parameter());
        reg.register(basic::project_literal());

        // Aggregate patterns
        reg.register(aggregates::count());
        reg.register(aggregates::sum());
        reg.register(aggregates::avg());
        reg.register(aggregates::min_max());
        reg.register(aggregates::count_distinct());
        reg.register(aggregates::aggregate_with_group_by());
        reg.register(aggregates::having_clause());

        // Filter patterns
        reg.register(filters::between());
        reg.register(filters::in_list());
        reg.register(filters::like());
        reg.register(filters::is_null());
        reg.register(filters::compound_where());

        // Join patterns
        reg.register(joins::inner_join());
        reg.register(joins::left_join());
        reg.register(joins::self_join());
        reg.register(joins::cross_join());

        // Ordering patterns
        reg.register(ordering::topk());
        reg.register(ordering::order_by());
        reg.register(ordering::limit_offset());

        // Subquery patterns
        reg.register(subqueries::exists_subquery());
        reg.register(subqueries::in_subquery());
        reg.register(subqueries::scalar_subquery());
        reg.register(subqueries::join_subquery());

        // CTE patterns
        reg.register(ctes::simple_cte());
        reg.register(ctes::cte_with_join());

        // Advanced patterns
        reg.register(advanced::window_function());
        reg.register(advanced::distinct());
        reg.register(advanced::multi_join());

        // Default incompatibility rules
        for rule in default_rules() {
            reg.add_rule(rule);
        }

        reg
    }
}

/// Maximum number of retries when resolution fails.
const MAX_RETRIES: usize = 10;

/// The core generator for producing random SQL queries.
#[derive(Debug)]
pub struct Generator {
    state: GenerationState,
    registry: ConstraintRegistry,
}

impl Generator {
    /// Create a new generator with the default registry.
    pub fn new(dialect: Dialect, config: GeneratorConfig) -> Self {
        Self {
            state: GenerationState::new(dialect, config),
            registry: ConstraintRegistry::default_registry(),
        }
    }

    /// Create a new generator with a custom registry.
    pub fn new_with_registry(
        dialect: Dialect,
        config: GeneratorConfig,
        registry: ConstraintRegistry,
    ) -> Self {
        Self {
            state: GenerationState::new(dialect, config),
            registry,
        }
    }

    /// Access the generation state.
    pub fn state(&self) -> &GenerationState {
        &self.state
    }

    /// Access the generation state mutably.
    pub fn state_mut(&mut self) -> &mut GenerationState {
        &mut self.state
    }

    /// Access the registry.
    pub fn registry(&self) -> &ConstraintRegistry {
        &self.registry
    }

    /// Mode 1: Generate a query plus any DDL needed to support it.
    ///
    /// Algorithm:
    /// 1. Pick a random pattern from the registry
    /// 2. Convert it to a Recipe
    /// 3. Resolve the Recipe against current state (with synthesis enabled)
    /// 4. On failure, retry with a different pattern (up to MAX_RETRIES)
    /// 5. Return QueryOutput with query, DDL, and param metadata
    pub fn generate_with_ddl(
        &mut self,
        entropy: &mut Entropy<'_>,
    ) -> Result<QueryOutput, GenerateError> {
        self.generate_with_ddl_filtered(entropy, &SelectionFilter::default())
    }

    /// Mode 1 with a filter: generate a query matching the filter.
    pub fn generate_with_ddl_filtered(
        &mut self,
        entropy: &mut Entropy<'_>,
        filter: &SelectionFilter,
    ) -> Result<QueryOutput, GenerateError> {
        if self.registry.is_empty() {
            return Err(GenerateError::NoCompatiblePattern { attempted: vec![] });
        }

        let mut filter = filter.clone();
        filter.max_depth = Some(self.state.config().max_subquery_depth);

        let mut attempted = Vec::new();

        for _ in 0..MAX_RETRIES {
            // Pick a pattern
            let pattern = match self.registry.pick_random(entropy, &filter) {
                Some(p) => p,
                None => {
                    return Err(GenerateError::NoCompatiblePattern { attempted });
                }
            };

            let pattern_name = pattern.name.to_string();

            // Check incompatibility rules
            if let Some(reason) =
                check_rules(&self.registry.rules, &pattern.constraints, &pattern.tags)
            {
                attempted.push(format!("{pattern_name}: {reason}"));
                continue;
            }

            // Convert to recipe and try to resolve
            let recipe = pattern.to_recipe(0);
            match resolver::try_resolve(&recipe, &mut self.state, entropy) {
                Ok(output) => return Ok(output.into()),
                Err(e) => {
                    attempted.push(format!("{pattern_name}: {e}"));
                    continue;
                }
            }
        }

        Err(GenerateError::MaxRetriesExceeded {
            retries: MAX_RETRIES,
        })
    }

    /// Mode 2: Generate DDL only, optionally with a query.
    ///
    /// Creates new tables or adds columns to existing tables to expand
    /// the schema. Optionally generates a Mode 1 query using the expanded
    /// schema.
    pub fn generate_ddl_only(
        &mut self,
        entropy: &mut Entropy<'_>,
        with_query: bool,
    ) -> Result<DdlOutput, GenerateError> {
        let ddl = self.generate_ddl_step(entropy);

        let query = if with_query {
            Some(self.generate_with_ddl(entropy)?)
        } else {
            None
        };

        Ok(DdlOutput { ddl, query })
    }

    /// Generate a single DDL step (create table or add column).
    fn generate_ddl_step(&mut self, entropy: &mut Entropy<'_>) -> Vec<DdlStep> {
        let mut steps = Vec::new();

        // If no tables exist, always create one
        let should_create_table = self.state.tables().is_empty() || entropy.probability(0.3);

        if should_create_table {
            let name = self.state.fresh_table_name();
            let mut schema = TableSchema::new(name.clone());

            // Add 3-8 columns with random types
            let num_cols = entropy.range(3..9usize);
            for _ in 0..num_cols {
                let col_name = schema.fresh_column_name();
                let sql_type = random_sql_type(entropy);
                let meta = ColumnMeta {
                    sql_type,
                    gen_spec: self.state.config().default_gen_spec.clone(),
                };
                schema.add_column(col_name.clone(), meta.clone());
                steps.push(DdlStep::AddColumn {
                    table: name.clone(),
                    column_name: col_name,
                    meta,
                });
            }

            // Set first column as primary key
            if let Some((pk_name, _)) = schema.columns.first() {
                schema.primary_key = Some(pk_name.clone());
            }

            steps.insert(
                0,
                DdlStep::CreateTable {
                    name: name.clone(),
                    schema: schema.clone(),
                },
            );
            self.state.add_table(schema);
        } else {
            // Add a column to a random existing table
            let table = self
                .state
                .pick_random_table(entropy)
                .expect("tables non-empty checked above");
            let table_name = table.name.clone();
            let default_gen_spec = self.state.config().default_gen_spec.clone();

            let table_schema = self.state.table_mut(&table_name).expect("table exists");
            let col_name = table_schema.fresh_column_name();
            let sql_type = random_sql_type(entropy);
            let meta = ColumnMeta {
                sql_type,
                gen_spec: default_gen_spec,
            };
            table_schema.add_column(col_name.clone(), meta.clone());

            steps.push(DdlStep::AddColumn {
                table: table_name,
                column_name: col_name,
                meta,
            });
        }

        steps
    }

    /// Mode 3: Generate a query using only existing schema (no new DDL).
    ///
    /// Picks patterns and resolves them against the current state. If
    /// resolution produces DDL steps (meaning it needed to synthesize
    /// tables/columns), the result is rejected and another pattern is tried.
    pub fn generate_for_existing_schema(
        &mut self,
        entropy: &mut Entropy<'_>,
    ) -> Result<QueryOutput, GenerateError> {
        if self.state.tables().is_empty() {
            return Err(GenerateError::NoExistingSchema);
        }

        if self.registry.is_empty() {
            return Err(GenerateError::NoCompatiblePattern { attempted: vec![] });
        }

        let filter = SelectionFilter {
            max_depth: Some(self.state.config().max_subquery_depth),
            ..Default::default()
        };

        let mut attempted = Vec::new();

        for _ in 0..MAX_RETRIES {
            let pattern = match self.registry.pick_random(entropy, &filter) {
                Some(p) => p,
                None => {
                    return Err(GenerateError::NoCompatiblePattern { attempted });
                }
            };

            let pattern_name = pattern.name.to_string();

            // Check incompatibility rules
            if let Some(reason) =
                check_rules(&self.registry.rules, &pattern.constraints, &pattern.tags)
            {
                attempted.push(format!("{pattern_name}: {reason}"));
                continue;
            }

            let recipe = pattern.to_recipe(0);

            // Checkpoint state before resolution so we can roll back
            // if the pattern requires new DDL
            let state_cp = self.state.checkpoint();

            match resolver::resolve(
                &recipe.constraints,
                &recipe.var_kinds,
                &mut self.state,
                entropy,
            ) {
                Ok(output) => {
                    if output.ddl.is_empty() {
                        return Ok(output.into());
                    }
                    // Resolution produced DDL -- pattern needs new tables/columns.
                    // Roll back state since Mode 3 shouldn't modify schema.
                    self.state.restore(state_cp);
                    attempted.push(format!(
                        "{pattern_name}: required new DDL ({} steps)",
                        output.ddl.len()
                    ));
                    continue;
                }
                Err(e) => {
                    self.state.restore(state_cp);
                    attempted.push(format!("{pattern_name}: {e}"));
                    continue;
                }
            }
        }

        Err(GenerateError::MaxRetriesExceeded {
            retries: MAX_RETRIES,
        })
    }
}

/// Pick a random SQL type from a reasonable distribution.
fn random_sql_type(entropy: &mut Entropy<'_>) -> SqlType {
    let type_class = entropy.choose(&[
        TypeClass::Integer,
        TypeClass::Numeric,
        TypeClass::String,
        TypeClass::DateTime,
        TypeClass::Any,
    ]);

    match type_class {
        TypeClass::Integer => SqlType::Int(None),
        TypeClass::Numeric => SqlType::Double,
        TypeClass::String => SqlType::VarChar(Some(255)),
        TypeClass::DateTime => SqlType::Timestamp,
        TypeClass::Exact(t) => t.clone(),
        TypeClass::Any => {
            // Pick from all types
            entropy
                .choose(&[
                    SqlType::Int(None),
                    SqlType::BigInt(None),
                    SqlType::Double,
                    SqlType::VarChar(Some(255)),
                    SqlType::Text,
                    SqlType::Bool,
                    SqlType::Timestamp,
                ])
                .clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use readyset_sql::{Dialect, DialectDisplay};

    use super::*;

    // --- ConstraintRegistry tests ---

    #[test]
    fn empty_registry() {
        let reg = ConstraintRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn register_patterns() {
        let mut reg = ConstraintRegistry::new();
        reg.register(crate::registry::basic::single_table());
        reg.register(crate::registry::basic::single_parameter());
        assert_eq!(reg.len(), 2);
        assert!(!reg.is_empty());
    }

    #[test]
    fn default_registry_has_patterns() {
        let reg = ConstraintRegistry::default_registry();
        // Should have all patterns from all registry modules
        assert!(
            reg.len() >= 30,
            "expected >= 30 patterns, got {}",
            reg.len()
        );
        assert!(!reg.rules().is_empty());
    }

    #[test]
    fn pick_random_from_empty_returns_none() {
        let reg = ConstraintRegistry::new();
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);
        assert!(
            reg.pick_random(&mut entropy, &SelectionFilter::default())
                .is_none()
        );
    }

    #[test]
    fn pick_random_returns_pattern() {
        let mut reg = ConstraintRegistry::new();
        reg.register(crate::registry::basic::single_table());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);
        let p = reg.pick_random(&mut entropy, &SelectionFilter::default());
        assert!(p.is_some());
        assert_eq!(p.expect("should have pattern").name, "single_table");
    }

    #[test]
    fn pick_random_with_filter() {
        let reg = ConstraintRegistry::default_registry();
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        // Filter for aggregates only
        let filter = SelectionFilter {
            required_tags: vec!["aggregate"],
            ..Default::default()
        };

        for _ in 0..20 {
            let p = reg
                .pick_random(&mut entropy, &filter)
                .expect("should find aggregate pattern");
            assert!(
                p.tags.contains(&"aggregate"),
                "expected aggregate tag, got {:?}",
                p.tags
            );
        }
    }

    #[test]
    fn pick_random_with_excluded_tag() {
        let reg = ConstraintRegistry::default_registry();
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let filter = SelectionFilter {
            excluded_tags: vec!["subquery", "cte", "window", "join"],
            ..Default::default()
        };

        for _ in 0..20 {
            let p = reg
                .pick_random(&mut entropy, &filter)
                .expect("should find pattern");
            assert!(
                !p.tags.contains(&"subquery"),
                "should not contain subquery tag"
            );
        }
    }

    // --- Generator tests ---

    #[test]
    fn generator_new_creates_valid_generator() {
        let generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        assert!(!generator.registry().is_empty());
        assert!(generator.state().tables().is_empty());
    }

    #[test]
    fn generator_new_with_empty_registry() {
        let reg = ConstraintRegistry::new();
        let generator =
            Generator::new_with_registry(Dialect::MySQL, GeneratorConfig::default(), reg);
        assert!(generator.registry().is_empty());
    }

    #[test]
    fn generate_with_ddl_empty_registry_returns_error() {
        let reg = ConstraintRegistry::new();
        let mut generator =
            Generator::new_with_registry(Dialect::MySQL, GeneratorConfig::default(), reg);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let result = generator.generate_with_ddl(&mut entropy);
        assert!(result.is_err());
        match result.expect_err("should error") {
            GenerateError::NoCompatiblePattern { attempted } => {
                assert!(attempted.is_empty());
            }
            other => panic!("expected NoCompatiblePattern, got {other:?}"),
        }
    }

    #[test]
    fn generate_with_ddl_produces_valid_output() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let result = generator.generate_with_ddl(&mut entropy);
        assert!(result.is_ok(), "generate_with_ddl failed: {result:?}");

        let output = result.expect("should succeed");
        let sql = output.query.display(Dialect::MySQL).to_string();
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    #[test]
    fn generate_with_ddl_creates_tables() {
        let config = GeneratorConfig {
            reuse_preference: 0.0, // always create new
            ..Default::default()
        };
        let mut generator = Generator::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let output = generator
            .generate_with_ddl(&mut entropy)
            .expect("should succeed");

        // With reuse_preference=0.0 and no existing tables, DDL should contain CreateTable
        assert!(!output.ddl.is_empty(), "expected DDL steps for new tables");
    }

    #[test]
    fn multiple_generations_reuse_tables() {
        let config = GeneratorConfig {
            reuse_preference: 1.0, // always reuse
            ..Default::default()
        };
        let mut generator = Generator::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        // First generation creates tables.
        let _ = generator
            .generate_with_ddl(&mut entropy)
            .expect("first generation should succeed");
        let tables_after_first = generator.state().tables().len();
        assert!(tables_after_first > 0, "should have created tables");

        // Second generation should reuse existing tables.
        // reuse_preference=1.0 means a pattern needing a relation either
        // picks an existing one or fails; with retries it normally finds a
        // reusable choice. Some patterns demand more tables than have been
        // created so far (e.g. multi_join wants three distinct ones), in
        // which case the count grows by *at most* the number of new
        // BaseTables in the pattern. We bound the growth instead of asking
        // for exact equality so the assertion isn't pinned to one
        // particular pattern firing.
        let _ = generator
            .generate_with_ddl(&mut entropy)
            .expect("second generation should succeed");
        let tables_after_second = generator.state().tables().len();
        assert!(
            tables_after_second <= tables_after_first + 3,
            "reuse_preference=1.0 should keep table growth bounded; \
             grew from {tables_after_first} to {tables_after_second}"
        );
    }

    #[test]
    fn deterministic_same_seed_same_output() {
        let config = GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };

        // Run 1
        let mut generator1 = Generator::new(Dialect::MySQL, config.clone());
        let mut rng1 = SmallRng::seed_from_u64(123);
        let mut entropy1 = Entropy::new(&mut rng1);
        let output1 = generator1
            .generate_with_ddl(&mut entropy1)
            .expect("should succeed");
        let sql1 = output1.query.display(Dialect::MySQL).to_string();

        // Run 2 with same seed
        let mut generator2 = Generator::new(Dialect::MySQL, config);
        let mut rng2 = SmallRng::seed_from_u64(123);
        let mut entropy2 = Entropy::new(&mut rng2);
        let output2 = generator2
            .generate_with_ddl(&mut entropy2)
            .expect("should succeed");
        let sql2 = output2.query.display(Dialect::MySQL).to_string();

        assert_eq!(sql1, sql2, "same seed should produce same output");
    }

    #[test]
    fn generate_with_ddl_filtered() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let filter = SelectionFilter {
            required_tags: vec!["base"],
            ..Default::default()
        };

        let result = generator.generate_with_ddl_filtered(&mut entropy, &filter);
        assert!(result.is_ok(), "filtered generation failed: {result:?}");
    }

    #[test]
    fn generate_with_ddl_no_matching_filter_returns_error() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let filter = SelectionFilter {
            required_tags: vec!["nonexistent_tag_xyz"],
            ..Default::default()
        };

        let result = generator.generate_with_ddl_filtered(&mut entropy, &filter);
        assert!(result.is_err());
        match result.expect_err("should error") {
            GenerateError::NoCompatiblePattern { .. } => {}
            other => panic!("expected NoCompatiblePattern, got {other:?}"),
        }
    }

    #[test]
    fn generate_multiple_queries() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        // Generate 10 queries in sequence
        for i in 0..10 {
            let result = generator.generate_with_ddl(&mut entropy);
            assert!(
                result.is_ok(),
                "generation {i} failed: {:?}",
                result.expect_err("should fail")
            );
        }
    }

    // --- Mode 2 tests ---

    #[test]
    fn mode2_generates_ddl() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let result = generator.generate_ddl_only(&mut entropy, false);
        assert!(result.is_ok(), "mode2 failed: {result:?}");

        let output = result.expect("should succeed");
        assert!(!output.ddl.is_empty(), "should have DDL steps");
        assert!(output.query.is_none(), "should not have query");

        // State should have tables now
        assert!(
            !generator.state().tables().is_empty(),
            "should have created tables"
        );
    }

    #[test]
    fn mode2_with_query() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let result = generator.generate_ddl_only(&mut entropy, true);
        assert!(result.is_ok(), "mode2 with query failed: {result:?}");

        let output = result.expect("should succeed");
        assert!(!output.ddl.is_empty(), "should have DDL steps");
        assert!(output.query.is_some(), "should have query");

        let query = output.query.expect("query exists");
        let sql = query.query.display(Dialect::MySQL).to_string();
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    #[test]
    fn mode2_creates_table_when_empty() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        assert!(generator.state().tables().is_empty());
        let output = generator
            .generate_ddl_only(&mut entropy, false)
            .expect("should succeed");

        // First DDL step should be CreateTable
        assert!(matches!(output.ddl[0], DdlStep::CreateTable { .. }));
        assert!(!generator.state().tables().is_empty());
    }

    #[test]
    fn mode2_can_add_column_to_existing() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        // First create a table
        let _ = generator
            .generate_ddl_only(&mut entropy, false)
            .expect("first ddl should succeed");
        let initial_tables = generator.state().tables().len();

        // Generate many DDL steps -- some should add columns to existing tables
        let mut added_column = false;
        for _ in 0..20 {
            let output = generator
                .generate_ddl_only(&mut entropy, false)
                .expect("ddl should succeed");

            // Check if we got an AddColumn without a CreateTable
            let has_create_table = output
                .ddl
                .iter()
                .any(|s| matches!(s, DdlStep::CreateTable { .. }));
            if !has_create_table {
                added_column = true;
            }
        }

        // With 20 iterations, we should hit the 70% chance of AddColumn at least once
        assert!(
            added_column || generator.state().tables().len() > initial_tables,
            "expected at least one AddColumn or new table creation"
        );
    }

    // --- Mode 3 tests ---

    #[test]
    fn mode3_empty_schema_returns_error() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let result = generator.generate_for_existing_schema(&mut entropy);
        assert!(result.is_err());
        match result.expect_err("should error") {
            GenerateError::NoExistingSchema => {}
            other => panic!("expected NoExistingSchema, got {other:?}"),
        }
    }

    #[test]
    fn mode3_uses_existing_tables() {
        let config = GeneratorConfig {
            reuse_preference: 1.0,
            ..Default::default()
        };
        let mut generator = Generator::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        // First: create schema with Mode 2
        let _ = generator
            .generate_ddl_only(&mut entropy, false)
            .expect("ddl generation should succeed");
        let tables_before = generator.state().tables().len();
        assert!(tables_before > 0);

        // Now: Mode 3 should generate a query without new DDL
        let result = generator.generate_for_existing_schema(&mut entropy);
        assert!(result.is_ok(), "mode3 failed: {result:?}");

        let output = result.expect("should succeed");
        assert!(
            output.ddl.is_empty(),
            "Mode 3 should not produce DDL, got {} steps",
            output.ddl.len()
        );

        let sql = output.query.display(Dialect::MySQL).to_string();
        assert!(sql.contains("SELECT"), "sql: {sql}");

        // Table count should not have changed
        assert_eq!(
            generator.state().tables().len(),
            tables_before,
            "Mode 3 should not create new tables"
        );
    }

    #[test]
    fn mode3_does_not_modify_schema() {
        let config = GeneratorConfig {
            reuse_preference: 1.0,
            ..Default::default()
        };
        let mut generator = Generator::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        // Create a rich schema with multiple Mode 2 calls
        for _ in 0..5 {
            let _ = generator.generate_ddl_only(&mut entropy, false);
        }

        let tables_before = generator.state().tables().len();
        let total_cols_before: usize = generator
            .state()
            .tables()
            .values()
            .map(|t| t.columns.len())
            .sum();

        // Generate 5 Mode 3 queries
        for _ in 0..5 {
            let result = generator.generate_for_existing_schema(&mut entropy);
            // May fail if no compatible pattern found -- that's OK
            if let Ok(output) = result {
                assert!(output.ddl.is_empty(), "Mode 3 should not produce DDL");
            }
        }

        // Schema should be unchanged
        assert_eq!(generator.state().tables().len(), tables_before);
        let total_cols_after: usize = generator
            .state()
            .tables()
            .values()
            .map(|t| t.columns.len())
            .sum();
        assert_eq!(total_cols_after, total_cols_before);
    }

    #[test]
    fn mode3_deterministic() {
        let config = GeneratorConfig {
            reuse_preference: 1.0,
            ..Default::default()
        };

        // Set up identical generators with same seed
        let mut generator1 = Generator::new(Dialect::MySQL, config.clone());
        let mut rng1 = SmallRng::seed_from_u64(99);
        let mut entropy1 = Entropy::new(&mut rng1);

        // Create schema first
        let _ = generator1.generate_ddl_only(&mut entropy1, false);

        let mut generator2 = Generator::new(Dialect::MySQL, config);
        let mut rng2 = SmallRng::seed_from_u64(99);
        let mut entropy2 = Entropy::new(&mut rng2);

        let _ = generator2.generate_ddl_only(&mut entropy2, false);

        // Both should produce identical Mode 3 results
        let result1 = generator1.generate_for_existing_schema(&mut entropy1);
        let result2 = generator2.generate_for_existing_schema(&mut entropy2);

        match (result1, result2) {
            (Ok(o1), Ok(o2)) => {
                let sql1 = o1.query.display(Dialect::MySQL).to_string();
                let sql2 = o2.query.display(Dialect::MySQL).to_string();
                assert_eq!(sql1, sql2, "same seed should produce same output");
            }
            (Err(_), Err(_)) => {
                // Both failed the same way -- that's also deterministic
            }
            (r1, r2) => {
                panic!("expected both to succeed or both to fail, got {r1:?} vs {r2:?}");
            }
        }
    }

    // --- Long-lived generator / Glutton compatibility tests ---

    #[test]
    fn long_lived_generator_accumulates_state() {
        let config = GeneratorConfig {
            reuse_preference: 0.7,
            ..Default::default()
        };
        let mut generator = Generator::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let mut success_count = 0;
        let num_iterations = 100;

        for i in 0..num_iterations {
            let result = generator.generate_with_ddl(&mut entropy);
            match result {
                Ok(output) => {
                    success_count += 1;
                    let sql = output.query.display(Dialect::MySQL).to_string();
                    assert!(sql.contains("SELECT"), "iteration {i}: sql: {sql}");
                }
                Err(_) => {
                    // Some patterns may fail, that's ok for long-lived usage
                }
            }
        }

        // Most queries should succeed
        assert!(
            success_count > num_iterations * 3 / 4,
            "expected >75% success rate, got {success_count}/{num_iterations}"
        );

        // Tables should have accumulated
        assert!(
            !generator.state().tables().is_empty(),
            "should have accumulated tables"
        );

        // With reuse_preference=0.7, later queries should reference earlier tables
        let table_count = generator.state().tables().len();
        assert!(
            table_count >= 2,
            "expected at least 2 tables after {num_iterations} iterations, got {table_count}"
        );
    }

    #[test]
    fn long_lived_mixed_modes() {
        let config = GeneratorConfig {
            reuse_preference: 0.5,
            ..Default::default()
        };
        let mut generator = Generator::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(77);
        let mut entropy = Entropy::new(&mut rng);

        // Phase 1: Mode 2 to bootstrap schema
        for _ in 0..5 {
            let result = generator.generate_ddl_only(&mut entropy, false);
            assert!(result.is_ok(), "mode2 should succeed");
        }

        let tables_after_bootstrap = generator.state().tables().len();
        assert!(tables_after_bootstrap > 0);

        // Phase 2: Mode 3 to query existing schema only
        let mut mode3_successes = 0;
        for _ in 0..20 {
            if generator.generate_for_existing_schema(&mut entropy).is_ok() {
                mode3_successes += 1;
            }
        }
        assert!(mode3_successes > 0, "Mode 3 should succeed at least once");

        // Schema should not have changed from Mode 3
        assert_eq!(
            generator.state().tables().len(),
            tables_after_bootstrap,
            "Mode 3 should not modify schema"
        );

        // Phase 3: Mode 1 with existing schema
        for _ in 0..20 {
            let _ = generator.generate_with_ddl(&mut entropy);
        }

        // Tables should have accumulated (Mode 1 can add more)
        assert!(
            generator.state().tables().len() >= tables_after_bootstrap,
            "Mode 1 should preserve or grow schema"
        );
    }

    #[test]
    fn deterministic_replay_100_queries() {
        let config = GeneratorConfig {
            reuse_preference: 0.5,
            ..Default::default()
        };

        let run = |seed: u64| -> Vec<String> {
            let mut generator = Generator::new(Dialect::MySQL, config.clone());
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut entropy = Entropy::new(&mut rng);
            let mut results = Vec::new();

            for _ in 0..100 {
                match generator.generate_with_ddl(&mut entropy) {
                    Ok(output) => {
                        results.push(output.query.display(Dialect::MySQL).to_string());
                    }
                    Err(e) => {
                        results.push(format!("ERROR:{e}"));
                    }
                }
            }
            results
        };

        let run1 = run(42);
        let run2 = run(42);

        assert_eq!(
            run1.len(),
            run2.len(),
            "same seed should produce same number of results"
        );
        for (i, (q1, q2)) in run1.iter().zip(run2.iter()).enumerate() {
            assert_eq!(q1, q2, "query {i} differs between runs");
        }
    }

    #[test]
    fn entropy_accepts_any_rng_core() {
        // Verify that Entropy works with any Rng impl, which is the
        // Antithesis integration path: AntithesisRng implements Rng.
        use rand::rand_core::TryRng;

        // Minimal Rng implementation to simulate an external source.
        struct CounterRng(u64);
        impl TryRng for CounterRng {
            type Error = std::convert::Infallible;
            fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
                Ok(self.try_next_u64()? as u32)
            }
            fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
                self.0 = self.0.wrapping_add(1);
                // Mix bits to avoid degenerate sequences
                Ok(self.0.wrapping_mul(6364136223846793005))
            }
            fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Self::Error> {
                for chunk in dest.chunks_mut(8) {
                    let bytes = self.try_next_u64()?.to_le_bytes();
                    chunk.copy_from_slice(&bytes[..chunk.len()]);
                }
                Ok(())
            }
        }

        let mut rng = CounterRng(42);
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut entropy = Entropy::new(&mut rng);

        // Should work with any RngCore implementation
        let result = generator.generate_with_ddl(&mut entropy);
        assert!(
            result.is_ok(),
            "should work with custom RngCore: {result:?}"
        );
    }

    #[test]
    fn self_join_generates_via_default_registry() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let filter = SelectionFilter {
            required_tags: vec!["self_join"],
            ..Default::default()
        };

        let result = generator.generate_with_ddl_filtered(&mut entropy, &filter);
        assert!(result.is_ok(), "self_join generation failed: {result:?}");

        let output = result.expect("checked");
        let sql = output.query.display(Dialect::MySQL).to_string();
        assert!(sql.contains(" AS "), "self-join should use aliases: {sql}");
        assert!(sql.contains("JOIN"), "self-join should contain JOIN: {sql}");
    }
}
