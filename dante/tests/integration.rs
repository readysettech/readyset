//! End-to-end integration test: full pipeline from Pattern through SelectStatement.

use dante::entropy::Entropy;
use dante::registry::basic;
use dante::resolver::{DdlStep, resolve};
use dante::state::{GenerationState, GeneratorConfig};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use readyset_sql::{Dialect, DialectDisplay};

/// Full pipeline test: single_parameter pattern -> SQL string + DDL + params.
#[test]
fn single_parameter_end_to_end() {
    let pattern = basic::single_parameter();

    // Convert to recipe (offset 0 for single pattern)
    let recipe = pattern.to_recipe(0);

    // Create a fresh state
    let config = GeneratorConfig {
        reuse_preference: 0.0, // always synthesize new tables
        ..Default::default()
    };
    let mut state = GenerationState::new(Dialect::MySQL, config);
    let mut rng = SmallRng::seed_from_u64(42);
    let mut entropy = Entropy::new(&mut rng);

    // Resolve
    let output = resolve(
        &recipe.constraints,
        &recipe.var_kinds,
        &mut state,
        &mut entropy,
    )
    .expect("resolution should succeed");

    // Verify SQL output
    let sql = output.query.display(Dialect::MySQL).to_string();
    assert!(sql.contains("SELECT"), "sql: {sql}");
    assert!(sql.contains("FROM"), "sql: {sql}");
    assert!(sql.contains("WHERE"), "sql: {sql}");
    assert!(sql.contains("= ?"), "sql: {sql}");

    // Verify DDL steps
    assert!(!output.ddl.is_empty(), "should have DDL steps");
    let has_create_table = output
        .ddl
        .iter()
        .any(|s| matches!(s, DdlStep::CreateTable { .. }));
    assert!(has_create_table, "should have a CreateTable DDL step");

    // Verify params
    assert_eq!(output.params.len(), 1, "should have exactly 1 parameter");
    assert_eq!(output.params[0].count, 1);

    // Verify the table was added to state
    assert!(!state.tables().is_empty());
}

/// Full pipeline test: single_table pattern -> simple SELECT without WHERE.
#[test]
fn single_table_end_to_end() {
    let pattern = basic::single_table();
    let recipe = pattern.to_recipe(0);

    let config = GeneratorConfig {
        reuse_preference: 0.0,
        ..Default::default()
    };
    let mut state = GenerationState::new(Dialect::PostgreSQL, config);
    let mut rng = SmallRng::seed_from_u64(99);
    let mut entropy = Entropy::new(&mut rng);

    let output = resolve(
        &recipe.constraints,
        &recipe.var_kinds,
        &mut state,
        &mut entropy,
    )
    .expect("resolution should succeed");

    let sql = output.query.display(Dialect::PostgreSQL).to_string();
    assert!(sql.contains("SELECT"), "sql: {sql}");
    assert!(sql.contains("FROM"), "sql: {sql}");
    assert!(!sql.contains("WHERE"), "should have no WHERE clause: {sql}");
    assert!(output.params.is_empty(), "should have no params");
}

/// Full pipeline test: project_literal pattern -> SELECT <literal> FROM t.
#[test]
fn project_literal_end_to_end() {
    let pattern = basic::project_literal();
    let recipe = pattern.to_recipe(0);

    let config = GeneratorConfig {
        reuse_preference: 0.0,
        ..Default::default()
    };
    let mut state = GenerationState::new(Dialect::MySQL, config);
    let mut rng = SmallRng::seed_from_u64(7);
    let mut entropy = Entropy::new(&mut rng);

    let output = resolve(
        &recipe.constraints,
        &recipe.var_kinds,
        &mut state,
        &mut entropy,
    )
    .expect("resolution should succeed");

    let sql = output.query.display(Dialect::MySQL).to_string();
    assert!(sql.contains("SELECT"), "sql: {sql}");
    assert!(sql.contains("FROM"), "sql: {sql}");
    assert!(sql.contains("1"), "should contain literal: {sql}");
    assert!(output.params.is_empty());
}

/// Verify DDL step contains the right table name that matches the FROM clause.
#[test]
fn ddl_matches_from_clause() {
    let pattern = basic::single_parameter();
    let recipe = pattern.to_recipe(0);

    let config = GeneratorConfig {
        reuse_preference: 0.0,
        ..Default::default()
    };
    let mut state = GenerationState::new(Dialect::MySQL, config);
    let mut rng = SmallRng::seed_from_u64(42);
    let mut entropy = Entropy::new(&mut rng);

    let output = resolve(
        &recipe.constraints,
        &recipe.var_kinds,
        &mut state,
        &mut entropy,
    )
    .expect("resolution should succeed");

    let sql = output.query.display(Dialect::MySQL).to_string();

    // Find the table name from DDL
    let table_name = output.ddl.iter().find_map(|s| match s {
        DdlStep::CreateTable { name, .. } => Some(name.to_string()),
        _ => None,
    });
    let table_name = table_name.expect("should have CreateTable DDL");

    // The SQL should contain the same table name
    assert!(
        sql.contains(&table_name),
        "SQL should reference DDL table name '{table_name}': {sql}"
    );
}

/// Multiple resolutions with the same state accumulate tables.
#[test]
fn multiple_resolutions_accumulate_state() {
    let pattern = basic::single_table();
    let recipe = pattern.to_recipe(0);

    let config = GeneratorConfig {
        reuse_preference: 0.0,
        ..Default::default()
    };
    let mut state = GenerationState::new(Dialect::MySQL, config);
    let mut rng = SmallRng::seed_from_u64(42);

    // First resolution
    {
        let mut entropy = Entropy::new(&mut rng);
        let _output = resolve(
            &recipe.constraints,
            &recipe.var_kinds,
            &mut state,
            &mut entropy,
        )
        .expect("first resolve");
    }

    let tables_after_first = state.tables().len();
    assert!(tables_after_first >= 1);

    // Second resolution
    {
        let mut entropy = Entropy::new(&mut rng);
        let _output = resolve(
            &recipe.constraints,
            &recipe.var_kinds,
            &mut state,
            &mut entropy,
        )
        .expect("second resolve");
    }

    // Should have at least as many tables (possibly more due to synthesis)
    assert!(state.tables().len() >= tables_after_first);
}
