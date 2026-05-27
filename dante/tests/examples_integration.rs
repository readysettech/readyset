//! End-to-end: Pattern with Constraint::Example -> QueryOutput.examples.

use dante::constraint::{DialectSupport, ExampleCell, ExampleValue, TypeClass};
use dante::entropy::Entropy;
use dante::generator::{ConstraintRegistry, Generator};
use dante::pattern::PatternBuilder;
use dante::state::GeneratorConfig;
use proptest::prelude::*;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use readyset_sql::{Dialect, ast::BinaryOperator};

#[test]
fn example_surfaces_for_matching_dialect() {
    // Drive the canonical registry pattern rather than a hand-built twin so
    // this test tracks the real `int_int_divide_bait` definition.
    let mut reg = ConstraintRegistry::new();
    reg.register(dante::registry::examples::int_int_divide_bait())
        .expect("int_int_divide_bait is a valid pattern");

    let config = GeneratorConfig {
        reuse_preference: 0.0,
        ..Default::default()
    };
    let mut generator = Generator::new_with_registry(Dialect::MySQL, config, reg);
    let mut rng = SmallRng::seed_from_u64(0xC0DE);
    let mut entropy = Entropy::new(&mut rng);
    let out = generator
        .generate_with_ddl(&mut entropy)
        .expect("generation should succeed for the registry pattern");

    // The generator may compose the single pattern with itself, so there could
    // be more than one example. Verify that at least one example with the
    // expected note is present and carries the right overrides.
    assert!(
        !out.examples.is_empty(),
        "expected at least one example, got none"
    );
    let ex = out
        .examples
        .iter()
        .find(|e| e.note == "int/int divide: c1=8, c2=3, param=2")
        .expect("expected to find example with note 'int/int divide: c1=8, c2=3, param=2'");
    assert_eq!(ex.row_overrides.len(), 2);
    assert_eq!(ex.param_overrides.len(), 1);
    // Assert the pinned param value carries through rather than its internal
    // placeholder index, which is an artifact of var numbering / composition.
    assert_eq!(ex.param_overrides[0].value, ExampleValue::Literal("2"));
}

#[test]
fn mysqlonly_example_dropped_for_postgres_dialect() {
    let mut b = PatternBuilder::new("int_int_divide_bait");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::Integer);
    b.from(t);
    b.project_column(c, t);
    b.example(
        "mysql-only",
        DialectSupport::MySqlOnly,
        vec![ExampleCell {
            var: c,
            value: ExampleValue::Literal("1"),
        }],
    );
    let pattern = b.build();

    let mut reg = ConstraintRegistry::new();
    reg.register(pattern).expect("pattern is a valid pattern");

    let config = GeneratorConfig {
        reuse_preference: 0.0,
        ..Default::default()
    };
    let mut generator = Generator::new_with_registry(Dialect::PostgreSQL, config, reg);
    let mut rng = SmallRng::seed_from_u64(0xC0DE);
    let mut entropy = Entropy::new(&mut rng);
    let out = generator
        .generate_with_ddl(&mut entropy)
        .expect("generation should succeed");
    assert_eq!(
        out.examples.len(),
        0,
        "dialect filter should drop MySqlOnly example for PG"
    );
}

#[test]
fn two_examples_in_one_pattern_both_surface() {
    // PatternBuilder::example called twice; both ResolvedExamples must appear
    // in QueryOutput.examples.
    let mut b = PatternBuilder::new("two_ex");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::Integer);
    b.from(t);
    b.project_column(c, t);
    b.example(
        "first",
        DialectSupport::Both,
        vec![ExampleCell {
            var: c,
            value: ExampleValue::Literal("1"),
        }],
    );
    b.example(
        "second",
        DialectSupport::Both,
        vec![ExampleCell {
            var: c,
            value: ExampleValue::Literal("2"),
        }],
    );
    let pattern = b.build();

    let mut reg = ConstraintRegistry::new();
    reg.register(pattern).expect("two_ex is a valid pattern");

    let config = GeneratorConfig {
        reuse_preference: 0.0,
        ..Default::default()
    };
    let mut generator = Generator::new_with_registry(Dialect::MySQL, config, reg);
    let mut rng = SmallRng::seed_from_u64(0x1234);
    let mut entropy = Entropy::new(&mut rng);
    let out = generator
        .generate_with_ddl(&mut entropy)
        .expect("generation should succeed");

    let first = out.examples.iter().find(|e| e.note == "first");
    let second = out.examples.iter().find(|e| e.note == "second");
    assert!(first.is_some(), "expected example with note 'first'");
    assert!(second.is_some(), "expected example with note 'second'");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|s| s.parse().ok()).unwrap_or(1)
    ))]

    #[test]
    fn arbitrary_example_resolves_every_cell(
        column_count in 1usize..4,
        param_count in 0usize..3,
        cell_choices in proptest::collection::vec(any::<usize>(), 1..6),
    ) {
        let mut b = PatternBuilder::new("propexample");
        let t = b.table();
        let cols: Vec<_> = (0..column_count).map(|_| {
            let c = b.column(t);
            b.column_type_class(c, TypeClass::Integer);
            c
        }).collect();
        b.from(t);
        for c in &cols {
            b.project_column(*c, t);
        }
        let params: Vec<_> = (0..param_count).map(|i| {
            b.where_param(cols[i % cols.len()], t, BinaryOperator::Equal)
        }).collect();

        let pool: Vec<_> = cols.iter().copied().chain(params.iter().copied()).collect();
        let mut cells = Vec::new();
        for choice in &cell_choices {
            if pool.is_empty() {
                continue;
            }
            let var = pool[choice % pool.len()];
            cells.push(ExampleCell { var, value: ExampleValue::Literal("1") });
        }
        prop_assume!(!cells.is_empty());
        let expected_count = cells.len();
        b.example("prop", DialectSupport::Both, cells);

        let pattern = b.build();
        let mut reg = ConstraintRegistry::new();
        reg.register(pattern).expect("propexample is a valid pattern");

        let config = GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut generator = Generator::new_with_registry(Dialect::MySQL, config, reg);
        let mut rng = SmallRng::seed_from_u64(0xAA);
        let mut entropy = Entropy::new(&mut rng);
        let out = generator
            .generate_with_ddl(&mut entropy)
            .map_err(|e| TestCaseError::fail(format!("generate_with_ddl failed: {e}")))?;

        let ex = out
            .examples
            .iter()
            .find(|e| e.note == "prop")
            .expect("expected to find example with note 'prop'");
        let total = ex.row_overrides.len() + ex.param_overrides.len();
        prop_assert_eq!(total, expected_count);
    }
}
