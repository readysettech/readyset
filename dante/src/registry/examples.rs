//! Example-pinned patterns: patterns that carry `Constraint::Example` cells
//! to exercise the bait pipeline end-to-end.
//!
//! These patterns produce deterministic probes (pinned row values + param
//! values) that the oracle can use to reproduce known divergences reliably,
//! rather than relying on random data to hit the interesting case.

use readyset_sql::ast::BinaryOperator;

use crate::constraint::{DialectSupport, ExampleCell, ExampleValue, TypeClass};
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t.c1, t.c2 FROM t WHERE t.c1 = ?  (MySQL-only)
///
/// First real consumer of `Constraint::Example`. Verifies the example-pinning
/// pipeline end-to-end: 2 row overrides (c1=8, c2=3) and 1 param override
/// (param=2) land in `QueryOutput.examples` after generation.
///
/// The bait name signals the future bug class: a `ProjectBinaryOp` shape
/// projecting `c1 / c2` lands later, at which point the pinned values
/// (8/3=2 in true arithmetic, 2 in MySQL int-div) become a divergence
/// witness. Until then, this pattern only exercises the example pipeline.
pub fn int_int_divide_bait() -> Pattern {
    let mut b = PatternBuilder::new("int_int_divide_bait");
    b.set_dialect_support(DialectSupport::MySqlOnly);

    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.column_type_class(c1, TypeClass::Integer);
    b.column_type_class(c2, TypeClass::Integer);
    b.from(t);
    b.project_column(c1, t);
    b.project_column(c2, t);
    let param = b.where_param(c1, t, BinaryOperator::Equal);

    b.example(
        "int/int divide: c1=8, c2=3, param=2",
        DialectSupport::MySqlOnly,
        vec![
            ExampleCell {
                var: c1,
                value: ExampleValue::Literal("8"),
            },
            ExampleCell {
                var: c2,
                value: ExampleValue::Literal("3"),
            },
            ExampleCell {
                var: param,
                value: ExampleValue::Literal("2"),
            },
        ],
    );

    b.tags(&["example", "filter", "parameter"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use readyset_sql::Dialect;

    use super::*;
    use crate::entropy::Entropy;
    use crate::generator::{ConstraintRegistry, Generator};
    use crate::state::GeneratorConfig;

    #[test]
    fn int_int_divide_bait_pattern_generates() {
        let mut reg = ConstraintRegistry::new();
        reg.register(int_int_divide_bait())
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
            .expect("should generate");

        let ex = out
            .examples
            .iter()
            .find(|e| e.note.contains("int/int divide"))
            .expect("int/int divide example must surface");
        assert_eq!(
            ex.row_overrides.len(),
            2,
            "expected 2 row overrides (c1, c2)"
        );
        assert_eq!(
            ex.param_overrides.len(),
            1,
            "expected 1 param override (param=2)"
        );
        assert_eq!(
            ex.param_overrides[0].placeholder_index, 0,
            "param override should reference placeholder 0"
        );
    }

    #[test]
    fn int_int_divide_bait_pattern_builds() {
        let p = int_int_divide_bait();
        assert_eq!(p.name, "int_int_divide_bait");
        assert!(p.tags.contains(&"example"));
        assert!(p.tags.contains(&"filter"));
        // table + c1 + c2 + param = 4 vars
        assert_eq!(p.num_vars(), 4);
    }
}
