//! Expression-evaluation patterns. These exercise the dataflow-expression
//! layer's binary-operator and cast surface -- the bug class catalogued in
//! `aidoc/projects/expression-type-coercion/promotion_audit.md`.
//!
//! All patterns in this module are tagged `expr_eval` so consumers can
//! restrict generation to expression shapes via
//! `SelectionFilter { required_tags: vec!["expr_eval"], .. }`.

use readyset_sql::ast::{BinaryOperator, SqlType};

use crate::constraint::{DialectSupport, ExampleCell, ExampleValue, TypeClass};
use crate::pattern::{Pattern, PatternBuilder};

/// Internal helper for `SELECT t.c1 <op> t.c2 FROM t` with both columns
/// pinned to the Integer class.
fn binary_op_int_int(name: &'static str, op: BinaryOperator) -> Pattern {
    let mut b = PatternBuilder::new(name);
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.column_type_class(c1, TypeClass::Integer);
    b.column_type_class(c2, TypeClass::Integer);
    b.from(t);
    b.project_binary_op((c1, t), op, (c2, t));
    b.tags(&["expr_eval", "binary_op", "arithmetic", "int_int"]);
    b.build()
}

pub fn binary_op_int_int_add() -> Pattern {
    binary_op_int_int("binary_op_int_int_add", BinaryOperator::Add)
}

pub fn binary_op_int_int_subtract() -> Pattern {
    binary_op_int_int("binary_op_int_int_subtract", BinaryOperator::Subtract)
}

pub fn binary_op_int_int_multiply() -> Pattern {
    binary_op_int_int("binary_op_int_int_multiply", BinaryOperator::Multiply)
}

/// Canonical MySQL `int / int -> DECIMAL` bug surface; the one originally
/// motivating the project (`promotion_audit.md` 100 MySQL Divide cells).
/// MySQL-only: PG int/int is integer division (no divergence with RS).
pub fn binary_op_int_int_divide() -> Pattern {
    let mut p = binary_op_int_int("binary_op_int_int_divide", BinaryOperator::Divide);
    p.dialect_support = DialectSupport::MySqlOnly;
    p
}

pub fn binary_op_int_int_modulo() -> Pattern {
    binary_op_int_int("binary_op_int_int_modulo", BinaryOperator::Modulo)
}

/// Internal helper for `SELECT t.c1 <op> t.c2 FROM t` with both columns
/// pinned to caller-chosen exact SQL types.
fn binary_op_exact_pair(
    name: &'static str,
    left_ty: SqlType,
    op: BinaryOperator,
    right_ty: SqlType,
    extra_tags: &'static [&'static str],
    dialect_support: DialectSupport,
) -> Pattern {
    let mut b = PatternBuilder::new(name);
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.column_type_class(c1, TypeClass::Exact(left_ty));
    b.column_type_class(c2, TypeClass::Exact(right_ty));
    b.from(t);
    b.project_binary_op((c1, t), op, (c2, t));
    let mut tags: Vec<&'static str> = vec!["expr_eval", "binary_op", "arithmetic"];
    tags.extend_from_slice(extra_tags);
    b.tags(&tags);
    b.set_dialect_support(dialect_support);
    b.build()
}

/// `SELECT t.c1 % t.c2 FROM t` with `c1: INT` (signed) and `c2: INT UNSIGNED`.
/// Reproduces the MySQL signed/unsigned Modulo sign-loss bug (25 cells in
/// the audit): the runtime coerces both to UnsignedBigInt so a negative
/// dividend's sign is lost.
pub fn binary_op_signed_unsigned_modulo() -> Pattern {
    binary_op_exact_pair(
        "binary_op_signed_unsigned_modulo",
        SqlType::Int(None),
        BinaryOperator::Modulo,
        SqlType::IntUnsigned(None),
        &["signedness", "mixed_type", "mysql_only"],
        DialectSupport::MySqlOnly,
    )
}

/// `SELECT t.c1 + t.c2 FROM t` with `c1: FLOAT` (f32) and `c2: DOUBLE` (f64).
/// Reproduces the PG Float x Double precision-drop bug (28 cells in the
/// audit): `(None, None)` operand coercion + the `arithmetic_operation!`
/// arm-5 fallback truncates both operands to f32 even though the table
/// declares Double.
pub fn binary_op_float_double_add() -> Pattern {
    binary_op_exact_pair(
        "binary_op_float_double_add",
        // SqlType::Real is f32 in PG (`SqlType::Float` maps to f64 via
        // `Dialect::float_type` -- bare `float` defaults to double-precision
        // in PG). To get the audit's `DfType::Float x DfType::Double` pair
        // we need REAL (f32) for the left operand.
        SqlType::Real,
        BinaryOperator::Add,
        SqlType::Double,
        &["mixed_precision", "float", "postgres_only"],
        DialectSupport::PostgresOnly,
    )
}

pub fn binary_op_float_double_subtract() -> Pattern {
    binary_op_exact_pair(
        "binary_op_float_double_subtract",
        SqlType::Real,
        BinaryOperator::Subtract,
        SqlType::Double,
        &["mixed_precision", "float", "postgres_only"],
        DialectSupport::PostgresOnly,
    )
}

pub fn binary_op_float_double_multiply() -> Pattern {
    binary_op_exact_pair(
        "binary_op_float_double_multiply",
        SqlType::Real,
        BinaryOperator::Multiply,
        SqlType::Double,
        &["mixed_precision", "float", "postgres_only"],
        DialectSupport::PostgresOnly,
    )
}

pub fn binary_op_float_double_divide() -> Pattern {
    binary_op_exact_pair(
        "binary_op_float_double_divide",
        SqlType::Real,
        BinaryOperator::Divide,
        SqlType::Double,
        &["mixed_precision", "float", "postgres_only"],
        DialectSupport::PostgresOnly,
    )
}

// PG does not provide `%` for floating-point types; only the 4 closed ops (+, -, *, /) are tested.

/// Helper for `SELECT CAST(c AS <target>) FROM t` with the source column
/// pinned to caller-chosen exact SQL type.
fn cast_exact(
    name: &'static str,
    source_ty: SqlType,
    target_ty: SqlType,
    extra_tags: &'static [&'static str],
    dialect_support: DialectSupport,
) -> Pattern {
    let mut b = PatternBuilder::new(name);
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::Exact(source_ty));
    b.from(t);
    b.project_cast(c, t, target_ty);
    let mut tags: Vec<&'static str> = vec!["expr_eval", "cast"];
    tags.extend_from_slice(extra_tags);
    b.tags(&tags);
    b.set_dialect_support(dialect_support);
    b.build()
}

/// `SELECT CAST(t.c AS DECIMAL(5, 2)) FROM t` with `c: DECIMAL(10, 4)`.
/// Exercises Numeric scale-narrowing -- the audit's `Numeric(p1,s1) ->
/// Numeric(p2,s2)` for `s1 != s2` conjectured family, parallel to the
/// confirmed `Time(N) -> Time(M)` cells (whose `SqlType` cannot express
/// subsecond precision today; tracked as ENG-1832).
pub fn cast_decimal_widen_to_narrow() -> Pattern {
    cast_exact(
        "cast_decimal_widen_to_narrow",
        SqlType::Decimal(10, 4),
        SqlType::Decimal(5, 2),
        &["numeric_scale"],
        DialectSupport::Both,
    )
}

/// `SELECT CAST(t.c AS BIGINT) FROM t` with `c: DECIMAL(10, 4)`.
/// Exercises decimal-to-int truncation; surfaces the divide between MySQL
/// (round-half-up) and PG (round-half-even) rules under fractional values.
/// PostgreSQL-only because MySQL requires `CAST(... AS SIGNED INTEGER)`
/// syntax for integer casts, which the existing `SqlType` rendering does
/// not emit.
pub fn cast_decimal_to_bigint() -> Pattern {
    cast_exact(
        "cast_decimal_to_bigint",
        SqlType::Decimal(10, 4),
        SqlType::BigInt(None),
        &["numeric_to_int", "postgres_only"],
        DialectSupport::PostgresOnly,
    )
}

/// `SELECT lookup_col, c1, c2 FROM t WHERE lookup_col = (c1 / c2)` --
/// surfaces the join/lookup-key coercion bug class. `lookup_col` is a
/// DECIMAL column; `c1`/`c2` are INT columns. RS computes `c1/c2` via
/// integer division (the audit's int_int_divide bug), then coerces the
/// `Int` result to `Numeric` for comparison against `lookup_col`, which
/// matches a `2.0000` row that MySQL's DECIMAL division (`2.6667`) would
/// not match. Bug-bait rows are chosen so:
///   - row (lookup_col=2.0000, c1=8, c2=3): RS includes (its 8/3=2 ->
///     2.0000 matches), MySQL excludes (its 8/3=2.6667 != 2.0000).
///   - row (lookup_col=2.6667, c1=8, c2=3): MySQL includes, RS excludes.
///   - row (lookup_col=1.0000, c1=4, c2=4): both include (8/8=1=1).
///
/// Resulting result sets diverge per row; the harness flags the mismatch.
/// MySQL-only: PG int/int is integer division so the bug does not manifest.
pub fn where_lookup_decimal_eq_int_div_int() -> Pattern {
    let mut b = PatternBuilder::new("where_lookup_decimal_eq_int_div_int");
    let t = b.table();
    let lookup = b.column(t);
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.column_type_class(lookup, TypeClass::Exact(SqlType::Decimal(10, 4)));
    // Use the broad `Integer` class for the operands so the resolver
    // synthesizes fresh columns rather than reusing the auto-allocated PK
    // (which is `SqlType::Int` and would collapse all three Integer
    // variables onto `c0`, erasing the divide). The Integer class still
    // produces signed-int columns suitable for the int x int divide bug.
    b.column_type_class(c1, TypeClass::Integer);
    b.column_type_class(c2, TypeClass::Integer);
    b.from(t);
    // Project all 3 columns so the SELECT returns something comparable.
    b.project_column(lookup, t);
    b.project_column(c1, t);
    b.project_column(c2, t);
    b.where_lookup_binary_op(
        (lookup, t),
        BinaryOperator::Equal,
        (c1, t),
        BinaryOperator::Divide,
        (c2, t),
    );
    b.tags(&[
        "expr_eval",
        "lookup_key",
        "where_filter",
        "int_int",
        "join_condition_class",
    ]);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    // Three example-pinned rows surface the lookup-key coercion class:
    //   - RS-includes-only: lookup=2.0000 matches RS's Int(8/3)=2 coerced
    //     to Numeric(2.0000); MySQL's 8/3=2.6667 does not match.
    //   - MySQL-includes-only: lookup=2.6667 is the MySQL DECIMAL result;
    //     RS's Int(2)->Numeric(2.0000) does not match.
    //   - Both-match: lookup=1.0000, c1=c2=4 -> 4/4=1 on both engines.
    b.example(
        "lookup=2.0000, c1=8, c2=3 -- RS-only match (int/int divide coerces to 2.0000)",
        DialectSupport::MySqlOnly,
        vec![
            ExampleCell {
                var: lookup,
                value: ExampleValue::Literal("2.0000"),
            },
            ExampleCell {
                var: c1,
                value: ExampleValue::Literal("8"),
            },
            ExampleCell {
                var: c2,
                value: ExampleValue::Literal("3"),
            },
        ],
    );
    b.example(
        "lookup=2.6667, c1=8, c2=3 -- MySQL-only match (DECIMAL division result)",
        DialectSupport::MySqlOnly,
        vec![
            ExampleCell {
                var: lookup,
                value: ExampleValue::Literal("2.6667"),
            },
            ExampleCell {
                var: c1,
                value: ExampleValue::Literal("8"),
            },
            ExampleCell {
                var: c2,
                value: ExampleValue::Literal("3"),
            },
        ],
    );
    b.example(
        "lookup=1.0000, c1=4, c2=4 -- both engines match (4/4=1)",
        DialectSupport::MySqlOnly,
        vec![
            ExampleCell {
                var: lookup,
                value: ExampleValue::Literal("1.0000"),
            },
            ExampleCell {
                var: c1,
                value: ExampleValue::Literal("4"),
            },
            ExampleCell {
                var: c2,
                value: ExampleValue::Literal("4"),
            },
        ],
    );
    b.build()
}

/// `SELECT CAST(t.c AS DOUBLE) FROM t` with `c: REAL` (f32). Surfaces the
/// f32->f64 widening: every f32 value is exactly representable as f64, so
/// the value should be identical -- but the RS path may still emit
/// `DfValue::Float` rather than `DfValue::Double` if the projection
/// inference is off. Uses `SqlType::Real` (f32 on both dialects) so the
/// cast is non-trivial on both MySQL and PG.
pub fn cast_float_to_double() -> Pattern {
    cast_exact(
        "cast_float_to_double",
        SqlType::Real,
        SqlType::Double,
        &["float_widen"],
        DialectSupport::Both,
    )
}
#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::test_util::resolve_pattern;

    #[test]
    fn binary_op_int_int_add_builds() {
        let p = binary_op_int_int_add();
        assert_eq!(p.name, "binary_op_int_int_add");
        assert!(p.tags.contains(&"expr_eval"));
        assert!(p.tags.contains(&"binary_op"));
        assert!(p.tags.contains(&"arithmetic"));
        assert!(p.tags.contains(&"int_int"));
    }

    #[test]
    fn binary_op_int_int_add_resolves_mysql() {
        let sql = resolve_pattern(&binary_op_int_int_add(), Dialect::MySQL);
        assert!(sql.contains(" + "), "expected ` + ` in sql: {sql}");
    }

    #[test]
    fn binary_op_int_int_add_resolves_psql() {
        let sql = resolve_pattern(&binary_op_int_int_add(), Dialect::PostgreSQL);
        assert!(sql.contains(" + "), "expected ` + ` in sql: {sql}");
    }

    #[test]
    fn binary_op_int_int_subtract_resolves() {
        let sql = resolve_pattern(&binary_op_int_int_subtract(), Dialect::MySQL);
        assert!(sql.contains(" - "), "expected ` - ` in sql: {sql}");
    }

    #[test]
    fn binary_op_int_int_subtract_resolves_psql() {
        let sql = resolve_pattern(&binary_op_int_int_subtract(), Dialect::PostgreSQL);
        assert!(sql.contains(" - "), "expected ` - ` in sql: {sql}");
    }

    #[test]
    fn binary_op_int_int_multiply_resolves() {
        let sql = resolve_pattern(&binary_op_int_int_multiply(), Dialect::MySQL);
        assert!(sql.contains(" * "), "expected ` * ` in sql: {sql}");
    }

    #[test]
    fn binary_op_int_int_multiply_resolves_psql() {
        let sql = resolve_pattern(&binary_op_int_int_multiply(), Dialect::PostgreSQL);
        assert!(sql.contains(" * "), "expected ` * ` in sql: {sql}");
    }

    #[test]
    fn binary_op_int_int_divide_resolves() {
        let p = binary_op_int_int_divide();
        assert_eq!(
            p.dialect_support,
            DialectSupport::MySqlOnly,
            "divide is mysql-only: PG int/int is integer division"
        );
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains(" / "), "expected ` / ` in sql: {sql}");
    }

    #[test]
    fn binary_op_int_int_modulo_resolves() {
        let sql = resolve_pattern(&binary_op_int_int_modulo(), Dialect::MySQL);
        assert!(sql.contains(" % "), "expected ` % ` in sql: {sql}");
    }

    #[test]
    fn binary_op_int_int_modulo_resolves_psql() {
        let sql = resolve_pattern(&binary_op_int_int_modulo(), Dialect::PostgreSQL);
        assert!(sql.contains(" % "), "expected ` % ` in sql: {sql}");
    }

    #[test]
    fn binary_op_signed_unsigned_modulo_resolves_mysql() {
        let p = binary_op_signed_unsigned_modulo();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains(" % "), "expected ` % ` in sql: {sql}");
        assert!(p.tags.contains(&"signedness"));
        assert!(p.tags.contains(&"mysql_only"));
    }

    #[test]
    fn binary_op_float_double_add_resolves_psql() {
        let p = binary_op_float_double_add();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains(" + "), "expected ` + ` in sql: {sql}");
        assert!(p.tags.contains(&"mixed_precision"));
    }

    #[test]
    fn binary_op_float_double_subtract_resolves_psql() {
        let p = binary_op_float_double_subtract();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains(" - "), "expected ` - ` in sql: {sql}");
    }

    #[test]
    fn binary_op_float_double_multiply_resolves_psql() {
        let p = binary_op_float_double_multiply();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains(" * "), "expected ` * ` in sql: {sql}");
    }

    #[test]
    fn binary_op_float_double_divide_resolves_psql() {
        let p = binary_op_float_double_divide();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains(" / "), "expected ` / ` in sql: {sql}");
    }

    #[test]
    fn cast_decimal_widen_to_narrow_resolves() {
        let p = cast_decimal_widen_to_narrow();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("CAST("),
            "expected CAST(...) in sql: {sql}"
        );
        assert!(p.tags.contains(&"cast"));
        assert!(p.tags.contains(&"numeric_scale"));
    }

    #[test]
    fn cast_decimal_widen_to_narrow_resolves_psql() {
        let p = cast_decimal_widen_to_narrow();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.to_uppercase().contains("CAST("),
            "expected CAST(...) in sql: {sql}"
        );
    }

    #[test]
    fn cast_decimal_to_bigint_resolves() {
        let p = cast_decimal_to_bigint();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.to_uppercase().contains("CAST("),
            "expected CAST(...) in sql: {sql}"
        );
        assert!(p.tags.contains(&"numeric_to_int"));
    }

    #[test]
    fn cast_float_to_double_resolves() {
        let p = cast_float_to_double();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("CAST("),
            "expected CAST(...) in sql (mysql): {sql}"
        );
    }

    #[test]
    fn cast_float_to_double_resolves_psql() {
        let p = cast_float_to_double();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.to_uppercase().contains("CAST("),
            "expected CAST(...) in sql (psql): {sql}"
        );
    }

    #[test]
    fn where_lookup_decimal_eq_int_div_int_resolves() {
        let p = where_lookup_decimal_eq_int_div_int();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains(" / "), "expected ` / ` in sql: {sql}");
        assert!(
            sql.to_uppercase().contains("WHERE"),
            "expected WHERE clause in sql: {sql}"
        );
        assert!(p.tags.contains(&"lookup_key"));
        let example_count = p
            .constraints
            .iter()
            .filter(|c| matches!(c, crate::constraint::Constraint::Example { .. }))
            .count();
        assert_eq!(example_count, 3, "expected 3 pinned examples");
    }

    #[test]
    fn where_lookup_decimal_eq_int_div_int_is_mysql_only() {
        let p = where_lookup_decimal_eq_int_div_int();
        assert_eq!(
            p.dialect_support,
            DialectSupport::MySqlOnly,
            "pattern is mysql-only: PG int/int is integer division, bug does not manifest"
        );
    }

    #[test]
    fn where_lookup_decimal_eq_int_div_int_example_cells_are_mysql_only() {
        // Example cells must carry the same dialect restriction as the pattern.
        // A MySqlOnly pattern with Both-tagged cells would be a landmine: if the
        // pattern is ever widened to Both, PG would pick up the examples without
        // any indication that they were written for MySQL semantics only.
        let p = where_lookup_decimal_eq_int_div_int();
        for c in &p.constraints {
            if let crate::constraint::Constraint::Example { dialect, .. } = c {
                assert_eq!(
                    *dialect,
                    DialectSupport::MySqlOnly,
                    "example cell dialect must match pattern's MySqlOnly restriction"
                );
            }
        }
    }
}
