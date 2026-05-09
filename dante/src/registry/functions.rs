//! Scalar function patterns: coalesce, concat, substring, round, ifnull,
//! length, month, dayofweek, greatest, least.
//!
//! These patterns exercise Readyset's built-in function evaluation in the
//! dataflow expression layer.

use crate::constraint::{DialectSupport, ScalarFn, TypeClass};
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT COALESCE(t.c1, 0) FROM t
pub fn coalesce() -> Pattern {
    let mut b = PatternBuilder::new("coalesce");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_function(ScalarFn::Coalesce, vec![(c, t)]);
    b.tags(&["function"]);
    b.build()
}

/// SELECT IFNULL(t.c1, 0) FROM t (MySQL only)
pub fn ifnull() -> Pattern {
    let mut b = PatternBuilder::new("ifnull");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_function(ScalarFn::IfNull, vec![(c, t)]);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["function", "mysql_only"]);
    b.build()
}

/// SELECT CONCAT(t.c1, t.c2) FROM t
pub fn concat() -> Pattern {
    let mut b = PatternBuilder::new("concat");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.column_type_class(c1, TypeClass::String);
    b.column_type_class(c2, TypeClass::String);
    b.from(t);
    b.project_function(ScalarFn::Concat, vec![(c1, t), (c2, t)]);
    b.tags(&["function", "string"]);
    b.build()
}

/// SELECT SUBSTRING(t.c, 1, 10) FROM t
pub fn substring() -> Pattern {
    let mut b = PatternBuilder::new("substring");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::String);
    b.from(t);
    b.project_function(ScalarFn::Substring, vec![(c, t)]);
    b.tags(&["function", "string"]);
    b.build()
}

/// SELECT ROUND(t.c, 2) FROM t
///
/// Constrained to exact fixed-point types (DECIMAL/NUMERIC) only. Postgres has
/// no `round(integer, integer)` (qp:ncmgueclkkys) and ALSO no
/// `round(double precision, integer)` / `round(real, integer)` — only
/// `round(numeric, integer)` — so FLOAT/DOUBLE/REAL columns must be excluded
/// too, or PG rejects the query with 42883. (qp:kdsnjpxcfwda)
pub fn round() -> Pattern {
    let mut b = PatternBuilder::new("round");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::FixedPoint);
    b.from(t);
    b.project_function(ScalarFn::Round, vec![(c, t)]);
    b.tags(&["function", "numeric"]);
    b.build()
}

/// SELECT LENGTH(t.c) FROM t
pub fn length() -> Pattern {
    let mut b = PatternBuilder::new("length");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::String);
    b.from(t);
    b.project_function(ScalarFn::Length, vec![(c, t)]);
    b.tags(&["function", "string"]);
    b.build()
}

/// SELECT MONTH(t.c) FROM t (MySQL only)
pub fn month() -> Pattern {
    let mut b = PatternBuilder::new("month");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::DateTime);
    b.from(t);
    b.project_function(ScalarFn::Month, vec![(c, t)]);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["function", "datetime", "mysql_only"]);
    b.build()
}

/// SELECT DAYOFWEEK(t.c) FROM t (MySQL only)
pub fn dayofweek() -> Pattern {
    let mut b = PatternBuilder::new("dayofweek");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::DateTime);
    b.from(t);
    b.project_function(ScalarFn::DayOfWeek, vec![(c, t)]);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["function", "datetime", "mysql_only"]);
    b.build()
}

/// SELECT GREATEST(t.c1, t.c2) FROM t
pub fn greatest() -> Pattern {
    let mut b = PatternBuilder::new("greatest");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.column_type_class(c1, TypeClass::Numeric);
    b.column_type_class(c2, TypeClass::Numeric);
    b.from(t);
    b.project_function(ScalarFn::Greatest, vec![(c1, t), (c2, t)]);
    b.tags(&["function", "numeric"]);
    b.build()
}

/// SELECT UPPER(t.c) FROM t
pub fn upper() -> Pattern {
    let mut b = PatternBuilder::new("upper");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::String);
    b.from(t);
    b.project_function(ScalarFn::Upper, vec![(c, t)]);
    b.tags(&["function", "string"]);
    b.build()
}

/// SELECT LOWER(t.c) FROM t
pub fn lower() -> Pattern {
    let mut b = PatternBuilder::new("lower");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::String);
    b.from(t);
    b.project_function(ScalarFn::Lower, vec![(c, t)]);
    b.tags(&["function", "string"]);
    b.build()
}

/// SELECT UPPER(t.c1) FROM t WHERE t.c2 IN (?, ?, ?) — post-lookup Upper.
pub fn upper_in_list() -> Pattern {
    let mut b = PatternBuilder::new("upper_in_list");
    let t = b.table();
    let c_fn = b.column(t);
    let c_filter = b.column(t);
    b.column_type_class(c_fn, TypeClass::String);
    b.from(t);
    b.project_function(ScalarFn::Upper, vec![(c_fn, t)]);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["function", "string", "post_lookup"]);
    b.build()
}

/// SELECT LOWER(t.c1) FROM t WHERE t.c2 IN (?, ?, ?) — post-lookup Lower.
pub fn lower_in_list() -> Pattern {
    let mut b = PatternBuilder::new("lower_in_list");
    let t = b.table();
    let c_fn = b.column(t);
    let c_filter = b.column(t);
    b.column_type_class(c_fn, TypeClass::String);
    b.from(t);
    b.project_function(ScalarFn::Lower, vec![(c_fn, t)]);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["function", "string", "post_lookup"]);
    b.build()
}

/// SELECT SUBSTRING(t.c1, 1, 10) FROM t WHERE t.c2 IN (?, ?, ?) — post-lookup Substring.
pub fn substring_in_list() -> Pattern {
    let mut b = PatternBuilder::new("substring_in_list");
    let t = b.table();
    let c_fn = b.column(t);
    let c_filter = b.column(t);
    b.column_type_class(c_fn, TypeClass::String);
    b.from(t);
    b.project_function(ScalarFn::Substring, vec![(c_fn, t)]);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["function", "string", "post_lookup"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::test_util::resolve_pattern;

    #[test]
    fn coalesce_builds() {
        let p = coalesce();
        assert_eq!(p.name, "coalesce");
        assert!(p.tags.contains(&"function"));
    }

    #[test]
    fn coalesce_resolves() {
        let p = coalesce();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("COALESCE("),
            "expected COALESCE in sql: {sql}"
        );
    }

    #[test]
    fn ifnull_resolves() {
        let p = ifnull();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("IFNULL("),
            "expected IFNULL in sql: {sql}"
        );
    }

    #[test]
    fn concat_resolves() {
        let p = concat();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("CONCAT("),
            "expected CONCAT in sql: {sql}"
        );
    }

    #[test]
    fn substring_resolves() {
        let p = substring();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("SUBSTRING("),
            "expected SUBSTRING in sql: {sql}"
        );
    }

    #[test]
    fn round_resolves() {
        let p = round();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("ROUND("),
            "expected ROUND in sql: {sql}"
        );
    }

    #[test]
    fn length_resolves() {
        let p = length();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("LENGTH("),
            "expected LENGTH in sql: {sql}"
        );
    }

    #[test]
    fn month_resolves() {
        let p = month();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("MONTH("),
            "expected MONTH in sql: {sql}"
        );
    }

    #[test]
    fn dayofweek_resolves() {
        let p = dayofweek();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("DAYOFWEEK("),
            "expected DAYOFWEEK in sql: {sql}"
        );
    }

    #[test]
    fn greatest_resolves() {
        let p = greatest();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.to_uppercase().contains("GREATEST("),
            "expected GREATEST in sql: {sql}"
        );
    }

    // Postgres has no round(integer, integer) overload; only round(numeric,
    // integer) is valid. The round() pattern must constrain its column to a
    // non-integer numeric type so PG does not reject the generated query.
    // (qp:ncmgueclkkys)
    #[test]
    fn round_column_is_not_integer_type() {
        use readyset_sql::ast::SqlType;

        use crate::constraint::Constraint;

        let p = round();
        // The pattern must have a ColumnTypeClass constraint on the ROUND
        // column, and that type class must not match integer SQL types.
        let type_class = p
            .constraints
            .iter()
            .find_map(|c| match c {
                Constraint::ColumnTypeClass { type_class, .. } => Some(type_class.clone()),
                _ => None,
            })
            .expect("round() must have a ColumnTypeClass constraint");

        // PG's two-arg round only accepts `numeric`/`decimal` — NOT integer and
        // NOT float/real/double precision. `round(double precision, integer)`
        // and `round(real, integer)` both raise 42883. (qp:kdsnjpxcfwda)
        let rejected_types = [
            SqlType::Int(None),
            SqlType::BigInt(None),
            SqlType::SmallInt(None),
            SqlType::Float,
            SqlType::Double,
            SqlType::Real,
        ];
        for t in &rejected_types {
            assert!(
                !type_class.matches(t),
                "round() type class must not accept {t:?} (PG round(x, int) needs numeric)"
            );
        }
        // It must still accept fixed-point numeric types.
        assert!(type_class.matches(&SqlType::Decimal(10, 2)));
        assert!(type_class.matches(&SqlType::Numeric(Some((10, Some(2))))));
    }
}
