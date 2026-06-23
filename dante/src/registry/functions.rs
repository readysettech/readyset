//! Scalar function patterns: coalesce, concat, substring, round, ifnull,
//! length, month, dayofweek, greatest, least.
//!
//! These patterns exercise Readyset's built-in function evaluation in the
//! dataflow expression layer.

use readyset_sql::ast::SqlType;

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
    b.tags(&["function"]);
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
    b.tags(&["function", "datetime"]);
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
    b.tags(&["function", "datetime"]);
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

/// Defines single-projection scalar-function patterns: `SELECT f(t.c0[, t.c1]) FROM t`.
///
/// Each row is `name => (ScalarFn, TypeClass, arity, DialectSupport, [tags])` and
/// expands to a `pub fn name() -> Pattern` equivalent to the hand-written builders
/// above: one table with `arity` columns all constrained to `TypeClass`, projected
/// through `ScalarFn`. The literal arguments each function takes (e.g. the `'day'`
/// in `DATE_TRUNC`) are supplied by the resolver, not the pattern.
macro_rules! scalar_fn_patterns {
    ($(
        $(#[doc = $doc:literal])*
        $name:ident => ($func:expr, $type_class:expr, $arity:literal, $dialect:expr, [$($tag:literal),* $(,)?])
    ),* $(,)?) => {
        $(
            $(#[doc = $doc])*
            pub fn $name() -> Pattern {
                let mut b = PatternBuilder::new(stringify!($name));
                let t = b.table();
                let cols: Vec<_> = (0..$arity)
                    .map(|_| {
                        let c = b.column(t);
                        b.column_type_class(c, $type_class);
                        (c, t)
                    })
                    .collect();
                b.from(t);
                b.project_function($func, cols);
                b.set_dialect_support($dialect);
                b.tags(&[$($tag),*]);
                b.build()
            }
        )*
    };
}

scalar_fn_patterns! {
    /// SELECT ST_AsText(t.c0) FROM t — geometry bytes to WKT text (PostGIS).
    st_astext => (ScalarFn::StAsText, TypeClass::Geometry, 1, DialectSupport::PostgresOnly, ["function", "spatial"]),
    /// SELECT ST_AsEWKT(t.c0) FROM t — PostGIS-only; EWKT with SRID prefix.
    st_asewkt => (ScalarFn::StAsEwkt, TypeClass::Geometry, 1, DialectSupport::PostgresOnly, ["function", "spatial"]),
    /// SELECT CONCAT_WS(',', t.c0, t.c1) FROM t
    concat_ws => (ScalarFn::ConcatWs, TypeClass::String, 2, DialectSupport::Both, ["function", "string"]),
    /// SELECT LEAST(t.c0, t.c1) FROM t
    least => (ScalarFn::Least, TypeClass::Numeric, 2, DialectSupport::Both, ["function", "numeric"]),
    /// SELECT ASCII(t.c0) FROM t
    ascii => (ScalarFn::Ascii, TypeClass::String, 1, DialectSupport::Both, ["function", "string"]),
    /// SELECT SPLIT_PART(t.c0, ',', 1) FROM t (PostgreSQL only)
    split_part => (ScalarFn::SplitPart, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "string"]),
    /// SELECT DATE_TRUNC('day', t.c0) FROM t (PostgreSQL only)
    date_trunc => (ScalarFn::DateTrunc, TypeClass::DateTime, 1, DialectSupport::PostgresOnly, ["function", "datetime"]),
    /// SELECT JSON_OBJECT('k', t.c0) FROM t (MySQL only)
    json_object => (ScalarFn::JsonObject, TypeClass::String, 1, DialectSupport::MySqlOnly, ["function", "json"]),
    /// SELECT JSON_QUOTE(t.c0) FROM t (MySQL only)
    json_quote => (ScalarFn::JsonQuote, TypeClass::String, 1, DialectSupport::MySqlOnly, ["function", "json"]),
    /// SELECT JSON_OVERLAPS(t.c0, '[]') FROM t (MySQL only)
    json_overlaps => (ScalarFn::JsonOverlaps, TypeClass::Exact(SqlType::Json), 1, DialectSupport::MySqlOnly, ["function", "json"]),
    /// SELECT JSON_VALID(t.c0) FROM t (MySQL only)
    json_valid => (ScalarFn::JsonValid, TypeClass::String, 1, DialectSupport::MySqlOnly, ["function", "json"]),
    /// SELECT JSON_DEPTH(t.c0) FROM t (MySQL only)
    json_depth => (ScalarFn::JsonDepth, TypeClass::Exact(SqlType::Json), 1, DialectSupport::MySqlOnly, ["function", "json"]),
    /// SELECT json_build_object('k', t.c0) FROM t (PostgreSQL only)
    json_build_object => (ScalarFn::JsonBuildObject, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT jsonb_build_object('k', t.c0) FROM t (PostgreSQL only)
    jsonb_build_object => (ScalarFn::JsonbBuildObject, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT json_strip_nulls(json_build_object('k', t.c0)) FROM t (PostgreSQL only)
    json_strip_nulls => (ScalarFn::JsonStripNulls, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT json_typeof(json_build_object('k', t.c0)) FROM t (PostgreSQL only)
    json_typeof => (ScalarFn::JsonTypeof, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT json_extract_path_text(json_build_object('k', t.c0), 'k') FROM t (PostgreSQL only)
    json_extract_path_text => (ScalarFn::JsonExtractPathText, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT DATE_FORMAT(t.c0, '%Y-%m-%d') FROM t (MySQL only)
    date_format => (ScalarFn::DateFormat, TypeClass::DateTime, 1, DialectSupport::MySqlOnly, ["function", "datetime"]),
    /// SELECT TIMEDIFF(t.c0, t.c1) FROM t (MySQL only)
    timediff => (ScalarFn::TimeDiff, TypeClass::DateTime, 2, DialectSupport::MySqlOnly, ["function", "datetime"]),
    /// SELECT ADDTIME(t.c0, t.c1) FROM t (MySQL only)
    addtime => (ScalarFn::AddTime, TypeClass::DateTime, 2, DialectSupport::MySqlOnly, ["function", "datetime"]),
    /// SELECT CONVERT_TZ(t.c0, '+00:00', '+05:30') FROM t (MySQL only)
    convert_tz => (ScalarFn::ConvertTz, TypeClass::DateTime, 1, DialectSupport::MySqlOnly, ["function", "datetime"]),
    /// SELECT HEX(t.c0) FROM t (MySQL only)
    hex => (ScalarFn::Hex, TypeClass::Integer, 1, DialectSupport::MySqlOnly, ["function", "string"]),
    /// SELECT jsonb_strip_nulls(jsonb_build_object('k', t.c0)) FROM t (PostgreSQL only)
    jsonb_strip_nulls => (ScalarFn::JsonbStripNulls, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT jsonb_pretty(jsonb_build_object('k', t.c0)) FROM t (PostgreSQL only)
    jsonb_pretty => (ScalarFn::JsonbPretty, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT jsonb_extract_path(jsonb_build_object('k', t.c0), 'k') FROM t (PostgreSQL only)
    jsonb_extract_path => (ScalarFn::JsonbExtractPath, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT jsonb_set(jsonb_build_object('k', t.c0), '{k}', '"new"') FROM t (PostgreSQL only)
    jsonb_set => (ScalarFn::JsonbSet, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT jsonb_set_lax(jsonb_build_object('k', t.c0), '{k}', '"new"') FROM t (PostgreSQL only)
    jsonb_set_lax => (ScalarFn::JsonbSetLax, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT jsonb_insert(jsonb_build_object('k', t.c0), '{k}', '"added"') FROM t (PostgreSQL only)
    jsonb_insert => (ScalarFn::JsonbInsert, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT json_array_length(json_build_array(t.c0)) FROM t (PostgreSQL only)
    json_array_length => (ScalarFn::JsonArrayLength, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
    /// SELECT jsonb_array_length(jsonb_build_array(t.c0)) FROM t (PostgreSQL only)
    jsonb_array_length => (ScalarFn::JsonbArrayLength, TypeClass::String, 1, DialectSupport::PostgresOnly, ["function", "json"]),
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

    /// (pattern builder, dialect to resolve under, expected upper-cased SQL fragment).
    type ResolveCase = (fn() -> Pattern, Dialect, &'static str);

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

    // Each generated single-projection scalar pattern resolves to SQL containing
    // its function call. The needle is upper-cased to match regardless of how the
    // dialect renders the function name.
    #[test]
    fn scalar_fn_patterns_resolve() {
        use Dialect::{MySQL, PostgreSQL};

        let cases: &[ResolveCase] = &[
            (concat_ws, MySQL, "CONCAT_WS("),
            (least, MySQL, "LEAST("),
            (ascii, MySQL, "ASCII("),
            (split_part, PostgreSQL, "SPLIT_PART("),
            (date_trunc, PostgreSQL, "DATE_TRUNC("),
            (json_object, MySQL, "JSON_OBJECT("),
            (json_quote, MySQL, "JSON_QUOTE("),
            (json_overlaps, MySQL, "JSON_OVERLAPS("),
            (json_valid, MySQL, "JSON_VALID("),
            (json_depth, MySQL, "JSON_DEPTH("),
            (json_build_object, PostgreSQL, "JSON_BUILD_OBJECT("),
            (jsonb_build_object, PostgreSQL, "JSONB_BUILD_OBJECT("),
            (json_strip_nulls, PostgreSQL, "JSON_STRIP_NULLS("),
        ];
        for (build, dialect, needle) in cases {
            let sql = resolve_pattern(&build(), *dialect).to_uppercase();
            assert!(sql.contains(needle), "expected {needle} in sql: {sql}");
        }
    }

    // MySQL-only datetime and string scalar functions added in this batch.
    #[test]
    fn mysql_scalar_fn_patterns_resolve() {
        let cases: &[ResolveCase] = &[
            (date_format, Dialect::MySQL, "DATE_FORMAT("),
            (timediff, Dialect::MySQL, "TIMEDIFF("),
            (addtime, Dialect::MySQL, "ADDTIME("),
            (convert_tz, Dialect::MySQL, "CONVERT_TZ("),
            (hex, Dialect::MySQL, "HEX("),
        ];
        for (build, dialect, needle) in cases {
            let sql = resolve_pattern(&build(), *dialect).to_uppercase();
            assert!(sql.contains(needle), "expected {needle} in sql: {sql}");
        }
    }

    #[test]
    fn mysql_scalar_fns_are_mysql_only() {
        use crate::constraint::DialectSupport;
        for (name, p) in [
            ("date_format", date_format()),
            ("timediff", timediff()),
            ("addtime", addtime()),
            ("convert_tz", convert_tz()),
            ("hex", hex()),
        ] {
            let support = p.dialect_support;
            assert_eq!(
                support,
                DialectSupport::MySqlOnly,
                "{name} must be MySqlOnly"
            );
        }
    }

    #[test]
    fn mysql_scalar_fns_tags() {
        let datetime_fns = [
            ("date_format", date_format()),
            ("timediff", timediff()),
            ("addtime", addtime()),
            ("convert_tz", convert_tz()),
        ];
        for (name, p) in &datetime_fns {
            assert!(
                p.tags.contains(&"function"),
                "{name} must have 'function' tag"
            );
            assert!(
                p.tags.contains(&"datetime"),
                "{name} must have 'datetime' tag"
            );
        }
        let hex_p = hex();
        assert!(
            hex_p.tags.contains(&"function"),
            "hex must have 'function' tag"
        );
        assert!(hex_p.tags.contains(&"string"), "hex must have 'string' tag");
    }

    // PG JSON functions that require a json-typed input. The resolver wraps the
    // VARCHAR column in json_build_object('k', col) to produce valid JSON, then
    // passes that as the argument.
    #[test]
    fn pg_json_scalar_fns_resolve() {
        let cases: &[ResolveCase] = &[
            (json_typeof, Dialect::PostgreSQL, "JSON_TYPEOF("),
            (
                json_extract_path_text,
                Dialect::PostgreSQL,
                "JSON_EXTRACT_PATH_TEXT(",
            ),
        ];
        for (build, dialect, needle) in cases {
            let sql = resolve_pattern(&build(), *dialect).to_uppercase();
            assert!(sql.contains(needle), "expected {needle} in sql: {sql}");
            // The resolver must wrap the column in json_build_object so PG
            // receives a json-typed argument (not raw VARCHAR).
            assert!(
                sql.contains("JSON_BUILD_OBJECT("),
                "expected JSON_BUILD_OBJECT( wrapper in sql: {sql}"
            );
        }
    }

    // json_strip_nulls(json val) requires a json-typed argument; PG has no
    // implicit varchar->json cast. The resolver must wrap the column in
    // json_build_object('k', col) so the argument is valid JSON.
    #[test]
    fn json_strip_nulls_wraps_arg_in_json_build_object() {
        let sql = resolve_pattern(&json_strip_nulls(), Dialect::PostgreSQL).to_uppercase();
        assert!(
            sql.contains("JSON_STRIP_NULLS("),
            "expected JSON_STRIP_NULLS( in sql: {sql}"
        );
        assert!(
            sql.contains("JSON_STRIP_NULLS(JSON_BUILD_OBJECT("),
            "json_strip_nulls must wrap its argument in json_build_object; got: {sql}"
        );
    }

    #[test]
    fn pg_json_scalar_fns_are_postgres_only() {
        use crate::constraint::DialectSupport;
        for (name, p) in [
            ("json_typeof", json_typeof()),
            ("json_extract_path_text", json_extract_path_text()),
        ] {
            assert_eq!(
                p.dialect_support,
                DialectSupport::PostgresOnly,
                "{name} must be PostgresOnly"
            );
        }
    }

    #[test]
    fn pg_json_scalar_fns_tags() {
        for (name, p) in [
            ("json_typeof", json_typeof()),
            ("json_extract_path_text", json_extract_path_text()),
        ] {
            assert!(
                p.tags.contains(&"function"),
                "{name} must have 'function' tag"
            );
            assert!(p.tags.contains(&"json"), "{name} must have 'json' tag");
        }
    }

    // PG jsonb scalar functions that require a jsonb-typed input. The resolver
    // wraps the VARCHAR column in jsonb_build_object('k', col) to produce
    // valid JSONB, then passes that as the argument.
    #[test]
    fn pg_jsonb_scalar_fns_resolve() {
        let cases: &[ResolveCase] = &[
            (jsonb_strip_nulls, Dialect::PostgreSQL, "JSONB_STRIP_NULLS("),
            (jsonb_pretty, Dialect::PostgreSQL, "JSONB_PRETTY("),
            (
                jsonb_extract_path,
                Dialect::PostgreSQL,
                "JSONB_EXTRACT_PATH(",
            ),
        ];
        for (build, dialect, needle) in cases {
            let sql = resolve_pattern(&build(), *dialect).to_uppercase();
            assert!(sql.contains(needle), "expected {needle} in sql: {sql}");
            assert!(
                sql.contains("JSONB_BUILD_OBJECT("),
                "expected JSONB_BUILD_OBJECT( wrapper in sql: {sql}"
            );
        }
    }

    #[test]
    fn pg_jsonb_scalar_fns_are_postgres_only() {
        use crate::constraint::DialectSupport;
        for (name, p) in [
            ("jsonb_strip_nulls", jsonb_strip_nulls()),
            ("jsonb_pretty", jsonb_pretty()),
            ("jsonb_extract_path", jsonb_extract_path()),
        ] {
            assert_eq!(
                p.dialect_support,
                DialectSupport::PostgresOnly,
                "{name} must be PostgresOnly"
            );
        }
    }

    #[test]
    fn pg_jsonb_scalar_fns_tags() {
        for (name, p) in [
            ("jsonb_strip_nulls", jsonb_strip_nulls()),
            ("jsonb_pretty", jsonb_pretty()),
            ("jsonb_extract_path", jsonb_extract_path()),
        ] {
            assert!(
                p.tags.contains(&"function"),
                "{name} must have 'function' tag"
            );
            assert!(p.tags.contains(&"json"), "{name} must have 'json' tag");
        }
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

    // json_depth and json_overlaps require valid JSON input from MySQL — random
    // strings cause ERROR 22032 and the query never caches. Both patterns must
    // constrain their column to SqlType::Json so the data generator emits valid
    // JSON values instead.
    #[test]
    fn json_depth_column_is_json_type() {
        use readyset_sql::ast::SqlType;

        use crate::constraint::Constraint;

        let p = json_depth();
        let type_class = p
            .constraints
            .iter()
            .find_map(|c| match c {
                Constraint::ColumnTypeClass { type_class, .. } => Some(type_class.clone()),
                _ => None,
            })
            .expect("json_depth() must have a ColumnTypeClass constraint");

        assert!(
            type_class.matches(&SqlType::Json),
            "json_depth() column must accept SqlType::Json, got {type_class:?}"
        );
        assert!(
            !type_class.matches(&SqlType::VarChar(Some(255))),
            "json_depth() column must not accept VARCHAR (random strings are invalid JSON)"
        );
    }

    #[test]
    fn json_overlaps_column_is_json_type() {
        use readyset_sql::ast::SqlType;

        use crate::constraint::Constraint;

        let p = json_overlaps();
        let type_class = p
            .constraints
            .iter()
            .find_map(|c| match c {
                Constraint::ColumnTypeClass { type_class, .. } => Some(type_class.clone()),
                _ => None,
            })
            .expect("json_overlaps() must have a ColumnTypeClass constraint");

        assert!(
            type_class.matches(&SqlType::Json),
            "json_overlaps() column must accept SqlType::Json, got {type_class:?}"
        );
        assert!(
            !type_class.matches(&SqlType::VarChar(Some(255))),
            "json_overlaps() column must not accept VARCHAR (random strings are invalid JSON)"
        );
    }

    // jsonb_set, jsonb_set_lax, jsonb_insert wrap the column in
    // jsonb_build_object and pass a fixed text-array path literal so PG
    // receives a jsonb-typed first arg and a text[] path second arg.
    #[test]
    fn jsonb_set_resolves() {
        let sql = resolve_pattern(&jsonb_set(), Dialect::PostgreSQL).to_uppercase();
        assert!(
            sql.contains("JSONB_SET("),
            "expected JSONB_SET( in sql: {sql}"
        );
        assert!(
            sql.contains("JSONB_BUILD_OBJECT("),
            "expected JSONB_BUILD_OBJECT( wrapper in sql: {sql}"
        );
    }

    #[test]
    fn jsonb_set_lax_resolves() {
        let sql = resolve_pattern(&jsonb_set_lax(), Dialect::PostgreSQL).to_uppercase();
        assert!(
            sql.contains("JSONB_SET_LAX("),
            "expected JSONB_SET_LAX( in sql: {sql}"
        );
        assert!(
            sql.contains("JSONB_BUILD_OBJECT("),
            "expected JSONB_BUILD_OBJECT( wrapper in sql: {sql}"
        );
    }

    #[test]
    fn jsonb_insert_resolves() {
        let sql = resolve_pattern(&jsonb_insert(), Dialect::PostgreSQL).to_uppercase();
        assert!(
            sql.contains("JSONB_INSERT("),
            "expected JSONB_INSERT( in sql: {sql}"
        );
        assert!(
            sql.contains("JSONB_BUILD_OBJECT("),
            "expected JSONB_BUILD_OBJECT( wrapper in sql: {sql}"
        );
    }

    #[test]
    fn jsonb_set_is_postgres_only() {
        use crate::constraint::DialectSupport;
        for (name, p) in [
            ("jsonb_set", jsonb_set()),
            ("jsonb_set_lax", jsonb_set_lax()),
            ("jsonb_insert", jsonb_insert()),
        ] {
            assert_eq!(
                p.dialect_support,
                DialectSupport::PostgresOnly,
                "{name} must be PostgresOnly"
            );
        }
    }

    #[test]
    fn jsonb_set_tags() {
        for (name, p) in [
            ("jsonb_set", jsonb_set()),
            ("jsonb_set_lax", jsonb_set_lax()),
            ("jsonb_insert", jsonb_insert()),
        ] {
            assert!(
                p.tags.contains(&"function"),
                "{name} must have 'function' tag"
            );
            assert!(p.tags.contains(&"json"), "{name} must have 'json' tag");
        }
    }

    #[test]
    fn st_astext_resolves() {
        let p = st_astext();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.to_uppercase().contains("ST_ASTEXT("),
            "expected ST_AsText in sql: {sql}"
        );
    }

    #[test]
    fn st_asewkt_resolves() {
        let p = st_asewkt();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.to_uppercase().contains("ST_ASEWKT("),
            "expected ST_AsEWKT in sql: {sql}"
        );
    }

    #[test]
    fn spatial_patterns_are_postgres_only() {
        use crate::constraint::DialectSupport;
        for (name, p) in [("st_astext", st_astext()), ("st_asewkt", st_asewkt())] {
            assert_eq!(
                p.dialect_support,
                DialectSupport::PostgresOnly,
                "{name} must be PostgresOnly"
            );
        }
    }

    #[test]
    fn spatial_patterns_have_geometry_type_class() {
        use readyset_sql::ast::SqlType;

        use crate::constraint::Constraint;

        for (name, p) in [("st_astext", st_astext()), ("st_asewkt", st_asewkt())] {
            let type_class = p
                .constraints
                .iter()
                .find_map(|c| match c {
                    Constraint::ColumnTypeClass { type_class, .. } => Some(type_class.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("{name}() must have a ColumnTypeClass constraint"));

            assert!(
                type_class.matches(&SqlType::PostgisPoint),
                "{name}() type class must accept PostgisPoint, got {type_class:?}"
            );
            assert!(
                !type_class.matches(&SqlType::VarChar(Some(255))),
                "{name}() type class must not accept VARCHAR"
            );
        }
    }

    #[test]
    fn spatial_patterns_have_spatial_tag() {
        for (name, p) in [("st_astext", st_astext()), ("st_asewkt", st_asewkt())] {
            assert!(
                p.tags.contains(&"function"),
                "{name} must have 'function' tag"
            );
            assert!(
                p.tags.contains(&"spatial"),
                "{name} must have 'spatial' tag"
            );
        }
    }

    // json_array_length(json_build_array(col)) and
    // jsonb_array_length(jsonb_build_array(col)) patterns must resolve to SQL
    // containing the outer function call and wrap the column in
    // json_build_array / jsonb_build_array to produce a valid JSON array arg.
    #[test]
    fn json_array_length_resolves() {
        let sql = resolve_pattern(&json_array_length(), Dialect::PostgreSQL).to_uppercase();
        assert!(
            sql.contains("JSON_ARRAY_LENGTH("),
            "expected JSON_ARRAY_LENGTH( in sql: {sql}"
        );
        assert!(
            sql.contains("JSON_BUILD_ARRAY("),
            "json_array_length must wrap its argument in json_build_array; got: {sql}"
        );
    }

    #[test]
    fn jsonb_array_length_resolves() {
        let sql = resolve_pattern(&jsonb_array_length(), Dialect::PostgreSQL).to_uppercase();
        assert!(
            sql.contains("JSONB_ARRAY_LENGTH("),
            "expected JSONB_ARRAY_LENGTH( in sql: {sql}"
        );
        assert!(
            sql.contains("JSONB_BUILD_ARRAY("),
            "jsonb_array_length must wrap its argument in jsonb_build_array; got: {sql}"
        );
    }

    #[test]
    fn json_array_length_is_postgres_only() {
        use crate::constraint::DialectSupport;
        for (name, p) in [
            ("json_array_length", json_array_length()),
            ("jsonb_array_length", jsonb_array_length()),
        ] {
            assert_eq!(
                p.dialect_support,
                DialectSupport::PostgresOnly,
                "{name} must be PostgresOnly"
            );
        }
    }

    #[test]
    fn json_array_length_tags() {
        for (name, p) in [
            ("json_array_length", json_array_length()),
            ("jsonb_array_length", jsonb_array_length()),
        ] {
            assert!(
                p.tags.contains(&"function"),
                "{name} must have 'function' tag"
            );
            assert!(p.tags.contains(&"json"), "{name} must have 'json' tag");
        }
    }
}
