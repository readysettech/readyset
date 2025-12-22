//! Rewrite passes for sqlparser AST used by shallow caches.
//!
//! This module provides rewrite passes that operate on the sqlparser AST, mirroring the
//! passes in the parent module that operate on the Readyset AST. Shallow caches use the
//! sqlparser AST directly to support queries with syntax not yet supported by Readyset.
//!
//! # Differences from Deep Cache Rewrites
//!
//! The key difference is that shallow cache rewrites always use operate on the sqlparser AST
//! directly, rather than the Readyset AST and the parameterization pass replaces all literals.
//!
//! - [`rewrite_equivalent_shallow`]: Equivalent to [`super::rewrite_equivalent`]
//! - [`anonymize_shallow_query`]: Equivalent to [`crate::anonymize::Anonymize`]
//! - [`literalize_shallow_query`]: Equivalent to [`super::literalize`]
//! - [`convert_placeholders_to_question_marks`]: Equivalent to [`super::convert_placeholders_to_question_marks`]

use std::convert::Infallible;
use std::mem;
use std::ops::ControlFlow;

use readyset_data::DfValue;
use readyset_errors::{ReadySetError, ReadySetResult, internal_err, unsupported_err};
use readyset_sql::ast::{ItemPlaceholder, Literal, ShallowCacheQuery};
use readyset_sql::{AstConversionError, Dialect, DialectDisplay};
use sqlparser::ast::{
    Expr, Ident, ObjectName, Value, ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut,
};
use tracing::{error, trace, trace_span};

use crate::adapter_rewrites::{AdapterRewriteParams, QueryParameters};
use crate::anonymize::Anonymizer;

macro_rules! break_err {
    ($e:expr) => {
        return std::ops::ControlFlow::Break($e)
    };
}

/// Rewrites a shallow cache query, producing parameterized output.
///
/// This is the sqlparser-AST equivalent of [`super::rewrite_equivalent`].
/// All literals are replaced with placeholders, not just those in supported positions.
pub fn rewrite_equivalent_shallow(
    query: &mut ShallowCacheQuery,
    flags: AdapterRewriteParams,
) -> ReadySetResult<QueryParameters> {
    let span = trace_span!("adapter_rewrites", part = "equivalent_shallow").entered();
    trace!(
        parent: &span,
        query = %query.display(flags.dialect),
        "Rewriting query placeholders"
    );

    let reordered_placeholders = reorder_numbered_placeholders(query);
    trace!(
        parent: &span,
        pass = "reorder_numbered_placeholders",
        query = %query.display(flags.dialect),
        placeholders = ?reordered_placeholders,
    );

    let auto_parameters = fully_parameterize_query(query)?;
    trace!(
        parent: &span,
        pass = "fully_parameterize_query",
        query = %query.display(flags.dialect),
        auto_parameters = ?auto_parameters,
    );

    number_placeholders(query)?;
    trace!(
        parent: &span,
        pass = "number_placeholders",
        query = %query.display(flags.dialect),
    );

    Ok(QueryParameters {
        dialect: flags.dialect,
        reordered_placeholders,
        auto_parameters,
    })
}

/// Anonymizes a sqlparser query for safe, PII-free logging and telemetry.
///
/// This is the sqlparser-AST equivalent of [`crate::anonymize::Anonymize`].
///
/// - Replaces identifiers (table and column names) using the provided [`Anonymizer`]
/// - Replaces literal values with `'<anonymized>'`
/// - Preserves placeholders, booleans, and `NULL` values
pub fn anonymize_shallow_query(query: &mut ShallowCacheQuery, anonymizer: &mut Anonymizer) {
    let span = trace_span!("adapter_rewrites", part = "anonymize_shallow").entered();
    trace!(
        parent: &span,
        query = %query.display(Dialect::MySQL),
        "Anonymizing shallow query"
    );

    let mut visitor = AnonymizeVisitor { anonymizer };
    let _ = VisitMut::visit(&mut **query, &mut visitor);
}

/// Substitutes placeholders in a cloned query with the provided DfValues.
///
/// This is primarily used for shallow cache refresh queries, which execute as raw SQL
/// against the upstream database without bound parameters.
///
/// Supports both MySQL-style `?` placeholders and PostgreSQL-style `$N` placeholders.
pub fn literalize_shallow_query(
    query: &ShallowCacheQuery,
    params: &[DfValue],
    dialect: Dialect,
) -> ReadySetResult<String> {
    let span = trace_span!("adapter_rewrites", part = "literalize_shallow").entered();
    trace!(
        parent: &span,
        query = %query.display(dialect),
        param_count = params.len(),
        "Literalizing shallow query"
    );

    let mut query_clone = (**query).clone();
    let mut visitor = LiteralizeVisitor {
        params,
        next_qmark_idx: 0,
    };

    match VisitMut::visit(&mut query_clone, &mut visitor) {
        ControlFlow::Continue(()) => Ok(format!("{query_clone}")),
        ControlFlow::Break(e) => Err(e),
    }
}

/// Collects a reorder map for out-of-order `$N` placeholders.
///
/// This is the sqlparser-AST equivalent of the deep cache's `reorder_numbered_placeholders`.
///
/// Unlike the deep cache version, this does NOT mutate the placeholders in place.
/// The deep cache version both collects the map AND renumbers placeholders, but that
/// mutation is redundant since [`number_placeholders`] renumbers everything at the end
/// anyway. We keep the two concerns separate for clarity.
///
/// Returns `None` if placeholders are already sequential (optimization to avoid work at runtime).
fn reorder_numbered_placeholders(query: &sqlparser::ast::Query) -> Option<Vec<usize>> {
    let mut visitor = ReorderNumberedPlaceholdersVisitor::default();
    let _ = Visit::visit(query, &mut visitor);

    // As an optimization, check if the placeholders were *already* ordered and contiguous, and
    // return None if so. This allows us to save some clones on the actual read-path.
    if visitor.out.is_empty() {
        return None;
    }

    let mut contiguous = true;
    let mut prev = visitor.out[0];
    for &n in &visitor.out[1..] {
        if n != prev + 1 {
            contiguous = false;
            break;
        }
        prev = n;
    }

    if contiguous { None } else { Some(visitor.out) }
}

/// Replaces ALL literals with placeholders, extracting them as parameters.
///
/// This is the sqlparser-AST equivalent of deep cache's `autoparameterize` pass.
/// Unlike the deep cache's auto-parameterization, this replaces ALL literals regardless
/// of their position in the query (not just those in WHERE clause comparisons).
fn fully_parameterize_query(
    query: &mut sqlparser::ast::Query,
) -> ReadySetResult<Vec<(usize, Literal)>> {
    let mut visitor = FullyParameterizeVisitor::default();
    let _ = VisitMut::visit(query, &mut visitor);
    Ok(visitor.out)
}

/// Renumbers all placeholders to sequential `$1`, `$2`, etc.
///
/// This is the sqlparser-AST equivalent of the deep cache's `number_placeholders`.
fn number_placeholders(query: &mut sqlparser::ast::Query) -> ReadySetResult<()> {
    let mut visitor = NumberPlaceholdersVisitor {
        next_param_number: 1,
    };
    match VisitMut::visit(query, &mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// Collects `$N` placeholder numbers in visitation order for building a reorder map.
#[derive(Default)]
struct ReorderNumberedPlaceholdersVisitor {
    out: Vec<usize>,
}

impl Visitor for ReorderNumberedPlaceholdersVisitor {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(ValueWithSpan {
            value: Value::Placeholder(ident),
            ..
        }) = expr
            && let Ok(ItemPlaceholder::DollarNumber(n)) = ItemPlaceholder::try_from(ident)
        {
            self.out.push(n as usize - 1);
        }
        ControlFlow::Continue(())
    }
}

/// Replaces all literals with `?` placeholders and collects the original values.
///
/// Mirrors `autoparameterize::FullyParameterizeVisitor` for the Readyset AST.
#[derive(Default)]
struct FullyParameterizeVisitor {
    param_idx: usize,
    out: Vec<(usize, Literal)>,
}

impl VisitorMut for FullyParameterizeVisitor {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Value(ValueWithSpan { value, .. }) = expr else {
            return ControlFlow::Continue(());
        };

        // Skip existing placeholders but count them for correct indexing
        if matches!(value, Value::Placeholder(_)) {
            self.param_idx += 1;
            return ControlFlow::Continue(());
        }

        match sqlparser_value_to_literal(value.clone()) {
            Ok(literal) => {
                let _ = mem::replace(value, Value::Placeholder("?".into()));
                self.out.push((self.param_idx, literal));
                self.param_idx += 1;
            }
            Err(e) => {
                error!(%e, "Autoparameterization pass failed to convert Value to Literal; Skipping Value.")
            }
        }

        ControlFlow::Continue(())
    }
}

/// Renumbers all placeholders to sequential `$1`, `$2`, etc.
///
/// Mirrors `NumberPlaceholdersVisitor` for the Readyset AST.
struct NumberPlaceholdersVisitor {
    next_param_number: u32,
}

impl VisitorMut for NumberPlaceholdersVisitor {
    type Break = ReadySetError;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Value(ValueWithSpan {
            value: Value::Placeholder(placeholder_str),
            ..
        }) = expr
        else {
            return ControlFlow::Continue(());
        };

        let placeholder = ItemPlaceholder::try_from(&*placeholder_str)
            .map_err(|e| internal_err!("Invalid placeholder: {:?}", e));

        match placeholder {
            Err(e) => break_err!(e),
            // client-provided queries aren't allowed to mix question-mark and dollar-number
            // placeholders, but autoparameterization creates question mark placeholders,
            // which in the intermediate state does end up with a query that has both styles.
            Ok(ItemPlaceholder::QuestionMark | ItemPlaceholder::DollarNumber(_)) => {
                *placeholder_str = format!("${}", self.next_param_number);
                self.next_param_number += 1;
            }
            Ok(ItemPlaceholder::ColonNumber(_)) => {
                break_err!(unsupported_err!(
                    "colon-number placeholders aren't supported"
                ))
            }
        }

        ControlFlow::Continue(())
    }
}

/// Converts all placeholders to MySQL-style `?` placeholders.
///
/// This is the sqlparser-AST equivalent of the deep cache's `convert_placeholders_to_question_marks`.
struct QuestionMarkPlaceholdersVisitor;

impl VisitorMut for QuestionMarkPlaceholdersVisitor {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Value(ValueWithSpan {
            value: Value::Placeholder(placeholder_str),
            ..
        }) = expr
        else {
            return ControlFlow::Continue(());
        };

        *placeholder_str = "?".to_string();
        ControlFlow::Continue(())
    }
}

/// Converts all placeholders to MySQL-style `?` placeholders.
///
/// This is primarily used for MySQL compatibility checks when preparing statements.
#[allow(dead_code)]
pub fn convert_placeholders_to_question_marks(query: &mut ShallowCacheQuery) -> ReadySetResult<()> {
    let mut visitor = QuestionMarkPlaceholdersVisitor;
    let _ = VisitMut::visit(&mut **query, &mut visitor);
    Ok(())
}

/// Replaces identifiers and literals with anonymized values.
struct AnonymizeVisitor<'a> {
    anonymizer: &'a mut Anonymizer,
}

impl VisitorMut for AnonymizeVisitor<'_> {
    type Break = Infallible;

    fn post_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        for part in relation.0.iter_mut() {
            if let sqlparser::ast::ObjectNamePart::Identifier(ident) = part {
                anonymize_ident(ident, self.anonymizer);
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Identifier(ident) => {
                anonymize_ident(ident, self.anonymizer);
            }
            Expr::CompoundIdentifier(idents) => {
                for ident in idents.iter_mut() {
                    anonymize_ident(ident, self.anonymizer);
                }
            }
            Expr::QualifiedWildcard(object_name, _) => {
                for part in object_name.0.iter_mut() {
                    if let sqlparser::ast::ObjectNamePart::Identifier(ident) = part {
                        anonymize_ident(ident, self.anonymizer);
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_value(&mut self, value: &mut Value) -> ControlFlow<Self::Break> {
        // Preserve placeholders, booleans, and NULL
        if matches!(
            value,
            Value::Placeholder(_) | Value::Boolean(_) | Value::Null
        ) {
            return ControlFlow::Continue(());
        }

        *value = Value::SingleQuotedString("<anonymized>".to_owned());
        ControlFlow::Continue(())
    }
}

/// Substitutes placeholders with literal values from parameters.
struct LiteralizeVisitor<'a> {
    params: &'a [DfValue],
    next_qmark_idx: usize,
}

impl VisitorMut for LiteralizeVisitor<'_> {
    type Break = ReadySetError;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Value(ValueWithSpan { value, .. }) = expr else {
            return ControlFlow::Continue(());
        };

        let Value::Placeholder(placeholder) = value else {
            return ControlFlow::Continue(());
        };

        // Determine parameter index based on placeholder format
        let param_idx = if *placeholder == "?" {
            let idx = self.next_qmark_idx;
            self.next_qmark_idx += 1;
            idx
        } else if let Some(rest) = placeholder.strip_prefix('$') {
            match rest.parse::<usize>() {
                Ok(n) if n > 0 => n - 1,
                _ => break_err!(unsupported_err!("Invalid placeholder: {placeholder}")),
            }
        } else {
            break_err!(unsupported_err!("Unsupported placeholder: {placeholder}"))
        };

        let Some(param) = self.params.get(param_idx) else {
            break_err!(internal_err!(
                "Missing parameter for placeholder {placeholder}: only {} parameters provided",
                self.params.len()
            ))
        };

        // TODO: should we use `sqlparser::ast::Value` instead and remove this conversion?
        // This requires switching shallow cache from DfValue to Value
        *value = match dfvalue_to_sql_value(param) {
            Ok(v) => v,
            Err(e) => break_err!(e),
        };
        ControlFlow::Continue(())
    }
}

/// Anonymizes a single identifier using the provided anonymizer.
fn anonymize_ident(ident: &mut Ident, anonymizer: &mut Anonymizer) {
    let mut sql_id: readyset_sql::ast::SqlIdentifier = ident.value.as_str().into();
    anonymizer.replace(&mut sql_id);
    ident.value = sql_id.to_string();
}

/// Converts a sqlparser [`Value`] to a Readyset [`Literal`].
fn sqlparser_value_to_literal(value: Value) -> ReadySetResult<Literal> {
    Literal::try_from(value).map_err(|e: AstConversionError| {
        ReadySetError::Internal(format!("Failed to convert sqlparser value: {:?}", e))
    })
}

/// Converts a [`DfValue`] to a sqlparser [`Value`] for SQL output.
///
/// Note: We pass raw string content to sqlparser. sqlparser handles escaping
/// (e.g., doubling single quotes) during `Display`.
///
/// Returns an error for types that cannot be safely represented as SQL literals.
fn dfvalue_to_sql_value(v: &DfValue) -> ReadySetResult<Value> {
    Ok(match v {
        DfValue::None => Value::Null,
        DfValue::Int(i) => Value::Number(i.to_string(), false),
        DfValue::UnsignedInt(u) => Value::Number(u.to_string(), false),
        DfValue::Float(f) => Value::Number(f.to_string(), false),
        DfValue::Double(f) => Value::Number(f.to_string(), false),
        DfValue::Numeric(d) => Value::Number(d.to_string(), false),
        DfValue::TinyText(t) => Value::SingleQuotedString(t.as_str().to_string()),
        DfValue::Text(t) => Value::SingleQuotedString(t.as_str().to_string()),
        // Timestamps and times are represented as quoted strings (matches deep cache behavior)
        DfValue::TimestampTz(ts) => Value::SingleQuotedString(ts.to_string()),
        DfValue::Time(t) => Value::SingleQuotedString(t.to_string()),
        // Byte arrays use hex string literal (sqlparser adds X'...' wrapper)
        DfValue::ByteArray(arr) => Value::HexStringLiteral(hex::encode(arr.as_ref())),
        // BitVector as binary string literal
        DfValue::BitVector(bits) => {
            let s: String = bits.iter().map(|b| if b { '1' } else { '0' }).collect();
            Value::SingleQuotedByteStringLiteral(s)
        }
        // maybe this needs to be changed later
        DfValue::Array(arr) => Value::SingleQuotedString(arr.to_string()),
        DfValue::PassThrough(_) => {
            return Err(internal_err!(
                "PassThrough has no representation as a literal"
            ));
        }
        DfValue::Default => {
            return Err(internal_err!("Default has no representation as a literal"));
        }
        DfValue::Max => {
            return Err(internal_err!("Max has no representation as a literal"));
        }
    })
}

#[cfg(test)]
mod tests {
    use readyset_data::DfValue;
    use readyset_sql::Dialect;
    use readyset_sql_parsing::parse_shallow_query;

    use super::*;
    use crate::adapter_rewrites::AdapterRewriteParams;
    use crate::anonymize::Anonymizer;

    fn parse_query(dialect: Dialect, sql: &str) -> ShallowCacheQuery {
        parse_shallow_query(dialect, sql).expect("Failed to parse query")
    }

    #[test]
    fn rewrite_basic_literal() {
        let dialect = Dialect::MySQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = 1");
        let flags = AdapterRewriteParams::new(dialect);

        let params = rewrite_equivalent_shallow(&mut query, flags).unwrap();

        assert_eq!(params.auto_parameters.len(), 1);
        assert_eq!(format!("{query}"), "SELECT * FROM t WHERE id = $1");
    }

    #[test]
    fn rewrite_multiple_literals() {
        let dialect = Dialect::MySQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = 1 AND val = 'test'");
        let flags = AdapterRewriteParams::new(dialect);

        let params = rewrite_equivalent_shallow(&mut query, flags).unwrap();

        assert_eq!(params.auto_parameters.len(), 2);
        let query_str = format!("{}", query);
        assert!(query_str.contains("$1") || query_str.contains("$2"));
    }

    #[test]
    fn rewrite_preserves_existing_placeholders() {
        let dialect = Dialect::PostgreSQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = $1 AND val = $2");
        let flags = AdapterRewriteParams::new(dialect);

        let _params = rewrite_equivalent_shallow(&mut query, flags).unwrap();

        let query_str = format!("{query}");
        assert!(query_str.contains("$1"));
        assert!(query_str.contains("$2"));
    }

    #[test]
    fn rewrite_reorders_out_of_order_placeholders() {
        let dialect = Dialect::PostgreSQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = $3 AND val = $1");
        let flags = AdapterRewriteParams::new(dialect);

        let _params = rewrite_equivalent_shallow(&mut query, flags).unwrap();

        let query_str = format!("{query}");
        assert!(query_str.contains("$1"));
        assert!(query_str.contains("$2"));
    }

    #[test]
    fn test_literalize_shallow_query_mysql_question_mark() {
        let dialect = Dialect::MySQL;
        let query = parse_query(dialect, "SELECT * FROM t WHERE id = ? AND val = ?");
        let params = vec![DfValue::Int(42), DfValue::Text("test".into())];

        let result = literalize_shallow_query(&query, &params, dialect).unwrap();

        assert_eq!(result, "SELECT * FROM t WHERE id = 42 AND val = 'test'");
    }

    #[test]
    fn literalize_postgres_dollar_placeholders() {
        let dialect = Dialect::PostgreSQL;
        let query = parse_query(dialect, "SELECT * FROM t WHERE id = $1 AND val = $2");
        let params = vec![DfValue::Int(42), DfValue::Text("test".into())];

        let result = literalize_shallow_query(&query, &params, dialect).unwrap();

        assert_eq!(result, "SELECT * FROM t WHERE id = 42 AND val = 'test'");
    }

    #[test]
    fn literalize_null_value() {
        let dialect = Dialect::MySQL;
        let query = parse_query(dialect, "SELECT * FROM t WHERE id = ?");
        let params = vec![DfValue::None];

        let result = literalize_shallow_query(&query, &params, dialect).unwrap();

        assert_eq!(result, "SELECT * FROM t WHERE id = NULL");
    }

    #[test]
    fn literalize_escapes_single_quotes() {
        // sqlparser handles standard SQL escaping: single quotes are doubled
        let dialect = Dialect::MySQL;
        let query = parse_query(dialect, "SELECT * FROM t WHERE val = ?");
        let params = vec![DfValue::Text("O'Reilly".into())];

        let result = literalize_shallow_query(&query, &params, dialect).unwrap();

        assert_eq!(result, "SELECT * FROM t WHERE val = 'O''Reilly'");
    }

    #[test]
    fn literalize_preserves_backslashes() {
        // sqlparser does NOT do MySQL-specific backslash escaping (that depends on
        // server settings like NO_BACKSLASH_ESCAPES). Backslashes are passed through as-is.
        let dialect = Dialect::MySQL;
        let query = parse_query(dialect, "SELECT * FROM t WHERE val = ?");
        let params = vec![DfValue::Text("test\\value".into())];

        let result = literalize_shallow_query(&query, &params, dialect).unwrap();

        assert_eq!(result, "SELECT * FROM t WHERE val = 'test\\value'");
    }

    #[test]
    fn literalize_numeric_types() {
        let dialect = Dialect::MySQL;
        let query = parse_query(dialect, "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?");
        let params = vec![
            DfValue::Int(-42),
            DfValue::Double(3.3),
            DfValue::UnsignedInt(100),
        ];

        let result = literalize_shallow_query(&query, &params, dialect).unwrap();

        assert!(result.contains("-42"));
        assert!(result.contains("3.3"));
        assert!(result.contains("100"));
    }

    #[test]
    fn literalize_missing_parameter_returns_error() {
        let dialect = Dialect::MySQL;
        let query = parse_query(dialect, "SELECT * FROM t WHERE id = ? AND val = ?");
        let params = vec![DfValue::Int(42)]; // Missing second parameter

        let result = literalize_shallow_query(&query, &params, dialect);

        assert!(result.is_err());
    }

    #[test]
    fn anonymize_replaces_identifiers_and_literals() {
        let dialect = Dialect::MySQL;
        let mut query = parse_query(
            dialect,
            "SELECT name, email FROM users WHERE id = 42 AND status = 'active'",
        );
        let mut anonymizer = Anonymizer::new();

        anonymize_shallow_query(&mut query, &mut anonymizer);

        let query_str = format!("{query}");
        assert!(!query_str.contains("users"));
        assert!(!query_str.contains("name"));
        assert!(!query_str.contains("email"));
        assert!(query_str.contains("<anonymized>"));
    }

    #[test]
    fn anonymize_preserves_placeholders() {
        let dialect = Dialect::MySQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = ?");
        let mut anonymizer = Anonymizer::new();

        anonymize_shallow_query(&mut query, &mut anonymizer);

        let query_str = format!("{query}");
        assert!(query_str.contains("?"));
    }

    #[test]
    fn convert_dollar_placeholders_to_question_marks() {
        let dialect = Dialect::PostgreSQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = $1 AND val = $2");

        convert_placeholders_to_question_marks(&mut query).unwrap();

        let query_str = format!("{query}");
        assert!(!query_str.contains("$1"));
        assert!(!query_str.contains("$2"));
        assert_eq!(query_str.matches('?').count(), 2);
    }

    #[test]
    fn convert_placeholders_preserves_question_marks() {
        let dialect = Dialect::MySQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = ? AND val = ?");

        convert_placeholders_to_question_marks(&mut query).unwrap();

        let query_str = format!("{query}");
        assert_eq!(query_str.matches('?').count(), 2);
    }

    #[test]
    fn convert_placeholders_no_placeholders_is_noop() {
        let dialect = Dialect::MySQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = 1");
        let original = format!("{query}");

        convert_placeholders_to_question_marks(&mut query).unwrap();

        assert_eq!(format!("{query}"), original);
    }
}
