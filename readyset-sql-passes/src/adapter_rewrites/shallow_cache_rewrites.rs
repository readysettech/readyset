//! Rewrite passes for sqlparser AST used by shallow caches.
//!
//! This module provides rewrite passes that operate on the sqlparser AST, mirroring the
//! passes in the parent module that operate on the Readyset AST. Shallow caches use the
//! sqlparser AST directly to support queries with syntax not yet supported by Readyset.
//!
//! # Differences from Deep Cache Rewrites
//!
//! The key difference is that shallow cache rewrites always operate on the sqlparser AST
//! directly, rather than the Readyset AST and the parameterization pass replaces all literals.
//!
//! - [`rewrite_shallow`]: Equivalent to [`super::rewrite_equivalent`]
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
use tracing::{trace, trace_span};

use crate::adapter_rewrites::{AdapterRewriteParams, InArrayParam, ShallowQueryParameters};
use crate::anonymize::Anonymizer;

macro_rules! break_err {
    ($e:expr) => {
        return std::ops::ControlFlow::Break($e)
    };
}

/// Rewrites a shallow cache query, producing parameterized output.
///
/// This is the sqlparser-AST equivalent of [`super::rewrite_equivalent`].
/// All literals are replaced with placeholders, and IN/NOT IN lists are treated as single array
/// parameters, allowing queries with different IN set sizes to share the same normalized query
/// form and cache entry.
///
/// Returns [`ShallowQueryParameters`] which can generate cache keys with array-valued parameters.
pub fn rewrite_shallow(
    query: &mut ShallowCacheQuery,
    flags: AdapterRewriteParams,
) -> ReadySetResult<ShallowQueryParameters> {
    let span = trace_span!("adapter_rewrites", part = "shallow").entered();
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

    let (auto_parameters, in_array_params) = fully_parameterize_query(query)?;
    trace!(
        parent: &span,
        pass = "fully_parameterize_query",
        query = %query.display(flags.dialect),
        auto_parameters = ?auto_parameters,
        in_array_params = ?in_array_params,
    );

    number_placeholders(query)?;
    trace!(
        parent: &span,
        pass = "number_placeholders",
        query = %query.display(flags.dialect),
    );

    Ok(ShallowQueryParameters::new(
        flags.dialect,
        reordered_placeholders,
        auto_parameters,
        in_array_params,
    ))
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
///
/// IN/NOT IN lists are treated as single array parameters:
/// - Collapses IN/NOT IN lists to single placeholders (`WHERE x IN (1,2,3)` → `WHERE x IN (?)`)
/// - Sorts IN list values for consistent normalization
/// - Tracks `InArrayParam` for each collapsed IN clause
#[allow(clippy::type_complexity)]
fn fully_parameterize_query(
    query: &mut sqlparser::ast::Query,
) -> ReadySetResult<(Vec<(usize, Literal)>, Vec<InArrayParam>)> {
    let mut visitor = FullyParameterizeVisitor::default();
    match VisitMut::visit(query, &mut visitor) {
        ControlFlow::Continue(()) => Ok((visitor.out, visitor.in_array_params)),
        ControlFlow::Break(e) => Err(e),
    }
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

/// Replaces all literals with `?` placeholders, collapsing IN lists to single placeholders.
///
/// Mirrors `autoparameterize::FullyParameterizeVisitor` for the Readyset AST.
#[derive(Default)]
struct FullyParameterizeVisitor {
    param_idx: usize,
    out: Vec<(usize, Literal)>,
    in_array_params: Vec<InArrayParam>,
    /// Number of placeholders we auto-inserted that haven't been visited yet.
    /// When post_visit_expr sees a placeholder, if this is > 0, we skip incrementing
    /// param_idx since we already accounted for it in pre_visit_expr.
    pending_auto_placeholders: usize,
}

impl VisitorMut for FullyParameterizeVisitor {
    type Break = ReadySetError;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // Handle IN/NOT IN specially - collapse to single placeholder
        if let Expr::InList { list, .. } = expr {
            // Check if all items are Value nodes (literals or placeholders)
            let collapse = list.iter().all(|e| matches!(e, Expr::Value(_)));

            if collapse && !list.is_empty() {
                let start_index = self.param_idx;
                let value_count = list.len();

                let owned_list = mem::take(list);

                // Extract only literal (non-placeholder) items as auto_parameters.
                // Placeholders represent user-provided values at execute time and are
                // NOT extracted — they will be supplied via make_keys at execute time.
                let mut literals: Vec<(usize, Literal)> = Vec::new();
                for (i, e) in owned_list.into_iter().enumerate() {
                    if let Expr::Value(ValueWithSpan { value, .. }) = e {
                        if matches!(value, Value::Placeholder(_)) {
                            // User-provided placeholder — don't extract as auto_param
                            continue;
                        }
                        match sqlparser_value_to_literal(value) {
                            Ok(lit) => literals.push((start_index + i, lit)),
                            Err(e) => break_err!(e),
                        }
                    }
                }

                // Sort literal values for consistent normalization (only when all items
                // are literals; for mixed cases, positions are fixed by auto_param indices)
                if literals.len() == value_count {
                    // All items were literals — sort values and assign sequential positions
                    literals.sort_by(|a, b| a.1.cmp(&b.1));
                    for (seq, (_, lit)) in literals.drain(..).enumerate() {
                        self.out.push((start_index + seq, lit));
                    }
                } else {
                    // Mixed or all-placeholder — keep positional indices as-is
                    for (idx, lit) in literals {
                        self.out.push((idx, lit));
                    }
                }

                self.param_idx += value_count;

                // Replace with single placeholder
                *list = vec![Expr::Value(ValueWithSpan {
                    value: Value::Placeholder("?".into()),
                    span: sqlparser::tokenizer::Span::empty(),
                })];

                self.in_array_params.push(InArrayParam {
                    param_index: start_index,
                    value_count,
                });

                // Track that we inserted a placeholder so post_visit_expr doesn't double-count
                self.pending_auto_placeholders += 1;

                // Note: returning Continue still allows visitor to traverse children,
                // but we've already handled them by replacing the list.
                return ControlFlow::Continue(());
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Value(ValueWithSpan { value, .. }) = expr else {
            return ControlFlow::Continue(());
        };

        // Handle placeholders: either auto-inserted ones (don't count) or user-provided ones
        // (count)
        if matches!(value, Value::Placeholder(_)) {
            if self.pending_auto_placeholders > 0 {
                // This is a placeholder we inserted in pre_visit_expr - don't increment param_idx
                // since we already accounted for it when processing the IN list
                self.pending_auto_placeholders -= 1;
            } else {
                // User-provided placeholder - count it for correct indexing
                self.param_idx += 1;
            }
            return ControlFlow::Continue(());
        }

        match sqlparser_value_to_literal(value.clone()) {
            Ok(literal) => {
                let _ = mem::replace(value, Value::Placeholder("?".into()));
                self.out.push((self.param_idx, literal));
                self.param_idx += 1;
            }
            Err(e) => break_err!(e),
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

/// Single-pass visitor that expands collapsed IN arrays and literalizes all placeholders.
///
/// After `rewrite_shallow`, IN lists are collapsed to single placeholders. This visitor
/// reverses that collapse and substitutes all placeholders with literal values in a single
/// AST traversal, combining the work that previously required three separate passes
/// (expand, renumber, literalize).
struct ExpandAndLiteralizeVisitor<'a> {
    merged_params: &'a [DfValue],
    in_array_params: &'a [InArrayParam],
    next_param_idx: usize,
    next_in_param: usize,
}

impl VisitorMut for ExpandAndLiteralizeVisitor<'_> {
    type Break = ReadySetError;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // Detect collapsed IN lists (single placeholder) and expand + literalize in one step
        if let Expr::InList { list, .. } = expr
            && list.len() == 1
            && matches!(
                &list[0],
                Expr::Value(ValueWithSpan {
                    value: Value::Placeholder(_),
                    ..
                })
            )
            && let Some(in_param) = self.in_array_params.get(self.next_in_param)
        {
            self.next_in_param += 1;
            let start = self.next_param_idx;
            let end = start + in_param.value_count;
            if end > self.merged_params.len() {
                break_err!(internal_err!(
                    "IN array expansion out of bounds: need {} values at index {}, have {}",
                    in_param.value_count,
                    start,
                    self.merged_params.len()
                ));
            }

            let mut new_list = Vec::with_capacity(in_param.value_count);
            for v in &self.merged_params[start..end] {
                match dfvalue_to_sql_value(v) {
                    Ok(val) => new_list.push(Expr::Value(ValueWithSpan {
                        value: val,
                        span: sqlparser::tokenizer::Span::empty(),
                    })),
                    Err(e) => break_err!(e),
                }
            }
            *list = new_list;
            self.next_param_idx = end;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Value(ValueWithSpan { value, .. }) = expr else {
            return ControlFlow::Continue(());
        };
        if !matches!(value, Value::Placeholder(_)) {
            return ControlFlow::Continue(());
        }

        if self.next_param_idx >= self.merged_params.len() {
            break_err!(internal_err!(
                "Missing parameter at index {}: only {} provided",
                self.next_param_idx,
                self.merged_params.len()
            ));
        }

        match dfvalue_to_sql_value(&self.merged_params[self.next_param_idx]) {
            Ok(v) => {
                *value = v;
                self.next_param_idx += 1;
            }
            Err(e) => break_err!(e),
        }
        ControlFlow::Continue(())
    }
}

/// Literalize a normalized shallow query for prepared statement execution.
///
/// This handles expanding collapsed IN arrays and literalizing all placeholders in a single
/// AST traversal, so that a query like `WHERE a = $1 AND b IN ($2)` with merged params
/// `[1, 10, 20]` and `in_array_params = [{param_index: 1, value_count: 2}]` produces
/// `WHERE a = 1 AND b IN (10, 20)`.
pub fn literalize_shallow_prepared(
    query: &ShallowCacheQuery,
    merged_params: &[DfValue],
    in_array_params: &[InArrayParam],
    dialect: Dialect,
) -> ReadySetResult<String> {
    let span = trace_span!("adapter_rewrites", part = "literalize_shallow_prepared").entered();
    trace!(
        parent: &span,
        query = %query.display(dialect),
        param_count = merged_params.len(),
        in_array_count = in_array_params.len(),
        "Literalizing shallow query for prepared execution"
    );

    let mut query_clone = (**query).clone();

    let mut visitor = ExpandAndLiteralizeVisitor {
        merged_params,
        in_array_params,
        next_param_idx: 0,
        next_in_param: 0,
    };
    match VisitMut::visit(&mut query_clone, &mut visitor) {
        ControlFlow::Continue(()) => {}
        ControlFlow::Break(e) => return Err(e),
    }

    if visitor.next_in_param != in_array_params.len() {
        return Err(internal_err!(
            "Expected to expand {} IN arrays but found {} in query",
            in_array_params.len(),
            visitor.next_in_param
        ));
    }

    Ok(format!("{query_clone}"))
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

        let params = rewrite_shallow(&mut query, flags).unwrap();

        assert_eq!(params.auto_parameters.len(), 1);
        assert_eq!(format!("{query}"), "SELECT * FROM t WHERE id = $1");
    }

    #[test]
    fn rewrite_multiple_literals() {
        let dialect = Dialect::MySQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = 1 AND val = 'test'");
        let flags = AdapterRewriteParams::new(dialect);

        let params = rewrite_shallow(&mut query, flags).unwrap();

        assert_eq!(params.auto_parameters.len(), 2);
        let query_str = format!("{}", query);
        assert!(query_str.contains("$1") || query_str.contains("$2"));
    }

    #[test]
    fn rewrite_preserves_existing_placeholders() {
        let dialect = Dialect::PostgreSQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = $1 AND val = $2");
        let flags = AdapterRewriteParams::new(dialect);

        let _params = rewrite_shallow(&mut query, flags).unwrap();

        let query_str = format!("{query}");
        assert!(query_str.contains("$1"));
        assert!(query_str.contains("$2"));
    }

    #[test]
    fn rewrite_reorders_out_of_order_placeholders() {
        let dialect = Dialect::PostgreSQL;
        let mut query = parse_query(dialect, "SELECT * FROM t WHERE id = $3 AND val = $1");
        let flags = AdapterRewriteParams::new(dialect);

        let _params = rewrite_shallow(&mut query, flags).unwrap();

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

    mod in_set_parameterization {
        use super::*;

        fn shallow_process(
            query: &str,
            params: Vec<DfValue>,
            dialect: Dialect,
        ) -> (Vec<DfValue>, String) {
            let mut query = parse_query(dialect, query);
            let flags = AdapterRewriteParams::new(dialect);
            let processed = rewrite_shallow(&mut query, flags).unwrap();
            (processed.make_keys(&params).unwrap(), format!("{query}"))
        }

        fn shallow_process_postgres(query: &str, params: Vec<DfValue>) -> (Vec<DfValue>, String) {
            shallow_process(query, params, Dialect::PostgreSQL)
        }

        fn shallow_process_mysql(query: &str, params: Vec<DfValue>) -> (Vec<DfValue>, String) {
            shallow_process(query, params, Dialect::MySQL)
        }

        #[test]
        fn in_clause_becomes_array_parameter() {
            let (keys, query_str) =
                shallow_process_postgres("SELECT * FROM foo WHERE x IN (1, 2, 3)", vec![]);

            // Query should have IN ($1) with single placeholder
            assert_eq!(query_str, "SELECT * FROM foo WHERE x IN ($1)");

            // Keys should have a single array parameter
            assert_eq!(keys.len(), 1);
            assert_eq!(
                keys[0],
                DfValue::from(vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)])
            );
        }

        #[test]
        fn in_clause_with_other_params() {
            let (keys, query_str) = shallow_process_postgres(
                "SELECT * FROM foo WHERE x IN (1, 2, 3) AND y = 'test'",
                vec![],
            );

            assert_eq!(query_str, "SELECT * FROM foo WHERE x IN ($1) AND y = $2");

            assert_eq!(keys.len(), 2);
            assert_eq!(
                keys[0],
                DfValue::from(vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)])
            );
            assert_eq!(keys[1], DfValue::from("test"));
        }

        #[test]
        fn multiple_in_clauses() {
            let (keys, query_str) = shallow_process_postgres(
                "SELECT * FROM foo WHERE x IN (1, 2) AND y IN (3, 4, 5)",
                vec![],
            );

            assert_eq!(query_str, "SELECT * FROM foo WHERE x IN ($1) AND y IN ($2)");

            assert_eq!(keys.len(), 2);
            assert_eq!(
                keys[0],
                DfValue::from(vec![DfValue::from(1), DfValue::from(2)])
            );
            assert_eq!(
                keys[1],
                DfValue::from(vec![DfValue::from(3), DfValue::from(4), DfValue::from(5)])
            );
        }

        #[test]
        fn not_in_clause_becomes_array() {
            let (keys, query_str) =
                shallow_process_postgres("SELECT * FROM foo WHERE x NOT IN (1, 2, 3)", vec![]);

            assert_eq!(query_str, "SELECT * FROM foo WHERE x NOT IN ($1)");

            assert_eq!(keys.len(), 1);
            assert_eq!(
                keys[0],
                DfValue::from(vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)])
            );
        }

        #[test]
        fn same_normalized_query_for_different_in_sizes() {
            let dialect = Dialect::PostgreSQL;
            let flags = AdapterRewriteParams::new(dialect);

            let mut query1 = parse_query(dialect, "SELECT * FROM foo WHERE x IN (1, 2)");
            let mut query2 = parse_query(dialect, "SELECT * FROM foo WHERE x IN (1, 2, 3, 4, 5)");

            rewrite_shallow(&mut query1, flags).unwrap();
            rewrite_shallow(&mut query2, flags).unwrap();

            // Both should produce the same normalized query
            assert_eq!(
                format!("{query1}"),
                format!("{query2}"),
                "Different IN set sizes should produce same normalized query"
            );
            assert_eq!(format!("{query1}"), "SELECT * FROM foo WHERE x IN ($1)");
        }

        #[test]
        fn mysql_in_clause() {
            let (keys, query_str) =
                shallow_process_mysql("SELECT * FROM foo WHERE x IN (1, 2, 3)", vec![]);

            assert_eq!(query_str, "SELECT * FROM foo WHERE x IN ($1)");

            assert_eq!(keys.len(), 1);
            assert_eq!(
                keys[0],
                DfValue::from(vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)])
            );
        }

        #[test]
        fn mixed_in_and_equality() {
            let (keys, query_str) = shallow_process_postgres(
                "SELECT * FROM t WHERE a = 1 AND b IN (2, 3) AND c = 4 AND d IN (5, 6, 7)",
                vec![],
            );

            assert_eq!(
                query_str,
                "SELECT * FROM t WHERE a = $1 AND b IN ($2) AND c = $3 AND d IN ($4)"
            );

            assert_eq!(keys.len(), 4);
            assert_eq!(keys[0], DfValue::from(1));
            assert_eq!(
                keys[1],
                DfValue::from(vec![DfValue::from(2), DfValue::from(3)])
            );
            assert_eq!(keys[2], DfValue::from(4));
            assert_eq!(
                keys[3],
                DfValue::from(vec![DfValue::from(5), DfValue::from(6), DfValue::from(7)])
            );
        }

        #[test]
        fn single_element_in_list() {
            let (keys, query_str) =
                shallow_process_postgres("SELECT * FROM foo WHERE x IN (42)", vec![]);

            // Single element IN list should still become an array parameter
            assert_eq!(query_str, "SELECT * FROM foo WHERE x IN ($1)");
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], DfValue::from(vec![DfValue::from(42)]));
        }

        #[test]
        fn null_value_in_list() {
            let (keys, query_str) =
                shallow_process_postgres("SELECT * FROM foo WHERE x IN (1, NULL, 3)", vec![]);

            assert_eq!(query_str, "SELECT * FROM foo WHERE x IN ($1)");
            assert_eq!(keys.len(), 1);
            assert_eq!(
                keys[0],
                DfValue::from(vec![DfValue::None, DfValue::from(1), DfValue::from(3)])
            );
        }

        #[test]
        fn mixed_placeholders_and_literals_collapsed() {
            // IN lists with mixed placeholders and literals are still collapsed
            let dialect = Dialect::PostgreSQL;
            let mut query = parse_query(dialect, "SELECT * FROM foo WHERE x IN ($1, 2, $2)");
            let flags = AdapterRewriteParams::new(dialect);

            let processed = rewrite_shallow(&mut query, flags).unwrap();

            assert_eq!(processed.in_array_params.len(), 1);
            assert_eq!(processed.in_array_params[0].value_count, 3);
        }

        #[test]
        fn in_clause_all_placeholders() {
            // Prepared statement: WHERE x IN (?, ?) with user-provided params
            let (keys, query_str) = shallow_process_mysql(
                "SELECT * FROM foo WHERE a = 1 AND x IN (?, ?)",
                vec![DfValue::from(10), DfValue::from(20)],
            );

            // Should normalize the same as an all-literal version
            assert_eq!(query_str, "SELECT * FROM foo WHERE a = $1 AND x IN ($2)");

            // Keys: [1, Array([10, 20])]
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], DfValue::from(1));
            assert_eq!(
                keys[1],
                DfValue::from(vec![DfValue::from(10), DfValue::from(20)])
            );
        }

        #[test]
        fn in_clause_placeholders_sorted() {
            // Ensure (20, 10) produces the same cache key as (10, 20)
            let (keys1, _) = shallow_process_mysql(
                "SELECT * FROM foo WHERE x IN (?, ?)",
                vec![DfValue::from(10), DfValue::from(20)],
            );
            let (keys2, _) = shallow_process_mysql(
                "SELECT * FROM foo WHERE x IN (?, ?)",
                vec![DfValue::from(20), DfValue::from(10)],
            );
            assert_eq!(keys1, keys2);
        }

        #[test]
        fn in_clause_placeholders_match_cache_definition() {
            // Cache defined with IN (?), prepared with IN (?, ?) — same normalized form
            let dialect = Dialect::MySQL;
            let flags = AdapterRewriteParams::new(dialect);

            let mut cache_query = parse_query(dialect, "SELECT * FROM foo WHERE x IN (?)");
            rewrite_shallow(&mut cache_query, flags).unwrap();

            let mut prep_query = parse_query(dialect, "SELECT * FROM foo WHERE x IN (?, ?)");
            rewrite_shallow(&mut prep_query, flags).unwrap();

            assert_eq!(
                format!("{cache_query}"),
                format!("{prep_query}"),
                "Cache definition and prepared statement should produce same normalized query"
            );
        }

        #[test]
        fn mixed_placeholders_and_literals_make_keys() {
            // IN ($1, 2, $2) with user params for $1 and $2
            let dialect = Dialect::PostgreSQL;
            let mut query = parse_query(dialect, "SELECT * FROM foo WHERE x IN ($1, 2, $2)");
            let flags = AdapterRewriteParams::new(dialect);

            let processed = rewrite_shallow(&mut query, flags).unwrap();

            assert_eq!(format!("{query}"), "SELECT * FROM foo WHERE x IN ($1)");
            assert_eq!(processed.in_array_params.len(), 1);
            assert_eq!(processed.in_array_params[0].value_count, 3);

            // User provides 2 params (for $1 and $2), auto_param fills in literal 2
            let keys = processed
                .make_keys(&[DfValue::from(10), DfValue::from(30)])
                .unwrap();
            assert_eq!(keys.len(), 1);
            assert_eq!(
                keys[0],
                DfValue::from(vec![DfValue::from(2), DfValue::from(10), DfValue::from(30)])
            );
        }

        #[test]
        fn literalize_for_execute_with_in_arrays() {
            let dialect = Dialect::MySQL;
            let mut query = parse_query(dialect, "SELECT * FROM t WHERE a = 1 AND b IN (?, ?)");
            let flags = AdapterRewriteParams::new(dialect);

            let params = rewrite_shallow(&mut query, flags).unwrap();

            let sql = params
                .literalize_for_execute(&query, &[DfValue::from(10), DfValue::from(20)])
                .unwrap();

            assert!(
                sql.contains("a = 1"),
                "should contain literalized auto_param: {sql}"
            );
            assert!(
                sql.contains("b IN (10, 20)"),
                "should expand IN array: {sql}"
            );
        }

        #[test]
        fn in_clause_placeholders_not_at_end() {
            // IN array in the middle: WHERE x = 1 AND y IN (?, ?, ?) AND z = 5
            let (keys, query_str) = shallow_process_mysql(
                "SELECT * FROM t WHERE x = 1 AND y IN (?, ?, ?) AND z = 5",
                vec![DfValue::from(2), DfValue::from(3), DfValue::from(4)],
            );

            assert_eq!(
                query_str,
                "SELECT * FROM t WHERE x = $1 AND y IN ($2) AND z = $3"
            );

            // Keys: [1, Array([2, 3, 4]), 5]
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0], DfValue::from(1));
            assert_eq!(
                keys[1],
                DfValue::from(vec![DfValue::from(2), DfValue::from(3), DfValue::from(4)])
            );
            assert_eq!(keys[2], DfValue::from(5));
        }

        #[test]
        fn literalize_in_clause_not_at_end() {
            // Verify literalization also works with IN array in the middle
            let dialect = Dialect::MySQL;
            let mut query = parse_query(
                dialect,
                "SELECT * FROM t WHERE x = 1 AND y IN (?, ?, ?) AND z = 5",
            );
            let flags = AdapterRewriteParams::new(dialect);
            let params = rewrite_shallow(&mut query, flags).unwrap();

            let sql = params
                .literalize_for_execute(
                    &query,
                    &[DfValue::from(2), DfValue::from(3), DfValue::from(4)],
                )
                .unwrap();

            assert!(sql.contains("x = 1"), "should contain x = 1: {sql}");
            assert!(
                sql.contains("y IN (2, 3, 4)"),
                "should expand IN array in middle: {sql}"
            );
            assert!(sql.contains("z = 5"), "should contain z = 5: {sql}");
        }

        #[test]
        fn literalize_for_execute_multiple_in_clauses() {
            let dialect = Dialect::MySQL;
            let mut query = parse_query(
                dialect,
                "SELECT * FROM t WHERE a IN (?, ?) AND b = 1 AND c IN (?, ?, ?)",
            );
            let flags = AdapterRewriteParams::new(dialect);
            let params = rewrite_shallow(&mut query, flags).unwrap();

            let sql = params
                .literalize_for_execute(
                    &query,
                    &[
                        DfValue::from(10),
                        DfValue::from(20),
                        DfValue::from(30),
                        DfValue::from(40),
                        DfValue::from(50),
                    ],
                )
                .unwrap();

            assert!(
                sql.contains("a IN (10, 20)"),
                "should expand first IN array: {sql}"
            );
            assert!(sql.contains("b = 1"), "should contain b = 1: {sql}");
            assert!(
                sql.contains("c IN (30, 40, 50)"),
                "should expand second IN array: {sql}"
            );
        }
    }
}
