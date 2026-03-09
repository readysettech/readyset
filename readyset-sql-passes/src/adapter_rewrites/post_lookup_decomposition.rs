//! Decomposition of aggregate functions for post-lookup aggregation (PLA).
//!
//! When queries have WHERE IN or range conditions, the adapter reads from the reader node
//! multiple times and consolidates results via PLA. Some aggregates (e.g. AVG) cannot be
//! naively re-aggregated, so this module rewrites them into simpler components (SUM, COUNT)
//! that *can* be re-aggregated, then recomputes the original value in
//! [`postprocess_decompositions`](crate::adapter_rewrites::postprocess_decompositions).

use readyset_data::{DfType, DfValue};
use readyset_errors::{ReadySetResult, unsupported};
use readyset_sql::analysis::visit::{self, Visitor};
use readyset_sql::ast::{
    Expr, FieldDefinitionExpr, FunctionExpr, Literal, SelectStatement, SqlIdentifier,
};
use readyset_sql::{Dialect, DialectDisplay};
use tracing::{error, warn};

/// Identifies which aggregate function was decomposed for post-lookup
/// recomputation.
///
/// Each variant knows how to decompose itself into simpler aggregates
/// ([`Self::decompose`]), compute the final value from those components
/// ([`Self::compute`]), and determine the result type ([`Self::result_type`]).
///
/// To add a new decomposable aggregate (e.g. STDDEV), add a variant here and
/// implement the four methods.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum PostLookupAggregateKind {
    /// `AVG(x)` decomposed into `SUM(x)`, `COUNT(x)`, and `MIN(x)`.
    ///
    /// `source_columns` order: `[SUM, COUNT, MIN]`.
    /// `MIN(x)` is a type-preserving sentinel whose *value* is unused — only
    /// its [`DfType`] matters for computing the dialect-correct output type.
    Avg,
}

/// A source column that feeds into a decomposed aggregate computation.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SourceColumn {
    /// Index of this column in the rewritten SELECT list.
    pub field_index: usize,
    /// `true` if this column was added by the decomposition (and must be
    /// stripped from the final result); `false` if it was already present in
    /// the query and is being reused.
    pub added: bool,
}

/// One decomposed aggregate: the original function replaced by N source
/// columns that the adapter recombines after post-lookup aggregation.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PostLookupDecomposition {
    /// Which aggregate was decomposed.
    pub kind: PostLookupAggregateKind,
    /// Position in the *original* SELECT list where the computed result should
    /// appear.
    pub result_index: usize,
    /// Source columns in kind-specific order (e.g. `[SUM, COUNT, MIN]` for
    /// [`PostLookupAggregateKind::Avg`]).
    pub source_columns: Box<[SourceColumn]>,
    /// The alias the column had before decomposition (restored on the output).
    pub original_alias: SqlIdentifier,
}

/// Describes how to produce a single column in the post-lookup output row.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum PostLookupColumn {
    /// Pass through the value at this index from the source row unchanged.
    Direct(usize),
    /// Compute the value from the decomposed aggregate at this index in
    /// [`PostLookupPlan::decompositions`].
    Computed(usize),
}

/// Pre-computed plan for applying [`PostLookupDecomposition`]s to result rows
/// and schema.
///
/// Built once at query-rewrite time via [`PostLookupPlan::new`] and reused
/// across every execution of that query, so the column-mapping logic runs once
/// rather than per-read.
///
/// `column_plan` and `columns_to_remove` are deterministically derived from
/// `decompositions` + the column count at rewrite time. They participate in
/// `Hash`/`Eq` because `DfQueryParameters` derives those traits.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PostLookupPlan {
    decompositions: Box<[PostLookupDecomposition]>,
    /// For each output position, how to produce the value.
    column_plan: Box<[PostLookupColumn]>,
    /// Indices of columns added by decomposition that must be stripped from
    /// the final output (sorted ascending).
    columns_to_remove: Box<[usize]>,
}

impl PostLookupAggregateKind {
    /// Try to match a [`FunctionExpr`] for decomposition.
    ///
    /// Returns `(kind, inner_expression)` if the function can be decomposed
    /// into simpler aggregates for post-lookup recomputation.
    pub(crate) fn try_match(func: &FunctionExpr) -> Option<(Self, Box<Expr>)> {
        match func {
            FunctionExpr::Avg {
                expr,
                distinct: false,
            } => Some((Self::Avg, expr.clone())),
            _ => None,
        }
    }

    /// Produce the replacement [`FunctionExpr`]s for the decomposition.
    ///
    /// The first element replaces the original aggregate in-place; subsequent
    /// elements are appended as new columns.
    pub(crate) fn decompose(&self, expr: Box<Expr>) -> Vec<FunctionExpr> {
        match self {
            Self::Avg => vec![
                FunctionExpr::Sum {
                    expr: expr.clone(),
                    distinct: false,
                },
                FunctionExpr::Count {
                    expr: expr.clone(),
                    distinct: false,
                },
                // MIN(x) preserves the original expression type in the View
                // schema. Its value is unused — only its DfType matters for
                // computing the dialect-aware AVG output type.
                FunctionExpr::Min(expr),
            ],
        }
    }

    /// The [`DfType`] of the recomputed result.
    ///
    /// - **MySQL** — delegates to [`DfType::mysql_avg_output_type`]
    ///   (`Numeric{14,4}` for ints, precision-extended for decimals, `Double`
    ///   for floats).
    /// - **PostgreSQL** — `Double` for float/double, `DEFAULT_NUMERIC` for
    ///   everything else.
    pub fn result_type(
        &self,
        source_col_types: &[&DfType],
        dialect: readyset_data::Dialect,
    ) -> DfType {
        match self {
            Self::Avg => {
                // Uses the third col (MIN) to recover the type, since MIN is type-preserving.
                let expr_type = source_col_types.get(2).copied().unwrap_or_else(|| {
                    warn!(
                        actual = source_col_types.len(),
                        "post-lookup AVG result_type: expected 3 source columns (SUM, COUNT, MIN)"
                    );
                    &DfType::Unknown
                });
                match dialect.engine() {
                    readyset_data::SqlEngine::MySQL => DfType::mysql_avg_output_type(expr_type),
                    readyset_data::SqlEngine::PostgreSQL => {
                        if expr_type.is_any_float() {
                            DfType::Double
                        } else {
                            DfType::DEFAULT_NUMERIC
                        }
                    }
                }
            }
        }
    }

    /// Compute the final value from source values (in kind-specific order).
    pub fn compute(&self, sources: &[&DfValue]) -> DfValue {
        match self {
            Self::Avg => {
                if sources.len() < 2 {
                    error!(
                        expected = ">=2",
                        actual = sources.len(),
                        "post-lookup AVG: wrong number of source columns"
                    );
                    return DfValue::None;
                }
                Self::compute_avg(sources[0], sources[1])
            }
        }
    }

    /// AVG = SUM / COUNT, producing a `Double`.
    ///
    /// Returns `None` (SQL NULL) when either input is NULL or count is zero.
    fn compute_avg(sum: &DfValue, count: &DfValue) -> DfValue {
        if *sum == DfValue::None || *count == DfValue::None {
            return DfValue::None;
        }
        let sum_f = match sum.coerce_to(&DfType::Double, &DfType::Unknown) {
            Ok(v) => v,
            Err(e) => {
                warn!(%e, ?sum, "post-lookup AVG: failed to coerce SUM to Double");
                return DfValue::None;
            }
        };
        let count_f = match count.coerce_to(&DfType::Double, &DfType::Unknown) {
            Ok(v) => v,
            Err(e) => {
                warn!(%e, ?count, "post-lookup AVG: failed to coerce COUNT to Double");
                return DfValue::None;
            }
        };
        if count_f == DfValue::Double(0.0) {
            return DfValue::None;
        }
        match &sum_f / &count_f {
            Ok(v) => v,
            Err(e) => {
                warn!(%e, ?sum_f, ?count_f, "post-lookup AVG: SUM/COUNT division failed");
                DfValue::None
            }
        }
    }
}

impl PostLookupPlan {
    /// Build a plan from decompositions and the total number of columns in the
    /// rewritten SELECT list.
    pub fn new(decompositions: Vec<PostLookupDecomposition>, num_columns: usize) -> Self {
        if decompositions.is_empty() {
            return Self {
                decompositions: Box::new([]),
                column_plan: Box::new([]),
                columns_to_remove: Box::new([]),
            };
        }
        let (column_plan, columns_to_remove) = build_column_plan(num_columns, &decompositions);
        Self {
            decompositions: decompositions.into_boxed_slice(),
            column_plan,
            columns_to_remove,
        }
    }

    /// Returns `true` if there are no decompositions to apply.
    pub fn is_empty(&self) -> bool {
        self.decompositions.is_empty()
    }

    /// The decomposed aggregates.
    pub fn decompositions(&self) -> &[PostLookupDecomposition] {
        &self.decompositions
    }

    /// The precomputed column plan for transforming rows.
    pub fn column_plan(&self) -> &[PostLookupColumn] {
        &self.column_plan
    }

    /// Indices of added columns to remove from the final output (ascending).
    pub fn columns_to_remove(&self) -> &[usize] {
        &self.columns_to_remove
    }
}

/// Build a plan mapping each output column to either a direct pass-through or
/// a computed value.
fn build_column_plan(
    num_columns: usize,
    decompositions: &[PostLookupDecomposition],
) -> (Box<[PostLookupColumn]>, Box<[usize]>) {
    let mut columns_to_remove: Vec<usize> = decompositions
        .iter()
        .flat_map(|d| {
            d.source_columns
                .iter()
                .filter(|s| s.added)
                .map(|s| s.field_index)
        })
        .collect();
    columns_to_remove.sort_unstable();
    columns_to_remove.dedup();

    let mut plan = Vec::with_capacity(num_columns - columns_to_remove.len());
    for col_idx in 0..num_columns {
        if columns_to_remove.binary_search(&col_idx).is_ok() {
            continue;
        }
        if let Some((decomp_idx, _)) = decompositions
            .iter()
            .enumerate()
            .find(|(_, d)| d.result_index == col_idx)
        {
            plan.push(PostLookupColumn::Computed(decomp_idx));
        } else {
            plan.push(PostLookupColumn::Direct(col_idx));
        }
    }

    (
        plan.into_boxed_slice(),
        columns_to_remove.into_boxed_slice(),
    )
}

/// Returns `true` if `target` appears in the query's HAVING or ORDER BY
/// clauses.
///
/// Used to skip decomposition for aggregates referenced in those clauses,
/// since we only rewrite the SELECT list.
fn expr_referenced_in_having_or_order_by(target: &Expr, query: &SelectStatement) -> bool {
    struct ContainsExpr<'n> {
        needle: &'n Expr,
        found: bool,
    }

    impl<'ast> Visitor<'ast> for ContainsExpr<'_> {
        type Error = std::convert::Infallible;

        fn visit_expr(&mut self, expr: &'ast Expr) -> Result<(), Self::Error> {
            if expr == self.needle {
                self.found = true;
                return Ok(());
            }
            visit::walk_expr(self, expr)
        }

        fn visit_select_statement(
            &mut self,
            _select_statement: &'ast SelectStatement,
        ) -> Result<(), Self::Error> {
            // Don't walk into subqueries — the needle is defined at the outer
            // query level and cannot appear inside a subquery's own scope.
            Ok(())
        }
    }

    fn expr_contains(haystack: &Expr, needle: &Expr) -> bool {
        let mut vis = ContainsExpr {
            needle,
            found: false,
        };
        let _ = vis.visit_expr(haystack);
        vis.found
    }

    if let Some(having) = &query.having
        && expr_contains(having, target)
    {
        return true;
    }
    if let Some(order) = &query.order {
        for ob in &order.order_by {
            match &ob.field {
                readyset_sql::ast::FieldReference::Expr(e) => {
                    if expr_contains(e, target) {
                        return true;
                    }
                    // ORDER BY may reference a SELECT alias (e.g. ORDER BY avg_col).
                    // Resolve alias to the underlying expression and check that.
                    if let Expr::Column(col) = e {
                        for field in &query.fields {
                            if let FieldDefinitionExpr::Expr { expr, alias } = field
                                && alias.as_ref() == Some(&col.name)
                                && expr_contains(expr, target)
                            {
                                return true;
                            }
                        }
                    }
                }
                // ORDER BY <n> references the n-th (1-based) SELECT field.
                readyset_sql::ast::FieldReference::Numeric(n) => {
                    let idx = (*n as usize).saturating_sub(1);
                    if let Some(FieldDefinitionExpr::Expr { expr, .. }) = query.fields.get(idx)
                        && expr_contains(expr, target)
                    {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Decompose aggregate functions into simpler components for post-lookup
/// recomputation.
///
/// Scans the SELECT list for aggregate functions that can be decomposed (as
/// defined by [`PostLookupAggregateKind::try_match`]). For each match:
///
/// 1. The first replacement overwrites the original field in place.
/// 2. Subsequent replacements are appended to the SELECT list as new columns.
/// 3. A [`PostLookupDecomposition`] is recorded so the adapter can recompute
///    the original aggregate value after reading results.
///
/// To add a new decomposable aggregate, add a variant to
/// [`PostLookupAggregateKind`] and implement `try_match`, `decompose`,
/// `result_type`, and `compute`.
pub(super) fn decompose_aggregates_for_post_lookup(
    query: &mut SelectStatement,
    dialect: Dialect,
) -> ReadySetResult<Vec<PostLookupDecomposition>> {
    let mut decompositions = Vec::new();

    let mut matches: Vec<(usize, PostLookupAggregateKind, Box<Expr>, SqlIdentifier)> = Vec::new();
    for (i, f) in query.fields.iter().enumerate() {
        let FieldDefinitionExpr::Expr { expr, alias } = f else {
            continue;
        };
        let Expr::Call(func) = expr else {
            continue;
        };
        let Some((kind, inner)) = PostLookupAggregateKind::try_match(func) else {
            continue;
        };
        // If HAVING or ORDER BY references this aggregate, we cannot
        // decompose it (we only rewrite the SELECT list). Surface a
        // clear error rather than letting the MIR layer reject it with
        // a confusing internal message.
        if expr_referenced_in_having_or_order_by(expr, query) {
            unsupported!(
                "AVG referenced in HAVING or ORDER BY is not supported \
                 with post-lookup aggregation (WHERE IN or range conditions)"
            );
        }
        let alias = alias
            .clone()
            .unwrap_or_else(|| SqlIdentifier::from(expr.display(dialect).to_string()));
        matches.push((i, kind, inner, alias));
    }

    for (field_idx, kind, inner_expr, original_alias) in matches {
        let replacements = kind.decompose(inner_expr);
        let mut source_columns = Vec::new();

        for (repl_idx, func) in replacements.into_iter().enumerate() {
            // Check if this replacement function already exists in another field.
            // Reusing avoids duplicate aggregate columns in the dataflow, which can
            // cause incorrect post-lookup re-aggregation when two identical aggregate
            // columns are deduplicated by the dataflow compiler.
            let existing_idx = query.fields.iter().enumerate().position(|(i, f)| {
                if i == field_idx {
                    return false;
                }
                matches!(
                    f,
                    FieldDefinitionExpr::Expr { expr: Expr::Call(existing), .. }
                    if *existing == func
                )
            });

            if let Some(idx) = existing_idx {
                if repl_idx == 0 {
                    // First replacement reused from an existing column. Replace the
                    // original aggregate (e.g. AVG) with a placeholder literal so it
                    // doesn't remain in the query sent to the dataflow engine, which
                    // would reject unsupported aggregates. The placeholder value is
                    // irrelevant — postprocess_decompositions overwrites it.
                    query.fields[field_idx] = FieldDefinitionExpr::Expr {
                        expr: Expr::Literal(Literal::Null),
                        alias: Some(original_alias.clone()),
                    };
                }
                source_columns.push(SourceColumn {
                    field_index: idx,
                    added: false,
                });
            } else if repl_idx == 0 {
                // First replacement with no reuse: overwrite the original field.
                query.fields[field_idx] = FieldDefinitionExpr::Expr {
                    expr: Expr::Call(func),
                    alias: Some(original_alias.clone()),
                };
                source_columns.push(SourceColumn {
                    field_index: field_idx,
                    added: false,
                });
            } else {
                // Subsequent replacement with no reuse: append a new column.
                let appended_idx = query.fields.len();
                query.fields.push(FieldDefinitionExpr::Expr {
                    expr: Expr::Call(func),
                    alias: None,
                });
                source_columns.push(SourceColumn {
                    field_index: appended_idx,
                    added: true,
                });
            }
        }

        decompositions.push(PostLookupDecomposition {
            kind,
            result_index: field_idx,
            source_columns: source_columns.into_boxed_slice(),
            original_alias,
        });
    }

    Ok(decompositions)
}

#[cfg(test)]
mod tests {
    use readyset_data::{DfType, DfValue};

    use super::*;

    #[test]
    fn build_column_plan_no_decompositions() {
        let plan = PostLookupPlan::new(vec![], 5);
        assert!(plan.is_empty());
        assert!(plan.column_plan().is_empty());
        assert!(plan.columns_to_remove().is_empty());
    }

    #[test]
    fn build_column_plan_with_avg() {
        // [col0, SUM(x), col2, COUNT(x), MIN(x)] — decomp at result_index=1
        let plan = PostLookupPlan::new(
            vec![PostLookupDecomposition {
                kind: PostLookupAggregateKind::Avg,
                result_index: 1,
                source_columns: Box::new([
                    SourceColumn {
                        field_index: 1,
                        added: false,
                    },
                    SourceColumn {
                        field_index: 3,
                        added: true,
                    },
                    SourceColumn {
                        field_index: 4,
                        added: true,
                    },
                ]),
                original_alias: SqlIdentifier::from("avg_x"),
            }],
            5,
        );
        assert_eq!(plan.columns_to_remove(), [3, 4]);
        assert_eq!(plan.column_plan().len(), 3);
        assert!(matches!(plan.column_plan()[0], PostLookupColumn::Direct(0)));
        assert!(matches!(
            plan.column_plan()[1],
            PostLookupColumn::Computed(0)
        ));
        assert!(matches!(plan.column_plan()[2], PostLookupColumn::Direct(2)));
    }

    #[test]
    fn compute_avg_basic() {
        let sum = DfValue::Int(100);
        let count = DfValue::Int(4);
        assert_eq!(
            PostLookupAggregateKind::Avg.compute(&[&sum, &count]),
            DfValue::try_from(25.0_f64).unwrap()
        );
    }

    #[test]
    fn compute_avg_null_inputs() {
        assert_eq!(
            PostLookupAggregateKind::Avg.compute(&[&DfValue::None, &DfValue::Int(1)]),
            DfValue::None
        );
        assert_eq!(
            PostLookupAggregateKind::Avg.compute(&[&DfValue::Int(1), &DfValue::None]),
            DfValue::None
        );
    }

    #[test]
    fn compute_avg_zero_count() {
        assert_eq!(
            PostLookupAggregateKind::Avg.compute(&[&DfValue::Int(100), &DfValue::Int(0)]),
            DfValue::None
        );
    }

    #[test]
    fn compute_avg_wrong_source_count() {
        let val = DfValue::Int(42);
        assert_eq!(PostLookupAggregateKind::Avg.compute(&[&val]), DfValue::None);
        assert_eq!(PostLookupAggregateKind::Avg.compute(&[]), DfValue::None);
    }

    #[test]
    fn result_type_avg_mysql_int() {
        let mysql = readyset_data::Dialect::DEFAULT_MYSQL;
        let types: Vec<&DfType> = vec![&DfType::BigInt, &DfType::BigInt, &DfType::Int];
        assert_eq!(
            PostLookupAggregateKind::Avg.result_type(&types, mysql),
            DfType::Numeric { prec: 14, scale: 4 },
        );
    }

    #[test]
    fn result_type_avg_mysql_float() {
        let mysql = readyset_data::Dialect::DEFAULT_MYSQL;
        let types: Vec<&DfType> = vec![&DfType::Double, &DfType::BigInt, &DfType::Float];
        assert_eq!(
            PostLookupAggregateKind::Avg.result_type(&types, mysql),
            DfType::Double,
        );
    }

    #[test]
    fn result_type_avg_pg_int() {
        let pg = readyset_data::Dialect::DEFAULT_POSTGRESQL;
        let types: Vec<&DfType> = vec![&DfType::BigInt, &DfType::BigInt, &DfType::Int];
        assert_eq!(
            PostLookupAggregateKind::Avg.result_type(&types, pg),
            DfType::DEFAULT_NUMERIC,
        );
    }

    #[test]
    fn result_type_avg_pg_float() {
        let pg = readyset_data::Dialect::DEFAULT_POSTGRESQL;
        let types: Vec<&DfType> = vec![&DfType::Double, &DfType::BigInt, &DfType::Double];
        assert_eq!(
            PostLookupAggregateKind::Avg.result_type(&types, pg),
            DfType::Double,
        );
    }

    #[test]
    fn result_type_avg_mysql_bigint() {
        let mysql = readyset_data::Dialect::DEFAULT_MYSQL;
        let types: Vec<&DfType> = vec![&DfType::BigInt, &DfType::BigInt, &DfType::BigInt];
        assert_eq!(
            PostLookupAggregateKind::Avg.result_type(&types, mysql),
            DfType::Numeric { prec: 14, scale: 4 },
        );
    }

    #[test]
    fn result_type_avg_mysql_decimal() {
        let mysql = readyset_data::Dialect::DEFAULT_MYSQL;
        let dec_type = DfType::Numeric { prec: 10, scale: 2 };
        let types: Vec<&DfType> = vec![&DfType::DEFAULT_NUMERIC, &DfType::BigInt, &dec_type];
        assert_eq!(
            PostLookupAggregateKind::Avg.result_type(&types, mysql),
            DfType::Numeric { prec: 14, scale: 6 },
        );
    }

    #[test]
    fn compute_avg_float_zero_count() {
        // Ensure zero-count guard works for Float type, not just Int.
        assert_eq!(
            PostLookupAggregateKind::Avg.compute(&[&DfValue::Float(100.0), &DfValue::Float(0.0)]),
            DfValue::None,
        );
    }

    #[test]
    fn compute_avg_three_sources_ignores_third() {
        // 3 sources is valid (SUM, COUNT, MIN) — the third is ignored.
        let val = DfValue::Int(42);
        assert_eq!(
            PostLookupAggregateKind::Avg.compute(&[&val, &val, &val]),
            DfValue::try_from(1.0_f64).unwrap(),
        );
    }
}
