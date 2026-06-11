//! Decomposition of aggregate functions for post-lookup aggregation (PLA).
//!
//! When queries have WHERE IN or range conditions, the adapter reads from the reader node
//! multiple times and consolidates results via PLA. Some aggregates (e.g. AVG) cannot be
//! naively re-aggregated, so this module rewrites them into simpler components (SUM, COUNT)
//! that *can* be re-aggregated, then recomputes the original value in
//! [`postprocess_decompositions`](crate::adapter_rewrites::postprocess_decompositions).

mod plan;

pub use self::plan::{
    PostLookupAggregateKind, PostLookupColumn, PostLookupDecomposition, PostLookupPlan,
    SourceColumn,
};

use readyset_errors::{ReadySetResult, unsupported};
use readyset_sql::analysis::visit::{self, Visitor};
use readyset_sql::ast::{Expr, FieldDefinitionExpr, Literal, SelectStatement, SqlIdentifier};
use readyset_sql::{Dialect, DialectDisplay};

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
    use readyset_data::{AvgScaleMode, DfType, DfValue, Dialect as DataDialect};

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

    fn avg_compute(sources: &[&DfValue]) -> ReadySetResult<DfValue> {
        PostLookupAggregateKind::Avg.compute(
            sources,
            &DfType::Numeric { prec: 14, scale: 4 },
            AvgScaleMode::Fixed(4),
        )
    }

    #[test]
    fn compute_avg_basic() {
        let sum = DfValue::Int(100);
        let count = DfValue::Int(4);
        assert_eq!(
            avg_compute(&[&sum, &count]).unwrap(),
            DfValue::try_from(25.0_f64).unwrap()
        );
    }

    #[test]
    fn compute_avg_null_inputs() {
        assert_eq!(
            avg_compute(&[&DfValue::None, &DfValue::Int(1)]).unwrap(),
            DfValue::None
        );
        assert_eq!(
            avg_compute(&[&DfValue::Int(1), &DfValue::None]).unwrap(),
            DfValue::None
        );
    }

    #[test]
    fn compute_avg_zero_count() {
        assert_eq!(
            avg_compute(&[&DfValue::Int(100), &DfValue::Int(0)]).unwrap(),
            DfValue::None
        );
    }

    #[test]
    fn compute_avg_wrong_source_count() {
        let val = DfValue::Int(42);
        assert!(avg_compute(&[&val]).is_err());
        assert!(avg_compute(&[]).is_err());
    }

    #[test]
    fn result_type_avg_mysql_int() {
        let types: Vec<&DfType> = vec![&DfType::BigInt, &DfType::BigInt, &DfType::Int];
        assert_eq!(
            PostLookupAggregateKind::Avg
                .result_type(&types, DataDialect::DEFAULT_MYSQL)
                .unwrap(),
            DfType::Numeric { prec: 14, scale: 4 },
        );
    }

    #[test]
    fn result_type_avg_mysql_float() {
        let types: Vec<&DfType> = vec![&DfType::Double, &DfType::BigInt, &DfType::Float];
        assert_eq!(
            PostLookupAggregateKind::Avg
                .result_type(&types, DataDialect::DEFAULT_MYSQL)
                .unwrap(),
            DfType::Double,
        );
    }

    #[test]
    fn result_type_avg_pg_int() {
        let types: Vec<&DfType> = vec![&DfType::BigInt, &DfType::BigInt, &DfType::Int];
        assert_eq!(
            PostLookupAggregateKind::Avg
                .result_type(&types, DataDialect::DEFAULT_POSTGRESQL)
                .unwrap(),
            DfType::DEFAULT_NUMERIC,
        );
    }

    #[test]
    fn result_type_avg_pg_float() {
        let types: Vec<&DfType> = vec![&DfType::Double, &DfType::BigInt, &DfType::Double];
        assert_eq!(
            PostLookupAggregateKind::Avg
                .result_type(&types, DataDialect::DEFAULT_POSTGRESQL)
                .unwrap(),
            DfType::Double,
        );
    }

    #[test]
    fn result_type_avg_mysql_bigint() {
        let types: Vec<&DfType> = vec![&DfType::BigInt, &DfType::BigInt, &DfType::BigInt];
        assert_eq!(
            PostLookupAggregateKind::Avg
                .result_type(&types, DataDialect::DEFAULT_MYSQL)
                .unwrap(),
            DfType::Numeric { prec: 23, scale: 4 },
        );
    }

    #[test]
    fn result_type_avg_mysql_decimal() {
        let dec_type = DfType::Numeric { prec: 10, scale: 2 };
        let types: Vec<&DfType> = vec![&DfType::DEFAULT_NUMERIC, &DfType::BigInt, &dec_type];
        assert_eq!(
            PostLookupAggregateKind::Avg
                .result_type(&types, DataDialect::DEFAULT_MYSQL)
                .unwrap(),
            DfType::Numeric { prec: 14, scale: 6 },
        );
    }

    #[test]
    fn result_type_avg_fewer_than_three_sources_errors() {
        let types: Vec<&DfType> = vec![&DfType::BigInt, &DfType::BigInt];
        assert!(
            PostLookupAggregateKind::Avg
                .result_type(&types, DataDialect::DEFAULT_MYSQL)
                .is_err()
        );
    }

    #[test]
    fn compute_avg_float_zero_count() {
        // Ensure zero-count guard works for Float type, not just Int.
        assert_eq!(
            avg_compute(&[&DfValue::Float(100.0), &DfValue::Float(0.0)]).unwrap(),
            DfValue::None,
        );
    }

    #[test]
    fn compute_avg_three_sources_ignores_third() {
        // 3 sources is valid (SUM, COUNT, MIN) — the third is ignored.
        let val = DfValue::Int(42);
        assert_eq!(
            avg_compute(&[&val, &val, &val]).unwrap(),
            DfValue::try_from(1.0_f64).unwrap(),
        );
    }
}
