//! SQL rewrite pass that decomposes aggregates for post-lookup aggregation.
//!
//! When queries have `WHERE IN` or range conditions, the adapter reads from
//! the reader node multiple times and consolidates results via PLA. Some
//! aggregates (e.g. `AVG`) cannot be naively re-aggregated, so this pass
//! rewrites them in the SELECT list into simpler components (`SUM`, `COUNT`,
//! `MIN`) that *can* be re-aggregated. The adapter recomputes the original
//! value after the read in
//! `readyset_client::post_processing::post_lookup::postprocess_decompositions`.
//!
//! Plan data types (`PostLookupPlan`, `PostLookupAggregateKind`, etc.) live
//! in the readyset-post-lookup crate.

use readyset_errors::{ReadySetResult, unsupported};
use readyset_post_lookup::{PostLookupAggregateKind, PostLookupDecomposition, SourceColumn};
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
