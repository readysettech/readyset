//! Plan data types for post-lookup aggregate decomposition.
//!
//! These types describe *what* decomposition was applied and *how* to
//! recompose the result after the read iterator returns. The planner that
//! produces a [`PostLookupPlan`] is a SQL rewrite pass and lives in
//! readyset-sql-passes (`adapter_rewrites::post_lookup_decomposition`). The
//! plan is consumed at read time by readyset-client's
//! `post_processing::post_lookup::postprocess_decompositions`.

use readyset_data::{AverageAccumulator, AvgScaleMode, DfType, DfValue, Dialect as DataDialect};
use readyset_errors::{ReadySetResult, internal};
use readyset_sql::ast::{Expr, FunctionExpr, SqlIdentifier};
use serde::{Deserialize, Serialize};

/// Identifies which aggregate function was decomposed for post-lookup
/// recomputation.
///
/// Each variant knows how to decompose itself into simpler aggregates
/// ([`Self::decompose`]), compute the final value from those components
/// ([`Self::compute`]), and determine the result type ([`Self::result_type`]).
///
/// To add a new decomposable aggregate (e.g. STDDEV), add a variant here and
/// implement the four methods.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostLookupAggregateKind {
    /// `AVG(x)` decomposed into `SUM(x)`, `COUNT(x)`, and `MIN(x)`.
    ///
    /// `source_columns` order: `[SUM, COUNT, MIN]`.
    /// `MIN(x)` is a type-preserving sentinel whose *value* is unused — only
    /// its [`DfType`] matters for computing the dialect-correct output type.
    Avg,
}

/// A source column that feeds into a decomposed aggregate computation.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
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
    pub fn try_match(func: &FunctionExpr) -> Option<(Self, Box<Expr>)> {
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
    pub fn decompose(&self, expr: Box<Expr>) -> Vec<FunctionExpr> {
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
        dialect: DataDialect,
    ) -> ReadySetResult<DfType> {
        match self {
            Self::Avg => {
                // Uses the third col (MIN) to recover the type, since MIN is type-preserving.
                let Some(expr_type) = source_col_types.get(2).copied() else {
                    internal!(
                        "post-lookup AVG result_type: expected 3 source columns (SUM, COUNT, MIN), got {}",
                        source_col_types.len()
                    );
                };
                Ok(match dialect.engine() {
                    readyset_data::SqlEngine::MySQL => DfType::mysql_avg_output_type(expr_type),
                    readyset_data::SqlEngine::PostgreSQL => {
                        if expr_type.is_any_float() {
                            DfType::Double
                        } else {
                            DfType::DEFAULT_NUMERIC
                        }
                    }
                })
            }
        }
    }

    /// Compute the final value from source values (in kind-specific
    /// order). `scale_mode` is pre-derived via [`AvgScaleMode::for_avg`].
    pub fn compute(
        &self,
        sources: &[&DfValue],
        result_type: &DfType,
        scale_mode: AvgScaleMode,
    ) -> ReadySetResult<DfValue> {
        match self {
            Self::Avg => {
                if sources.len() < 2 {
                    internal!(
                        "post-lookup AVG: wrong number of source columns (expected >=2, got {})",
                        sources.len()
                    );
                }
                Self::compute_avg(sources[0], sources[1], result_type, scale_mode)
            }
        }
    }

    /// AVG = SUM / COUNT rounded per `scale_mode`. Returns NULL when
    /// either input is NULL, count is zero, or the COUNT cannot be
    /// converted to `i64`.
    fn compute_avg(
        sum: &DfValue,
        count: &DfValue,
        result_type: &DfType,
        scale_mode: AvgScaleMode,
    ) -> ReadySetResult<DfValue> {
        if sum.is_none() || count.is_none() {
            return Ok(DfValue::None);
        }
        let Ok(count_i64) = i64::try_from(count) else {
            return Ok(DfValue::None);
        };
        if count_i64 == 0 {
            return Ok(DfValue::None);
        }
        AverageAccumulator::from_totals(result_type, scale_mode, sum, count_i64)?.result()
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
