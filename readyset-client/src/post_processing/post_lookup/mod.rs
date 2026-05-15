//! Operations applied to reader results after a lookup returns.
//!
//! [`spec`] defines the static spec stored on the reader node.
//! [`iter`] implements the streaming pipeline driven by that spec.
//! [`decompose`] recomposes aggregates that were split at query-rewrite time
//! (e.g. `AVG` to `SUM`/`COUNT`/`MIN`).
//! [`run_post_processing_pipeline`] composes every stage in one function.

pub mod decompose;
pub mod iter;
pub mod spec;

use dataflow_expression::Expr;
use readyset_data::Dialect;
use readyset_errors::ReadySetResult;

use crate::schema::ColumnSchema;

pub use decompose::{
    apply_post_lookup_to_prepared_schema, postprocess_decompositions, transform_schema,
};
pub use iter::{
    effective_aggregates, Key, ReadReplyStats, ResultIterator, Results, Row, SharedResults,
    SharedRows,
};
pub use readyset_post_lookup::PostLookupPlan;
pub use spec::{
    PostLookup, PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates,
    PostLookupDistinct,
};

/// Run every post-lookup stage over `rows`, in order: the runtime order is the
/// textual order of the calls below. `filter` is routed inside
/// [`ResultIterator::pipeline`] so it applies with the correct semantics relative
/// to aggregation; everything else is an outer stage.
///
/// `result_schema` is the pre-decompose reader schema used by the recompose stage
/// to look up source column types. The client-facing schema is rebuilt
/// independently on the adapter via [`transform_schema`].
#[allow(clippy::too_many_arguments)]
pub fn run_post_processing_pipeline(
    rows: SharedResults,
    result_schema: &[ColumnSchema],
    post_lookup: &PostLookup,
    plan: Option<&PostLookupPlan>,
    limit: Option<usize>,
    offset: Option<usize>,
    filter: Option<Expr>,
    dialect: Dialect,
) -> ReadySetResult<ResultIterator> {
    let rows = ResultIterator::pipeline(
        rows,
        post_lookup.order_by.as_ref(),
        effective_aggregates(post_lookup),
        filter,
        post_lookup.default_row.as_ref(),
    );

    let rows = match plan {
        Some(plan) if !plan.is_empty() => {
            postprocess_decompositions(rows, result_schema, plan, dialect)?
        }
        _ => rows,
    };

    // Dedup runs *after* recompose so the HashSet sees recomposed `AVG` values,
    // not raw `(SUM, COUNT, MIN)` triples (REA-6581).
    let rows = if matches!(post_lookup.distinct, PostLookupDistinct::HashBased) {
        rows.with_hash_dedup()
    } else {
        rows
    };

    let rows = rows
        .with_limit(limit.or(post_lookup.limit))
        .with_offset(offset)
        .with_projection(post_lookup.returned_cols.as_deref());

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use readyset_data::{DfType, DfValue};
    use readyset_post_lookup::{PostLookupAggregateKind, PostLookupDecomposition, SourceColumn};
    use readyset_sql::ast::{Column, SqlIdentifier};
    use smallvec::SmallVec;

    use super::*;
    use crate::schema::ColumnSchema;

    fn shared(rows: Vec<Vec<Vec<DfValue>>>) -> SharedResults {
        rows.into_iter()
            .map(|set| {
                let boxed: SmallVec<[Row; 1]> =
                    set.into_iter().map(|r| r.into_boxed_slice()).collect();
                triomphe::Arc::new(boxed)
            })
            .collect()
    }

    fn col_schema(name: &str, ty: DfType) -> ColumnSchema {
        ColumnSchema {
            column: Column {
                name: name.into(),
                table: None,
            },
            column_type: ty,
            base: None,
        }
    }

    /// Baseline: no plan, no distinct, no aggregates; rows in, rows out.
    #[test]
    fn empty_plan_passes_rows_through() {
        let data = shared(vec![vec![
            vec![DfValue::from(1i64), DfValue::from("a")],
            vec![DfValue::from(2i64), DfValue::from("b")],
        ]]);

        let rows = run_post_processing_pipeline(
            data,
            &[],
            &PostLookup::default(),
            None,
            None,
            None,
            None,
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()
        .into_vec();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![DfValue::from(1i64), DfValue::from("a")]);
        assert_eq!(rows[1], vec![DfValue::from(2i64), DfValue::from("b")]);
    }

    /// Caller-supplied `limit` takes precedence over `post_lookup.limit`, and
    /// `offset` slices into the input set.
    #[test]
    fn applies_limit_and_offset() {
        let data = shared(vec![vec![
            vec![DfValue::from(1i64)],
            vec![DfValue::from(2i64)],
            vec![DfValue::from(3i64)],
            vec![DfValue::from(4i64)],
        ]]);
        let post_lookup = PostLookup {
            limit: Some(99), // overridden by caller-supplied limit
            ..PostLookup::default()
        };

        let rows = run_post_processing_pipeline(
            data,
            &[],
            &post_lookup,
            None,
            Some(2),
            Some(1),
            None,
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()
        .into_vec();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], DfValue::from(2i64));
        assert_eq!(rows[1][0], DfValue::from(3i64));
    }

    /// HashBased distinct with no plan: dedup is a streaming outer stage.
    #[test]
    fn hash_based_distinct_no_plan() {
        let data = shared(vec![vec![
            vec![DfValue::from(1i64), DfValue::from("a")],
            vec![DfValue::from(1i64), DfValue::from("a")],
            vec![DfValue::from(2i64), DfValue::from("b")],
        ]]);
        let post_lookup = PostLookup {
            distinct: PostLookupDistinct::HashBased,
            ..PostLookup::default()
        };

        let rows = run_post_processing_pipeline(
            data,
            &[],
            &post_lookup,
            None,
            None,
            None,
            None,
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()
        .into_vec();

        assert_eq!(rows.len(), 2);
    }

    /// Sorted DISTINCT collapses consecutive duplicates via the synthetic
    /// aggregate path (group_by = all projected columns).
    #[test]
    fn sorted_distinct_collapses_consecutive_duplicates() {
        let data = shared(vec![vec![
            vec![DfValue::from(1i64), DfValue::from("a")],
            vec![DfValue::from(1i64), DfValue::from("a")],
            vec![DfValue::from(2i64), DfValue::from("b")],
        ]]);
        let post_lookup = PostLookup {
            distinct: PostLookupDistinct::Sorted {
                dedup_aggregates: PostLookupAggregates {
                    group_by: vec![0, 1],
                    aggregates: vec![],
                },
            },
            ..PostLookup::default()
        };

        let rows = run_post_processing_pipeline(
            data,
            &[],
            &post_lookup,
            None,
            None,
            None,
            None,
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()
        .into_vec();

        assert_eq!(rows.len(), 2);
    }

    fn avg_plan(sum_idx: usize, count_idx: usize, min_idx: usize, alias: &str) -> PostLookupPlan {
        PostLookupPlan::new(
            vec![PostLookupDecomposition {
                kind: PostLookupAggregateKind::Avg,
                result_index: sum_idx,
                source_columns: Box::new([
                    SourceColumn {
                        field_index: sum_idx,
                        added: false,
                    },
                    SourceColumn {
                        field_index: count_idx,
                        added: true,
                    },
                    SourceColumn {
                        field_index: min_idx,
                        added: true,
                    },
                ]),
                original_alias: SqlIdentifier::from(alias),
            }],
            min_idx + 1,
        )
    }

    /// Non-empty plan: orchestrator hands the rows to `postprocess_decompositions`
    /// and the result has recomposed AVG values + COUNT/MIN columns stripped.
    #[test]
    fn recompose_with_plan() {
        // Row layout: [SUM(x)=10, COUNT(x)=2, MIN(x)=3] -> AVG = 5.0
        let data = shared(vec![vec![vec![
            DfValue::Int(10),
            DfValue::Int(2),
            DfValue::Int(3),
        ]]]);
        let result_schema = vec![
            col_schema("sum_x", DfType::BigInt),
            col_schema("count_x", DfType::BigInt),
            col_schema("min_x", DfType::Int),
        ];
        let plan = avg_plan(0, 1, 2, "avg_x");

        let rows = run_post_processing_pipeline(
            data,
            &result_schema,
            &PostLookup::default(),
            Some(&plan),
            None,
            None,
            None,
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()
        .into_vec();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1, "COUNT and MIN stripped");
        assert_eq!(rows[0][0], DfValue::try_from(5.0_f64).unwrap());
    }

    /// REA-6581 guard: when HashBased distinct is combined with a non-empty
    /// plan, dedup must run *after* recompose so two groups whose raw
    /// (SUM, COUNT) differ but whose recomposed AVG agrees both collapse to
    /// one row.
    #[test]
    fn hash_dedup_runs_after_recompose() {
        // Two groups with identical AVG=5.0 but different SUM/COUNT triples.
        let data = shared(vec![vec![
            vec![DfValue::Int(10), DfValue::Int(2), DfValue::Int(3)], // AVG = 5
            vec![DfValue::Int(20), DfValue::Int(4), DfValue::Int(3)], // AVG = 5
        ]]);
        let result_schema = vec![
            col_schema("sum_x", DfType::BigInt),
            col_schema("count_x", DfType::BigInt),
            col_schema("min_x", DfType::Int),
        ];
        let plan = avg_plan(0, 1, 2, "avg_x");
        let post_lookup = PostLookup {
            distinct: PostLookupDistinct::HashBased,
            ..PostLookup::default()
        };

        let rows = run_post_processing_pipeline(
            data,
            &result_schema,
            &post_lookup,
            Some(&plan),
            None,
            None,
            None,
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()
        .into_vec();

        assert_eq!(
            rows.len(),
            1,
            "duplicate recomposed AVG values must collapse"
        );
        assert_eq!(rows[0][0], DfValue::try_from(5.0_f64).unwrap());
    }

    /// An empty `data` set with `default_row` set yields the default row exactly
    /// once. Regression guard for the builder collapse: the orchestrator must
    /// not double-emit.
    #[test]
    fn empty_input_emits_default_row_once() {
        let default = std::sync::Arc::new(Box::new([DfValue::Int(42)]) as Box<[DfValue]>);
        let post_lookup = PostLookup {
            default_row: Some(default),
            ..PostLookup::default()
        };

        let rows = run_post_processing_pipeline(
            shared(vec![vec![]]),
            &[],
            &post_lookup,
            None,
            None,
            None,
            None,
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()
        .into_vec();

        assert_eq!(rows, vec![vec![DfValue::Int(42)]]);
    }

    /// `returned_cols` truncates each emitted row to the projected width.
    #[test]
    fn projection_truncates_rows() {
        let data = shared(vec![vec![
            vec![DfValue::Int(1), DfValue::from("a"), DfValue::Int(99)],
            vec![DfValue::Int(2), DfValue::from("b"), DfValue::Int(99)],
        ]]);
        let post_lookup = PostLookup {
            returned_cols: Some(vec![0, 1]),
            ..PostLookup::default()
        };

        let rows = run_post_processing_pipeline(
            data,
            &[],
            &post_lookup,
            None,
            None,
            None,
            None,
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()
        .into_vec();

        assert!(rows.iter().all(|r| r.len() == 2));
    }
}
