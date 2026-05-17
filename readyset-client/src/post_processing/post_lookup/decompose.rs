//! Recompose decomposed aggregates back into the client-visible schema.
//!
//! `readyset-sql-passes` may split an aggregate like `AVG(x)` into
//! `(SUM, COUNT, MIN)` so post-lookup aggregation re-aggregates correctly.
//! After the read iterator returns, this module recomputes the original
//! values, fixes up types (e.g. `BigInt` to `Numeric` for MySQL AVG of ints),
//! and strips helper columns. [`postprocess_decompositions`] is the entry
//! point.

use std::borrow::Cow;
use std::mem;

use readyset_data::{AvgScaleMode, DfType, DfValue, Dialect};
use readyset_errors::{internal_err, ReadySetResult};
use readyset_post_lookup::{PostLookupColumn, PostLookupDecomposition, PostLookupPlan};
use tracing::{error, warn};

use crate::post_processing::post_lookup::iter::{ResultIterator, Results};
use crate::schema::{ColumnSchema, SelectSchema};

/// Result type and AVG scale-mode policy per decomposition (positional).
fn compute_result_types(
    decompositions: &[PostLookupDecomposition],
    schema: &[ColumnSchema],
    dialect: Dialect,
) -> ReadySetResult<Vec<(DfType, AvgScaleMode)>> {
    decompositions
        .iter()
        .map(|d| {
            let source_types: Vec<&DfType> = d
                .source_columns
                .iter()
                .map(|s| {
                    schema
                        .get(s.field_index)
                        .map(|cs| &cs.column_type)
                        .ok_or_else(|| {
                            internal_err!(
                                "post-lookup decomposition: schema missing source column {} (schema has {} cols)",
                                s.field_index,
                                schema.len()
                            )
                        })
                })
                .collect::<ReadySetResult<Vec<_>>>()?;
            let result_type = d.kind.result_type(&source_types, dialect)?;
            let scale_mode = AvgScaleMode::for_avg(dialect, &result_type)?;
            Ok((result_type, scale_mode))
        })
        .collect()
}

/// Transform a [`SelectSchema`] to match what the recompose stage will emit:
/// rename aliases at decomposition `result_index`es, widen output types, and
/// strip helper columns from both the schema and the column list.
///
/// Pass-through when `plan` is empty.
pub fn transform_schema<'a>(
    schema: SelectSchema<'a>,
    plan: &PostLookupPlan,
    dialect: Dialect,
) -> ReadySetResult<SelectSchema<'a>> {
    if plan.is_empty() {
        return Ok(schema);
    }

    let decompositions = plan.decompositions();
    let result_types = compute_result_types(decompositions, &schema.schema, dialect)?;

    let mut new_schema_vec = schema.schema.into_owned();
    let mut new_columns_vec = schema.columns.into_owned();

    // Set widened types and updated aliases. Must happen before column
    // removal since `result_index` refers to pre-removal positions.
    for (d, (ty, _)) in decompositions.iter().zip(result_types) {
        if d.result_index < new_schema_vec.len() {
            new_schema_vec[d.result_index].column_type = ty;
        }
        if d.result_index < new_columns_vec.len() {
            new_columns_vec[d.result_index] = d.original_alias.clone();
        }
    }

    for &idx in plan.columns_to_remove().iter().rev() {
        if idx < new_schema_vec.len() {
            new_schema_vec.remove(idx);
        }
        if idx < new_columns_vec.len() {
            new_columns_vec.remove(idx);
        }
    }

    Ok(SelectSchema {
        schema: Cow::Owned(new_schema_vec),
        columns: Cow::Owned(new_columns_vec),
    })
}

/// Transform a raw view schema into the client-facing schema the post-lookup
/// pipeline will produce for this plan.
///
/// Widens decomposed result-column types (e.g. `BigInt` to `Numeric` for MySQL
/// AVG of ints) and strips helper columns added by decomposition.
///
/// Used by the adapter's prepare path so the prepared-statement
/// `RowDescription` declares the same types that
/// [`postprocess_decompositions`] will emit at execute time.
pub fn apply_post_lookup_to_prepared_schema(
    schema: &mut Vec<ColumnSchema>,
    plan: &PostLookupPlan,
    dialect: Dialect,
) -> ReadySetResult<()> {
    if plan.is_empty() {
        return Ok(());
    }
    let decompositions = plan.decompositions();
    let result_types = compute_result_types(decompositions, schema, dialect)?;
    for (d, (ty, _)) in decompositions.iter().zip(result_types) {
        if d.result_index < schema.len() {
            schema[d.result_index].column_type = ty;
        }
    }
    for &idx in plan.columns_to_remove().iter().rev() {
        if idx < schema.len() {
            schema.remove(idx);
        }
    }
    Ok(())
}

/// Apply the decomposition pipeline to a result set returned by the
/// server-side read iterator.
///
/// For each decomposed aggregate (e.g. `AVG` decomposed into `SUM` + `COUNT`):
/// 1. Computes the final aggregate value from source columns (e.g. `SUM/COUNT`)
/// 2. Places the computed value at the original result position
/// 3. Removes columns that were added by the decomposition (e.g. extra COUNT)
/// 4. Coerces the recomposed value to the schema's target type
/// 5. Updates the schema to match
///
/// Pass-through when `plan` is empty.
pub fn postprocess_decompositions<'a>(
    rows: ResultIterator,
    schema: SelectSchema<'a>,
    plan: &PostLookupPlan,
    dialect: Dialect,
) -> ReadySetResult<(ResultIterator, SelectSchema<'a>)> {
    if plan.is_empty() {
        return Ok((rows, schema));
    }

    let decompositions = plan.decompositions();
    let column_plan = plan.column_plan();
    let columns_to_remove = plan.columns_to_remove();

    let result_types = compute_result_types(decompositions, &schema.schema, dialect)?;

    let mut transformed_rows: Vec<Vec<DfValue>> = Vec::new();
    let mut computed: Vec<DfValue> = Vec::with_capacity(decompositions.len());
    for mut row in rows {
        // Pre-compute decomposed values while the row is intact, before any
        // values are moved out by Direct columns below.
        computed.clear();
        // Allocate once per row (borrows row, so can't hoist further), reused
        // across decompositions via clear(). Capacity is taken from the first
        // decomposition; subsequent ones reuse or grow as needed.
        let mut sources: Vec<&DfValue> =
            Vec::with_capacity(decompositions.first().map_or(0, |d| d.source_columns.len()));
        for (d, (result_type, scale_mode)) in decompositions.iter().zip(&result_types) {
            sources.clear();
            for s in d.source_columns.iter() {
                sources.push(row.get(s.field_index).unwrap_or_else(|| {
                    error!(
                        field_index = s.field_index,
                        row_len = row.len(),
                        "post-lookup decomposition: source column OOB"
                    );
                    &DfValue::None
                }));
            }
            computed.push(d.kind.compute(&sources, result_type, *scale_mode)?);
        }

        let mut out_row: Vec<DfValue> = Vec::with_capacity(column_plan.len());
        for col in column_plan {
            out_row.push(match col {
                PostLookupColumn::Direct(idx) => {
                    row.get_mut(*idx).map(mem::take).unwrap_or_else(|| {
                        error!(
                            idx,
                            row_len = row.len(),
                            "post-lookup decomposition: direct column OOB"
                        );
                        DfValue::None
                    })
                }
                PostLookupColumn::Computed(decomp_idx) => mem::take(&mut computed[*decomp_idx]),
            });
        }
        transformed_rows.push(out_row);
    }

    // Build output schema/aliases. Schema types must be set before coercion
    // below and before column removal (`result_index` is pre-removal).
    let mut new_schema_vec = schema.schema.into_owned();
    let mut new_columns_vec = schema.columns.into_owned();
    for (d, (ty, _)) in decompositions.iter().zip(result_types) {
        if d.result_index < new_schema_vec.len() {
            new_schema_vec[d.result_index].column_type = ty;
        }
        if d.result_index < new_columns_vec.len() {
            new_columns_vec[d.result_index] = d.original_alias.clone();
        }
    }

    for d in decompositions {
        if d.result_index < new_schema_vec.len() {
            let target_type = &new_schema_vec[d.result_index].column_type;
            for row in &mut transformed_rows {
                if let Some(val) = row.get(d.result_index).filter(|val| **val != DfValue::None) {
                    match val.coerce_to(target_type, &DfType::Double) {
                        Ok(coerced) => row[d.result_index] = coerced,
                        Err(e) => {
                            warn!(
                                %e,
                                ?val,
                                ?target_type,
                                result_index = d.result_index,
                                "post-lookup decomposition: coercion failed, replacing with NULL"
                            );
                            row[d.result_index] = DfValue::None;
                        }
                    }
                }
            }
        }
    }

    for &idx in columns_to_remove.iter().rev() {
        if idx < new_schema_vec.len() {
            new_schema_vec.remove(idx);
        }
        if idx < new_columns_vec.len() {
            new_columns_vec.remove(idx);
        }
    }

    Ok((
        ResultIterator::owned(vec![Results::new(transformed_rows)]),
        SelectSchema {
            schema: Cow::Owned(new_schema_vec),
            columns: Cow::Owned(new_columns_vec),
        },
    ))
}

#[cfg(test)]
mod tests {
    use readyset_data::{DfType, DfValue};
    use readyset_post_lookup::{PostLookupAggregateKind, PostLookupDecomposition, SourceColumn};
    use readyset_sql::ast::{Column, SqlIdentifier};

    use super::*;

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

    fn select_schema(cols: Vec<(&str, DfType)>) -> SelectSchema<'static> {
        let columns: Vec<SqlIdentifier> = cols.iter().map(|(n, _)| (*n).into()).collect();
        let schema: Vec<ColumnSchema> = cols.into_iter().map(|(n, t)| col_schema(n, t)).collect();
        SelectSchema {
            schema: Cow::Owned(schema),
            columns: Cow::Owned(columns),
        }
    }

    fn make_rows(rows: Vec<Vec<DfValue>>) -> ResultIterator {
        ResultIterator::owned(vec![Results::new(rows)])
    }

    /// AVG(x) decomposed into [SUM, COUNT, MIN] starting at `result_index`.
    fn avg_decomposition(
        result_index: usize,
        sum_idx: usize,
        count_idx: usize,
        min_idx: usize,
        alias: &str,
    ) -> PostLookupDecomposition {
        PostLookupDecomposition {
            kind: PostLookupAggregateKind::Avg,
            result_index,
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
        }
    }

    #[test]
    fn empty_plan_is_passthrough() {
        let schema = select_schema(vec![]);
        let rows = make_rows(vec![]);
        let plan = PostLookupPlan::new(vec![], 0);

        let (rows, _schema) =
            postprocess_decompositions(rows, schema, &plan, Dialect::DEFAULT_MYSQL).unwrap();
        assert!(rows.into_vec().is_empty());
    }

    /// Simulates: SELECT AVG(x) FROM t (x is INT).
    /// Row [SUM=10, COUNT=2, MIN=3] -> [AVG=5.0] with type Numeric{14,4}.
    #[test]
    fn recomposes_single_avg() {
        let schema = select_schema(vec![
            ("sum_x", DfType::BigInt),
            ("count_x", DfType::BigInt),
            ("min_x", DfType::Int),
        ]);
        let rows = make_rows(vec![vec![
            DfValue::Int(10),
            DfValue::Int(2),
            DfValue::Int(3),
        ]]);
        let plan = PostLookupPlan::new(vec![avg_decomposition(0, 0, 1, 2, "avg_x")], 3);

        let (rows, schema) =
            postprocess_decompositions(rows, schema, &plan, Dialect::DEFAULT_MYSQL).unwrap();
        let data = rows.into_vec();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].len(), 1, "COUNT and MIN columns should be stripped");
        assert_eq!(data[0][0], DfValue::try_from(5.0_f64).unwrap());

        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0], SqlIdentifier::from("avg_x"));
        assert_eq!(
            schema.schema[0].column_type,
            DfType::Numeric { prec: 14, scale: 4 }
        );
    }

    /// SELECT id, AVG(x), name -> row [1, 30, "alice", 3, 5] -> [1, 10.0, "alice"].
    #[test]
    fn recomposes_with_mixed_columns() {
        let schema = select_schema(vec![
            ("id", DfType::Int),
            ("sum_x", DfType::BigInt),
            ("name", DfType::DEFAULT_TEXT),
            ("count_x", DfType::BigInt),
            ("min_x", DfType::Int),
        ]);
        let rows = make_rows(vec![vec![
            DfValue::Int(1),
            DfValue::Int(30),
            DfValue::from("alice"),
            DfValue::Int(3),
            DfValue::Int(5),
        ]]);
        let plan = PostLookupPlan::new(vec![avg_decomposition(1, 1, 3, 4, "avg_x")], 5);

        let (rows, schema) =
            postprocess_decompositions(rows, schema, &plan, Dialect::DEFAULT_MYSQL).unwrap();
        let data = rows.into_vec();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].len(), 3);
        assert_eq!(data[0][0], DfValue::Int(1));
        assert_eq!(data[0][1], DfValue::try_from(10.0_f64).unwrap());
        assert_eq!(data[0][2], DfValue::from("alice"));

        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[1], SqlIdentifier::from("avg_x"));
        assert_eq!(
            schema.schema[1].column_type,
            DfType::Numeric { prec: 14, scale: 4 }
        );
    }

    /// SELECT AVG(x), AVG(y) with x INT, y DOUBLE.
    #[test]
    fn recomposes_multiple_avgs() {
        let schema = select_schema(vec![
            ("sum_x", DfType::BigInt),
            ("sum_y", DfType::Double),
            ("count_x", DfType::BigInt),
            ("count_y", DfType::BigInt),
            ("min_x", DfType::Int),
            ("min_y", DfType::Float),
        ]);
        let rows = make_rows(vec![vec![
            DfValue::Int(20),
            DfValue::try_from(90.0_f64).unwrap(),
            DfValue::Int(4),
            DfValue::Int(3),
            DfValue::Int(2),
            DfValue::try_from(10.0_f64).unwrap(),
        ]]);
        let plan = PostLookupPlan::new(
            vec![
                avg_decomposition(0, 0, 2, 4, "avg_x"),
                avg_decomposition(1, 1, 3, 5, "avg_y"),
            ],
            6,
        );

        let (rows, schema) =
            postprocess_decompositions(rows, schema, &plan, Dialect::DEFAULT_MYSQL).unwrap();
        let data = rows.into_vec();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].len(), 2);
        assert!(matches!(&data[0][0], DfValue::Numeric(_)));
        assert_eq!(data[0][1], DfValue::try_from(30.0_f64).unwrap());

        assert_eq!(
            schema.schema[0].column_type,
            DfType::Numeric { prec: 14, scale: 4 }
        );
        assert_eq!(schema.schema[1].column_type, DfType::Double);
    }

    /// The execute path must rewrite the AVG column type from BigInt (the SUM
    /// column) to NUMERIC so that adapter `RowDescription` (from prepare) and
    /// `DataRow` (from execute) declare the same type.
    #[test]
    fn fixes_avg_type_on_postgres() {
        let schema = select_schema(vec![
            ("sum_x", DfType::BigInt),
            ("count_x", DfType::BigInt),
            ("min_x", DfType::Int),
        ]);
        let rows = make_rows(vec![vec![
            DfValue::Int(10),
            DfValue::Int(2),
            DfValue::Int(3),
        ]]);
        let plan = PostLookupPlan::new(vec![avg_decomposition(0, 0, 1, 2, "avg_x")], 3);

        let (_rows, schema) =
            postprocess_decompositions(rows, schema, &plan, Dialect::DEFAULT_POSTGRESQL).unwrap();
        assert_eq!(schema.schema.len(), 1);
        assert_eq!(schema.schema[0].column_type, DfType::DEFAULT_NUMERIC);
    }
}
