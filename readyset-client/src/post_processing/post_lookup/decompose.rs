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
use readyset_sql_passes::adapter_rewrites::{
    PostLookupColumn, PostLookupDecomposition, PostLookupPlan,
};
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
                    schema.get(s.field_index).map(|cs| &cs.column_type).ok_or_else(|| {
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

/// Update result column types in the schema for post-lookup decompositions.
///
/// For each decomposition, computes the correct result type (e.g. AVG of
/// integer columns → Numeric) and updates the schema entry. This must be
/// called BEFORE column removal since `result_index` refers to positions
/// in the original schema.
///
/// Shared between prepare (RowDescription) and execute
/// (postprocess_decompositions) so they always declare the same types.
pub fn apply_post_lookup_type_transforms(
    schema: &mut [ColumnSchema],
    plan: &PostLookupPlan,
    dialect: Dialect,
) -> ReadySetResult<()> {
    let decompositions = plan.decompositions();
    let result_types = compute_result_types(decompositions, schema, dialect)?;
    for (d, (ty, _scale_mode)) in decompositions.iter().zip(result_types) {
        if d.result_index < schema.len() {
            schema[d.result_index].column_type = ty;
        }
    }
    Ok(())
}

/// Remove added helper columns (e.g. COUNT, MIN) from the schema in
/// reverse index order to preserve earlier indices.
pub fn remove_post_lookup_columns(schema: &mut Vec<ColumnSchema>, plan: &PostLookupPlan) {
    for &idx in plan.columns_to_remove().iter().rev() {
        if idx < schema.len() {
            schema.remove(idx);
        }
    }
}

/// Apply post-lookup decompositions to transform result rows and schema.
///
/// For each decomposed aggregate (e.g. AVG decomposed into SUM + COUNT), this:
/// 1. Computes the final aggregate value from source columns (e.g. SUM / COUNT)
/// 2. Places the computed value at the original result position
/// 3. Removes any columns that were added by the decomposition (e.g. the extra COUNT column)
/// 4. Updates the schema to match
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
        // Pre-compute decomposed values while the row is intact, before
        // any values are moved out by Direct columns below.
        computed.clear();
        // Allocate once per row (borrows row, so can't hoist further),
        // reused across decompositions via clear(). Capacity is taken from
        // the first decomposition; subsequent ones reuse or grow as needed.
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

    // Build output schema/aliases. Schema types must be set before
    // coercion below and before column removal (`result_index` is
    // pre-removal).
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

    // Remove added helper columns from schema and column names
    remove_post_lookup_columns(&mut new_schema_vec, plan);
    for &idx in columns_to_remove.iter().rev() {
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
    use readyset_sql::ast::{self, SqlIdentifier};
    use readyset_sql_passes::adapter_rewrites::{
        PostLookupAggregateKind, PostLookupDecomposition, SourceColumn,
    };

    use super::*;

    // Unit tests for PostLookupPlan construction, compute_avg, and result_type live in
    // readyset-sql-passes::adapter_rewrites::post_lookup_decomposition::tests.
    // Tests below exercise postprocess_decompositions (the row transformation).

    #[test]
    fn postprocess_empty_decompositions() {
        let schema = SelectSchema {
            schema: Cow::Owned(vec![]),
            columns: Cow::Owned(vec![]),
        };
        let rows = ResultIterator::owned(vec![]);

        // No decompositions means pass-through
        let empty_plan = PostLookupPlan::new(vec![], 0);
        let (rows, _schema) =
            postprocess_decompositions(rows, schema, &empty_plan, Dialect::DEFAULT_MYSQL).unwrap();
        assert!(rows.into_vec().is_empty());
    }

    #[test]
    fn postprocess_avg_decomposition() {
        // Simulate: SELECT AVG(x) FROM t  (x is INT)
        // Rewritten to: SELECT SUM(x), COUNT(x), MIN(x) FROM t
        // Row: [SUM=10, COUNT=2, MIN=3]
        // Expected output: [AVG=5.0] with type Numeric{14,4} (MySQL AVG of int)
        let schema = SelectSchema {
            schema: Cow::Owned(vec![
                ColumnSchema {
                    column: ast::Column {
                        name: "sum_x".into(),
                        table: None,
                    },
                    column_type: DfType::BigInt,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "count_x".into(),
                        table: None,
                    },
                    column_type: DfType::BigInt,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "min_x".into(),
                        table: None,
                    },
                    column_type: DfType::Int,
                    base: None,
                },
            ]),
            columns: Cow::Owned(vec!["sum_x".into(), "count_x".into(), "min_x".into()]),
        };
        let rows_data = vec![vec![DfValue::Int(10), DfValue::Int(2), DfValue::Int(3)]];
        let rows = ResultIterator::owned(vec![Results::new(rows_data)]);

        let plan = PostLookupPlan::new(
            vec![PostLookupDecomposition {
                kind: PostLookupAggregateKind::Avg,
                result_index: 0,
                source_columns: Box::new([
                    SourceColumn {
                        field_index: 0,
                        added: false,
                    },
                    SourceColumn {
                        field_index: 1,
                        added: true,
                    },
                    SourceColumn {
                        field_index: 2,
                        added: true,
                    },
                ]),
                original_alias: SqlIdentifier::from("avg_x"),
            }],
            3,
        );

        let (rows, schema) =
            postprocess_decompositions(rows, schema, &plan, Dialect::DEFAULT_MYSQL).unwrap();
        let data = rows.into_vec();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].len(), 1); // COUNT and MIN columns removed
        assert_eq!(data[0][0], DfValue::try_from(5.0_f64).unwrap());

        // Schema should have 1 column with correct name and type.
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0], SqlIdentifier::from("avg_x"));
        // MySQL AVG(int) -> Numeric{14,4}
        assert_eq!(
            schema.schema[0].column_type,
            DfType::Numeric { prec: 14, scale: 4 }
        );
    }

    #[test]
    fn postprocess_mixed_columns_with_avg() {
        // Simulate: SELECT id, AVG(x), name FROM t GROUP BY id, name  (x is INT)
        // Rewritten to: SELECT id, SUM(x), name, COUNT(x), MIN(x) FROM t GROUP BY id, name
        // Row: [1, 30, "alice", 3, 5]
        // Expected output: [1, 10.0, "alice"]
        let schema = SelectSchema {
            schema: Cow::Owned(vec![
                ColumnSchema {
                    column: ast::Column {
                        name: "id".into(),
                        table: None,
                    },
                    column_type: DfType::Int,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "sum_x".into(),
                        table: None,
                    },
                    column_type: DfType::BigInt,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "name".into(),
                        table: None,
                    },
                    column_type: DfType::DEFAULT_TEXT,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "count_x".into(),
                        table: None,
                    },
                    column_type: DfType::BigInt,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "min_x".into(),
                        table: None,
                    },
                    column_type: DfType::Int,
                    base: None,
                },
            ]),
            columns: Cow::Owned(vec![
                "id".into(),
                "sum_x".into(),
                "name".into(),
                "count_x".into(),
                "min_x".into(),
            ]),
        };
        let rows_data = vec![vec![
            DfValue::Int(1),
            DfValue::Int(30),
            DfValue::from("alice"),
            DfValue::Int(3),
            DfValue::Int(5),
        ]];
        let rows = ResultIterator::owned(vec![Results::new(rows_data)]);

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

        let (rows, schema) =
            postprocess_decompositions(rows, schema, &plan, Dialect::DEFAULT_MYSQL).unwrap();
        let data = rows.into_vec();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].len(), 3); // 5 columns - 2 removed = 3
        assert_eq!(data[0][0], DfValue::Int(1));
        assert_eq!(data[0][1], DfValue::try_from(10.0_f64).unwrap());
        assert_eq!(data[0][2], DfValue::from("alice"));

        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[1], SqlIdentifier::from("avg_x"));
        // MySQL AVG(int) -> Numeric{14,4}
        assert_eq!(
            schema.schema[1].column_type,
            DfType::Numeric { prec: 14, scale: 4 }
        );
    }

    #[test]
    fn postprocess_multiple_avg_decompositions() {
        // Simulate: SELECT AVG(x), AVG(y) FROM t  (x is INT, y is FLOAT)
        // Rewritten to: SELECT SUM(x), SUM(y), COUNT(x), COUNT(y), MIN(x), MIN(y)
        // Row: [SUM(x)=20, SUM(y)=90.0, COUNT(x)=4, COUNT(y)=3, MIN(x)=2, MIN(y)=10.0]
        // Expected: [AVG(x)=5.0, AVG(y)=30.0]
        let schema = SelectSchema {
            schema: Cow::Owned(vec![
                ColumnSchema {
                    column: ast::Column {
                        name: "sum_x".into(),
                        table: None,
                    },
                    column_type: DfType::BigInt,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "sum_y".into(),
                        table: None,
                    },
                    column_type: DfType::Double,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "count_x".into(),
                        table: None,
                    },
                    column_type: DfType::BigInt,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "count_y".into(),
                        table: None,
                    },
                    column_type: DfType::BigInt,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "min_x".into(),
                        table: None,
                    },
                    column_type: DfType::Int,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "min_y".into(),
                        table: None,
                    },
                    column_type: DfType::Float,
                    base: None,
                },
            ]),
            columns: Cow::Owned(vec![
                "sum_x".into(),
                "sum_y".into(),
                "count_x".into(),
                "count_y".into(),
                "min_x".into(),
                "min_y".into(),
            ]),
        };
        let rows_data = vec![vec![
            DfValue::Int(20),
            DfValue::try_from(90.0_f64).unwrap(),
            DfValue::Int(4),
            DfValue::Int(3),
            DfValue::Int(2),
            DfValue::try_from(10.0_f64).unwrap(),
        ]];
        let rows = ResultIterator::owned(vec![Results::new(rows_data)]);

        let plan = PostLookupPlan::new(
            vec![
                PostLookupDecomposition {
                    kind: PostLookupAggregateKind::Avg,
                    result_index: 0,
                    source_columns: Box::new([
                        SourceColumn {
                            field_index: 0,
                            added: false,
                        },
                        SourceColumn {
                            field_index: 2,
                            added: true,
                        },
                        SourceColumn {
                            field_index: 4,
                            added: true,
                        },
                    ]),
                    original_alias: SqlIdentifier::from("avg_x"),
                },
                PostLookupDecomposition {
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
                            field_index: 5,
                            added: true,
                        },
                    ]),
                    original_alias: SqlIdentifier::from("avg_y"),
                },
            ],
            6,
        );

        let (rows, schema) =
            postprocess_decompositions(rows, schema, &plan, Dialect::DEFAULT_MYSQL).unwrap();
        let data = rows.into_vec();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].len(), 2); // COUNT and MIN columns removed
                                      // AVG(x) = 20/4 = 5.0, coerced to Numeric (int input)
        assert!(matches!(&data[0][0], DfValue::Numeric(_)));
        // AVG(y) = 90/3 = 30.0, stays Double (float input)
        assert_eq!(data[0][1], DfValue::try_from(30.0_f64).unwrap());

        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0], SqlIdentifier::from("avg_x"));
        assert_eq!(schema.columns[1], SqlIdentifier::from("avg_y"));
        // MySQL AVG(int) -> Numeric{14,4}, AVG(float) -> Double
        assert_eq!(
            schema.schema[0].column_type,
            DfType::Numeric { prec: 14, scale: 4 }
        );
        assert_eq!(schema.schema[1].column_type, DfType::Double);
    }

    /// The execute path (postprocess_decompositions) fixes the schema type for
    /// decomposed AVG from BigInt (SUM column) to NUMERIC. The prepare path
    /// must apply the same fix, otherwise RowDescription says INT8 but DataRow
    /// sends NUMERIC, causing "invalid buffer size" in the PostgreSQL binary
    /// protocol.
    #[test]
    fn postprocess_decompositions_changes_avg_type_from_view_schema() {
        // View returns: [SUM(x) BigInt, COUNT(x) BigInt, MIN(x) Int]
        let view_schema = vec![
            ColumnSchema {
                column: ast::Column {
                    name: "sum_x".into(),
                    table: None,
                },
                column_type: DfType::BigInt,
                base: None,
            },
            ColumnSchema {
                column: ast::Column {
                    name: "count_x".into(),
                    table: None,
                },
                column_type: DfType::BigInt,
                base: None,
            },
            ColumnSchema {
                column: ast::Column {
                    name: "min_x".into(),
                    table: None,
                },
                column_type: DfType::Int,
                base: None,
            },
        ];

        let plan = PostLookupPlan::new(
            vec![PostLookupDecomposition {
                kind: PostLookupAggregateKind::Avg,
                result_index: 0,
                source_columns: Box::new([
                    SourceColumn {
                        field_index: 0,
                        added: false,
                    },
                    SourceColumn {
                        field_index: 1,
                        added: true,
                    },
                    SourceColumn {
                        field_index: 2,
                        added: true,
                    },
                ]),
                original_alias: SqlIdentifier::from("avg_x"),
            }],
            3,
        );

        // Raw view schema at result_index 0 is BigInt (the SUM column).
        // This is what prepare_select would return WITHOUT the fix.
        assert_eq!(view_schema[0].column_type, DfType::BigInt);

        // postprocess_decompositions changes it to NUMERIC.
        let rows = ResultIterator::owned(vec![Results::new(vec![vec![
            DfValue::Int(10),
            DfValue::Int(2),
            DfValue::Int(3),
        ]])]);
        let schema = SelectSchema {
            schema: Cow::Owned(view_schema),
            columns: Cow::Owned(vec!["sum_x".into(), "count_x".into(), "min_x".into()]),
        };
        let (_rows, schema) =
            postprocess_decompositions(rows, schema, &plan, Dialect::DEFAULT_POSTGRESQL).unwrap();
        assert_eq!(schema.schema.len(), 1);
        // PostgreSQL AVG(int) -> NUMERIC, not BigInt.
        // prepare_select must return this same type in RowDescription.
        assert_eq!(schema.schema[0].column_type, DfType::DEFAULT_NUMERIC);
    }
}
