use std::convert::TryFrom;
use std::iter;
use std::sync::Arc;

use psql_srv as ps;
use readyset::results::{ResultIterator, Results};
use readyset_data::DataType;
use tokio_postgres::types::Type;

use crate::row::Row;
use crate::schema::{type_to_pgsql, SelectSchema};

/// A structure that contains a `ResultIterator`, as provided by `QueryResult::NoriaSelect`, and
/// facilitates iteration over these results as `Row` values.
pub struct Resultset {
    /// The query result data, comprising nested `Vec`s of rows that may come from separate Noria
    /// interface lookups performed by the backend.
    results: ResultIterator,

    /// The fields to project for each row. A `Results` returned by a Noria interface lookup may
    /// contain extraneous fields that should not be projected into the query result output. In
    /// particular, bogokeys and other lookup keys that are not requested for projection by the SQL
    /// query may be present in `results` but should be excluded from query output. This
    /// `project_fields` attribute contains the indices of the fields that _should_ be projected
    /// into the output.
    project_fields: Arc<Vec<usize>>,

    /// The data types of the projected fields for each row.
    project_field_types: Arc<Vec<Type>>,
}

impl Resultset {
    pub fn try_new(results: ResultIterator, schema: &SelectSchema) -> Result<Self, ps::Error> {
        // Extract the indices of the schema's `schema` items within the schema's `columns` list.
        // Because the ordering of `columns` is the same as the ordering of fields within the rows
        // emitted by a `Results`, these indices also reference the fields within each row
        // corresponding to entries in the schema's `schema`. They are the fields to be projected
        // when iteration is performed over each `Row`'s `Value`s.
        let project_fields = Arc::new(
            schema
                .0
                .schema
                .iter()
                .map(|col| -> Result<usize, ps::Error> {
                    schema
                        .0
                        .columns
                        .iter()
                        .position(|name| col.spec.column.name == name)
                        .ok_or_else(|| ps::Error::InternalError("inconsistent schema".to_string()))
                })
                .collect::<Result<Vec<usize>, ps::Error>>()?,
        );
        // Extract the appropriate `Type` for each column in the schema.
        let project_field_types = Arc::new(
            schema
                .0
                .schema
                .iter()
                .map(|c| type_to_pgsql(&c.spec.sql_type))
                .collect::<Result<Vec<_>, _>>()?,
        );
        Ok(Resultset {
            results,
            project_fields,
            project_field_types,
        })
    }
}

// An iterator over the rows contained within the `Resultset`.
impl IntoIterator for Resultset {
    type Item = Row;
    type IntoIter = impl Iterator<Item = Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.results
            .into_iter()
            .zip(iter::repeat((
                self.project_fields,
                self.project_field_types,
            )))
            .map(|(values, (project_fields, project_field_types))| Row {
                values,
                project_fields,
                project_field_types,
            })
    }
}

impl TryFrom<Vec<tokio_postgres::Row>> for Resultset {
    type Error = psql_srv::Error;

    fn try_from(rows: Vec<tokio_postgres::Row>) -> Result<Self, Self::Error> {
        let columns = match rows.first() {
            Some(row) => row.columns(),
            None => &[],
        };
        let mut result_rows = Vec::new();
        for row in rows.iter() {
            let mut result_row = Vec::new();
            for i in 0..columns.len() {
                let val: DataType = row.try_get(i).map_err(|e| {
                    psql_srv::Error::InternalError(format!(
                        "could not retrieve expected column index {} from row while parsing psql result: {}",
                        i,
                        e
                    ))
                })?;
                result_row.push(val);
            }
            result_rows.push(result_row);
        }
        let column_types: Vec<Type> = columns.iter().map(|c| c.type_().clone()).collect();
        Ok(Resultset {
            results: ResultIterator::owned(vec![Results::new(result_rows)]),
            project_fields: Arc::new((0_usize..columns.len()).collect()),
            project_field_types: Arc::new(column_types),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::convert::TryFrom;

    use nom_sql::{ColumnSpecification, SqlType};
    use readyset::ColumnSchema;
    use readyset_client::backend as cl;
    use readyset_data::DataType;

    use super::*;

    fn collect_resultset_values(resultset: Resultset) -> Vec<Vec<ps::Value>> {
        resultset
            .into_iter()
            .map(|r| {
                r.into_iter()
                    .map(|v| ps::Value::try_from(v).unwrap())
                    .collect::<Vec<ps::Value>>()
            })
            .collect::<Vec<Vec<ps::Value>>>()
    }

    #[test]
    fn create_resultset() {
        let results = vec![];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: false,
            schema: Cow::Owned(vec![ColumnSchema {
                spec: ColumnSpecification {
                    column: "tab1.col1".into(),
                    sql_type: SqlType::BigInt(None),
                    comment: None,
                    constraints: vec![],
                },
                base: None,
            }]),
            columns: Cow::Owned(vec!["col1".into()]),
        });
        let resultset = Resultset::try_new(ResultIterator::owned(results), &schema).unwrap();
        assert_eq!(resultset.results.into_vec(), Vec::<Vec<DataType>>::new());
        assert_eq!(resultset.project_fields, Arc::new(vec![0]));
        assert_eq!(resultset.project_field_types, Arc::new(vec![Type::INT8]));
    }

    #[test]
    fn iterate_resultset() {
        let results = vec![Results::new(vec![vec![DataType::Int(10)]])];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: false,
            schema: Cow::Owned(vec![ColumnSchema {
                spec: ColumnSpecification {
                    column: "tab1.col1".into(),
                    sql_type: SqlType::BigInt(None),
                    comment: None,
                    constraints: vec![],
                },
                base: None,
            }]),
            columns: Cow::Owned(vec!["col1".into()]),
        });
        let resultset = Resultset::try_new(ResultIterator::owned(results), &schema).unwrap();
        assert_eq!(
            collect_resultset_values(resultset),
            vec![vec![ps::Value::BigInt(10)]]
        );
    }

    #[test]
    fn iterate_resultset_with_multiple_results() {
        let results = vec![
            Results::new(vec![vec![DataType::Int(10)]]),
            Results::new(Vec::<Vec<DataType>>::new()),
            Results::new(vec![vec![DataType::Int(11)], vec![DataType::Int(12)]]),
        ];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: false,
            schema: Cow::Owned(vec![ColumnSchema {
                spec: ColumnSpecification {
                    column: "tab1.col1".into(),
                    sql_type: SqlType::BigInt(None),
                    comment: None,
                    constraints: vec![],
                },
                base: None,
            }]),
            columns: Cow::Owned(vec!["col1".into()]),
        });
        let resultset = Resultset::try_new(ResultIterator::owned(results), &schema).unwrap();
        assert_eq!(
            collect_resultset_values(resultset),
            vec![
                vec![ps::Value::BigInt(10)],
                vec![ps::Value::BigInt(11)],
                vec![ps::Value::BigInt(12)]
            ]
        );
    }

    #[test]
    fn create_resultset_with_unprojected_fields() {
        let results = vec![];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: true,
            schema: Cow::Owned(vec![
                ColumnSchema {
                    spec: ColumnSpecification {
                        column: "tab1.col1".into(),
                        sql_type: SqlType::BigInt(None),
                        comment: None,
                        constraints: vec![],
                    },
                    base: None,
                },
                ColumnSchema {
                    spec: ColumnSpecification {
                        column: "tab1.col2".into(),
                        sql_type: SqlType::Text,
                        comment: None,
                        constraints: vec![],
                    },
                    base: None,
                },
            ]),
            columns: Cow::Owned(vec![
                "col1".into(),
                "col3".into(),
                "col2".into(),
                "bogokey".into(),
            ]),
        });
        let resultset = Resultset::try_new(ResultIterator::owned(results), &schema).unwrap();
        assert_eq!(resultset.results.into_vec(), Vec::<Vec<DataType>>::new());
        // The projected field indices of "col1" and "col2" within `columns` are 0 and 2. The
        // unprojected "col3" and "bogokey" fields are excluded.
        assert_eq!(resultset.project_fields, Arc::new(vec![0, 2]));
        assert_eq!(
            resultset.project_field_types,
            Arc::new(vec![Type::INT8, Type::TEXT])
        );
    }

    #[test]
    fn iterate_resultset_with_unprojected_fields() {
        let results = vec![Results::new(vec![
            vec![
                DataType::Int(10),
                DataType::Int(99),
                DataType::Text("abcdef".into()),
                DataType::Int(0),
            ],
            vec![
                DataType::Int(11),
                DataType::Int(99),
                DataType::Text("ghijkl".into()),
                DataType::Int(0),
            ],
        ])];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: true,
            schema: Cow::Owned(vec![
                ColumnSchema {
                    spec: ColumnSpecification {
                        column: "tab1.col1".into(),
                        sql_type: SqlType::BigInt(None),
                        comment: None,
                        constraints: vec![],
                    },
                    base: None,
                },
                ColumnSchema {
                    spec: ColumnSpecification {
                        column: "tab1.col2".into(),
                        sql_type: SqlType::Text,
                        comment: None,
                        constraints: vec![],
                    },
                    base: None,
                },
            ]),
            columns: Cow::Owned(vec![
                "col1".into(),
                "col3".into(),
                "col2".into(),
                "bogokey".into(),
            ]),
        });
        let resultset = Resultset::try_new(ResultIterator::owned(results), &schema).unwrap();
        // Only the columns to be projected (col1 and col2) are included in the collected values.
        assert_eq!(
            collect_resultset_values(resultset),
            vec![
                vec![ps::Value::BigInt(10), ps::Value::Text("abcdef".into())],
                vec![ps::Value::BigInt(11), ps::Value::Text("ghijkl".into())]
            ]
        );
    }
}
