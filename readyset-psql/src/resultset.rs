use std::convert::TryFrom;
use std::iter;
use std::sync::Arc;

use psql_srv as ps;
use readyset_client::results::{ResultIterator, Results};
use readyset_data::DfValue;
use tokio_postgres::types::Type;

use crate::row::Row;
use crate::schema::{type_to_pgsql, SelectSchema};

/// A structure that contains a `ResultIterator` and facilitates iteration over these results as
/// `Row` values.
pub struct Resultset {
    /// The query result data, comprising nested `Vec`s of rows that may come from separate
    /// ReadySet interface lookups performed by the backend.
    results: ResultIterator,

    /// The data types of the projected fields for each row.
    project_field_types: Arc<Vec<Type>>,
}

impl Resultset {
    pub fn try_new(results: ResultIterator, schema: &SelectSchema) -> Result<Self, ps::Error> {
        // Extract the appropriate `tokio_postgres` `Type` for each column in the schema.
        let project_field_types = Arc::new(
            schema
                .0
                .schema
                .iter()
                .map(|c| type_to_pgsql(&c.column_type))
                .collect::<Result<Vec<_>, _>>()?,
        );
        Ok(Resultset {
            results,
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
            .zip(iter::repeat(self.project_field_types))
            .map(|(values, project_field_types)| Row {
                values,
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
                let val: DfValue = row.try_get(i).map_err(|e| {
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
            project_field_types: Arc::new(column_types),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::convert::TryFrom;

    use readyset_adapter::backend as cl;
    use readyset_client::ColumnSchema;
    use readyset_data::{DfType, DfValue};

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
                column: "tab1.col1".into(),
                column_type: DfType::BigInt,
                base: None,
            }]),
            columns: Cow::Owned(vec!["col1".into()]),
        });
        let resultset = Resultset::try_new(ResultIterator::owned(results), &schema).unwrap();
        assert_eq!(resultset.results.into_vec(), Vec::<Vec<DfValue>>::new());
        assert_eq!(resultset.project_field_types, Arc::new(vec![Type::INT8]));
    }

    #[test]
    fn iterate_resultset() {
        let results = vec![Results::new(vec![vec![DfValue::Int(10)]])];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: false,
            schema: Cow::Owned(vec![ColumnSchema {
                column: "tab1.col1".into(),
                column_type: DfType::BigInt,
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
            Results::new(vec![vec![DfValue::Int(10)]]),
            Results::new(Vec::<Vec<DfValue>>::new()),
            Results::new(vec![vec![DfValue::Int(11)], vec![DfValue::Int(12)]]),
        ];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: false,
            schema: Cow::Owned(vec![ColumnSchema {
                column: "tab1.col1".into(),
                column_type: DfType::BigInt,
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
}
