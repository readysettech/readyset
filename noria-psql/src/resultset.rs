use crate::row::Row;
use crate::schema::SelectSchema;
use noria::results::Results;
use psql_srv as ps;
use std::iter;
use std::sync::Arc;

/// A structure that contains a `Vec<Results>`, as provided by `QueryResult::NoriaSelect`, and
/// facilitates iteration over these results as `Row` values.
pub struct Resultset {
    /// The query result data, comprising nested `Vec`s of rows that may come from separate Noria
    /// interface lookups performed by the backend.
    results: Vec<Results>,

    /// The fields to project for each row. A `Results` returned by a Noria interface lookup may
    /// contain extraneous fields that should not be projected into the query result output. In
    /// particular, bogokeys and other lookup keys that are not requested for projection by the SQL
    /// query may be present in `results` but should be excluded from query output. This
    /// `project_fields` attribute contains the indices of the fields that _should_ be projected
    /// into the output.
    project_fields: Arc<Vec<usize>>,

    /// The data types of the projected fields for each row.
    project_field_types: Arc<Vec<ps::ColType>>,
}

impl Resultset {
    pub fn try_new(results: Vec<Results>, schema: &SelectSchema) -> Result<Self, ps::Error> {
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
                        .position(|name| name == &col.spec.column.name)
                        .ok_or_else(|| ps::Error::InternalError("inconsistent schema".to_string()))
                })
                .collect::<Result<Vec<usize>, ps::Error>>()?,
        );
        // Extract the appropriate `psql_srv::ColType` for each column in the schema.
        let project_field_types = Arc::new(
            schema
                .0
                .schema
                .iter()
                .map(|c| c.spec.sql_type.clone())
                .collect::<Vec<ps::ColType>>(),
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
    #[allow(clippy::type_complexity)]
    type IntoIter = std::iter::Map<
        std::iter::Zip<
            std::iter::Flatten<std::vec::IntoIter<Results>>,
            std::iter::Repeat<(Arc<Vec<usize>>, Arc<Vec<ps::ColType>>)>,
        >,
        fn(
            (
                noria::results::Row,
                (Arc<Vec<usize>>, Arc<Vec<ps::ColType>>),
            ),
        ) -> Row,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.results
            .into_iter()
            .flatten()
            .zip(iter::repeat((
                self.project_fields,
                self.project_field_types,
            )))
            .map(|(values, (project_fields, project_field_types))| Row {
                values: values.into(),
                project_fields,
                project_field_types,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arccstr::ArcCStr;
    use nom_sql::ColumnSpecification;
    use noria::{ColumnSchema, DataType};
    use noria_client::backend as cl;
    use ps::ColType;
    use std::convert::TryFrom;

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
            schema: vec![ColumnSchema {
                spec: ColumnSpecification {
                    column: "tab1.col1".into(),
                    sql_type: ColType::Bigint(None),
                    comment: None,
                    constraints: vec![],
                },
                base: None,
            }],
            columns: vec!["col1".to_string()],
        });
        let resultset = Resultset::try_new(results, &schema).unwrap();
        assert_eq!(resultset.results, Vec::<Results>::new());
        assert_eq!(resultset.project_fields, Arc::new(vec![0]));
        assert_eq!(
            resultset.project_field_types,
            Arc::new(vec![ps::ColType::Bigint(None)])
        );
    }

    #[test]
    fn iterate_resultset() {
        let results = vec![Results::new(
            vec![vec![DataType::BigInt(10)]],
            Arc::new(["col1".to_string()]),
        )];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: false,
            schema: vec![ColumnSchema {
                spec: ColumnSpecification {
                    column: "tab1.col1".into(),
                    sql_type: ColType::Bigint(None),
                    comment: None,
                    constraints: vec![],
                },
                base: None,
            }],
            columns: vec!["col1".to_string()],
        });
        let resultset = Resultset::try_new(results, &schema).unwrap();
        assert_eq!(
            collect_resultset_values(resultset),
            vec![vec![ps::Value::Bigint(10)]]
        );
    }

    #[test]
    fn iterate_resultset_with_multiple_results() {
        let results = vec![
            Results::new(
                vec![vec![DataType::BigInt(10)]],
                Arc::new(["col1".to_string()]),
            ),
            Results::new(Vec::<Vec<DataType>>::new(), Arc::new(["col1".to_string()])),
            Results::new(
                vec![vec![DataType::BigInt(11)], vec![DataType::BigInt(12)]],
                Arc::new(["col1".to_string()]),
            ),
        ];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: false,
            schema: vec![ColumnSchema {
                spec: ColumnSpecification {
                    column: "tab1.col1".into(),
                    sql_type: ColType::Bigint(None),
                    comment: None,
                    constraints: vec![],
                },
                base: None,
            }],
            columns: vec!["col1".to_string()],
        });
        let resultset = Resultset::try_new(results, &schema).unwrap();
        assert_eq!(
            collect_resultset_values(resultset),
            vec![
                vec![ps::Value::Bigint(10)],
                vec![ps::Value::Bigint(11)],
                vec![ps::Value::Bigint(12)]
            ]
        );
    }

    #[test]
    fn create_resultset_with_unprojected_fields() {
        let results = vec![];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: true,
            schema: vec![
                ColumnSchema {
                    spec: ColumnSpecification {
                        column: "tab1.col1".into(),
                        sql_type: ColType::Bigint(None),
                        comment: None,
                        constraints: vec![],
                    },
                    base: None,
                },
                ColumnSchema {
                    spec: ColumnSpecification {
                        column: "tab1.col2".into(),
                        sql_type: ColType::Text,
                        comment: None,
                        constraints: vec![],
                    },
                    base: None,
                },
            ],
            columns: vec![
                "col1".to_string(),
                "col3".to_string(),
                "col2".to_string(),
                "bogokey".to_string(),
            ],
        });
        let resultset = Resultset::try_new(results, &schema).unwrap();
        assert_eq!(resultset.results, Vec::<Results>::new());
        // The projected field indices of "col1" and "col2" within `columns` are 0 and 2. The
        // unprojected "col3" and "bogokey" fields are excluded.
        assert_eq!(resultset.project_fields, Arc::new(vec![0, 2]));
        assert_eq!(
            resultset.project_field_types,
            Arc::new(vec![ps::ColType::Bigint(None), ps::ColType::Text])
        );
    }

    #[test]
    fn iterate_resultset_with_unprojected_fields() {
        let results = vec![Results::new(
            vec![
                vec![
                    DataType::BigInt(10),
                    DataType::BigInt(99),
                    DataType::Text(ArcCStr::try_from("abcdef").unwrap()),
                    DataType::Int(0),
                ],
                vec![
                    DataType::BigInt(11),
                    DataType::BigInt(99),
                    DataType::Text(ArcCStr::try_from("ghijkl").unwrap()),
                    DataType::Int(0),
                ],
            ],
            Arc::new([
                "col1".to_string(),
                "col3".to_string(),
                "col2".to_string(),
                "bogokey".to_string(),
            ]),
        )];
        let schema = SelectSchema(cl::SelectSchema {
            use_bogo: true,
            schema: vec![
                ColumnSchema {
                    spec: ColumnSpecification {
                        column: "tab1.col1".into(),
                        sql_type: ColType::Bigint(None),
                        comment: None,
                        constraints: vec![],
                    },
                    base: None,
                },
                ColumnSchema {
                    spec: ColumnSpecification {
                        column: "tab1.col2".into(),
                        sql_type: ColType::Text,
                        comment: None,
                        constraints: vec![],
                    },
                    base: None,
                },
            ],
            columns: vec![
                "col1".to_string(),
                "col3".to_string(),
                "col2".to_string(),
                "bogokey".to_string(),
            ],
        });
        let resultset = Resultset::try_new(results, &schema).unwrap();
        // Only the columns to be projected (col1 and col2) are included in the collected values.
        assert_eq!(
            collect_resultset_values(resultset),
            vec![
                vec![
                    ps::Value::Bigint(10),
                    ps::Value::Text(ArcCStr::try_from("abcdef").unwrap())
                ],
                vec![
                    ps::Value::Bigint(11),
                    ps::Value::Text(ArcCStr::try_from("ghijkl").unwrap())
                ]
            ]
        );
    }
}
