use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{ready, Stream};
use ps::{PsqlSrvRow, PsqlValue};
use psql_srv as ps;
use rayon::prelude::*;
use readyset_client::results::ResultIterator;
use tokio_postgres::types::Type;
use tokio_postgres::{GenericResult, ResultStream, SimpleQueryMessage, SimpleQueryStream};

use crate::schema::{type_to_pgsql, SelectSchema};
use crate::value::TypedDfValue;

/// Approximate memory sizes for different PostgreSQL types (in bytes)
const fn approximate_type_size(ty: &Type) -> usize {
    match *ty {
        // Fixed-size types
        Type::BOOL => 1,
        Type::INT2 => 2,
        Type::INT4 => 4,
        Type::INT8 => 8,
        Type::FLOAT4 => 4,
        Type::FLOAT8 => 8,
        Type::DATE => 4,
        Type::TIMESTAMP => 8,
        Type::TIMESTAMPTZ => 8,

        // Variable-size types - use conservative estimates
        Type::TEXT | Type::VARCHAR => 64, // Assume average string length
        Type::BYTEA => 256,               // Assume moderate binary data
        Type::JSON | Type::JSONB => 128,  // Assume moderate JSON

        // Default for unknown types
        _ => 32,
    }
}

/// The target batch size in bytes
const TARGET_BATCH_BYTES: usize = 4 * 1024 * 1024; // 4MB
/// Minimum number of rows to process in a batch
const MIN_BATCH_SIZE: usize = 100;
/// Maximum number of rows to process in a batch
const MAX_BATCH_SIZE: usize = 100_000;

enum ResultsetInner {
    Empty,
    ReadySet(Box<<ResultIterator as IntoIterator>::IntoIter>),
    Stream {
        first_row: Option<tokio_postgres::Row>,
        stream: Pin<Box<ResultStream>>,
    },
    SimpleQueryStream {
        first_message: Option<SimpleQueryMessage>,
        stream: Pin<Box<SimpleQueryStream>>,
    },
}

/// A structure that contains a `ResultIterator` and facilitates iteration over these results as
/// `Row` values.
pub struct Resultset {
    /// The query result data, comprising nested `Vec`s of rows that may come from separate
    /// ReadySet interface lookups performed by the backend.
    results: ResultsetInner,

    /// The data types of the projected fields for each row.
    project_field_types: Arc<Vec<Type>>,

    /// A buffer for processed rows
    processed_buffer: VecDeque<Result<PsqlSrvRow, psql_srv::Error>>,
}

impl Resultset {
    pub fn empty() -> Self {
        Self {
            results: ResultsetInner::Empty,
            project_field_types: Arc::new(vec![]),
            processed_buffer: VecDeque::new(),
        }
    }

    pub fn from_readyset(
        results: ResultIterator,
        schema: &SelectSchema,
    ) -> Result<Self, ps::Error> {
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
            results: ResultsetInner::ReadySet(Box::new(results.into_iter())),
            project_field_types,
            processed_buffer: VecDeque::new(),
        })
    }

    pub fn from_stream(
        stream: Pin<Box<ResultStream>>,
        first_row: tokio_postgres::Row,
        schema: Vec<Type>,
    ) -> Self {
        Self {
            results: ResultsetInner::Stream {
                first_row: Some(first_row),
                stream,
            },
            project_field_types: Arc::new(schema),
            processed_buffer: VecDeque::new(),
        }
    }

    pub fn from_simple_query_stream(
        stream: Pin<Box<SimpleQueryStream>>,
        first_msg: tokio_postgres::SimpleQueryMessage,
    ) -> Self {
        Self {
            results: ResultsetInner::SimpleQueryStream {
                first_message: Some(first_msg),
                stream,
            },
            project_field_types: Arc::new(vec![]),
            processed_buffer: VecDeque::new(),
        }
    }
}

impl Stream for Resultset {
    type Item = Result<PsqlSrvRow, psql_srv::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // First, try to return from the buffer if available
        if let Some(row) = this.processed_buffer.pop_front() {
            return Poll::Ready(Some(row));
        }

        let project_field_types = Arc::clone(&this.project_field_types);
        let next = match &mut this.results {
            ResultsetInner::Empty => None,
            ResultsetInner::ReadySet(i) => {
                // Calculate estimated row size based on column types
                let estimated_row_size: usize =
                    project_field_types.iter().map(approximate_type_size).sum();

                // Calculate batch size based on estimated memory usage
                let batch_size = if estimated_row_size > 0 {
                    (TARGET_BATCH_BYTES / estimated_row_size).clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE)
                } else {
                    MAX_BATCH_SIZE
                };

                let batch: Vec<_> = i.take(batch_size).collect();
                if batch.is_empty() {
                    None
                } else {
                    // Process the batch in parallel while maintaining order
                    let results: Result<Vec<_>, _> = batch
                        .into_par_iter()
                        .enumerate() // Add indices to track original order
                        .map(|(idx, values)| {
                            let mut converted_values =
                                Vec::with_capacity(project_field_types.len());
                            for (value, col_type) in
                                values.into_iter().zip(project_field_types.iter())
                            {
                                let converted =
                                    PsqlValue::try_from(TypedDfValue { value, col_type })?;
                                converted_values.push(converted);
                            }
                            Ok((idx, PsqlSrvRow::ValueVec(converted_values)))
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map(|v| {
                            let mut v = v;
                            v.sort_by_key(|(idx, _)| *idx); // Sort by original index
                            v.into_iter().map(|(_, row)| row).collect()
                        });

                    match results {
                        Ok(mut rows) => {
                            // Store all but the first row in the buffer
                            this.processed_buffer = rows.drain(1..).map(Ok).collect();
                            // Return the first row
                            let first_row = rows.into_iter().next().unwrap();
                            Some(Ok(first_row))
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
            }
            ResultsetInner::Stream { first_row, stream } => {
                let row = match first_row.take() {
                    Some(row) => Some(Ok(row)),
                    None => loop {
                        match ready!(stream.as_mut().poll_next(cx)) {
                            None => break None,
                            Some(Err(e)) => break Some(Err(psql_srv::Error::from(e))),
                            Some(Ok(GenericResult::Row(r))) => break Some(Ok(r)),
                            Some(Ok(GenericResult::Command(_, _))) => {}
                        }
                    },
                };

                row.map(|res| res.map(PsqlSrvRow::RawRow))
            }
            ResultsetInner::SimpleQueryStream {
                first_message,
                stream,
            } => {
                let row = match first_message.take() {
                    Some(row) => Some(Ok(row)),
                    None => match ready!(stream.as_mut().poll_next(cx)) {
                        None => None,
                        Some(Err(e)) => Some(Err(psql_srv::Error::from(e))),
                        Some(Ok(msg)) => Some(Ok(msg)),
                    },
                };
                row.map(|res| res.map(PsqlSrvRow::SimpleQueryMessage))
            }
        };

        Poll::Ready(next)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use futures::{StreamExt, TryStreamExt};
    use psql_srv::PsqlValue;
    use readyset_adapter::backend as cl;
    use readyset_client::results::Results;
    use readyset_client::ColumnSchema;
    use readyset_data::{DfType, DfValue};

    use super::*;

    async fn collect_resultset_values(resultset: Resultset) -> Vec<Vec<PsqlValue>> {
        resultset
            .map(|r| {
                r.map(|r| {
                    match r {
                        // We only call this helper function with resultsets from
                        // Resultset::from_readyset, so we should never get raw rows:
                        PsqlSrvRow::ValueVec(row) => row,
                        _ => panic!(),
                    }
                })
            })
            .try_collect()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn create_resultset() {
        let results = vec![];
        let schema = SelectSchema(cl::SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: "tab1.col1".into(),
                column_type: DfType::BigInt,
                base: None,
            }]),
            columns: Cow::Owned(vec!["col1".into()]),
        });
        let resultset = Resultset::from_readyset(ResultIterator::owned(results), &schema).unwrap();
        assert_eq!(resultset.project_field_types, Arc::new(vec![Type::INT8]));
        assert_eq!(
            collect_resultset_values(resultset).await,
            Vec::<Vec<PsqlValue>>::new()
        );
    }

    #[tokio::test]
    async fn stream_resultset() {
        let results = vec![Results::new(vec![vec![DfValue::Int(10)]])];
        let schema = SelectSchema(cl::SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: "tab1.col1".into(),
                column_type: DfType::BigInt,
                base: None,
            }]),
            columns: Cow::Owned(vec!["col1".into()]),
        });
        let resultset = Resultset::from_readyset(ResultIterator::owned(results), &schema).unwrap();
        assert_eq!(
            collect_resultset_values(resultset).await,
            vec![vec![PsqlValue::BigInt(10)]]
        );
    }

    #[tokio::test]
    async fn stream_resultset_with_multiple_results() {
        let results = vec![
            Results::new(vec![vec![DfValue::Int(10)]]),
            Results::new(Vec::<Vec<DfValue>>::new()),
            Results::new(vec![vec![DfValue::Int(11)], vec![DfValue::Int(12)]]),
        ];
        let schema = SelectSchema(cl::SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: "tab1.col1".into(),
                column_type: DfType::BigInt,
                base: None,
            }]),
            columns: Cow::Owned(vec!["col1".into()]),
        });
        let resultset = Resultset::from_readyset(ResultIterator::owned(results), &schema).unwrap();
        assert_eq!(
            collect_resultset_values(resultset).await,
            vec![
                vec![PsqlValue::BigInt(10)],
                vec![PsqlValue::BigInt(11)],
                vec![PsqlValue::BigInt(12)]
            ]
        );
    }

    #[tokio::test]
    async fn test_parallel_processing_with_buffer() {
        // Create a large enough dataset to test batching
        let results = vec![Results::new(vec![
            vec![DfValue::Int(1)],
            vec![DfValue::Int(2)],
            vec![DfValue::Int(3)],
            vec![DfValue::Int(4)],
            vec![DfValue::Int(5)],
        ])];

        let schema = SelectSchema(cl::SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: "test.col".into(),
                column_type: DfType::BigInt,
                base: None,
            }]),
            columns: Cow::Owned(vec!["col".into()]),
        });

        let resultset = Resultset::from_readyset(ResultIterator::owned(results), &schema).unwrap();

        // Collect all values and verify they're all present and in order
        let values = collect_resultset_values(resultset).await;

        assert_eq!(values.len(), 5, "Should have received all 5 rows");
        assert_eq!(
            values,
            vec![
                vec![PsqlValue::BigInt(1)],
                vec![PsqlValue::BigInt(2)],
                vec![PsqlValue::BigInt(3)],
                vec![PsqlValue::BigInt(4)],
                vec![PsqlValue::BigInt(5)],
            ],
            "Values should be in correct order"
        );
    }

    #[tokio::test]
    async fn test_empty_batch_handling() {
        let results = vec![Results::new(vec![])];
        let schema = SelectSchema(cl::SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: "test.col".into(),
                column_type: DfType::BigInt,
                base: None,
            }]),
            columns: Cow::Owned(vec!["col".into()]),
        });

        let resultset = Resultset::from_readyset(ResultIterator::owned(results), &schema).unwrap();
        let values = collect_resultset_values(resultset).await;
        assert!(
            values.is_empty(),
            "Should handle empty result set correctly"
        );
    }

    #[tokio::test]
    async fn test_large_batch_processing() {
        // Create a dataset larger than MAX_BATCH_SIZE
        let large_dataset = (0..MAX_BATCH_SIZE + 10)
            .map(|i| vec![DfValue::Int(i as i64)])
            .collect();

        let results = vec![Results::new(large_dataset)];
        let schema = SelectSchema(cl::SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: "test.col".into(),
                column_type: DfType::BigInt,
                base: None,
            }]),
            columns: Cow::Owned(vec!["col".into()]),
        });

        let resultset = Resultset::from_readyset(ResultIterator::owned(results), &schema).unwrap();
        let values = collect_resultset_values(resultset).await;

        assert_eq!(
            values.len(),
            MAX_BATCH_SIZE + 10,
            "Should process all rows even when larger than batch size"
        );

        // Verify the values are in correct order
        for (i, row) in values.iter().enumerate() {
            assert_eq!(
                row[0],
                PsqlValue::BigInt(i as i64),
                "Row {} should contain value {}",
                i,
                i
            );
        }
    }

    #[tokio::test]
    async fn test_parallel_processing_order_preservation() {
        // Create a dataset with distinctive values to clearly show any ordering issues
        let results = vec![Results::new(vec![
            vec![DfValue::Text("first".into())],
            vec![DfValue::Text("second".into())],
            vec![DfValue::Text("third".into())],
            vec![DfValue::Text("fourth".into())],
            vec![DfValue::Text("fifth".into())],
            vec![DfValue::Text("sixth".into())],
            vec![DfValue::Text("seventh".into())],
            vec![DfValue::Text("eighth".into())],
            vec![DfValue::Text("ninth".into())],
            vec![DfValue::Text("tenth".into())],
        ])];

        let schema = SelectSchema(cl::SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: "test.col".into(),
                column_type: DfType::Text(readyset_data::Collation::default()),
                base: None,
            }]),
            columns: Cow::Owned(vec!["col".into()]),
        });

        let resultset = Resultset::from_readyset(ResultIterator::owned(results), &schema).unwrap();

        // Collect all values
        let values = collect_resultset_values(resultset).await;

        // Verify order is preserved
        let expected = vec![
            vec![PsqlValue::Text("first".into())],
            vec![PsqlValue::Text("second".into())],
            vec![PsqlValue::Text("third".into())],
            vec![PsqlValue::Text("fourth".into())],
            vec![PsqlValue::Text("fifth".into())],
            vec![PsqlValue::Text("sixth".into())],
            vec![PsqlValue::Text("seventh".into())],
            vec![PsqlValue::Text("eighth".into())],
            vec![PsqlValue::Text("ninth".into())],
            vec![PsqlValue::Text("tenth".into())],
        ];

        assert_eq!(
            values, expected,
            "Values should maintain their original order after parallel processing"
        );

        // Additional verification for exact ordering
        for (i, row) in values.iter().enumerate() {
            let expected_text = match i {
                0 => "first",
                1 => "second",
                2 => "third",
                3 => "fourth",
                4 => "fifth",
                5 => "sixth",
                6 => "seventh",
                7 => "eighth",
                8 => "ninth",
                9 => "tenth",
                _ => unreachable!(),
            };

            match &row[0] {
                PsqlValue::Text(text) => assert_eq!(
                    text.as_str(),
                    expected_text,
                    "Row {} should contain '{}'",
                    i,
                    expected_text
                ),
                _ => panic!("Expected Text value"),
            }
        }
    }
}
