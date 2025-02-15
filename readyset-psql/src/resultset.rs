use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{ready, Stream};
use ps::{PsqlSrvRow, PsqlValue};
use psql_srv as ps;
use readyset_client::results::ResultIterator;
use tokio_postgres::types::Type;
use tokio_postgres::{
    GenericResult, ResultStream, RowStream, SimpleQueryMessage, SimpleQueryStream,
};

use crate::schema::{type_to_pgsql, SelectSchema};
use crate::value::TypedDfValue;

enum ResultsetInner {
    Empty,
    ReadySet(Box<<ResultIterator as IntoIterator>::IntoIter>),
    Stream {
        first_row: Option<tokio_postgres::Row>,
        stream: Pin<Box<ResultStream>>,
    },
    RowStream {
        first_row: Option<tokio_postgres::Row>,
        stream: Pin<Box<RowStream>>,
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
}

impl Resultset {
    pub fn empty() -> Self {
        Self {
            results: ResultsetInner::Empty,
            project_field_types: Arc::new(vec![]),
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
        }
    }

    pub fn from_row_stream(
        stream: Pin<Box<RowStream>>,
        first_row: tokio_postgres::Row,
        schema: Vec<Type>,
    ) -> Self {
        Self {
            results: ResultsetInner::RowStream {
                first_row: Some(first_row),
                stream,
            },
            project_field_types: Arc::new(schema),
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
        }
    }
}

impl Stream for Resultset {
    type Item = Result<PsqlSrvRow, psql_srv::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let s = self.get_mut();
        let project_field_types = &s.project_field_types;
        let next = match &mut s.results {
            ResultsetInner::Empty => None,
            ResultsetInner::ReadySet(i) => {
                let next_values = i.next();
                if let Some(mut values) = next_values {
                    // make sure we have at least as many values as types. if there are more values than types, those are bogokeys,
                    // which we can ignore as we do not send those back to callers.
                    let len = project_field_types.len();
                    if values.len() < len {
                        return Poll::Ready(Some(Err(psql_srv::Error::IncorrectFormatCount(len))));
                    }

                    let mut converted_values = Vec::with_capacity(len);
                    let spare = converted_values.spare_capacity_mut();

                    for i in 0..len {
                        let col_type = project_field_types.get(i).unwrap();
                        let value = values.get_mut(i).unwrap();
                        // as we are on the super hot path, we do an `unsafe` operation here:
                        // it's more efficient to directly allocate into the converted_values
                        // vector, rather than allocate locally and `push` into the vector.
                        spare[i].write(PsqlValue::try_from(TypedDfValue {
                            value: std::mem::take(value),
                            col_type,
                        })?);
                    }
                    unsafe {
                        converted_values.set_len(len);
                    }

                    Some(Ok(PsqlSrvRow::ValueVec(converted_values)))
                } else {
                    // at the end of all rows in the resultset ...
                    None
                }
            }
            ResultsetInner::RowStream { first_row, stream } => {
                let row = match first_row.take() {
                    Some(row) => Some(Ok(row)),
                    None => loop {
                        match ready!(stream.as_mut().poll_next(cx)) {
                            None => break None,
                            Some(Err(e)) => break Some(Err(psql_srv::Error::from(e))),
                            Some(Ok(GenericResult::Row(row))) => break Some(Ok(row)),
                            Some(Ok(GenericResult::Command(_, _))) => {}
                        }
                    },
                };
                row.map(|res| res.map(PsqlSrvRow::RawRow))
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
}
