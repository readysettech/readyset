use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, ready};
use ps::{PsqlSrvRow, PsqlValue};
use psql_srv as ps;
use readyset_client::results::ResultIterator;
use readyset_data::DfValue;
use readyset_shallow::{CacheInsertGuard, PostgreSqlMetadata, QueryMetadata};
use readyset_sql_passes::adapter_rewrites::ProcessedQueryParams;
use tokio_postgres::types::Type;
use tokio_postgres::{
    GenericResult, ResultStream, RowStream, SimpleQueryMessage, SimpleQueryStream,
};

use crate::schema::{SelectSchema, type_to_pgsql};
use crate::upstream::CacheEntry;
use crate::value::{TypedDfValue, row_to_df_values};

// Let's not make the dependency fork any worse than it is.
pub(crate) fn copy_simple_query_message(
    msg: &SimpleQueryMessage,
) -> Result<SimpleQueryMessage, ps::Error> {
    match msg {
        SimpleQueryMessage::Row(r) => {
            let fields = r.fields().to_vec().into();
            let body = r.body().clone();
            tokio_postgres::SimpleQueryRow::new(fields, body)
                .map(SimpleQueryMessage::Row)
                .map_err(|e| {
                    ps::Error::InternalError(format!("Failed to copy SimpleQueryRow: {e}"))
                })
        }
        SimpleQueryMessage::CommandComplete(contents) => Ok(SimpleQueryMessage::CommandComplete(
            tokio_postgres::CommandCompleteContents {
                fields: contents.fields.clone(),
                rows: contents.rows,
                tag: contents.tag.clone(),
            },
        )),
        _ => Err(ps::Error::InternalError(
            "Unsupported SimpleQueryMessage variant for caching".to_string(),
        )),
    }
}

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
    project_field_types: Vec<Type>,

    /// The names of the projected fields for each row (for shallow cache metadata).
    project_field_names: Vec<String>,

    /// Optional cache guard for shallow cache insertion during streaming
    cache: Option<CacheInsertGuard<ProcessedQueryParams, CacheEntry>>,
}

impl Resultset {
    fn finalize_cache(&mut self) {
        if let Some(cache) = &mut self.cache {
            let schema = self
                .project_field_names
                .iter()
                .zip(&self.project_field_types)
                .enumerate()
                .map(|(i, (name, col_type))| ps::Column::Column {
                    name: name.clone().into(),
                    col_type: col_type.clone(),
                    table_oid: Some(0),
                    attnum: Some(i as i16),
                })
                .collect();

            cache.set_metadata(QueryMetadata::PostgreSql(PostgreSqlMetadata {
                schema,
                types: self.project_field_types.clone(),
            }));
            cache.filled();
        }
    }

    fn copy_row_to_cache(
        &mut self,
        row: &Option<Result<tokio_postgres::Row, psql_srv::Error>>,
    ) -> Result<(), ps::Error> {
        if let Some(Ok(row)) = row
            && let Some(cache) = &mut self.cache
        {
            let row = row_to_df_values(row)?;
            cache.push(CacheEntry::DfValue(row));
        }
        Ok(())
    }

    pub fn empty() -> Self {
        Self {
            results: ResultsetInner::Empty,
            project_field_types: Vec::new(),
            project_field_names: Vec::new(),
            cache: None,
        }
    }

    pub fn from_readyset(
        results: ResultIterator,
        schema: &SelectSchema,
    ) -> Result<Self, ps::Error> {
        // Extract the appropriate `tokio_postgres` `Type` for each column in the schema.
        let project_field_types = schema
            .0
            .schema
            .iter()
            .map(|c| type_to_pgsql(&c.column_type))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Resultset {
            results: ResultsetInner::ReadySet(Box::new(results.into_iter())),
            project_field_types,
            project_field_names: Vec::new(),
            cache: None,
        })
    }

    pub fn from_stream(
        stream: Pin<Box<ResultStream>>,
        first_row: tokio_postgres::Row,
        schema: Vec<Type>,
        cache: Option<CacheInsertGuard<ProcessedQueryParams, CacheEntry>>,
    ) -> Self {
        let names = first_row
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        Self {
            results: ResultsetInner::Stream {
                first_row: Some(first_row),
                stream,
            },
            project_field_types: schema,
            project_field_names: names,
            cache,
        }
    }

    pub fn from_row_stream(
        stream: Pin<Box<RowStream>>,
        first_row: tokio_postgres::Row,
        schema: Vec<Type>,
        cache: Option<CacheInsertGuard<ProcessedQueryParams, CacheEntry>>,
    ) -> Self {
        let names = first_row
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        Self {
            results: ResultsetInner::RowStream {
                first_row: Some(first_row),
                stream,
            },
            project_field_types: schema,
            project_field_names: names,
            cache,
        }
    }

    pub fn from_simple_query_stream(
        stream: Pin<Box<SimpleQueryStream>>,
        first_msg: tokio_postgres::SimpleQueryMessage,
        cache: Option<CacheInsertGuard<ProcessedQueryParams, CacheEntry>>,
    ) -> Self {
        Self {
            results: ResultsetInner::SimpleQueryStream {
                first_message: Some(first_msg),
                stream,
            },
            project_field_types: Vec::new(),
            project_field_names: Vec::new(),
            cache,
        }
    }

    pub fn from_shallow(values: Arc<Vec<Vec<DfValue>>>, project_field_types: Vec<Type>) -> Self {
        Self {
            results: ResultsetInner::ReadySet(Box::new(
                ResultIterator::arc_owned(values).into_iter(),
            )),
            project_field_types,
            project_field_names: Vec::new(),
            cache: None,
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
                    // make sure we have at least as many values as types. if there are more
                    // values than types, those are bogokeys, which we can ignore as we do not
                    // send those back to callers.
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
                    // SAFETY: capacity is already set to `len`, and `old_len..new_len` was just
                    // initialized above via `spare`.
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
                            None => {
                                s.finalize_cache();
                                break None;
                            }
                            Some(Err(e)) => break Some(Err(psql_srv::Error::from(e))),
                            Some(Ok(GenericResult::Row(row))) => break Some(Ok(row)),
                            Some(Ok(GenericResult::Command(_, _))) => {}
                        }
                    },
                };

                s.copy_row_to_cache(&row)?;
                row.map(|res| res.map(PsqlSrvRow::RawRow))
            }
            ResultsetInner::Stream { first_row, stream } => {
                let row = match first_row.take() {
                    Some(row) => Some(Ok(row)),
                    None => loop {
                        match ready!(stream.as_mut().poll_next(cx)) {
                            None => {
                                s.finalize_cache();
                                break None;
                            }
                            Some(Err(e)) => break Some(Err(psql_srv::Error::from(e))),
                            Some(Ok(GenericResult::Row(r))) => break Some(Ok(r)),
                            Some(Ok(GenericResult::Command(_, _))) => {}
                        }
                    },
                };

                s.copy_row_to_cache(&row)?;
                row.map(|res| res.map(PsqlSrvRow::RawRow))
            }
            ResultsetInner::SimpleQueryStream {
                first_message,
                stream,
            } => {
                let row = match first_message.take() {
                    Some(row) => Some(Ok(row)),
                    None => match ready!(stream.as_mut().poll_next(cx)) {
                        None => {
                            s.finalize_cache();
                            None
                        }
                        Some(Err(e)) => Some(Err(psql_srv::Error::from(e))),
                        Some(Ok(msg)) => Some(Ok(msg)),
                    },
                };

                if let Some(Ok(msg)) = &row
                    && let Some(cache) = &mut s.cache
                {
                    let msg = copy_simple_query_message(msg)?;
                    cache.push(CacheEntry::Simple(msg));
                }

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
    use readyset_client::ColumnSchema;
    use readyset_client::results::Results;
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
        assert_eq!(resultset.project_field_types, vec![Type::INT8]);
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
