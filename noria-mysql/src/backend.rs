use std::convert::TryFrom;

use async_trait::async_trait;
use derive_more::{Deref, DerefMut};
use tokio::io::{self, AsyncWrite};
use tracing::trace;

use msql_srv::{
    Column, ColumnFlags, ColumnType, MysqlShim, QueryResultWriter, RowWriter, StatementMetaWriter,
};
use noria::consensus::Authority;
use noria::errors::internal_err;
use noria::{internal, DataType, ReadySetError};
use noria_client::backend::noria_connector;
use noria_client::backend::{PrepareResult, QueryResult, UpstreamPrepare};
use upstream::StatementMeta;

use crate::schema::convert_column;
use crate::upstream::{self, MySqlUpstream};
use crate::value::mysql_value_to_datatype;
use crate::Error;
use core::iter;

async fn write_column<W: AsyncWrite + Unpin>(
    rw: &mut RowWriter<'_, W>,
    c: &DataType,
    cs: &msql_srv::Column,
) -> Result<(), Error> {
    let written = match *c {
        DataType::None => rw.write_col(None::<i32>).await,
        // NOTE(malte): the code repetition here is unfortunate, but it's hard to factor
        // this out into a helper since i has a different time depending on the DataType
        // variant.
        DataType::Int(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize).await
            } else {
                rw.write_col(i as isize).await
            }
        }
        DataType::BigInt(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize).await
            } else {
                rw.write_col(i as isize).await
            }
        }
        DataType::UnsignedInt(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize).await
            } else {
                rw.write_col(i as isize).await
            }
        }
        DataType::UnsignedBigInt(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize).await
            } else {
                rw.write_col(i as isize).await
            }
        }
        DataType::Text(ref t) => rw.write_col(t.to_str().unwrap()).await,
        ref dt @ DataType::TinyText(_) => rw.write_col::<&str>(<&str>::try_from(dt)?).await,
        ref dt @ (DataType::Float(..) | DataType::Double(..)) => match cs.coltype {
            msql_srv::ColumnType::MYSQL_TYPE_DECIMAL => {
                let f = dt.to_string();
                rw.write_col(f).await
            }
            msql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                let f: f64 = <f64>::try_from(dt)?;
                rw.write_col(f).await
            }
            msql_srv::ColumnType::MYSQL_TYPE_FLOAT => {
                let f: f32 = <f32>::try_from(dt)?;
                rw.write_col(f).await
            }
            _ => {
                internal!()
            }
        },
        DataType::Timestamp(ts) => rw.write_col(ts).await,
        DataType::Time(ref t) => rw.write_col(t.as_ref()).await,
    };
    Ok(written?)
}

async fn write_query_results<W: AsyncWrite + Unpin>(
    r: Result<(u64, u64), Error>,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    match r {
        Ok((row_count, last_insert)) => results.completed(row_count, last_insert).await,
        Err(e) => {
            results
                .error(e.error_kind(), e.to_string().as_bytes())
                .await
        }
    }
}

#[derive(Deref, DerefMut)]
pub struct Backend<A: 'static + Authority>(pub noria_client::Backend<A, MySqlUpstream>);

#[async_trait]
impl<W, A> MysqlShim<W> for Backend<A>
where
    W: AsyncWrite + Unpin + Send + 'static,
    A: 'static + Authority,
{
    type Error = Error;

    async fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, W>,
    ) -> Result<(), Error> {
        use noria_connector::PrepareResult::*;

        trace!("delegate");
        let res = match self.prepare(query).await {
            Ok(PrepareResult::Noria(
                Select {
                    statement_id,
                    params,
                    schema,
                }
                | Insert {
                    statement_id,
                    params,
                    schema,
                },
            )) => {
                let params = params
                    .into_iter()
                    .map(|c| convert_column(&c.spec))
                    .collect::<Vec<_>>();
                let schema = schema
                    .into_iter()
                    .map(|c| convert_column(&c.spec))
                    .collect::<Vec<_>>();
                info.reply(statement_id, &params, &schema).await
            }
            Ok(PrepareResult::Noria(Update { params, .. })) => {
                let params = params
                    .into_iter()
                    .map(|c| convert_column(&c.spec))
                    .collect::<Vec<_>>();
                info.reply(self.prepared_count(), &params, &[]).await
            }
            Ok(PrepareResult::Upstream(UpstreamPrepare {
                meta: StatementMeta { params, schema },
                ..
            })) => {
                let params = params.iter().map(|c| c.into()).collect::<Vec<_>>();
                let schema = schema.iter().map(|c| c.into()).collect::<Vec<_>>();

                // TODO(grfn): make statement ID part of prepareresult
                info.reply(self.prepared_count(), &params, &schema).await
            }
            Err(Error::MySql(mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message,
                ..
            }))) => info.error(code.into(), message.as_bytes()).await,
            Err(e) => info.error(e.error_kind(), e.to_string().as_bytes()).await,
        };

        Ok(res?)
    }

    async fn on_execute(
        &mut self,
        id: u32,
        params: msql_srv::ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
    ) -> Result<(), Error> {
        let mut datatype_params = Vec::new();
        // TODO(DAN): Param conversions are unecessary for fallback execution. Params should be
        // derived directly from ParamParser.
        for p in params {
            datatype_params.push(mysql_value_to_datatype(p?.value)?);
        }

        let res = match self.execute(id, datatype_params).await {
            Ok(QueryResult::Noria(noria_connector::QueryResult::Select {
                data,
                select_schema,
            } )) => {
                let schema = select_schema.schema.iter().map(|cs| convert_column(&cs.spec)).collect::<Vec<_>>();
                let mut rw = results.start(&schema).await?;
                for resultsets in data {
                    for r in resultsets {
                        for c in &schema {
                            let coli = select_schema
                                .columns
                                .iter()
                                .position(|f| f == &c.column)
                                .ok_or_else(|| {
                                    internal_err(format!(
                                        "tried to emit column {:?} not in getter with schema {:?}",
                                        c.column, select_schema.columns
                                    ))
                                })?;
                            write_column(&mut rw, &r[coli], c).await?;
                        }
                        rw.end_row().await?;
                    }
                }
                rw.finish().await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Insert {
                num_rows_inserted,
                first_inserted_id,
            } )) => write_query_results(Ok((num_rows_inserted, first_inserted_id)), results).await,
            Ok(QueryResult::Noria(noria_connector::QueryResult::Update {
                num_rows_updated,
                last_inserted_id
            })) => write_query_results(Ok((num_rows_updated, last_inserted_id)), results).await,
            Ok(QueryResult::Upstream(upstream::QueryResult::WriteResult {
                num_rows_affected,
                last_inserted_id,
            })) => {
                write_query_results(
                    Ok((num_rows_affected, last_inserted_id)),
                    results,
                )
                .await
            }
            Ok(QueryResult::Upstream(upstream::QueryResult::ReadResult { data, columns })) => {
                let mut data = data.iter().peekable();
                if let Some(cols) = data.peek() {
                    let cols = cols.columns_ref();
                    let formatted_cols = cols
                        .iter()
                        .map(|c| c.into())
                        .collect::<Vec<_>>();
                    let mut rw = results.start(&formatted_cols).await?;
                    for r in data {
                        for (coli, _) in formatted_cols.iter().enumerate() {
                            rw.write_col(&r[coli]).await?;
                        }
                        rw.end_row().await?
                    }
                    rw.finish().await
                } else {
                    let formatted_cols = if let Some(c) = columns {
                        c.iter()
                            .map(|c| c.into())
                            .collect::<Vec<_>>()
                    } else {
                        vec![]
                    };
                    let rw = results.start(&formatted_cols).await?;
                    rw.finish().await
                }
            }
            Err(e @ Error::ReadySet(ReadySetError::PreparedStatementMissing { .. })) => {
                return results
                    .error(
                        e.error_kind(),
                        "non-existent statement".as_bytes(),
                    )
                    .await
                    .map_err(Error::from)
            }
            Err(Error::MySql(mysql_async::Error::Server(mysql_async::ServerError{code,
                message, ..}))) => {
                results.error(code.into(), message.as_bytes()).await
            }
            Err(e) => results.error(e.error_kind(), e.to_string().as_bytes()).await,
            _ => internal!("Matched a QueryResult that is not supported by on_prepare/on_execute in on_execute."),
        };

        Ok(res?)
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> Result<(), Error> {
        let query_result = if query.starts_with("SELECT @@") || query.starts_with("select @@") {
            // We are selecting system variables, so we would want to fallback to MySQL.
            if self.has_fallback() {
                self.query_fallback(query).await
            } else {
                // There's no fallback, so we return nothing, except if the variable is
                // `@@max_allowed_packet`, because that is a result that the MySQL library
                // needs to establish a connection (when using `[mysql::Conn::new]`).
                let var = &query.get(b"SELECT @@".len()..);
                return match var {
                    Some("max_allowed_packet") => {
                        let cols = &[Column {
                            table: String::new(),
                            column: "@@max_allowed_packet".to_owned(),
                            coltype: ColumnType::MYSQL_TYPE_LONG,
                            colflags: ColumnFlags::UNSIGNED_FLAG,
                        }];
                        let mut w = results.start(cols).await?;
                        w.write_row(iter::once(67108864u32)).await?;
                        Ok(w.finish().await?)
                    }
                    _ => Ok(results.completed(0, 0).await?),
                };
            }
        } else {
            self.query(query).await
        };
        let res = match query_result {
            Ok(QueryResult::Noria(
                noria_connector::QueryResult::CreateTable
                | noria_connector::QueryResult::CreateView,
            )) => results.completed(0, 0).await,
            Ok(QueryResult::Noria(noria_connector::QueryResult::Insert {
                num_rows_inserted,
                first_inserted_id,
            })) => write_query_results(Ok((num_rows_inserted, first_inserted_id)), results).await,
            Ok(QueryResult::Noria(noria_connector::QueryResult::Select {
                data,
                select_schema,
            })) => {
                let schema = select_schema
                    .schema
                    .iter()
                    .map(|cs| convert_column(&cs.spec))
                    .collect::<Vec<_>>();
                let mut rw = results.start(&schema).await?;
                for resultsets in data {
                    for r in resultsets {
                        for c in &schema {
                            let coli = select_schema
                                .columns
                                .iter()
                                .position(|f| f == &c.column)
                                .ok_or_else(|| {
                                    internal_err(format!(
                                        "tried to emit column {:?} not in getter with schema {:?}",
                                        c.column, select_schema.columns
                                    ))
                                })?;
                            write_column(&mut rw, &r[coli], c).await?;
                        }
                        rw.end_row().await?;
                    }
                }
                rw.finish().await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Update {
                num_rows_updated,
                last_inserted_id,
            })) => write_query_results(Ok((num_rows_updated, last_inserted_id)), results).await,
            Ok(QueryResult::Noria(noria_connector::QueryResult::Delete { num_rows_deleted })) => {
                results.completed(num_rows_deleted, 0).await
            }
            Ok(QueryResult::Upstream(upstream::QueryResult::WriteResult {
                num_rows_affected,
                last_inserted_id,
            })) => write_query_results(Ok((num_rows_affected, last_inserted_id)), results).await,
            Ok(QueryResult::Upstream(upstream::QueryResult::ReadResult { data, columns })) => {
                if let Some(cols) = data.get(0).cloned() {
                    let cols = cols.columns_ref();
                    let formatted_cols = cols.iter().map(|c| c.into()).collect::<Vec<_>>();
                    let mut rw = results.start(&formatted_cols).await?;
                    for r in data {
                        for (coli, _) in formatted_cols.iter().enumerate() {
                            rw.write_col(&r[coli]).await?;
                        }
                        rw.end_row().await?
                    }
                    rw.finish().await
                } else {
                    let formatted_cols = if let Some(c) = columns {
                        c.iter().map(|c| c.into()).collect::<Vec<_>>()
                    } else {
                        vec![]
                    };
                    let rw = results.start(&formatted_cols).await?;
                    rw.finish().await
                }
            }
            Ok(QueryResult::Upstream(upstream::QueryResult::None)) => {
                let rw = results.start(&[]).await?;
                rw.finish().await
            }
            Err(Error::MySql(mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message,
                ..
            }))) => results.error(code.into(), message.as_bytes()).await,
            Err(e) => {
                results
                    .error(e.error_kind(), e.to_string().as_bytes())
                    .await
            }
        };

        Ok(res?)
    }

    fn password_for_username(&self, username: &[u8]) -> Option<Vec<u8>> {
        String::from_utf8(username.to_vec())
            .ok()
            .and_then(|un| self.users.get(&un))
            .cloned()
            .map(String::into_bytes)
    }

    fn require_authentication(&self) -> bool {
        self.require_authentication
    }
}
