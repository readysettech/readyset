use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::consts::StatusFlags;
use mysql_srv::{
    Column, ColumnFlags, ColumnType, InitWriter, MsqlSrvError, MysqlShim, QueryResultWriter,
    RowWriter, StatementMetaWriter,
};
use noria_client::backend::noria_connector::MetaVariable;
use noria_client::backend::{noria_connector, QueryResult, SinglePrepareResult, UpstreamPrepare};
use noria_data::DataType;
use noria_errors::{internal, internal_err, ReadySetError};
use tokio::io::{self, AsyncWrite};
use tracing::{error, trace};
use upstream::StatementMeta;

use crate::schema::convert_column;
use crate::upstream::{self, MySqlUpstream};
use crate::value::mysql_value_to_datatype;
use crate::{Error, MySqlQueryHandler};

async fn write_column<W: AsyncWrite + Unpin>(
    rw: &mut RowWriter<'_, W>,
    c: &DataType,
    cs: &mysql_srv::Column,
) -> Result<(), Error> {
    let written = match *c {
        DataType::None | DataType::Max => rw.write_col(None::<i32>),
        // NOTE(malte): the code repetition here is unfortunate, but it's hard to factor
        // this out into a helper since i has a different time depending on the DataType
        // variant.
        DataType::Int(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize)
            } else {
                rw.write_col(i as isize)
            }
        }
        DataType::UnsignedInt(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize)
            } else {
                rw.write_col(i as isize)
            }
        }
        DataType::Text(ref t) => rw.write_col(t.as_str()),
        DataType::TinyText(ref t) => rw.write_col(t.as_str()),
        ref dt @ (DataType::Float(..) | DataType::Double(..)) => match cs.coltype {
            mysql_srv::ColumnType::MYSQL_TYPE_DECIMAL
            | mysql_srv::ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                let f = dt.to_string();
                rw.write_col(f)
            }
            mysql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                let f: f64 = <f64>::try_from(dt)?;
                rw.write_col(f)
            }
            mysql_srv::ColumnType::MYSQL_TYPE_FLOAT => {
                let f: f32 = <f32>::try_from(dt)?;
                rw.write_col(f)
            }
            _ => {
                internal!()
            }
        },
        DataType::TimestampTz(ts) => match cs.coltype {
            mysql_srv::ColumnType::MYSQL_TYPE_DATETIME
            | mysql_srv::ColumnType::MYSQL_TYPE_DATETIME2
            | mysql_srv::ColumnType::MYSQL_TYPE_TIMESTAMP
            | mysql_srv::ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
                rw.write_col(ts.to_chrono().naive_local())
            }
            ColumnType::MYSQL_TYPE_DATE => rw.write_col(ts.to_chrono().naive_local().date()),
            _ => internal!("Expecing a timestamp-like type, got {:?}", cs.coltype),
        },
        DataType::Time(ref t) => rw.write_col(t),
        DataType::ByteArray(ref bytes) => rw.write_col(bytes.as_ref()),
        DataType::Numeric(_) => unimplemented!("MySQL does not implement the type NUMERIC"),
        // These types are PostgreSQL specific
        DataType::BitVector(_) => {
            internal!("Cannot write MySQL column: MySQL does not support bit vectors")
        }
    };
    Ok(written?)
}

async fn write_query_results<W: AsyncWrite + Unpin>(
    r: Result<(u64, u64), Error>,
    results: QueryResultWriter<'_, W>,
    status_flags: Option<StatusFlags>,
) -> io::Result<()> {
    match r {
        Ok((row_count, last_insert)) => {
            results
                .completed(row_count, last_insert, status_flags)
                .await
        }
        Err(e) => {
            results
                .error(e.error_kind(), e.to_string().as_bytes())
                .await
        }
    }
}

/// Writes a Vec of [`MetaVariable`] as a table with a single row, where the column names correspond
/// to the variable names and the row values correspond to the variable values
async fn write_meta_table<W: AsyncWrite + Unpin>(
    vars: Vec<MetaVariable>,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let cols = vars
        .iter()
        .map(|v| Column {
            table: "".to_owned(),
            column: v.name.to_string(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            colflags: ColumnFlags::empty(),
        })
        .collect::<Vec<_>>();

    let mut writer = results.start(&cols).await?;

    for var in vars {
        writer.write_col(var.value)?;
    }
    writer.end_row()?;
    writer.finish().await
}

/// Writes a Vec of [`MetaVariable`] as a table with two columns, where each row represents one
/// varaible, with the first column being the variable name and the second column its value
async fn write_meta_variables<W: AsyncWrite + Unpin>(
    vars: Vec<MetaVariable>,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    // Assign column schema to match MySQL
    // [`SHOW STATUS`](https://dev.mysql.com/doc/refman/8.0/en/show-status.html)
    let cols = vec![
        Column {
            table: "".to_owned(),
            column: "Variable_name".to_string(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            colflags: ColumnFlags::empty(),
        },
        Column {
            table: "".to_owned(),
            column: "Value".to_string(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            colflags: ColumnFlags::empty(),
        },
    ];
    let mut writer = results.start(&cols).await?;
    for v in vars {
        writer.write_col(v.name.as_str())?;
        writer.write_col(v.value)?;
        writer.end_row()?;
    }
    writer.finish().await
}

pub struct Backend {
    /// Handle to the backing noria client
    noria: noria_client::Backend<MySqlUpstream, MySqlQueryHandler>,
    /// A cache of schemas per statement id
    schema_cache: HashMap<u32, CachedSchema>,
}

impl Backend {
    pub fn new(noria: noria_client::Backend<MySqlUpstream, MySqlQueryHandler>) -> Self {
        Backend {
            noria,
            schema_cache: HashMap::new(),
        }
    }
}

impl Deref for Backend {
    type Target = noria_client::Backend<MySqlUpstream, MySqlQueryHandler>;

    fn deref(&self) -> &Self::Target {
        &self.noria
    }
}

impl DerefMut for Backend {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.noria
    }
}

struct CachedSchema {
    mysql_schema: Vec<mysql_srv::Column>,
    column_map: Vec<Option<usize>>,
    preencoded_schema: Arc<[u8]>,
}

macro_rules! convert_columns {
    ($columns: expr, $results: expr) => {{
        match $columns
            .into_iter()
            .map(|c| convert_column(&c.spec))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(res) => res,
            Err(e) => {
                return $results
                    .error(
                        mysql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                        e.to_string().as_bytes(),
                    )
                    .await;
            }
        }
    }};
}

#[async_trait]
impl<W> MysqlShim<W> for Backend
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, W>,
    ) -> io::Result<()> {
        use noria_connector::PrepareResult::*;

        trace!("delegate");
        let prepare_result = self.prepare(query).await.map(|p| p.upstream_biased());
        let res = match prepare_result {
            Ok(SinglePrepareResult::Noria(
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
                let statement_id = *statement_id; // Just to break borrow dependency
                let params = convert_columns!(params, info);
                let schema = convert_columns!(schema, info);
                self.schema_cache.remove(&statement_id);
                info.reply(statement_id, &params, &schema).await
            }
            Ok(SinglePrepareResult::Noria(Update { params, .. } | Delete { params, .. })) => {
                let params = convert_columns!(params, info);
                info.reply(self.last_prepared_id(), &params, &[]).await
            }
            Ok(SinglePrepareResult::Upstream(UpstreamPrepare {
                meta: StatementMeta { params, schema },
                ..
            })) => {
                let params = params.iter().map(|c| c.into()).collect::<Vec<_>>();
                let schema = schema.iter().map(|c| c.into()).collect::<Vec<_>>();

                // TODO(grfn): make statement ID part of prepareresult
                info.reply(self.last_prepared_id(), &params, &schema).await
            }

            Err(Error::MySql(mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message,
                ..
            }))) => info.error(code.into(), message.as_bytes()).await,
            Err(Error::MySql(mysql_async::Error::Driver(
                mysql_async::DriverError::ConnectionClosed,
            ))) => {
                // In this case connection to fallback closed, so
                // we should close our connection to the client.
                // This should cause them to re-initiate a
                // connection, allowing us to form a new connection
                // to fallback.
                error!("upstream connection closed");
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "upstream connection closed",
                ));
            }
            Err(Error::Io(e))
            | Err(Error::MySql(mysql_async::Error::Io(mysql_async::IoError::Io(e))))
            | Err(Error::MsqlSrv(MsqlSrvError::IoError(e))) => {
                #[cfg(feature = "display_literals")]
                error!(err = %e, "encountered io error preparing query: {}", query);
                #[cfg(not(feature = "display_literals"))]
                error!(err = %e, "encountered io error preparing query");
                // In the case that we encountered an io error, we should bubble it up so the
                // connection can be closed. This is usually an unrecoverable error, and the client
                // should re-initiate a connection with us so we can start with a fresh slate.
                return Err(e);
            }
            Err(e) => info.error(e.error_kind(), e.to_string().as_bytes()).await,
        };

        Ok(res?)
    }

    async fn on_execute(
        &mut self,
        id: u32,
        params: mysql_srv::ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        // TODO(DAN): Param conversions are unecessary for fallback execution. Params should be
        // derived directly from ParamParser.
        let params_result = params
            .into_iter()
            .flat_map(|p| p.map(|pval| mysql_value_to_datatype(pval.value).map_err(Error::from)))
            .collect::<Result<Vec<DataType>, Error>>();

        let datatype_params = match params_result {
            Ok(r) => r,
            Err(e) => {
                error!(err = %e, "encountered error parsing execute params");
                return results
                    .error(e.error_kind(), e.to_string().as_bytes())
                    .await;
            }
        };

        // We have to perform this check before self is mutably borrowed by execute
        let is_cached = self.schema_cache.contains_key(&id);

        let res = match self.execute(id, &datatype_params).await {
            Ok(QueryResult::Noria(noria_connector::QueryResult::Select {
                data,
                select_schema,
            })) => {
                let CachedSchema {
                    mysql_schema,
                    column_map,
                    preencoded_schema,
                } = if is_cached {
                    // Unwrap here is ok because we know the map contains that key
                    self.schema_cache.get(&id).unwrap()
                } else {
                    let mysql_schema = convert_columns!(select_schema.schema, results);
                    let preencoded_schema = mysql_srv::prepare_column_definitions(&mysql_schema);

                    // Now append the right position too
                    let column_map = mysql_schema
                        .iter()
                        .map(|c| {
                            select_schema
                                .columns
                                .iter()
                                .position(|f| c.column == f.as_str())
                        })
                        .collect::<Vec<_>>();

                    drop(select_schema);
                    self.schema_cache.entry(id).or_insert(CachedSchema {
                        mysql_schema,
                        column_map,
                        preencoded_schema: preencoded_schema.into(),
                    })
                };

                let mut rw = results
                    .start_with_cache(mysql_schema, preencoded_schema.clone())
                    .await?;
                for r in data.into_iter().flatten() {
                    for (c, pos) in mysql_schema.iter().zip(column_map.iter()) {
                        match pos {
                            Some(coli) => {
                                if let Err(e) = write_column(&mut rw, &r[*coli], c).await {
                                    return handle_column_write_err(e, rw).await;
                                };
                            }
                            None => {
                                let e = Error::from(internal_err(format!(
                                    "tried to emit column {:?} not in getter with schema {:?}",
                                    c.column, mysql_schema
                                )));
                                error!(err = %e);
                                return rw.error(e.error_kind(), e.to_string().as_bytes()).await;
                            }
                        }
                    }
                    rw.end_row()?;
                }
                rw.finish().await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Insert {
                num_rows_inserted,
                first_inserted_id,
            })) => {
                write_query_results(Ok((num_rows_inserted, first_inserted_id)), results, None).await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Update {
                num_rows_updated,
                last_inserted_id,
            })) => {
                write_query_results(Ok((num_rows_updated, last_inserted_id)), results, None).await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Delete { num_rows_deleted })) => {
                write_query_results(Ok((num_rows_deleted, 0)), results, None).await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Meta(vars))) => {
                write_meta_table(vars, results).await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::MetaVariables(vars))) => {
                write_meta_variables(vars, results).await
            }
            Ok(QueryResult::Upstream(upstream::QueryResult::WriteResult {
                num_rows_affected,
                last_inserted_id,
                status_flags,
            })) => {
                write_query_results(
                    Ok((num_rows_affected, last_inserted_id)),
                    results,
                    Some(status_flags),
                )
                .await
            }
            Ok(QueryResult::Upstream(upstream::QueryResult::ReadResult {
                data,
                columns,
                status_flags,
            })) => {
                let mut data = data.iter().peekable();
                if let Some(cols) = data.peek() {
                    let cols = cols.columns_ref();
                    let formatted_cols = cols.iter().map(|c| c.into()).collect::<Vec<_>>();
                    let mut rw = results.start(&formatted_cols).await?;
                    for r in data {
                        for (coli, _) in formatted_cols.iter().enumerate() {
                            rw.write_col(&r[coli])?;
                        }
                        rw.end_row()?
                    }
                    rw.set_status_flags(status_flags).finish().await
                } else {
                    let formatted_cols = if let Some(c) = columns {
                        c.iter().map(|c| c.into()).collect::<Vec<_>>()
                    } else {
                        vec![]
                    };
                    let rw = results.start(&formatted_cols).await?;
                    rw.set_status_flags(status_flags).finish().await
                }
            }
            Err(e @ Error::ReadySet(ReadySetError::PreparedStatementMissing { .. })) => {
                return results
                    .error(e.error_kind(), "non-existent statement".as_bytes())
                    .await;
            }
            Err(Error::MySql(mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message,
                ..
            }))) => results.error(code.into(), message.as_bytes()).await,
            Err(Error::MySql(mysql_async::Error::Driver(
                mysql_async::DriverError::ConnectionClosed,
            ))) => {
                // In this case connection to fallback closed, so
                // we should close our connection to the client.
                // This should cause them to re-initiate a
                // connection, allowing us to form a new connection
                // to fallback.
                error!("upstream connection closed");
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "upstream connection closed",
                ));
            }
            Err(Error::Io(e))
            | Err(Error::MySql(mysql_async::Error::Io(mysql_async::IoError::Io(e))))
            | Err(Error::MsqlSrv(MsqlSrvError::IoError(e))) => {
                error!(err = %e, "encountered io error while attempting to execute a prepared statement");
                // In the case that we encountered an io error, we should bubble it up so the
                // connection can be closed. This is usually an unrecoverable error, and the client
                // should re-initiate a connection with us so we can start with a fresh slate.
                return Err(e);
            }
            Err(e) => {
                results
                    .error(e.error_kind(), e.to_string().as_bytes())
                    .await
            }
            _ => {
                let e = Error::from(internal_err("Matched a QueryResult that is not supported by on_prepare/on_execute in on_execute."));
                error!(err = %e);
                results
                    .error(e.error_kind(), e.to_string().as_bytes())
                    .await
            }
        };

        Ok(res?)
    }

    async fn on_init(&mut self, database: &str, w: InitWriter<'_, W>) -> io::Result<()> {
        let res = if self.has_fallback() {
            match self.database() {
                Some(db_name) => {
                    if db_name.to_ascii_lowercase() == database.to_ascii_lowercase() {
                        // We are already using the correct database. Write back an ok packet.
                        w.ok().await
                    } else {
                        w.error(
                            mysql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                            "Tried to use database that ReadySet is not replicating from"
                                .to_string()
                                .as_bytes(),
                        )
                        .await
                    }
                }
                None => w.ok().await,
            }
        } else {
            w.ok().await
        };

        Ok(res?)
    }
    async fn on_close(&mut self, _: u32) {}

    async fn on_query(&mut self, query: &str, results: QueryResultWriter<'_, W>) -> io::Result<()> {
        let res = match self.query(query).await {
            Ok(QueryResult::Noria(noria_connector::QueryResult::Empty)) => {
                results.completed(0, 0, None).await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Insert {
                num_rows_inserted,
                first_inserted_id,
            })) => {
                write_query_results(Ok((num_rows_inserted, first_inserted_id)), results, None).await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Select {
                data,
                select_schema,
            })) => {
                let schema = convert_columns!(select_schema.schema, results);
                let mut rw = results.start(&schema).await?;
                for resultsets in data {
                    for r in resultsets {
                        for c in &schema {
                            match select_schema
                                .columns
                                .iter()
                                .position(|f| f.as_str() == c.column)
                            {
                                Some(coli) => {
                                    if let Err(e) = write_column(&mut rw, &r[coli], c).await {
                                        return handle_column_write_err(e, rw).await;
                                    }
                                }
                                None => {
                                    let e = Error::from(internal_err(format!(
                                        "tried to emit column {:?} not in getter with schema {:?}",
                                        c.column, select_schema.columns
                                    )));
                                    error!(err = %e);
                                    return rw
                                        .error(e.error_kind(), e.to_string().as_bytes())
                                        .await;
                                }
                            }
                        }
                        rw.end_row()?;
                    }
                }
                rw.finish().await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Update {
                num_rows_updated,
                last_inserted_id,
            })) => {
                write_query_results(Ok((num_rows_updated, last_inserted_id)), results, None).await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Delete { num_rows_deleted })) => {
                results.completed(num_rows_deleted, 0, None).await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::Meta(vars))) => {
                write_meta_table(vars, results).await
            }
            Ok(QueryResult::Noria(noria_connector::QueryResult::MetaVariables(vars))) => {
                write_meta_variables(vars, results).await
            }
            Ok(QueryResult::Upstream(upstream::QueryResult::WriteResult {
                num_rows_affected,
                last_inserted_id,
                status_flags,
            })) => {
                write_query_results(
                    Ok((num_rows_affected, last_inserted_id)),
                    results,
                    Some(status_flags),
                )
                .await
            }
            Ok(QueryResult::Upstream(upstream::QueryResult::ReadResult {
                data,
                columns,
                status_flags,
            })) => {
                if let Some(cols) = data.get(0).cloned() {
                    let cols = cols.columns_ref();
                    let formatted_cols = cols.iter().map(|c| c.into()).collect::<Vec<_>>();
                    let mut rw = results.start(&formatted_cols).await?;
                    for r in data {
                        for (coli, _) in formatted_cols.iter().enumerate() {
                            rw.write_col(&r[coli])?;
                        }
                        rw.end_row()?
                    }
                    rw.set_status_flags(status_flags).finish().await
                } else {
                    let formatted_cols = if let Some(c) = columns {
                        c.iter().map(|c| c.into()).collect::<Vec<_>>()
                    } else {
                        vec![]
                    };
                    let rw = results.start(&formatted_cols).await?;
                    rw.set_status_flags(status_flags).finish().await
                }
            }
            Ok(QueryResult::Upstream(upstream::QueryResult::Command { status_flags })) => {
                let rw = results.start(&[]).await?;
                rw.set_status_flags(status_flags).finish().await
            }
            Err(Error::MySql(mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message,
                ..
            }))) => results.error(code.into(), message.as_bytes()).await,
            Err(Error::MySql(mysql_async::Error::Driver(
                mysql_async::DriverError::ConnectionClosed,
            ))) => {
                // In this case connection to fallback closed, so
                // we should close our connection to the client.
                // This should cause them to re-initiate a
                // connection, allowing us to form a new connection
                // to fallback.
                error!("upstream connection closed");
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "upstream connection closed",
                ));
            }
            Err(Error::Io(e))
            | Err(Error::MySql(mysql_async::Error::Io(mysql_async::IoError::Io(e))))
            | Err(Error::MsqlSrv(MsqlSrvError::IoError(e))) => {
                #[cfg(feature = "display_literals")]
                error!(err = %e, "encountered io error while attempting to execute query: {}", query);

                #[cfg(not(feature = "display_literals"))]
                error!(err = %e, "encountered io error while attempting to execute query");
                // In the case that we encountered an io error, we should bubble it up so the
                // connection can be closed. This is usually an unrecoverable error, and the client
                // should re-initiate a connection with us so we can start with a fresh slate.
                return Err(e);
            }
            Err(e) => {
                results
                    .error(e.error_kind(), e.to_string().as_bytes())
                    .await
            }
        };

        Ok(res?)
    }

    fn password_for_username(&self, username: &str) -> Option<Vec<u8>> {
        self.users.get(username).cloned().map(String::into_bytes)
    }

    fn require_authentication(&self) -> bool {
        self.require_authentication
    }
}

async fn handle_column_write_err<W: AsyncWrite + Unpin>(
    e: Error,
    rw: RowWriter<'_, W>,
) -> io::Result<()> {
    error!(err = %e, "encountered error while attempting to write column packet");
    match e {
        Error::Io(io_e) => {
            // In the case that we encountered an io error, we should bubble it up so the
            // connection can be closed. This is usually an unrecoverable error, and the client
            // should re-initiate a connection with us so we can start with a fresh slate.
            Err(io_e)
        }
        Error::MySql(mysql_async::Error::Driver(mysql_async::DriverError::ConnectionClosed)) => {
            // In this case connection to fallback closed, so
            // we should close our connection to the client.
            // This should cause them to re-initiate a
            // connection, allowing us to form a new connection
            // to fallback.
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "upstream connection closed",
            ))
        }
        _ => rw.error(e.error_kind(), e.to_string().as_bytes()).await,
    }
}
