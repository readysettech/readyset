use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::{Deref, DerefMut};

use async_trait::async_trait;
use futures_util::StreamExt;
use itertools::izip;
use launchpad::redacted::Sensitive;
use mysql_async::consts::StatusFlags;
use mysql_common::bigdecimal03::ToPrimitive;
use mysql_srv::{
    CachedSchema, Column, ColumnFlags, ColumnType, InitWriter, MsqlSrvError, MySqlShim,
    QueryResultWriter, RowWriter, StatementMetaWriter,
};
use nom_sql::Dialect;
use readyset_client::backend::noria_connector::MetaVariable;
use readyset_client::backend::{
    noria_connector, QueryResult, SinglePrepareResult, UpstreamPrepare,
};
use readyset_data::{Collation, DfType, DfValue, DfValueKind};
use readyset_errors::{internal, internal_err, ReadySetError};
use streaming_iterator::StreamingIterator;
use tokio::io::{self, AsyncWrite};
use tracing::{error, trace};
use upstream::StatementMeta;

use crate::constants::DEFAULT_CHARACTER_SET;
use crate::schema::convert_column;
use crate::upstream::{self, MySqlUpstream};
use crate::value::mysql_value_to_dataflow_value;
use crate::{Error, MySqlQueryHandler};

async fn write_column<W: AsyncWrite + Unpin>(
    rw: &mut RowWriter<'_, W>,
    c: &DfValue,
    cs: &mysql_srv::Column,
    ty: &DfType,
) -> Result<(), Error> {
    let conv_error = || ReadySetError::DfValueConversionError {
        src_type: format!("{:?}", DfValueKind::from(c)),
        target_type: format!("{:?}", cs.coltype),
        details: "Unhandled type conversion in `write_column`".to_string(),
    };

    let written = match *c {
        DfValue::None | DfValue::Max => rw.write_col(None::<i32>),
        // NOTE(malte): the code repetition here is unfortunate, but it's hard to factor
        // this out into a helper since i has a different time depending on the DfValue
        // variant.
        DfValue::Int(i) => {
            if ty.is_enum() {
                rw.write_col(
                    c.coerce_to(&DfType::Text(Collation::default()), ty)?
                        .to_string(),
                )
            } else if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize)
            } else {
                rw.write_col(i as isize)
            }
        }
        DfValue::UnsignedInt(i) => {
            if ty.is_enum() {
                rw.write_col(
                    c.coerce_to(&DfType::Text(Collation::default()), ty)?
                        .to_string(),
                )
            } else if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize)
            } else {
                rw.write_col(i as isize)
            }
        }
        DfValue::Text(ref t) => rw.write_col(t.as_str()),
        DfValue::TinyText(ref t) => rw.write_col(t.as_str()),
        ref dt @ (DfValue::Float(..) | DfValue::Double(..)) => match cs.coltype {
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
            _ => return Err(conv_error())?,
        },
        DfValue::Numeric(ref v) => match cs.coltype {
            mysql_srv::ColumnType::MYSQL_TYPE_DECIMAL
            | mysql_srv::ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                let f = v.to_string();
                rw.write_col(f)
            }
            mysql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                let f = v.to_f64().ok_or_else(conv_error)?;
                rw.write_col(f)
            }
            mysql_srv::ColumnType::MYSQL_TYPE_FLOAT => {
                let f = v.to_f64().ok_or_else(conv_error)?;
                rw.write_col(f)
            }
            _ => return Err(conv_error())?,
        },

        DfValue::TimestampTz(ts) => match cs.coltype {
            mysql_srv::ColumnType::MYSQL_TYPE_DATETIME
            | mysql_srv::ColumnType::MYSQL_TYPE_DATETIME2
            | mysql_srv::ColumnType::MYSQL_TYPE_TIMESTAMP
            | mysql_srv::ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
                rw.write_col(ts.to_chrono().naive_local())
            }
            ColumnType::MYSQL_TYPE_DATE => rw.write_col(ts.to_chrono().naive_local().date()),
            _ => return Err(conv_error())?,
        },
        DfValue::Time(ref t) => rw.write_col(t),
        DfValue::ByteArray(ref bytes) => rw.write_col(bytes.as_ref()),
        // These types are PostgreSQL specific
        DfValue::Array(_) => {
            internal!("Cannot write MySQL column: MySQL does not support arrays")
        }
        DfValue::BitVector(_) => {
            internal!("Cannot write MySQL column: MySQL does not support bit vectors")
        }
        DfValue::PassThrough(_) => {
            internal!("Cannot write MySQL column: PassThrough types aren't supported for MySQL")
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
            column_length: None,
            colflags: ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
        })
        .collect::<Vec<_>>();

    let mut writer = results.start(&cols).await?;

    for var in vars {
        writer.write_col(var.value)?;
    }
    writer.end_row().await?;
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
            column_length: None,
            colflags: ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
        },
        Column {
            table: "".to_owned(),
            column: "Value".to_string(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            column_length: None,
            colflags: ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
        },
    ];
    let mut writer = results.start(&cols).await?;
    for v in vars {
        writer.write_col(v.name.as_str())?;
        writer.write_col(v.value)?;
        writer.end_row().await?;
    }
    writer.finish().await
}

pub struct Backend {
    /// Handle to the backing noria client
    noria: readyset_client::Backend<MySqlUpstream, MySqlQueryHandler>,
}

impl Backend {
    #[allow(dead_code)]
    pub fn new(noria: readyset_client::Backend<MySqlUpstream, MySqlQueryHandler>) -> Self {
        Backend { noria }
    }
}

impl Deref for Backend {
    type Target = readyset_client::Backend<MySqlUpstream, MySqlQueryHandler>;

    fn deref(&self) -> &Self::Target {
        &self.noria
    }
}

impl DerefMut for Backend {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.noria
    }
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

macro_rules! handle_error {
    ($error: expr, $writer: expr) => {
        match $error {
            Error::MySql(mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message,
                ..
            })) => $writer.error(code.into(), message.as_bytes()).await,
            Error::MySql(mysql_async::Error::Driver(
                mysql_async::DriverError::ConnectionClosed,
            )) => {
                // In this case connection to fallback closed, so
                // we should close our connection to the client.
                // This should cause them to re-initiate a
                // connection, allowing us to form a new connection
                // to fallback.
                error!("upstream connection closed");
                 Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "upstream connection closed",
                ))
            }
            Error::Io(e)
            | Error::MySql(mysql_async::Error::Io(mysql_async::IoError::Io(e)))
            | Error::MsqlSrv(MsqlSrvError::IoError(e)) => {
                error!(err = %e, "encountered io error while attempting to execute query");
                // In the case that we encountered an io error, we should bubble it up so the
                // connection can be closed. This is usually an unrecoverable error, and the client
                // should re-initiate a connection with us so we can start with a fresh slate.
                Err(e)
            }
            err => {
                $writer
                    .error(err.error_kind(), err.to_string().as_bytes())
                    .await
            }
        }
    };
}

async fn handle_readyset_result<'a, W>(
    result: noria_connector::QueryResult<'a>,
    writer: QueryResultWriter<'_, W>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match result {
        noria_connector::QueryResult::Empty => writer.completed(0, 0, None).await,
        noria_connector::QueryResult::Insert {
            num_rows_inserted,
            first_inserted_id,
        } => write_query_results(Ok((num_rows_inserted, first_inserted_id)), writer, None).await,
        noria_connector::QueryResult::Update {
            num_rows_updated,
            last_inserted_id,
        } => write_query_results(Ok((num_rows_updated, last_inserted_id)), writer, None).await,
        noria_connector::QueryResult::Delete { num_rows_deleted } => {
            writer.completed(num_rows_deleted, 0, None).await
        }
        noria_connector::QueryResult::Meta(vars) => write_meta_table(vars, writer).await,
        noria_connector::QueryResult::MetaVariables(vars) => {
            write_meta_variables(vars, writer).await
        }
        noria_connector::QueryResult::Select { mut rows, schema } => {
            let mysql_schema = convert_columns!(schema.schema, writer);
            let columns = schema.columns;
            let mut rw = writer.start(&mysql_schema).await?;
            while let Some(row) = rows.next() {
                for c in &mysql_schema {
                    match columns.iter().position(|f| f.as_str() == c.column) {
                        Some(coli) => {
                            let ty = schema
                                .schema
                                .get(coli)
                                .map(|cs| DfType::from_sql_type(&cs.spec.sql_type, Dialect::MySQL))
                                .unwrap_or_default();

                            if let Err(e) = write_column(&mut rw, &row[coli], c, &ty).await {
                                return handle_column_write_err(e, rw).await;
                            }
                        }
                        None => {
                            let e = Error::from(internal_err!(
                                "tried to emit column {:?} not in getter with schema {:?}",
                                c.column,
                                columns
                            ));
                            error!(err = %e);
                            return rw.error(e.error_kind(), e.to_string().as_bytes()).await;
                        }
                    }
                }
                rw.end_row().await?;
            }
            rw.finish().await
        }
    }
}

async fn handle_upstream_result<'a, W>(
    result: upstream::QueryResult<'a>,
    writer: QueryResultWriter<'_, W>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match result {
        upstream::QueryResult::Command { status_flags } => {
            let rw = writer.start(&[]).await?;
            rw.set_status_flags(status_flags).finish().await
        }
        upstream::QueryResult::WriteResult {
            num_rows_affected,
            last_inserted_id,
            status_flags,
        } => {
            write_query_results(
                Ok((num_rows_affected, last_inserted_id)),
                writer,
                Some(status_flags),
            )
            .await
        }
        upstream::QueryResult::ReadResult {
            mut stream,
            columns,
        } => {
            let formatted_cols = columns.iter().map(|c| c.into()).collect::<Vec<_>>();
            let mut rw = writer.start(&formatted_cols).await?;
            while let Some(row) = stream.next().await {
                let row = match row {
                    Ok(row) => row,
                    Err(err) => return handle_error!(Error::MySql(err), rw),
                };

                for (i, _) in row.columns_ref().iter().enumerate() {
                    rw.write_col(row.as_ref(i).expect("Must match column number"))?;
                }
                rw.end_row().await?;
            }

            if let Some(status_flags) = stream.status_flags() {
                rw = rw.set_status_flags(status_flags)
            }

            rw.finish().await
        }
    }
}

async fn handle_query_result<'a, W>(
    result: Result<QueryResult<'a, MySqlUpstream>, Error>,
    writer: QueryResultWriter<'_, W>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match result {
        Ok(QueryResult::Noria(result)) => handle_readyset_result(result, writer).await,
        Ok(QueryResult::Upstream(result)) => handle_upstream_result(result, writer).await,
        Err(error) => handle_error!(error, writer),
    }
}

#[async_trait]
impl<W> MySqlShim<W> for Backend
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, W>,
        schema_cache: &mut HashMap<u32, CachedSchema>,
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
                schema_cache.remove(&statement_id);
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
                error!(err = %e, "encountered io error preparing query: {}", Sensitive(&query));
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
        schema_cache: &mut HashMap<u32, CachedSchema>,
    ) -> io::Result<()> {
        // TODO(DAN): Param conversions are unnecessary for fallback execution. Params should be
        // derived directly from ParamParser.
        let params_result = params
            .into_iter()
            .flat_map(|p| {
                p.map(|pval| mysql_value_to_dataflow_value(pval.value).map_err(Error::from))
            })
            .collect::<Result<Vec<DfValue>, Error>>();

        let value_params = match params_result {
            Ok(r) => r,
            Err(e) => {
                error!(err = %e, "encountered error parsing execute params");
                return results
                    .error(e.error_kind(), e.to_string().as_bytes())
                    .await;
            }
        };

        match self.execute(id, &value_params).await {
            Ok(QueryResult::Noria(noria_connector::QueryResult::Select { mut rows, schema })) => {
                let CachedSchema {
                    mysql_schema,
                    column_types,
                    column_map,
                    preencoded_schema,
                } = match schema_cache.entry(id) {
                    // `or_insert_with` would be cleaner but we need an async closure here
                    Entry::Occupied(schema) => schema.into_mut(),
                    Entry::Vacant(entry) => {
                        let mysql_schema = convert_columns!(schema.schema, results);
                        let column_types = schema
                            .schema
                            .iter()
                            .map(|cs| DfType::from_sql_type(&cs.spec.sql_type, Dialect::MySQL))
                            .collect();

                        let preencoded_schema =
                            mysql_srv::prepare_column_definitions(&mysql_schema);

                        // Now append the right position too
                        let column_map = mysql_schema
                            .iter()
                            .map(|c| schema.columns.iter().position(|f| f == c.column.as_str()))
                            .collect::<Vec<_>>();

                        entry.insert(CachedSchema {
                            mysql_schema,
                            column_types,
                            column_map,
                            preencoded_schema: preencoded_schema.into(),
                        })
                    }
                };

                let mut rw = results
                    .start_with_cache(mysql_schema, preencoded_schema.clone())
                    .await?;
                while let Some(row) = rows.next() {
                    for (c, ty, pos) in
                        izip!(mysql_schema.iter(), column_types.iter(), column_map.iter())
                    {
                        match pos {
                            Some(coli) => {
                                if let Err(e) = write_column(&mut rw, &row[*coli], c, ty).await {
                                    return handle_column_write_err(e, rw).await;
                                };
                            }
                            None => {
                                let e = Error::from(internal_err!(
                                    "tried to emit column {:?} not in getter with schema {:?}",
                                    c.column,
                                    mysql_schema
                                ));
                                error!(err = %e);
                                return rw.error(e.error_kind(), e.to_string().as_bytes()).await;
                            }
                        }
                    }
                    rw.end_row().await?;
                }
                rw.finish().await
            }
            execute_result => handle_query_result(execute_result, results).await,
        }
    }

    async fn on_init(&mut self, database: &str, w: InitWriter<'_, W>) -> io::Result<()> {
        match self.set_database(database).await {
            Ok(()) => w.ok().await,
            Err(e) => {
                w.error(
                    mysql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    e.to_string().as_bytes(),
                )
                .await
            }
        }
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query(&mut self, query: &str, results: QueryResultWriter<'_, W>) -> io::Result<()> {
        let query_result = self.query(query).await;
        handle_query_result(query_result, results).await
    }

    fn password_for_username(&self, username: &str) -> Option<Vec<u8>> {
        self.users.get(username).cloned().map(String::into_bytes)
    }

    fn require_authentication(&self) -> bool {
        self.does_require_authentication()
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
