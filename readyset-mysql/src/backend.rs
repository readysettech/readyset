use core::fmt;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::ops::{Deref, DerefMut};

use itertools::{izip, Itertools};
use mysql_async::consts::StatusFlags;
use mysql_srv::{
    CachedSchema, Column, ColumnFlags, ColumnType, InitWriter, MsqlSrvError, MySqlShim,
    QueryResultWriter, QueryResultsResponse, RowWriter, StatementMetaWriter,
};
use readyset_adapter::backend::noria_connector::{
    MetaVariable, PreparedSelectTypes, SelectPrepareResultInner,
};
use readyset_adapter::backend::{
    noria_connector, QueryResult, SinglePrepareResult, UpstreamPrepare,
};
use readyset_adapter::upstream_database::LazyUpstream;
use readyset_adapter_types::{DeallocateId, PreparedStatementType};
use readyset_data::encoding::Encoding;
use readyset_data::{DfType, DfValue, DfValueKind};
use readyset_errors::{internal, ReadySetError};
use readyset_shallow::{CacheInsertGuard, QueryMetadata};
use readyset_util::redacted::{RedactedString, Sensitive};
use std::io::ErrorKind;
use streaming_iterator::StreamingIterator;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tracing::{error, info, trace};
use upstream::StatementMeta;

use crate::constants::DEFAULT_CHARACTER_SET;
use crate::schema::convert_column;
use crate::upstream::{self, CacheEntry, MySqlUpstream};
use crate::value::mysql_value_to_dataflow_value;
use crate::{Error, MySqlQueryHandler};

/// Helper struct to correctly transform a binary type value into its correct [`String`]
/// representation.
// TODO(fran): We can't keep using the `Display` impl of `DfValue`, since types such as binary or
//  byte array have different display formats depending on the database (Postgres even has two
//  possible display formats based on configuration).
//  This is a temporary workaround, since the actual fix might involve a bigger effort.
struct BinaryDisplay<'a>(&'a [u8]);

impl fmt::Display for BinaryDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "0x{}",
            self.0.iter().map(|byte| format!("{byte:02x}")).join("")
        )
    }
}

async fn write_column<S: AsyncRead + AsyncWrite + Unpin>(
    rw: &mut RowWriter<'_, S>,
    c: &DfValue,
    cs: &mysql_srv::Column,
    ty: &DfType,
    encoding: Encoding,
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
                rw.write_col(c.coerce_to(&DfType::DEFAULT_TEXT, ty)?.to_string())
            } else if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize)
            } else {
                rw.write_col(i as isize)
            }
        }
        DfValue::UnsignedInt(i) => {
            if ty.is_enum() {
                rw.write_col(c.coerce_to(&DfType::DEFAULT_TEXT, ty)?.to_string())
            } else if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize)
            } else {
                rw.write_col(i as isize)
            }
        }
        DfValue::Text(ref t) => {
            if ty.is_binary() {
                rw.write_col(BinaryDisplay(c.as_bytes()?).to_string())
            } else {
                let s = if ty.is_char() {
                    t.as_str().trim_end_matches(' ')
                } else {
                    t.as_str()
                };
                let b = encoding.encode(s)?;
                rw.write_col(&*b)
            }
        }
        DfValue::TinyText(ref t) => {
            if ty.is_binary() {
                rw.write_col(BinaryDisplay(c.as_bytes()?).to_string())
            } else {
                let s = if ty.is_char() {
                    t.as_str().trim_end_matches(' ')
                } else {
                    t.as_str()
                };
                let b = encoding.encode(s)?;
                rw.write_col(&*b)
            }
        }
        ref dt @ (DfValue::Float(..) | DfValue::Double(..)) => match cs.coltype {
            mysql_srv::ColumnType::MYSQL_TYPE_DECIMAL
            | mysql_srv::ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                let f = format!("{dt:.0$}", cs.decimals as usize);
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
                let f = format!("{v:.0$}", cs.decimals as usize);
                rw.write_col(f)
            }
            mysql_srv::ColumnType::MYSQL_TYPE_FLOAT | mysql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                let f: f64 = v.as_ref().try_into().map_err(|_| conv_error())?;
                rw.write_col(f)
            }
            _ => return Err(conv_error())?,
        },

        DfValue::TimestampTz(ts) => match cs.coltype {
            mysql_srv::ColumnType::MYSQL_TYPE_DATETIME
            | mysql_srv::ColumnType::MYSQL_TYPE_DATETIME2 => rw.write_col(ts),
            mysql_srv::ColumnType::MYSQL_TYPE_TIMESTAMP
            | mysql_srv::ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
                if ts.is_zero() {
                    rw.write_col(ts)
                } else {
                    rw.write_col(ts.to_local())
                }
            }
            ColumnType::MYSQL_TYPE_DATE => {
                if ts.is_zero() {
                    rw.write_col(ts)
                } else {
                    rw.write_col(ts.to_chrono().naive_local().date())
                }
            }
            _ => return Err(conv_error())?,
        },
        DfValue::Time(ref t) => rw.write_col(t),
        DfValue::ByteArray(ref bytes) => rw.write_col(bytes.as_ref()),
        DfValue::Default => internal!("Cannot write MySQL column DEFAULT"),
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

pub(crate) async fn write_query_results<S: AsyncRead + AsyncWrite + Unpin>(
    r: Result<(u64, u64), Error>,
    results: QueryResultWriter<'_, S>,
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
async fn write_meta_table<S: AsyncRead + AsyncWrite + Unpin>(
    vars: Vec<MetaVariable>,
    results: QueryResultWriter<'_, S>,
    status_flags: StatusFlags,
) -> io::Result<()> {
    let cols = vars
        .iter()
        .map(|v| Column {
            table: "".to_owned(),
            column: v.name.to_string(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            column_length: 1024,
            colflags: ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        })
        .collect::<Vec<_>>();

    let mut writer = results.start(&cols).await?;

    for var in vars {
        writer.write_col(var.value)?;
    }
    writer.end_row().await?;
    writer.set_status_flags(status_flags).finish().await
}

/// Writes a Vec of [`MetaVariable`] as a table with two columns, where each row represents one
/// variable, with the first column being the variable name and the second column its value
async fn write_meta_variables<S: AsyncRead + AsyncWrite + Unpin>(
    vars: Vec<MetaVariable>,
    results: QueryResultWriter<'_, S>,
    status_flags: StatusFlags,
) -> io::Result<()> {
    // Assign column schema to match MySQL
    // [`SHOW STATUS`](https://dev.mysql.com/doc/refman/8.0/en/show-status.html)
    let cols = vec![
        Column {
            table: "".to_owned(),
            column: "Variable_name".to_string(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            column_length: 1024,
            colflags: ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: "".to_owned(),
            column: "Value".to_string(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            column_length: 1024,
            colflags: ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
    ];
    let mut writer = results.start(&cols).await?;
    for v in vars {
        writer.write_col(v.name.as_str())?;
        writer.write_col(v.value)?;
        writer.end_row().await?;
    }
    writer.set_status_flags(status_flags).finish().await
}

/// Writes a Vec of [`MetaVariable`] as a table with a single row, where the column names correspond
/// to the variable names and the row values correspond to the variable values
/// The first item in vars serves as the column headers
async fn write_meta_with_header<S: AsyncRead + AsyncWrite + Unpin>(
    vars: Vec<MetaVariable>,
    results: QueryResultWriter<'_, S>,
    status_flags: StatusFlags,
) -> io::Result<()> {
    let cols = vec![
        Column {
            table: "".to_owned(),
            column: vars[0].name.to_string(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            column_length: 1024,
            colflags: ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
        Column {
            table: "".to_owned(),
            column: vars[0].value.to_string(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            column_length: 1024,
            colflags: ColumnFlags::empty(),
            character_set: DEFAULT_CHARACTER_SET,
            decimals: 0,
        },
    ];
    let mut writer = results.start(&cols).await?;
    for v in vars.into_iter().skip(1) {
        writer.write_col(v.name.as_str())?;
        writer.write_col(v.value)?;
        writer.end_row().await?;
    }
    writer.set_status_flags(status_flags).finish().await
}

pub struct Backend {
    /// Handle to the backing noria client
    pub noria: readyset_adapter::Backend<LazyUpstream<MySqlUpstream>, MySqlQueryHandler>,
    /// Enables logging of statements received from the client. The `Backend` only logs Query,
    /// Prepare and Execute statements.
    pub enable_statement_logging: bool,
}

impl Deref for Backend {
    type Target = readyset_adapter::Backend<LazyUpstream<MySqlUpstream>, MySqlQueryHandler>;

    fn deref(&self) -> &Self::Target {
        &self.noria
    }
}

impl DerefMut for Backend {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.noria
    }
}

impl Backend {
    /// Constructs the MySQL server status flags from a `ProxyState` snapshot.
    fn flags_from_proxy_state(state: readyset_adapter::backend::ProxyState) -> StatusFlags {
        let mut flags = StatusFlags::empty();
        if state.is_autocommit() {
            flags |= StatusFlags::SERVER_STATUS_AUTOCOMMIT;
        }
        if state.in_transaction_or_implicit() {
            flags |= StatusFlags::SERVER_STATUS_IN_TRANS;
        }
        flags
    }

    /// Constructs the MySQL server status flags bitmask from the current
    /// adapter state (autocommit and transaction tracking via `ProxyState`).
    fn build_status_flags(&self) -> StatusFlags {
        Self::flags_from_proxy_state(self.noria.proxy_state())
    }
}

macro_rules! convert_columns {
    ($columns: expr, $results: expr) => {{
        match $columns
            .into_iter()
            .map(|c| convert_column(&c))
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

#[macro_export]
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
            Error::ReadySet(ReadySetError::ConnectionClosed(ref msg)) => {
                error!(%msg, "connection closed");
                Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    msg.clone(),
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

async fn handle_readyset_result<S>(
    result: noria_connector::QueryResult<'_>,
    writer: QueryResultWriter<'_, S>,
    results_encoding: Encoding,
    status_flags: StatusFlags,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let flags = Some(status_flags);
    match result {
        noria_connector::QueryResult::Empty => writer.completed(0, 0, flags).await,
        noria_connector::QueryResult::Insert {
            num_rows_inserted,
            first_inserted_id,
        } => write_query_results(Ok((num_rows_inserted, first_inserted_id)), writer, flags).await,
        noria_connector::QueryResult::Update {
            num_rows_updated,
            last_inserted_id,
        } => write_query_results(Ok((num_rows_updated, last_inserted_id)), writer, flags).await,
        noria_connector::QueryResult::Delete { num_rows_deleted } => {
            writer.completed(num_rows_deleted, 0, flags).await
        }
        noria_connector::QueryResult::Meta(vars) => {
            write_meta_table(vars, writer, status_flags).await
        }
        noria_connector::QueryResult::MetaVariables(vars) => {
            write_meta_variables(vars, writer, status_flags).await
        }
        noria_connector::QueryResult::MetaWithHeader(vars) => {
            write_meta_with_header(vars, writer, status_flags).await
        }
        noria_connector::QueryResult::Select { mut rows, schema } => {
            let mysql_schema = convert_columns!(schema.schema, writer);
            let mut rw = writer.start(&mysql_schema).await?;
            while let Some(row) = rows.next() {
                // make sure we have at least as many row columns as schema types.
                // if there are more row columns than schema types, those are bogokeys,
                // which we can ignore as we do not send those back to callers.
                let len = mysql_schema.len();
                if row.len() < len {
                    return handle_column_write_err(
                        Error::ReadySet(readyset_errors::ReadySetError::WrongColumnCount(
                            len,
                            row.len(),
                        )),
                        rw,
                    )
                    .await;
                }

                for i in 0..len {
                    let val = row.get(i).unwrap();
                    let c = mysql_schema.get(i).unwrap();

                    let s = schema.schema.get(i);
                    let ty = match s {
                        Some(cs) => &cs.column_type,
                        None => &DfType::Unknown,
                    };

                    if let Err(e) = write_column(&mut rw, val, c, ty, results_encoding).await {
                        return handle_column_write_err(e, rw).await;
                    }
                }
                rw.end_row().await?;
            }
            rw.set_status_flags(status_flags).finish().await
        }
    }
}

async fn handle_shallow_result<S>(
    result: readyset_shallow::QueryResult<CacheEntry>,
    writer: QueryResultWriter<'_, S>,
    status_flags: StatusFlags,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let QueryMetadata::MySql(metadata) = result.metadata.as_ref() else {
        unreachable!("wrong metadata type: {:?}", result.metadata);
    };

    let formatted_cols = metadata
        .columns
        .iter()
        .map(|c| c.into())
        .collect::<Vec<_>>();
    let mut rw = writer.start(&formatted_cols).await?;

    for entry in result.values.iter() {
        let row = match entry {
            CacheEntry::Text(values) | CacheEntry::Binary(values) => values,
        };
        for val in row.iter() {
            let val: mysql_async::Value = val.try_into().map_err(|e| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!("Failed to convert DfValue to mysql::Value: {val:?}, error: {e}"),
                )
            })?;
            rw.write_col(val)?;
        }
        rw.end_row().await?;
    }
    rw.set_status_flags(status_flags).finish().await
}

async fn handle_upstream_result<S>(
    result: upstream::QueryResult<'_>,
    writer: QueryResultWriter<'_, S>,
    cache: Option<CacheInsertGuard<Vec<DfValue>, CacheEntry>>,
    status_flags_override: Option<StatusFlags>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    result
        .process(Some(writer), cache, status_flags_override)
        .await
}

async fn handle_execute_result<S>(
    result: Result<QueryResult<'_, LazyUpstream<MySqlUpstream>>, Error>,
    writer: QueryResultWriter<'_, S>,
    results_encoding: Encoding,
    status_flags: StatusFlags,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match result {
        Ok(QueryResult::Noria(result)) => {
            handle_readyset_result(result, writer, results_encoding, status_flags).await
        }
        Ok(QueryResult::Shallow(result)) => {
            handle_shallow_result(result, writer, status_flags).await
        }
        Ok(QueryResult::Upstream(result, cache, _)) => {
            handle_upstream_result(result, writer, cache, Some(status_flags)).await
        }
        Ok(QueryResult::UpstreamBufferedInMemory(..)) => handle_error!(
            Error::ReadySet(readyset_errors::unsupported_err!(
                "Execute should not use simple query protocol"
            )),
            writer
        ),
        Ok(QueryResult::Parser(..)) => handle_error!(
            Error::ReadySet(readyset_errors::unsupported_err!(
                "Should not parse SQL commands in execute"
            )),
            writer
        ),
        Err(error) => handle_error!(error, writer),
    }
}

async fn handle_query_result<S>(
    result: Result<QueryResult<'_, LazyUpstream<MySqlUpstream>>, Error>,
    writer: QueryResultWriter<'_, S>,
    results_encoding: Encoding,
    status_flags: StatusFlags,
) -> QueryResultsResponse
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match result {
        Ok(QueryResult::Parser(command)) => QueryResultsResponse::Command(command),
        res => QueryResultsResponse::IoResult(
            handle_execute_result(res, writer, results_encoding, status_flags).await,
        ),
    }
}

impl<S> MySqlShim<S> for Backend
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn server_status_flags(&self) -> StatusFlags {
        self.build_status_flags()
    }

    fn on_connect_attrs(&mut self, attrs: &HashMap<&str, &str>) {
        if attrs
            .get("_program_name")
            .is_some_and(|v| *v == readyset_adapter::backend::READYSET_QUERY_SAMPLER)
        {
            self.noria.set_internal_connection(true);
        }
    }

    async fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, S>,
        schema_cache: &mut HashMap<u32, CachedSchema>,
    ) -> io::Result<()> {
        if self.enable_statement_logging {
            info!(target: "client_statement", "Prepare: {query}");
        }
        use noria_connector::PrepareResult::*;

        trace!("delegate");
        let prepare_result = self
            .prepare(query, (), PreparedStatementType::Named)
            .await
            .map(|p| (p.statement_id, p.upstream_biased()));
        let res = match prepare_result {
            Ok((
                statement_id,
                SinglePrepareResult::Noria(
                    Select {
                        types:
                            PreparedSelectTypes::Schema(SelectPrepareResultInner { params, schema }),
                        ..
                    }
                    | Insert { params, schema, .. },
                ),
            )) => {
                let params = convert_columns!(params, info);
                let schema = convert_columns!(schema, info);
                schema_cache.remove(&statement_id);
                info.reply(statement_id, &params, &schema).await
            }
            Ok((
                _,
                SinglePrepareResult::Noria(Select {
                    types: PreparedSelectTypes::NoSchema,
                    ..
                }),
            )) => {
                info.error(
                    mysql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    "Unreachable".as_bytes(),
                )
                .await
            }
            Ok((
                statement_id,
                SinglePrepareResult::Noria(Update { params, .. } | Delete { params, .. }),
            )) => {
                let params = convert_columns!(params, info);
                schema_cache.remove(&statement_id);
                info.reply(statement_id, &params, &[]).await
            }
            Ok((statement_id, SinglePrepareResult::Noria(Set { .. }))) => {
                schema_cache.remove(&statement_id);
                info.reply(statement_id, &[], &[]).await
            }
            Ok((
                statement_id,
                SinglePrepareResult::Upstream(UpstreamPrepare {
                    meta: StatementMeta { params, schema },
                    ..
                }),
            )) => {
                let params = params.iter().map(|c| c.into()).collect::<Vec<_>>();
                let schema = schema.iter().map(|c| c.into()).collect::<Vec<_>>();
                schema_cache.remove(&statement_id);
                info.reply(statement_id, &params, &schema).await
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
            Err(Error::ReadySet(ReadySetError::ConnectionClosed(ref msg))) => {
                error!(%msg, "connection closed");
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    msg.clone(),
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

        res
    }

    async fn on_execute(
        &mut self,
        id: u32,
        params: mysql_srv::ParamParser<'_>,
        results: QueryResultWriter<'_, S>,
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

        if self.enable_statement_logging {
            info!(target: "client_statement", "Execute: {{id: {id}, params: {:?}}}", value_params)
        }

        let results_encoding = self.noria.noria.results_encoding();
        let pre_flags = self.build_status_flags();

        let (execute_result, post_state) = match self.execute(id, &value_params, &()).await {
            Ok((result, state)) => (Ok(result), Some(state)),
            Err(e) => (Err(e), None),
        };
        let status_flags = post_state
            .map(Self::flags_from_proxy_state)
            .unwrap_or(pre_flags);

        match execute_result {
            Ok(QueryResult::Noria(noria_connector::QueryResult::Select { mut rows, schema })) => {
                let CachedSchema {
                    mysql_schema,
                    column_types,
                    preencoded_schema,
                } = match schema_cache.entry(id) {
                    // `or_insert_with` would be cleaner but we need an async closure here
                    Entry::Occupied(schema) => schema.into_mut(),
                    Entry::Vacant(entry) => {
                        let mysql_schema = convert_columns!(schema.schema, results);
                        let column_types = schema
                            .schema
                            .iter()
                            .map(|cs| cs.column_type.clone())
                            .collect();

                        let preencoded_schema =
                            mysql_srv::prepare_column_definitions(&mysql_schema);

                        entry.insert(CachedSchema {
                            mysql_schema,
                            column_types,
                            preencoded_schema: preencoded_schema.into(),
                        })
                    }
                };

                let mut rw = results
                    .start_with_cache(mysql_schema, preencoded_schema.clone())
                    .await?;
                while let Some(row) = rows.next() {
                    for (c, ty, val) in izip!(mysql_schema.iter(), column_types.iter(), row.iter())
                    {
                        if let Err(e) = write_column(&mut rw, val, c, ty, results_encoding).await {
                            return handle_column_write_err(e, rw).await;
                        };
                    }
                    rw.end_row().await?;
                }
                rw.set_status_flags(status_flags).finish().await
            }
            execute_result => {
                handle_execute_result(execute_result, results, results_encoding, status_flags).await
            }
        }
    }

    async fn set_auth_info(
        &mut self,
        user: &str,
        password: Option<RedactedString>,
    ) -> io::Result<()> {
        if let Some(password) = password {
            let _ = self.set_user(user, password).await;
        }
        Ok(())
    }

    async fn set_charset(&mut self, charset: u16) -> io::Result<()> {
        metrics::counter!(
            readyset_client_metrics::recorded::CHARACTER_SET_USAGE,
            "type" => "protocol",
            "charset" => charset.to_string(),
        )
        .increment(1);
        let encoding = readyset_data::encoding::Encoding::from_mysql_collation_id(charset);
        self.noria.noria.set_results_encoding(encoding);
        Ok(())
    }

    async fn on_init(&mut self, database: &str, w: Option<InitWriter<'_, S>>) -> io::Result<()> {
        if self.enable_statement_logging {
            info!(target: "client_statement", "database: {database}");
        }
        match self.set_database(database).await {
            Ok(()) => {
                if let Some(w) = w {
                    w.ok().await
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                if let Some(w) = w {
                    w.error(
                        mysql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                        e.to_string().as_bytes(),
                    )
                    .await
                } else {
                    Err(io::Error::other(e.to_string()))
                }
            }
        }
    }

    async fn on_change_user(
        &mut self,
        user: &str,
        password: &str,
        database: &str,
    ) -> io::Result<()> {
        if self.enable_statement_logging {
            info!(target: "client_statement", "change user: {database}");
        }
        self.change_user(user, password, database)
            .await
            .map_err(std::io::Error::other)
    }

    async fn on_close(&mut self, statement_id: DeallocateId) {
        let _ = self.noria.remove_statement(statement_id).await;
    }

    async fn on_ping(&mut self) -> std::io::Result<()> {
        self.ping().await.map_err(std::io::Error::other)
    }

    async fn on_reset(&mut self) -> io::Result<()> {
        self.reset().await.map_err(io::Error::other)
    }

    async fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, S>,
    ) -> QueryResultsResponse {
        if self.enable_statement_logging {
            info!(target: "client_statement", "Query: {query}");
        }

        let results_encoding = self.noria.noria.results_encoding();
        let pre_flags = self.build_status_flags();
        let (query_result, status_flags) = match self.query(query).await {
            Ok((result, state)) => (Ok(result), Self::flags_from_proxy_state(state)),
            Err(e) => (Err(e), pre_flags),
        };
        handle_query_result(query_result, results, results_encoding, status_flags).await
    }

    fn password_for_username(&self, username: &str) -> Option<Vec<u8>> {
        self.users.get(username).cloned().map(String::into_bytes)
    }

    fn require_authentication(&self) -> bool {
        self.does_require_authentication()
    }

    fn version(&self) -> String {
        self.noria.version()
    }
}

async fn handle_column_write_err<S: AsyncRead + AsyncWrite + Unpin>(
    e: Error,
    rw: RowWriter<'_, S>,
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
        Error::ReadySet(ReadySetError::ConnectionClosed(ref msg)) => Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            msg.clone(),
        )),
        _ => rw.error(e.error_kind(), e.to_string().as_bytes()).await,
    }
}

#[cfg(test)]
mod tests {
    use mysql_async::consts::StatusFlags;
    use readyset_adapter::backend::ProxyState;

    use super::Backend;

    /// Flag truth table matching the design doc (section 5):
    ///
    /// | ProxyState     | is_autocommit | in_tx_or_implicit | AUTOCOMMIT | IN_TRANS |
    /// |----------------|---------------|-------------------|------------|----------|
    /// | Never          | true          | false             | 1          | 0        |
    /// | Fallback       | true          | false             | 1          | 0        |
    /// | InTransaction  | true          | true              | 1          | 1        |
    /// | AutocommitOff  | false         | true              | 0          | 1        |
    /// | ProxyAlways    | true          | false             | 1          | 0        |
    #[test]
    fn status_flags_truth_table() {
        let ac = StatusFlags::SERVER_STATUS_AUTOCOMMIT;
        let tx = StatusFlags::SERVER_STATUS_IN_TRANS;

        // Never / Fallback / ProxyAlways: autocommit on, not in transaction
        assert_eq!(Backend::flags_from_proxy_state(ProxyState::Never), ac);
        assert_eq!(Backend::flags_from_proxy_state(ProxyState::Fallback), ac);
        assert_eq!(Backend::flags_from_proxy_state(ProxyState::ProxyAlways), ac);

        // InTransaction: autocommit on, in explicit transaction
        assert_eq!(
            Backend::flags_from_proxy_state(ProxyState::InTransaction),
            ac | tx
        );

        // AutocommitOff: autocommit off, in implicit transaction
        assert_eq!(
            Backend::flags_from_proxy_state(ProxyState::AutocommitOff),
            tx
        );
    }
}
