#[cfg(feature = "fallback_cache")]
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::convert::TryInto;
#[cfg(feature = "fallback_cache")]
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::Stream;
#[cfg(feature = "fallback_cache")]
use futures_util::StreamExt;
use mysql_async::consts::{CapabilityFlags, StatusFlags};
use mysql_async::prelude::Queryable;
use mysql_async::{
    Column, Conn, Opts, OptsBuilder, ResultSetStream, Row, SslOpts, TxOpts, UrlError,
};
use nom_sql::SqlIdentifier;
use pin_project::pin_project;
use readyset_adapter::fallback_cache::FallbackCache;
#[cfg(feature = "fallback_cache")]
use readyset_adapter::fallback_cache::FallbackCacheApi;
use readyset_adapter::upstream_database::{NoriaCompare, UpstreamDestination};
use readyset_adapter::{UpstreamConfig, UpstreamDatabase, UpstreamPrepare};
use readyset_client::ColumnSchema;
use readyset_client_metrics::QueryDestination;
use readyset_data::DfValue;
use readyset_errors::{internal_err, ReadySetError};
use tracing::{error, info, info_span, Instrument};

use crate::schema::{convert_column, is_subtype};
use crate::Error;

type StatementID = u32;

fn dt_to_value_params(
    dt: &[DfValue],
) -> Result<Vec<mysql_async::Value>, readyset_client::ReadySetError> {
    dt.iter().map(|v| v.try_into()).collect()
}

#[pin_project(project = ReadResultStreamProj)]
#[derive(Debug)]
pub enum ReadResultStream<'a> {
    Text(#[pin] ResultSetStream<'a, 'a, 'static, Row, mysql_async::TextProtocol>),
    Binary(#[pin] ResultSetStream<'a, 'a, 'static, Row, mysql_async::BinaryProtocol>),
}

impl<'a> From<ResultSetStream<'a, 'a, 'static, Row, mysql_async::TextProtocol>>
    for ReadResultStream<'a>
{
    fn from(s: ResultSetStream<'a, 'a, 'static, Row, mysql_async::TextProtocol>) -> Self {
        ReadResultStream::Text(s)
    }
}

impl<'a> From<ResultSetStream<'a, 'a, 'static, Row, mysql_async::BinaryProtocol>>
    for ReadResultStream<'a>
{
    fn from(s: ResultSetStream<'a, 'a, 'static, Row, mysql_async::BinaryProtocol>) -> Self {
        ReadResultStream::Binary(s)
    }
}

#[derive(Debug)]
pub enum QueryResult<'a> {
    WriteResult {
        num_rows_affected: u64,
        last_inserted_id: u64,
        status_flags: StatusFlags,
    },
    ReadResult {
        stream: ReadResultStream<'a>,
        columns: Arc<[Column]>,
    },
    CachedReadResult(CachedReadResult),
    Command {
        status_flags: StatusFlags,
    },
}

impl<'a> UpstreamDestination for QueryResult<'a> {
    #[cfg(feature = "fallback_cache")]
    fn destination(&self) -> QueryDestination {
        if matches!(self, QueryResult::CachedReadResult(..)) {
            QueryDestination::FallbackCache
        } else {
            QueryDestination::Upstream
        }
    }

    #[cfg(not(feature = "fallback_cache"))]
    fn destination(&self) -> QueryDestination {
        QueryDestination::Upstream
    }
}

#[derive(Debug, Clone)]
pub struct CachedReadResult {
    pub data: Vec<Row>,
    pub columns: Arc<[Column]>,
    pub status_flags: Option<StatusFlags>,
}

impl<'a> From<CachedReadResult> for QueryResult<'a> {
    fn from(r: CachedReadResult) -> Self {
        QueryResult::CachedReadResult(r)
    }
}

#[cfg(feature = "fallback_cache")]
impl<'a> QueryResult<'a> {
    /// Can convert a stream ReadResult into a CachedReadResult.
    async fn async_try_into(self) -> Result<CachedReadResult, Error> {
        match self {
            QueryResult::ReadResult {
                mut stream,
                columns,
            } => {
                let mut rows = vec![];
                while let Some(row) = stream.next().await {
                    match row {
                        Ok(row) => rows.push(row),
                        Err(e) => {
                            // TODO: Update to be more sophisticated than this hack.
                            return Err(Error::ReadySet(internal_err!(
                                "Found error from MySQL rather than row {}",
                                e
                            )));
                        }
                    }
                }
                let status_flags = stream.status_flags();
                Ok(CachedReadResult {
                    data: rows,
                    columns: columns.clone(),
                    status_flags,
                })
            }
            _ => Err(Error::ReadySet(internal_err!(
                "Temp error: Can't convert because not a read result"
            ))),
        }
    }
}

/// A connector to an underlying mysql store. This is really just a wrapper for the mysql crate.
pub struct MySqlUpstream {
    conn: Conn,
    prepared_statements: HashMap<StatementID, mysql_async::Statement>,
    upstream_config: UpstreamConfig,
    #[cfg(feature = "fallback_cache")]
    fallback_cache: Option<FallbackCache<CachedReadResult>>,
}

#[derive(Debug, Clone)]
pub struct StatementMeta {
    /// Metadata about the query parameters for this statement
    pub params: Vec<Column>,
    /// Metadata about the types of the columns in the rows returned by this statement
    pub schema: Vec<Column>,
}

fn schema_column_match(schema: &[ColumnSchema], columns: &[Column]) -> Result<(), Error> {
    if schema.len() != columns.len() {
        return Err(Error::ReadySet(ReadySetError::WrongColumnCount(
            columns.len(),
            schema.len(),
        )));
    }

    if cfg!(feature = "schema-check") {
        for (sch, col) in schema.iter().zip(columns.iter()) {
            let noria_column_type = convert_column(sch)?.coltype;
            if !is_subtype(noria_column_type, col.column_type()) {
                return Err(Error::ReadySet(ReadySetError::WrongColumnType(
                    format!("{:?}", col.column_type()),
                    format!("{:?}", noria_column_type),
                )));
            }
        }
    }

    Ok(())
}

impl<'a> Stream for ReadResultStream<'a> {
    type Item = Result<Row, mysql_async::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.project() {
            ReadResultStreamProj::Text(s) => s.poll_next(cx),
            ReadResultStreamProj::Binary(s) => s.poll_next(cx),
        }
    }
}

impl<'a> ReadResultStream<'a> {
    pub fn status_flags(&self) -> Option<StatusFlags> {
        match self {
            ReadResultStream::Text(s) => s.ok_packet().map(|o| o.status_flags()),
            ReadResultStream::Binary(s) => s.ok_packet().map(|o| o.status_flags()),
        }
    }
}

impl NoriaCompare for StatementMeta {
    type Error = Error;
    fn compare(
        &self,
        columns: &[ColumnSchema],
        params: &[ColumnSchema],
    ) -> Result<(), Self::Error> {
        schema_column_match(params, &self.params)?;
        schema_column_match(columns, &self.schema)?;

        Ok(())
    }
}

macro_rules! handle_query_result {
    ($result: expr) => {{
        let columns = ($result).columns().ok_or_else(|| {
            ReadySetError::Internal("The mysql_async result was already consumed".to_string())
        })?;

        if columns.len() > 0 {
            Ok(QueryResult::ReadResult {
                stream: $result
                    .stream_and_drop()
                    .await?
                    .ok_or_else(|| {
                        ReadySetError::Internal(
                            "The mysql_async resultset was already consumed".to_string(),
                        )
                    })?
                    .into(),
                columns,
            })
        } else {
            // Kinda sad that can't get status from conn, since it is mutably borrowed above
            let resultset = $result.stream_and_drop::<Row>().await?.ok_or_else(|| {
                ReadySetError::Internal("The mysql_async result has no resultsets".to_string())
            })?;

            Ok(QueryResult::WriteResult {
                num_rows_affected: resultset.affected_rows(),
                last_inserted_id: resultset.last_insert_id().unwrap_or(1),
                status_flags: resultset
                    .ok_packet()
                    .ok_or_else(|| {
                        ReadySetError::Internal(
                            "The mysql_async result has no ok packet".to_string(),
                        )
                    })?
                    .status_flags(),
            })
        }
    }};
}

impl MySqlUpstream {
    async fn connect_inner(
        upstream_config: UpstreamConfig,
    ) -> Result<
        (
            Conn,
            HashMap<StatementID, mysql_async::Statement>,
            UpstreamConfig,
        ),
        Error,
    > {
        // CLIENT_SESSION_TRACK is required for GTID information to be sent in OK packets on commits
        // GTID information is used for RYW
        // Currently this causes rows affected to return an incorrect result, so this is feature
        // gated.
        let url = upstream_config
            .upstream_db_url
            .as_deref()
            .ok_or(ReadySetError::InvalidUpstreamDatabase)?;

        let mut opts =
            Opts::from_url(url).map_err(|e: UrlError| Error::MySql(mysql_async::Error::Url(e)))?;

        if let Some(cert_path) = upstream_config.ssl_root_cert.clone() {
            let ssl_opts = SslOpts::default().with_root_cert_path(Some(cert_path));
            opts = OptsBuilder::from_opts(opts).ssl_opts(ssl_opts).into();
        }

        let span = info_span!(
            "Connecting to MySQL upstream",
            host = %opts.ip_or_hostname(),
            port = %opts.tcp_port(),
            user = %opts.user().unwrap_or("<NO USER>"),
        );
        span.in_scope(|| info!("Establishing connection"));
        let conn = if cfg!(feature = "ryw") {
            Conn::new(
                OptsBuilder::from_opts(opts).add_capability(CapabilityFlags::CLIENT_SESSION_TRACK),
            )
            .instrument(span.clone())
            .await?
        } else {
            Conn::new(OptsBuilder::from_opts(opts))
                .instrument(span.clone())
                .await?
        };
        span.in_scope(|| info!("Established connection to upstream"));
        let prepared_statements = HashMap::new();
        Ok((conn, prepared_statements, upstream_config))
    }
}

#[async_trait]
impl UpstreamDatabase for MySqlUpstream {
    type QueryResult<'a> = QueryResult<'a>;
    type CachedReadResult = CachedReadResult;
    type StatementMeta = StatementMeta;
    type Error = Error;
    const DEFAULT_DB_VERSION: &'static str = "8.0.26-readyset\0";

    #[cfg(feature = "fallback_cache")]
    async fn connect(
        upstream_config: UpstreamConfig,
        fallback_cache: Option<FallbackCache<Self::CachedReadResult>>,
    ) -> Result<Self, Error> {
        let (conn, prepared_statements, upstream_config) =
            Self::connect_inner(upstream_config).await?;
        Ok(Self {
            conn,
            prepared_statements,
            upstream_config,
            fallback_cache,
        })
    }

    #[cfg(not(feature = "fallback_cache"))]
    async fn connect(
        upstream_config: UpstreamConfig,
        _: Option<FallbackCache<Self::CachedReadResult>>,
    ) -> Result<Self, Error> {
        let (conn, prepared_statements, upstream_config) =
            Self::connect_inner(upstream_config).await?;
        Ok(Self {
            conn,
            prepared_statements,
            upstream_config,
        })
    }

    fn url(&self) -> &str {
        self.upstream_config.upstream_db_url.as_deref().unwrap()
    }

    fn database(&self) -> Option<&str> {
        self.conn.opts().db_name()
    }

    fn version(&self) -> String {
        // The server's version relayed back to the client as the current server version. Most
        // clients will interpret the version numbers and use that to dictate which dialect they
        // send us. Anything after the version can be any text we desire. Additionally, the version
        // string must be null terminated.
        let (major, minor, patch) = self.conn.server_version();
        format!("{major}.{minor}.{patch}-readyset\0")
    }

    #[cfg(feature = "fallback_cache")]
    async fn reset(&mut self) -> Result<(), Error> {
        let opts = self.conn.opts().clone();
        let conn = Conn::new(opts).await?;
        let prepared_statements = HashMap::new();
        let upstream_config = self.upstream_config.clone();
        let fallback_cache = if let Some(ref cache) = self.fallback_cache {
            cache.clear().await;
            Some(cache.clone())
        } else {
            None
        };
        let old_self = std::mem::replace(
            self,
            Self {
                conn,
                prepared_statements,
                upstream_config,
                fallback_cache,
            },
        );
        let _ = old_self.conn.disconnect().await as Result<(), _>;
        Ok(())
    }

    #[cfg(not(feature = "fallback_cache"))]
    async fn reset(&mut self) -> Result<(), Error> {
        let opts = self.conn.opts().clone();
        let conn = Conn::new(opts).await?;
        let prepared_statements = HashMap::new();
        let upstream_config = self.upstream_config.clone();
        let old_self = std::mem::replace(
            self,
            Self {
                conn,
                prepared_statements,
                upstream_config,
            },
        );
        let _ = old_self.conn.disconnect().await as Result<(), _>;
        Ok(())
    }

    /// Prepares the given query using the mysql connection. Note, queries are prepared on a
    /// per connection basis. They are not universal.
    async fn prepare<'a, S>(&'a mut self, query: S) -> Result<UpstreamPrepare<Self>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let statement = self.conn.prep(query).await?;
        self.prepared_statements
            .insert(statement.id(), statement.clone());
        Ok(UpstreamPrepare {
            statement_id: statement.id(),
            meta: StatementMeta {
                params: statement.params().to_owned(),
                schema: statement.columns().to_owned(),
            },
        })
    }

    #[cfg(feature = "fallback_cache")]
    async fn execute<'a>(
        &'a mut self,
        id: u32,
        params: &[DfValue],
    ) -> Result<Self::QueryResult<'a>, Error> {
        if let Some(ref mut cache) = self.fallback_cache {
            let mut s = DefaultHasher::new();
            (id, params).hash(&mut s);
            let hash = format!("{:x}", s.finish());
            if let Some(query_r) = cache.get(&hash).await {
                return Ok(query_r.into());
            }
            let params = dt_to_value_params(params)?;
            let result = self
                .conn
                .exec_iter(
                    self.prepared_statements.get(&id).ok_or(Error::ReadySet(
                        ReadySetError::PreparedStatementMissing { statement_id: id },
                    ))?,
                    params,
                )
                .await?;
            let r = handle_query_result!(result);
            match r {
                Ok(query_result @ QueryResult::ReadResult { .. }) => {
                    let cached_result: CachedReadResult = query_result.async_try_into().await?;
                    cache.insert(hash, cached_result.clone()).await;
                    Ok(cached_result.into())
                }
                _ => r,
            }
        } else {
            let params = dt_to_value_params(params)?;
            let result = self
                .conn
                .exec_iter(
                    self.prepared_statements.get(&id).ok_or(Error::ReadySet(
                        ReadySetError::PreparedStatementMissing { statement_id: id },
                    ))?,
                    params,
                )
                .await?;
            handle_query_result!(result)
        }
    }

    #[cfg(not(feature = "fallback_cache"))]
    async fn execute<'a>(
        &'a mut self,
        id: u32,
        params: &[DfValue],
    ) -> Result<Self::QueryResult<'a>, Error> {
        let params = dt_to_value_params(params)?;
        let result = self
            .conn
            .exec_iter(
                self.prepared_statements.get(&id).ok_or(Error::ReadySet(
                    ReadySetError::PreparedStatementMissing { statement_id: id },
                ))?,
                params,
            )
            .await?;
        handle_query_result!(result)
    }

    #[cfg(feature = "fallback_cache")]
    async fn query<'a, S>(&'a mut self, query: S) -> Result<Self::QueryResult<'a>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        if let Some(ref mut cache) = self.fallback_cache {
            if let Some(query_r) = cache.get(query.as_ref()).await {
                return Ok(query_r.into());
            }
            let query_str = query.as_ref().to_owned();
            let result = self.conn.query_iter(query).await?;
            let r = handle_query_result!(result);
            match r {
                Ok(query_result @ QueryResult::ReadResult { .. }) => {
                    let cached_result: CachedReadResult = query_result.async_try_into().await?;
                    cache.insert(query_str, cached_result.clone()).await;
                    Ok(cached_result.into())
                }
                _ => r,
            }
        } else {
            let result = self.conn.query_iter(query).await?;
            handle_query_result!(result)
        }
    }

    #[cfg(not(feature = "fallback_cache"))]
    async fn query<'a, S>(&'a mut self, query: S) -> Result<Self::QueryResult<'a>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let result = self.conn.query_iter(query).await?;
        handle_query_result!(result)
    }

    /// Executes the given query on the mysql backend.
    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        query: S,
    ) -> Result<(Self::QueryResult<'a>, String), Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let mut transaction = self.conn.start_transaction(TxOpts::default()).await?;
        transaction.query_drop(query).await.map_err(|e| {
            error!("Could not execute query in mysql : {:?}", e);
            e
        })?;

        let affected_rows = transaction.affected_rows();
        let last_insert_id = transaction.last_insert_id();
        let status_flags = transaction.status();
        let txid = transaction.commit_returning_gtid().await.map_err(|e| {
            internal_err!(
                "Error obtaining GTID from MySQL for RYW-enabled commit: {}",
                e
            )
        })?;
        Ok((
            QueryResult::WriteResult {
                num_rows_affected: affected_rows,
                last_inserted_id: last_insert_id.unwrap_or(0),
                status_flags,
            },
            txid,
        ))
    }

    async fn start_tx<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        self.conn.query_drop("START TRANSACTION").await?;

        Ok(QueryResult::Command {
            status_flags: self.conn.status(),
        })
    }

    async fn commit<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        let result = self.conn.query_iter("COMMIT").await?;
        result.drop_result().await?;

        Ok(QueryResult::Command {
            status_flags: self.conn.status(),
        })
    }

    async fn rollback<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        let result = self.conn.query_iter("ROLLBACK").await?;
        result.drop_result().await?;

        Ok(QueryResult::Command {
            status_flags: self.conn.status(),
        })
    }

    async fn schema_dump(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        let tables: Vec<String> = self.conn.query_iter("SHOW TABLES").await?.collect().await?;
        let mut dump = String::with_capacity(tables.len());
        for table in &tables {
            if let Some(create) = self
                .conn
                .query_first(format!("SHOW CREATE TABLE `{}`", &table))
                .await?
                .map(|row: (String, String)| row.1)
            {
                dump.push_str(&create);
                dump.push('\n');
            }
        }
        Ok(dump.into_bytes())
    }

    async fn schema_search_path(&mut self) -> Result<Vec<SqlIdentifier>, Self::Error> {
        Ok(self.database().into_iter().map(|s| s.into()).collect())
    }
}

#[cfg(test)]
mod tests {
    use mysql_srv::ColumnType;
    use nom_sql::{Column as NomColumn, ColumnSpecification, SqlType};
    use readyset_data::Dialect;

    use super::*;

    fn test_column() -> NomColumn {
        NomColumn {
            name: "t".into(),
            table: None,
        }
    }

    #[test]
    fn compare_matching_schema() {
        let s: StatementMeta = StatementMeta {
            params: vec![
                Column::new(ColumnType::MYSQL_TYPE_VAR_STRING),
                Column::new(ColumnType::MYSQL_TYPE_DOUBLE),
            ],
            schema: vec![Column::new(ColumnType::MYSQL_TYPE_LONG)],
        };

        let param_specs = vec![
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::VarChar(Some(10))),
                "table1".into(),
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Double),
                "table1".into(),
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        ];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::Int(None)),
            "table1".into(),
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()];

        assert!(s.compare(&schema_spec, &param_specs).is_ok());
    }

    #[test]
    fn compare_different_len_schema() {
        let s: StatementMeta = StatementMeta {
            params: vec![
                Column::new(ColumnType::MYSQL_TYPE_VARCHAR),
                Column::new(ColumnType::MYSQL_TYPE_DOUBLE),
            ],
            schema: vec![Column::new(ColumnType::MYSQL_TYPE_LONG)],
        };

        let param_specs = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::VarChar(Some(10))),
            "table1".into(),
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::Int(None)),
            "table1".into(),
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()];

        assert!(s.compare(&schema_spec, &param_specs).is_err());
    }

    #[cfg(feature = "schema-check")]
    #[test]
    fn compare_different_type_schema() {
        let s: StatementMeta = StatementMeta {
            params: vec![
                Column::new(ColumnType::MYSQL_TYPE_VARCHAR),
                Column::new(ColumnType::MYSQL_TYPE_DOUBLE),
            ],
            schema: vec![Column::new(ColumnType::MYSQL_TYPE_LONG)],
        };

        let param_specs = vec![
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Bool),
                "table1".into(),
            ),
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Double),
                "table1".into(),
            ),
        ];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::VarChar(Some(8))),
            "table1".into(),
        )];

        assert!(s.compare(&schema_spec, &param_specs).is_err());
    }
}
