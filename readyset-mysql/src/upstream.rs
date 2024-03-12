use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::Stream;
use mysql_async::consts::{CapabilityFlags, StatusFlags};
use mysql_async::prelude::Queryable;
use mysql_async::{
    Column, Conn, Opts, OptsBuilder, ResultSetStream, Row, SslOpts, TxOpts, UrlError,
};
use nom_sql::{SqlIdentifier, StartTransactionStatement};
use pin_project::pin_project;
use readyset_adapter::upstream_database::UpstreamDestination;
use readyset_adapter::{UpstreamConfig, UpstreamDatabase, UpstreamPrepare};
use readyset_adapter_types::DeallocateId;
use readyset_client_metrics::{recorded, QueryDestination};
use readyset_data::DfValue;
use readyset_errors::{internal_err, unsupported, ReadySetError, ReadySetResult};
use tracing::{debug, error, info_span, Instrument};

use crate::Error;

type StatementID = u32;

/// Indicates the minimum upstream server version that we currently support. Used to error out
/// during connection phase if the version for the upstream server is too low.
const MIN_UPSTREAM_VERSION: u16 = 8;

fn dt_to_value_params(dt: &[DfValue]) -> ReadySetResult<Vec<mysql_async::Value>> {
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
    Command {
        status_flags: StatusFlags,
    },
}

impl<'a> UpstreamDestination for QueryResult<'a> {
    fn destination(&self) -> QueryDestination {
        QueryDestination::Upstream
    }
}

/// A connector to an underlying mysql store. This is really just a wrapper for the mysql crate.
pub struct MySqlUpstream {
    conn: Conn,
    prepared_statements: HashMap<StatementID, mysql_async::Statement>,
}

#[derive(Debug, Clone)]
pub struct StatementMeta {
    /// Metadata about the query parameters for this statement
    pub params: Vec<Column>,
    /// Metadata about the types of the columns in the rows returned by this statement
    pub schema: Vec<Column>,
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
    ) -> Result<(Conn, HashMap<StatementID, mysql_async::Statement>), Error> {
        // CLIENT_SESSION_TRACK is required for GTID information to be sent in OK packets on commits
        // GTID information is used for RYW
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
        span.in_scope(|| debug!("Establishing connection"));
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

        // Check that the server version is supported.
        let (major, minor, _) = conn.server_version();
        if major < MIN_UPSTREAM_VERSION {
            return Err(Error::ReadySet(ReadySetError::UnsupportedServerVersion {
                major,
                minor: minor.to_string(),
                min: MIN_UPSTREAM_VERSION,
            }));
        }

        span.in_scope(|| debug!("Established connection to upstream"));
        metrics::increment_gauge!(recorded::CLIENT_UPSTREAM_CONNECTIONS, 1.0);
        let prepared_statements = HashMap::new();
        Ok((conn, prepared_statements))
    }
}

#[async_trait]
impl UpstreamDatabase for MySqlUpstream {
    type QueryResult<'a> = QueryResult<'a>;
    type StatementMeta = StatementMeta;
    type PrepareData<'a> = ();
    type ExecMeta<'a> = ();
    type Error = Error;
    const DEFAULT_DB_VERSION: &'static str = "8.0.26-readyset\0";
    const SQL_DIALECT: nom_sql::Dialect = nom_sql::Dialect::MySQL;

    async fn connect(upstream_config: UpstreamConfig) -> Result<Self, Error> {
        let (conn, prepared_statements) = Self::connect_inner(upstream_config).await?;
        Ok(Self {
            conn,
            prepared_statements,
        })
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

    async fn is_connected(&mut self) -> Result<bool, Self::Error> {
        Ok(self.conn.ping().await.is_ok())
    }

    /// Prepares the given query using the mysql connection. Note, queries are prepared on a
    /// per connection basis. They are not universal.
    async fn prepare<'a, 'b, S>(
        &'a mut self,
        query: S,
        _: (),
    ) -> Result<UpstreamPrepare<Self>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let statement = self.conn.prep(query.as_ref()).await?;
        if let Some(old_stmt) = self
            .prepared_statements
            .insert(statement.id(), statement.clone())
        {
            self.conn.close(old_stmt).await?;
        }
        Ok(UpstreamPrepare {
            statement_id: statement.id(),
            meta: StatementMeta {
                params: statement.params().to_owned(),
                schema: statement.columns().to_owned(),
            },
        })
    }

    async fn execute<'a>(
        &'a mut self,
        id: u32,
        params: &[DfValue],
        _exec_meta: Self::ExecMeta<'_>,
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

    async fn remove_statement(&mut self, statement_id: DeallocateId) -> Result<(), Self::Error> {
        match statement_id {
            DeallocateId::Numeric(id) => match self.prepared_statements.remove(&id) {
                Some(statement) => self.conn.close(statement).await?,
                None => {
                    // It's highly unlikely that a numeric statement id was _not_
                    // prepared via the mysql wire protocol (COM_STMT_PREPARE), but
                    // send it to the upstream for completeness and let mysql complain
                    // if the id is not found.
                    self.conn
                        .query_drop(format!("DEALLOCATE PREPARE {}", id))
                        .await?;
                }
            },
            DeallocateId::Named(name) => {
                self.conn
                    .query_drop(format!("DEALLOCATE PREPARE {}", name))
                    .await?
            }
            DeallocateId::All => {
                unsupported!("MySQL does not support a DEALLOCATE ALL behavior");
            }
        }

        Ok(())
    }

    async fn query<'a>(&'a mut self, query: &'a str) -> Result<Self::QueryResult<'a>, Error> {
        let result = self.conn.query_iter(query).await?;
        handle_query_result!(result)
    }

    // MySQL does not have a separation of Simple/Extended query protocols like Postgres does.
    async fn simple_query<'a>(
        &'a mut self,
        _query: &'a str,
    ) -> Result<Self::QueryResult<'a>, Error> {
        unsupported!("MySQL does not have a simple_query protocol");
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
        transaction.query_drop(query.as_ref()).await.map_err(|e| {
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

    async fn start_tx<'a>(
        &'a mut self,
        stmt: &StartTransactionStatement,
    ) -> Result<Self::QueryResult<'a>, Error> {
        self.conn.query_drop(stmt.to_string()).await?;

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

    async fn schema_search_path(&mut self) -> Result<Vec<SqlIdentifier>, Self::Error> {
        Ok(self.database().into_iter().map(|s| s.into()).collect())
    }
}

impl Drop for MySqlUpstream {
    fn drop(&mut self) {
        metrics::decrement_gauge!(recorded::CLIENT_UPSTREAM_CONNECTIONS, 1.0);
    }
}
