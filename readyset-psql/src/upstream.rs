use std::fmt::Debug;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use postgres_types::Kind;
use psql_srv::{Column, TransferFormat};
use readyset_adapter::upstream_database::{UpstreamDestination, UpstreamStatementId};
use readyset_adapter::{UpstreamConfig, UpstreamDatabase, UpstreamPrepare};
use readyset_adapter_types::DeallocateId;
use readyset_client_metrics::recorded;
use readyset_data::DfValue;
use readyset_errors::{internal_err, invariant_eq, unsupported, ReadySetError, ReadySetResult};
use readyset_sql::ast::{SqlIdentifier, StartTransactionStatement};
use tokio::task::JoinHandle;
use tokio_postgres::types::Type;
use tokio_postgres::{
    Client, Config, GenericResult, ResultStream, Row, RowStream, SimpleQueryMessage,
    SimpleQueryStream, Statement,
};
use tracing::{debug, info_span};
use tracing_futures::Instrument;

use crate::schema::type_to_pgsql;
use crate::Error;

/// Indicates the minimum upstream server version that we currently support. Used to error out
/// during connection phase if the version for the upstream server is too low.
const MIN_UPSTREAM_MAJOR_VERSION: u16 = 13;
const MIN_UPSTREAM_MINOR_VERSION: u16 = 0;

/// A connector to an underlying PostgreSQL database
pub struct PostgreSqlUpstream {
    /// This is the underlying (regular) PostgreSQL client
    client: Client,
    /// A tokio task that handles the connection, required by `tokio_postgres` to operate
    _connection_handle: JoinHandle<Result<(), tokio_postgres::Error>>,
    /// Map from prepared statement IDs to prepared statements
    prepared_statements: Vec<Option<Statement>>,
    /// ID for the next prepared statement
    statement_id_counter: u32,
    /// The user used to connect to the upstream, if any
    user: Option<String>,

    /// ReadySet-wrapped Postgresql version string, to return to clients
    version: String,
}

pub enum QueryResult {
    EmptyRead,
    /// A stream of rows from the upstream database; this is the type returned when executing a
    /// prepared statement.
    Stream {
        // Stashing the first row lets us send a RowDescription before sending data rows
        first_row: Row,
        stream: Pin<Box<ResultStream>>,
    },
    Write {
        num_rows_affected: u64,
    },
    Command {
        tag: String,
    },
    SimpleQuery(Vec<SimpleQueryMessage>),
    /// A stream of rows from the upstream database; this is the type returned when executing a
    /// simple text query with no parameters.
    SimpleQueryStream {
        // Stashing the first message lets us send a RowDescription before sending data rows
        first_message: SimpleQueryMessage,
        stream: Pin<Box<SimpleQueryStream>>,
    },
    /// A stream of rows from the upstream database; this is the type returned when executing an
    /// unnamed prepared statement (with parameters) over the extended query protocol.
    RowStream {
        // Stashing the first row lets us send a RowDescription before sending data rows
        first_row: Row,
        stream: Pin<Box<RowStream>>,
    },
}

impl Debug for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyRead => write!(f, "EmptyRead"),
            Self::Stream {
                first_row,
                stream: _,
            } => f
                .debug_struct("Stream")
                .field("first_row", first_row)
                .finish_non_exhaustive(),
            Self::Write { num_rows_affected } => f
                .debug_struct("Write")
                .field("num_rows_affected", num_rows_affected)
                .finish(),
            Self::Command { tag } => f.debug_struct("Command").field("tag", tag).finish(),
            Self::SimpleQuery(ms) => f.debug_tuple("SimpleQuery").field(ms).finish(),
            Self::SimpleQueryStream {
                first_message,
                stream: _,
            } => f
                .debug_struct("SimpleQueryStream")
                .field("first_message", first_message)
                .finish_non_exhaustive(),
            Self::RowStream {
                first_row,
                stream: _,
            } => f
                .debug_struct("RowStream")
                .field("first_row", first_row)
                .finish_non_exhaustive(),
        }
    }
}

impl UpstreamDestination for QueryResult {}

#[derive(Debug, Clone)]
pub struct StatementMeta {
    /// The types of the query parameters used for this statement
    pub params: Vec<Type>,
    /// Metadata about the types of the columns in the rows returned by this statement
    pub schema: Vec<Column>,
}

/// Convert the given list of parameters for a statement that's being proxied upstream to the format
/// that the upstream database expects, according to the given list of parameter types
///
/// # Invariants
///
/// * `params` and `types` must have the same length
fn convert_params_for_upstream(params: &[DfValue], types: &[Type]) -> ReadySetResult<Vec<DfValue>> {
    invariant_eq!(params.len(), types.len());

    params
        .iter()
        .zip(types)
        .map(|(val, t)| {
            let mut v = val.clone();
            // Convert arrays of enums, which we represent internally as 1-based variant
            // indices, to the representation postgres expects, which is variant labels.
            if let (DfValue::Array(arr), Kind::Array(member_type)) = (&mut v, t.kind()) {
                // Get a mutable reference to the array by ensuring there's only one reference
                // to the underlying array data
                let arr = match Arc::get_mut(arr) {
                    Some(arr) => arr, // there was already only one reference, so just use that
                    None => {
                        // There were other references - we have to clone the underlying array, then
                        // make a fresh Arc, then get a mutable reference to that underlying array
                        v = DfValue::Array(Arc::new((**arr).clone()));
                        match &mut v {
                            DfValue::Array(arr) => Arc::get_mut(arr).expect(
                                "We just made the Arc, so we know we have the only copy of it",
                            ),
                            _ => unreachable!("We just made this, we know it's an Array"),
                        }
                    }
                };

                if let Kind::Enum(variants) = member_type.kind() {
                    for array_elt in arr.values_mut() {
                        if let Some(idx) = array_elt.as_int() {
                            *array_elt = DfValue::from(
                                variants
                                    .get((idx - 1/* indexes are 1-based! */) as usize)
                                    .ok_or_else(|| {
                                        internal_err!(
                                            "Out-of-bounds index {idx} for enum with {} variants",
                                            variants.len()
                                        )
                                    })?
                                    .clone(),
                            )
                        }
                    }
                }
            }

            Ok(v)
        })
        .collect()
}

#[async_trait]
impl UpstreamDatabase for PostgreSqlUpstream {
    type StatementMeta = StatementMeta;
    type QueryResult<'a> = QueryResult;
    type PrepareData<'a> = &'a [Type];
    type ExecMeta<'a> = &'a [TransferFormat];
    type Error = Error;
    const DEFAULT_DB_VERSION: &'static str = "13.4 (ReadySet)";
    const SQL_DIALECT: readyset_sql::Dialect = readyset_sql::Dialect::PostgreSQL;

    async fn connect(upstream_config: UpstreamConfig) -> Result<Self, Error> {
        let url = upstream_config
            .upstream_db_url
            .as_ref()
            .ok_or(ReadySetError::InvalidUpstreamDatabase)?;

        let pg_config = Config::from_str(url)?;
        let user = pg_config.get_user().map(|s| s.to_owned());
        let connector = {
            let mut builder = native_tls::TlsConnector::builder();
            if upstream_config.disable_upstream_ssl_verification {
                builder.danger_accept_invalid_certs(true);
            }
            if let Some(cert) = upstream_config.get_root_cert().await {
                builder.add_root_certificate(cert?);
            }
            builder.build().unwrap() // Never returns an error
        };
        let tls = postgres_native_tls::MakeTlsConnector::new(connector);
        let span = info_span!(
            "Connecting to PostgreSQL upstream",
            host = ?pg_config.get_hosts(),
            port = ?pg_config.get_ports()
        );
        span.in_scope(|| debug!("Establishing connection"));
        let (client, connection) = pg_config.connect(tls).instrument(span.clone()).await?;
        let version = connection.parameter("server_version").ok_or_else(|| {
            ReadySetError::Internal("Upstream database failed to send server version".to_string())
        })?;
        let (major, minor) = version
            .split_once('.')
            .ok_or(Error::ReadySet(ReadySetError::UnparseableServerVersion))?;
        let major = major
            .parse()
            .map_err(|_| Error::ReadySet(ReadySetError::UnparseableServerVersion))?;
        let minor: u16 = minor
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .map_err(|_| Error::ReadySet(ReadySetError::UnparseableServerVersion))?;

        #[allow(clippy::absurd_extreme_comparisons)]
        if major < MIN_UPSTREAM_MAJOR_VERSION
            || (major == MIN_UPSTREAM_MAJOR_VERSION && minor < MIN_UPSTREAM_MINOR_VERSION)
        {
            return Err(Error::ReadySet(ReadySetError::UnsupportedServerVersion {
                major,
                minor: minor.to_string(),
                min_major: MIN_UPSTREAM_MAJOR_VERSION,
                min_minor: MIN_UPSTREAM_MINOR_VERSION,
            }));
        }
        let version = format!("{version} ReadySet");
        let _connection_handle = tokio::spawn(connection);
        span.in_scope(|| debug!("Established connection to upstream"));
        metrics::gauge!(recorded::CLIENT_UPSTREAM_CONNECTIONS).increment(1.0);

        Ok(Self {
            client,
            _connection_handle,
            prepared_statements: Default::default(),
            statement_id_counter: 0,
            user,
            version,
        })
    }

    async fn reset(&mut self) -> Result<(), Self::Error> {
        self.client.simple_query("DISCARD ALL").await?;
        Ok(())
    }

    async fn is_connected(&mut self) -> Result<bool, Self::Error> {
        Ok(!self.client.simple_query("select 1").await?.is_empty())
    }

    async fn ping(&mut self) -> Result<(), Self::Error> {
        self.client.simple_query("select 1").await?;
        Ok(())
    }

    async fn change_user(
        &mut self,
        _user: &str,
        _password: &str,
        _database: &str,
    ) -> Result<(), Self::Error> {
        unsupported!("Changing user not yet implemented for PostgreSQL")
    }

    // Returns the upstream server's version, with ReadySet's info appended, to indicate to clients
    // that they're going via ReadySet
    fn version(&self) -> String {
        self.version.clone()
    }

    async fn prepare<'a, S>(
        &'a mut self,
        query: S,
        parameter_data_types: &[Type],
    ) -> Result<UpstreamPrepare<Self>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let query = query.as_ref();
        let statement = self
            .client
            .prepare_typed(query, parameter_data_types)
            .await?;

        let meta = StatementMeta {
            params: statement.params().to_vec(),
            schema: statement
                .columns()
                .iter()
                .map(|col| -> Result<_, Error> {
                    Ok(Column::Column {
                        name: col.name().into(),
                        col_type: col.type_().clone(),
                        table_oid: col.table_oid(),
                        attnum: col.column_id(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        };

        self.statement_id_counter += 1;
        let statement_id = self.statement_id_counter;
        match self.prepared_statements.get_mut(statement_id as usize) {
            Some(existing) => {
                *existing = Some(statement);
            }
            None => {
                let diff = (statement_id as usize) - self.prepared_statements.len();
                self.prepared_statements.reserve(diff + 1);
                for _ in 0..diff {
                    self.prepared_statements.push(None);
                }
                self.prepared_statements.push(Some(statement));
            }
        }

        Ok(UpstreamPrepare {
            statement_id: statement_id.into(),
            meta,
        })
    }

    async fn query<'a>(&'a mut self, query: &'a str) -> Result<Self::QueryResult<'a>, Error> {
        let mut stream = Box::pin(self.client.simple_query_raw(query).await?);

        match stream.next().await {
            None => Ok(QueryResult::EmptyRead),
            Some(Err(e)) => Err(e.into()),
            Some(Ok(first_message)) => Ok(QueryResult::SimpleQueryStream {
                first_message,
                stream,
            }),
        }
    }

    async fn simple_query<'a>(
        &'a mut self,
        query: &'a str,
    ) -> Result<Self::QueryResult<'a>, Error> {
        let res = self.client.simple_query(query).await?;
        Ok(QueryResult::SimpleQuery(res))
    }

    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        _query: S,
    ) -> Result<(Self::QueryResult<'a>, String), Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        unsupported!("Read-Your-Write not yet implemented for PostgreSQL")
    }

    async fn execute<'a>(
        &'a mut self,
        statement_id: &UpstreamStatementId,
        params: &[DfValue],
        exec_meta: &'_ [TransferFormat],
    ) -> Result<Self::QueryResult<'a>, Error> {
        match statement_id {
            UpstreamStatementId::Unprepared(query) => {
                // we need to get the postgres type for each DfValue parameter, and stuff a tuple of
                // those into a Vec, which we can then pass to `query_typed_raw()`
                let mut typed_params = Vec::with_capacity(params.len());
                for p in params {
                    let postgres_type = type_to_pgsql(&p.infer_dataflow_type())?;
                    typed_params.push((p, postgres_type));
                }

                let mut stream = Box::pin(self.client.query_typed_raw(query, typed_params).await?);
                match stream.next().await {
                    None => Ok(QueryResult::EmptyRead),
                    Some(Err(e)) => Err(e.into()),
                    Some(Ok(first_row)) => Ok(QueryResult::RowStream { first_row, stream }),
                }
            }
            UpstreamStatementId::Prepared(id) => {
                let statement = self
                    .prepared_statements
                    .get(*id as usize)
                    .and_then(|s| s.as_ref())
                    .ok_or(ReadySetError::PreparedStatementMissing { statement_id: *id })?;

                let mut stream = Box::pin(
                    self.client
                        .generic_query_raw(
                            statement,
                            &convert_params_for_upstream(params, statement.params())?,
                            exec_meta.iter().map(|tf| (*tf).into()),
                        )
                        .await?,
                );

                match stream.next().await {
                    None => Ok(QueryResult::EmptyRead),
                    Some(Err(e)) => Err(e.into()),
                    Some(Ok(GenericResult::Command(_, tag))) => Ok(QueryResult::Command { tag }),
                    Some(Ok(GenericResult::Row(first_row))) => {
                        Ok(QueryResult::Stream { first_row, stream })
                    }
                }
            }
        }
    }

    async fn remove_statement(&mut self, statement_id: DeallocateId) -> Result<(), Self::Error> {
        match statement_id {
            DeallocateId::Numeric(id) => {
                if let Some(a) = self.prepared_statements.get_mut(id as usize) {
                    // Note: we don't need to explictly send a close/deallocate message to the
                    // upstream as that is handled by the Drop impl on the
                    // Statement object.
                    *a = None;
                } else {
                    self.client
                        .simple_query(format!("DEALLOCATE {}", id).as_str())
                        .await?;
                }
            }
            DeallocateId::Named(name) => {
                self.client
                    .simple_query(format!("DEALLOCATE {}", name).as_str())
                    .await?;
            }
            DeallocateId::All => {
                // Note: we don't need to explictly send a close/deallocate message to the
                // upstream as that is handled by the Drop impl on the
                // Statement object. Of course, in the DeallocateId::All case, that will require a
                // round-trip DEALLOCATE message for each statement object in the
                // map, sent serially. It is assumed this API is invoked _very_
                // infrequently.
                self.prepared_statements.clear();

                // Adding to the fun, clearing the `self.prepared_statements` only deallocates
                // the statements we have _explicitly_ handled and prepared. If a user ran a
                // `PREPARE` SQL statement, we don't handle that (and we will not have an entry in
                // `self.prepared_statements`). Thus, for completeness, we need to send a
                // `DEALLOCATE ALL` statement. Lastly, this needs to happen after the `clear()` as
                // that will send a Close message for each statement, with the id. If we deallocate
                // all, then trying closing by id, PG will return an error (statement not found).
                self.client.simple_query("DEALLOCATE ALL").await?;
            }
        }

        Ok(())
    }

    /// Handle starting a transaction with the upstream database.
    async fn start_tx<'a>(
        &'a mut self,
        stmt: &StartTransactionStatement,
    ) -> Result<Self::QueryResult<'a>, Error> {
        Ok(QueryResult::SimpleQuery(
            self.client.simple_query(&stmt.to_string()).await?,
        ))
    }

    /// Handle committing a transaction to the upstream database.
    async fn commit<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        Ok(QueryResult::SimpleQuery(
            self.client.simple_query("COMMIT").await?,
        ))
    }

    /// Handle rolling back the ongoing transaction for this connection to the upstream db.
    async fn rollback<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        Ok(QueryResult::SimpleQuery(
            self.client.simple_query("ROLLBACK").await?,
        ))
    }

    async fn schema_search_path(&mut self) -> Result<Vec<SqlIdentifier>, Self::Error> {
        let raw_search_path = self
            .client
            .query_one("SHOW search_path", &[])
            .await?
            .get::<_, String>("search_path");
        debug!(%raw_search_path, "Loaded search path from upstream");

        Ok(raw_search_path
            .split(", ")
            .map(|schema| schema.trim_matches('"'))
            .filter_map(|schema| {
                if schema == "$user" {
                    self.user.clone().map(Into::into)
                } else {
                    Some(schema.into())
                }
            })
            .collect())
    }

    async fn timezone_name(&mut self) -> Result<SqlIdentifier, Self::Error> {
        let tz_name = self
            .client
            .query_one("SHOW timezone", &[])
            .await?
            .get::<_, String>("TimeZone");
        debug!(%tz_name, "Loaded system timezone from upstream");
        Ok(tz_name.into())
    }
}

impl Drop for PostgreSqlUpstream {
    fn drop(&mut self) {
        metrics::gauge!(recorded::CLIENT_UPSTREAM_CONNECTIONS).decrement(1.0);
    }
}
