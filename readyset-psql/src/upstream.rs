use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use nom_sql::{SqlIdentifier, StartTransactionStatement};
use pgsql::types::Type;
use pgsql::{GenericResult, ResultStream, Row, SimpleQueryMessage};
use postgres_types::Kind;
use psql_srv::{Column, TransferFormat};
use readyset_adapter::upstream_database::UpstreamDestination;
use readyset_adapter::{UpstreamConfig, UpstreamDatabase, UpstreamPrepare};
use readyset_data::DfValue;
use readyset_errors::{internal_err, invariant_eq, unsupported, ReadySetError, ReadySetResult};
use tokio_postgres as pgsql;
use tracing::{debug, info, info_span};
use tracing_futures::Instrument;

use crate::Error;

/// Indicates the minimum upstream server version that we currently support. Used to error out
/// during connection phase if the version for the upstream server is too low.
const MIN_UPSTREAM_VERSION: u16 = 13;

/// A connector to an underlying PostgreSQL database
pub struct PostgreSqlUpstream {
    /// This is the underlying (regular) PostgreSQL client
    client: pgsql::Client,
    /// A tokio task that handles the connection, required by `tokio_postgres` to operate
    _connection_handle: tokio::task::JoinHandle<Result<(), pgsql::Error>>,
    /// Map from prepared statement IDs to prepared statements
    prepared_statements: HashMap<u32, pgsql::Statement>,
    /// ID for the next prepared statement
    statement_id_counter: u32,
    /// The user used to connect to the upstream, if any
    user: Option<String>,
    /// Upstream db configuration
    upstream_config: UpstreamConfig,

    /// ReadySet-wrapped Postgresql version string, to return to clients
    version: String,
}

pub enum QueryResult {
    EmptyRead,
    Stream {
        first_row: Row,
        stream: Pin<Box<ResultStream>>,
    },
    Write {
        num_rows_affected: u64,
    },
    Command,
    SimpleQuery(Vec<SimpleQueryMessage>),
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
                .field("stream", &"...")
                .finish(),
            Self::Write { num_rows_affected } => f
                .debug_struct("Write")
                .field("num_rows_affected", num_rows_affected)
                .finish(),
            Self::Command => write!(f, "Command"),
            Self::SimpleQuery(ms) => f.debug_tuple("SimpleQuery").field(ms).finish(),
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
    const SQL_DIALECT: nom_sql::Dialect = nom_sql::Dialect::PostgreSQL;

    async fn connect(upstream_config: UpstreamConfig) -> Result<Self, Error> {
        let url = upstream_config
            .upstream_db_url
            .as_ref()
            .ok_or(ReadySetError::InvalidUpstreamDatabase)?;

        let pg_config = pgsql::Config::from_str(url)?;
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
        span.in_scope(|| info!("Establishing connection"));
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
        if major < MIN_UPSTREAM_VERSION {
            return Err(Error::ReadySet(ReadySetError::UnsupportedServerVersion {
                major,
                minor: minor.to_owned(),
                min: MIN_UPSTREAM_VERSION,
            }));
        }
        let version = format!("{version} ReadySet");
        let _connection_handle = tokio::spawn(connection);
        span.in_scope(|| info!("Established connection to upstream"));

        Ok(Self {
            client,
            _connection_handle,
            prepared_statements: Default::default(),
            statement_id_counter: 0,
            user,
            upstream_config,
            version,
        })
    }

    async fn reset(&mut self) -> Result<(), Error> {
        let old_self = std::mem::replace(self, Self::connect(self.upstream_config.clone()).await?);
        drop(old_self);
        Ok(())
    }

    async fn is_connected(&mut self) -> Result<bool, Self::Error> {
        Ok(!self.client.is_closed())
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
                    Ok(Column {
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
        self.prepared_statements.insert(statement_id, statement);

        Ok(UpstreamPrepare { statement_id, meta })
    }

    async fn query<'a>(&'a mut self, query: &'a str) -> Result<Self::QueryResult<'a>, Error> {
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
        statement_id: u32,
        params: &[DfValue],
        exec_meta: &'_ [TransferFormat],
    ) -> Result<Self::QueryResult<'a>, Error> {
        let statement = self
            .prepared_statements
            .get(&statement_id)
            .ok_or(ReadySetError::PreparedStatementMissing { statement_id })?;

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
            Some(Ok(GenericResult::NumRows(num_rows_affected))) => {
                Ok(QueryResult::Write { num_rows_affected })
            }
            Some(Ok(GenericResult::Row(first_row))) => {
                Ok(QueryResult::Stream { first_row, stream })
            }
        }
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
}
