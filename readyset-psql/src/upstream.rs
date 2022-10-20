use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::str::FromStr;

use async_trait::async_trait;
use futures::TryStreamExt;
use nom_sql::SqlIdentifier;
use pgsql::config::Host;
use pgsql::types::Type;
use pgsql::{GenericResult, Row, SimpleQueryMessage};
use psql_srv::Column;
use readyset::ColumnSchema;
use readyset_client::fallback_cache::FallbackCache;
use readyset_client::upstream_database::{NoriaCompare, UpstreamDestination};
use readyset_client::{UpstreamConfig, UpstreamDatabase, UpstreamPrepare};
use readyset_data::DfValue;
use readyset_errors::{unsupported, ReadySetError};
use tokio::process::Command;
use tokio_postgres as pgsql;
use tracing::{debug, info, info_span};
use tracing_futures::Instrument;

use crate::schema::type_to_pgsql;
use crate::Error;

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
}

#[derive(Debug)]
pub enum QueryResult {
    Read { data: Vec<Row> },
    Write { num_rows_affected: u64 },
    Command,
    SimpleQuery(Vec<SimpleQueryMessage>),
}

impl UpstreamDestination for QueryResult {}

#[derive(Debug, Clone)]
pub struct StatementMeta {
    /// The types of the query parameters used for this statement
    pub params: Vec<Type>,
    /// Metadata about the types of the columns in the rows returned by this statement
    pub schema: Vec<Column>,
}

// Returns if the schema plausibly matches the sets of columns.
fn schema_column_match(schema: &[ColumnSchema], columns: &[Type]) -> Result<(), Error> {
    if schema.len() != columns.len() {
        return Err(Error::ReadySet(ReadySetError::WrongColumnCount(
            columns.len(),
            schema.len(),
        )));
    }

    if cfg!(feature = "schema-check") {
        for (sch, col) in schema.iter().zip(columns.iter()) {
            let noria_type = type_to_pgsql(&sch.column_type)?;
            if &noria_type != col {
                return Err(Error::ReadySet(ReadySetError::WrongColumnType(
                    col.to_string(),
                    noria_type.to_string(),
                )));
            }
        }
    }
    Ok(())
}

impl NoriaCompare for StatementMeta {
    type Error = Error;
    fn compare(
        &self,
        columns: &[ColumnSchema],
        params: &[ColumnSchema],
    ) -> Result<(), Self::Error> {
        schema_column_match(params, &self.params)?;
        schema_column_match(
            columns,
            &self
                .schema
                .iter()
                .map(|c| c.col_type.clone())
                .collect::<Vec<_>>(),
        )?;

        Ok(())
    }
}

#[async_trait]
impl UpstreamDatabase for PostgreSqlUpstream {
    type StatementMeta = StatementMeta;
    type QueryResult<'a> = QueryResult;
    // TODO: Actually fill this in.
    type CachedReadResult = ();
    type Error = Error;

    async fn connect(
        upstream_config: UpstreamConfig,
        _: Option<FallbackCache<Self::CachedReadResult>>,
    ) -> Result<Self, Error> {
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
        let _connection_handle = tokio::spawn(connection);
        span.in_scope(|| info!("Established connection to upstream"));

        Ok(Self {
            client,
            _connection_handle,
            prepared_statements: Default::default(),
            statement_id_counter: 0,
            user,
            upstream_config,
        })
    }

    fn url(&self) -> &str {
        self.upstream_config.upstream_db_url.as_deref().unwrap()
    }

    async fn reset(&mut self) -> Result<(), Error> {
        let old_self = std::mem::replace(
            self,
            Self::connect(self.upstream_config.clone(), None).await?,
        );
        drop(old_self);
        Ok(())
    }

    async fn prepare<'a, S>(&'a mut self, query: S) -> Result<UpstreamPrepare<Self>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let query = query.as_ref();
        let statement = self.client.prepare(query).await?;

        let meta = StatementMeta {
            params: statement.params().to_vec(),
            schema: statement
                .columns()
                .iter()
                .map(|col| -> Result<_, Error> {
                    Ok(Column {
                        name: col.name().to_owned(),
                        col_type: col.type_().clone(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        };

        self.statement_id_counter += 1;
        let statement_id = self.statement_id_counter;
        self.prepared_statements.insert(statement_id, statement);

        Ok(UpstreamPrepare { statement_id, meta })
    }

    async fn query<'a, S>(&'a mut self, query: S) -> Result<Self::QueryResult<'a>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let res = self.client.simple_query(query.as_ref()).await?;
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
    ) -> Result<Self::QueryResult<'a>, Error> {
        let statement = self
            .prepared_statements
            .get(&statement_id)
            .ok_or(ReadySetError::PreparedStatementMissing { statement_id })?;

        let results: Vec<GenericResult> = self
            .client
            .generic_query_raw(statement, params)
            .await?
            .try_collect()
            .await?;

        let mut results = results.into_iter().peekable();

        // If results starts with a command complete then return a write result.
        // This could happen if a write returns no results, which is fine
        //
        // Otherwise return all the rows we get and ignore the command complete at the end
        if let Some(GenericResult::NumRows(n)) = results.peek() {
            Ok(QueryResult::Write {
                num_rows_affected: *n,
            })
        } else {
            let mut data = Vec::new();
            while let Some(GenericResult::Row(r)) = results.next() {
                data.push(r);
            }
            Ok(QueryResult::Read { data })
        }
    }

    /// Handle starting a transaction with the upstream database.
    async fn start_tx<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        self.client.query("START TRANSACTION", &[]).await?;
        Ok(QueryResult::Command)
    }

    /// Handle committing a transaction to the upstream database.
    async fn commit<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        self.client.query("COMMIT", &[]).await?;
        Ok(QueryResult::Command)
    }

    /// Handle rolling back the ongoing transaction for this connection to the upstream db.
    async fn rollback<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        self.client.query("ROLLBACK", &[]).await?;
        Ok(QueryResult::Command)
    }

    async fn schema_dump(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        let config = pgsql::Config::from_str(self.url())?;
        let mut pg_dump = Command::new("pg_dump");
        pg_dump.arg("--schema-only");
        if let Some(host) = config
            .get_hosts()
            .iter()
            .filter_map(|h| match h {
                Host::Tcp(v) => Some(v),
                _ => None,
            })
            .next()
        {
            pg_dump.arg(&format!("-h{}", host));
        } else {
            anyhow::bail!("Postgres TCP host not found");
        }
        if let Some(user) = config.get_user() {
            pg_dump.arg(format!("-U{}", user));
        }
        if let Some(dbname) = config.get_dbname() {
            pg_dump.arg(dbname);
        }
        if let Some(password) = config.get_password() {
            pg_dump.env("PGPASSWORD", OsStr::from_bytes(password));
        }
        Ok(pg_dump.output().await?.stdout)
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

#[cfg(test)]
mod tests {
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
            params: vec![Type::BOOL, Type::INT8],
            schema: vec![Column {
                name: "c1".into(),
                col_type: Type::VARCHAR,
            }],
        };

        let param_specs = vec![
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Bool),
                "table1".into(),
                Dialect::DEFAULT_POSTGRESQL,
            ),
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::BigInt(Some(10))),
                "table1".into(),
                Dialect::DEFAULT_POSTGRESQL,
            ),
        ];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::VarChar(Some(8))),
            "table1".into(),
            Dialect::DEFAULT_POSTGRESQL,
        )];

        assert!(s.compare(&schema_spec, &param_specs).is_ok());
    }

    #[test]
    fn compare_different_len_schema() {
        let s: StatementMeta = StatementMeta {
            params: vec![Type::BOOL, Type::INT8],
            schema: vec![Column {
                name: "c1".into(),
                col_type: Type::VARCHAR,
            }],
        };

        let param_specs = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::Bool),
            "table1".into(),
            Dialect::DEFAULT_POSTGRESQL,
        )];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::VarChar(Some(8))),
            "table1".into(),
            Dialect::DEFAULT_POSTGRESQL,
        )];

        assert!(s.compare(&schema_spec, &param_specs).is_err());
    }

    #[cfg(feature = "schema-check")]
    #[test]
    fn compare_different_type_schema() {
        let s: StatementMeta = StatementMeta {
            params: vec![Type::BOOL, Type::INT8],
            schema: vec![Column {
                name: "c1".into(),
                col_type: Type::VARCHAR,
            }],
        };

        let param_specs = vec![
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Bool),
                "table1".into(),
            ),
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::VarChar(Some(10))),
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
