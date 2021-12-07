use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::str::FromStr;

use async_trait::async_trait;
use futures::TryStreamExt;
use noria::{ColumnSchema, DataType};
use noria_client::upstream_database::NoriaCompare;
use noria_client::{UpstreamDatabase, UpstreamPrepare};
use noria_errors::{unsupported, ReadySetError};
use pgsql::config::Host;
use pgsql::types::Type;
use pgsql::{Config, GenericResult, Row};
use psql_srv::Column;
use tokio::process::Command;
use tokio_postgres as pgsql;
use tracing::{info, info_span};
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
    /// The original URL used to create the connection
    url: String,
    /// Indicates whether we are currently in a transaction.
    in_transaction: bool,
}

#[derive(Debug)]
pub enum QueryResult {
    Read { data: Vec<Row> },
    Write { num_rows_affected: u64 },
    Command,
}

#[derive(Debug)]
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
            let noria_type = type_to_pgsql(&sch.spec.sql_type)?;
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
    type QueryResult = QueryResult;
    type Error = Error;

    async fn connect(url: String) -> Result<Self, Error> {
        let config = Config::from_str(&url)?;
        let connector = native_tls::TlsConnector::builder().build().unwrap(); // Never returns an error
        let tls = postgres_native_tls::MakeTlsConnector::new(connector);
        let span = info_span!(
            "Connecting to PostgreSQL upstream",
            host = ?config.get_hosts(),
            port = ?config.get_ports()
        );
        span.in_scope(|| info!("Establishing connection"));
        let (client, connection) = config.connect(tls).instrument(span.clone()).await?;
        let _connection_handle = tokio::spawn(connection);
        span.in_scope(|| info!("Established connection to upstream"));

        Ok(Self {
            client,
            _connection_handle,
            prepared_statements: Default::default(),
            statement_id_counter: 0,
            url,
            in_transaction: false,
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn reset(&mut self) -> Result<(), Error> {
        let url = self.url.clone();
        let old_self = std::mem::replace(self, Self::connect(url).await?);
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

    async fn query<'a, S>(&'a mut self, query: S) -> Result<Self::QueryResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let results = self.client.generic_query(query.as_ref(), &[]).await?;
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

    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        _query: S,
    ) -> Result<(Self::QueryResult, String), Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        unsupported!("Read-Your-Write not yet implemented for PostgreSQL")
    }

    async fn execute(
        &mut self,
        statement_id: u32,
        params: &[DataType],
    ) -> Result<Self::QueryResult, Error> {
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
    async fn start_tx(&mut self) -> Result<Self::QueryResult, Error> {
        self.client.query("START TRANSACTION", &[]).await?;
        self.in_transaction = true;
        Ok(QueryResult::Command)
    }

    /// Return whether we are currently in a transaction or not.
    fn is_in_tx(&self) -> bool {
        self.in_transaction
    }

    /// Handle committing a transaction to the upstream database.
    async fn commit(&mut self) -> Result<Self::QueryResult, Error> {
        self.client.query("COMMIT", &[]).await?;
        self.in_transaction = false;
        Ok(QueryResult::Command)
    }

    /// Handle rolling back the ongoing transaction for this connection to the upstream db.
    async fn rollback(&mut self) -> Result<Self::QueryResult, Error> {
        self.client.query("ROLLBACK", &[]).await?;
        self.in_transaction = false;
        Ok(QueryResult::Command)
    }

    async fn schema_dump(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        let config = Config::from_str(&self.url)?;
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
}

#[cfg(test)]
mod tests {
    use nom_sql::{Column as NomColumn, ColumnSpecification, SqlType};

    use super::*;

    fn test_column() -> NomColumn {
        NomColumn {
            name: "t".to_string(),
            table: None,
            function: None,
        }
    }

    #[test]
    fn compare_matching_schema() {
        let s: StatementMeta = StatementMeta {
            params: vec![Type::BOOL, Type::INT8],
            schema: vec![Column {
                name: "c1".to_string(),
                col_type: Type::VARCHAR,
            }],
        };

        let param_specs = vec![
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Bool),
                "table1".to_string(),
            ),
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Bigint(Some(10))),
                "table1".to_string(),
            ),
        ];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::Varchar(Some(8))),
            "table1".to_string(),
        )];

        assert!(s.compare(&schema_spec, &param_specs).is_ok());
    }

    #[test]
    fn compare_different_len_schema() {
        let s: StatementMeta = StatementMeta {
            params: vec![Type::BOOL, Type::INT8],
            schema: vec![Column {
                name: "c1".to_string(),
                col_type: Type::VARCHAR,
            }],
        };

        let param_specs = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::Bool),
            "table1".to_string(),
        )];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::Varchar(Some(8))),
            "table1".to_string(),
        )];

        assert!(s.compare(&schema_spec, &param_specs).is_err());
    }

    #[cfg(feature = "schema-check")]
    #[test]
    fn compare_different_type_schema() {
        let s: StatementMeta = StatementMeta {
            params: vec![Type::BOOL, Type::INT8],
            schema: vec![Column {
                name: "c1".to_string(),
                col_type: Type::VARCHAR,
            }],
        };

        let param_specs = vec![
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Bool),
                "table1".to_string(),
            ),
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Varchar(Some(10))),
                "table1".to_string(),
            ),
        ];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::Varchar(Some(8))),
            "table1".to_string(),
        )];

        assert!(s.compare(&schema_spec, &param_specs).is_err());
    }
}
