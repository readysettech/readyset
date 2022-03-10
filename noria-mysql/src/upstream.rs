use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::consts::{CapabilityFlags, StatusFlags};
use mysql_async::prelude::Queryable;
use mysql_async::{Column, Conn, Opts, OptsBuilder, Row, TxOpts, UrlError};
use noria::ColumnSchema;
use noria_client::upstream_database::NoriaCompare;
use noria_client::{UpstreamDatabase, UpstreamPrepare};
use noria_data::DataType;
use noria_errors::{internal_err, ReadySetError};
use tracing::{debug, error, info, info_span, Instrument};

use crate::schema::{convert_column, is_subtype};
use crate::Error;

type StatementID = u32;

fn dt_to_value_params(dt: &[DataType]) -> Result<Vec<mysql_async::Value>, noria::ReadySetError> {
    dt.iter().map(|v| v.try_into()).collect()
}

#[derive(Debug)]
pub enum QueryResult {
    WriteResult {
        num_rows_affected: u64,
        last_inserted_id: u64,
        status_flags: StatusFlags,
    },
    ReadResult {
        data: Vec<Row>,
        columns: Option<Arc<[Column]>>,
        status_flags: StatusFlags,
    },
    Command {
        status_flags: StatusFlags,
    },
}

/// A connector to an underlying mysql store. This is really just a wrapper for the mysql crate.
pub struct MySqlUpstream {
    conn: Conn,
    prepared_statements: HashMap<StatementID, mysql_async::Statement>,
    url: String,
    in_transaction: bool,
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
            let noria_column_type = convert_column(&sch.spec)?.coltype;
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

#[async_trait]
impl UpstreamDatabase for MySqlUpstream {
    type QueryResult = QueryResult;
    type StatementMeta = StatementMeta;
    type Error = Error;

    async fn connect(url: String) -> Result<Self, Error> {
        // CLIENT_SESSION_TRACK is required for GTID information to be sent in OK packets on commits
        // GTID information is used for RYW
        // Currently this causes rows affected to return an incorrect result, so this is feature
        // gated.
        let opts =
            Opts::from_url(&url).map_err(|e: UrlError| Error::MySql(mysql_async::Error::Url(e)))?;
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
        Ok(Self {
            conn,
            prepared_statements,
            url,
            in_transaction: false,
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    fn database(&self) -> Option<&str> {
        self.conn.opts().db_name()
    }

    async fn reset(&mut self) -> Result<(), Error> {
        let opts = self.conn.opts().clone();
        let conn = Conn::new(opts).await?;
        let prepared_statements = HashMap::new();
        let url = self.url.clone();
        let old_self = std::mem::replace(
            self,
            Self {
                conn,
                prepared_statements,
                url,
                in_transaction: false,
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

    async fn execute(&mut self, id: u32, params: &[DataType]) -> Result<Self::QueryResult, Error> {
        let params = dt_to_value_params(params)?;
        let mut result = self
            .conn
            .exec_iter(
                self.prepared_statements.get(&id).ok_or(Error::ReadySet(
                    ReadySetError::PreparedStatementMissing { statement_id: id },
                ))?,
                params,
            )
            .await?;
        let columns = result.columns().ok_or_else(|| {
            ReadySetError::Internal("The mysql_async result was already consumed".to_string())
        })?;

        if columns.len() > 0 {
            Ok(QueryResult::ReadResult {
                data: result.collect().await?,
                columns: Some(columns),
                status_flags: self.conn.status(),
            })
        } else {
            Ok(QueryResult::WriteResult {
                num_rows_affected: result.affected_rows(),
                last_inserted_id: result.last_insert_id().unwrap_or(1),
                status_flags: self.conn.status(),
            })
        }
    }

    async fn query<'a, S>(&'a mut self, query: S) -> Result<Self::QueryResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let mut result = self.conn.query_iter(query).await?;

        let columns = result.columns().ok_or_else(|| {
            ReadySetError::Internal("The mysql_async result was already consumed".to_string())
        })?;
        if columns.len() > 0 {
            Ok(QueryResult::ReadResult {
                data: result.collect().await?,
                columns: Some(columns),
                status_flags: self.conn.status(),
            })
        } else {
            Ok(QueryResult::WriteResult {
                num_rows_affected: result.affected_rows(),
                last_inserted_id: result.last_insert_id().unwrap_or(1),
                status_flags: self.conn.status(),
            })
        }
    }

    /// Executes the given query on the mysql backend.
    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        query: S,
    ) -> Result<(Self::QueryResult, String), Error>
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
        debug!("results : {:?}, {:?}", affected_rows, last_insert_id);

        let status_flags = transaction.status();
        let txid = transaction.commit_returning_gtid().await.map_err(|e| {
            internal_err(format!(
                "Error obtaining GTID from MySQL for RYW-enabled commit: {}",
                e
            ))
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

    async fn start_tx(&mut self) -> Result<Self::QueryResult, Error> {
        if self.in_transaction {
            return Err(
                mysql_async::Error::Driver(mysql_async::DriverError::NestedTransaction).into(),
            );
        }

        self.conn.query_drop("START TRANSACTION").await?;

        self.in_transaction = true;
        Ok(QueryResult::Command {
            status_flags: self.conn.status(),
        })
    }

    fn is_in_tx(&self) -> bool {
        self.in_transaction
    }

    async fn commit(&mut self) -> Result<Self::QueryResult, Error> {
        let result = self.conn.query_iter("COMMIT").await?;
        result.drop_result().await?;
        self.in_transaction = false;

        Ok(QueryResult::Command {
            status_flags: self.conn.status(),
        })
    }

    async fn rollback(&mut self) -> Result<Self::QueryResult, Error> {
        let result = self.conn.query_iter("ROLLBACK").await?;
        result.drop_result().await?;
        self.in_transaction = false;

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
}

#[cfg(test)]
mod tests {
    use mysql_srv::ColumnType;
    use nom_sql::{Column as NomColumn, ColumnSpecification, SqlType};

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
                ColumnSpecification::new(test_column(), SqlType::Varchar(Some(10))),
                "table1".into(),
            ),
            ColumnSchema::from_base(
                ColumnSpecification::new(test_column(), SqlType::Double),
                "table1".into(),
            ),
        ];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::Int(None)),
            "table1".into(),
        )];

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
            ColumnSpecification::new(test_column(), SqlType::Varchar(Some(10))),
            "table1".into(),
        )];

        let schema_spec = vec![ColumnSchema::from_base(
            ColumnSpecification::new(test_column(), SqlType::Int(None)),
            "table1".into(),
        )];

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
            ColumnSpecification::new(test_column(), SqlType::Varchar(Some(8))),
            "table1".into(),
        )];

        assert!(s.compare(&schema_spec, &param_specs).is_err());
    }
}
