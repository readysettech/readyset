//! DDL streaming replication for PostgreSQL
//!
//! Postgresql's native streaming logical replication protocol doesn't support replicating schema
//! changes. There's a [plugin][pglogical] that adds support for replicating DDL, but we want
//! ReadySet to be deployable in situations where users might not be able to install arbitrary
//! plugins (such as AWS RDS). The method we use to implement streaming replication of DDL for
//! PostgreSQL under these constraints is to use [event triggers][], which allow registering
//! arbitrary functions written in PL/PGSQL to be run on DDL change events, and inject information
//! about changes to DDL into the WAL. Those events are then replicated as a WAL record of type
//! Message, with a prefix of "readyset" using special json encoding.
//!
//! The definition of the functions and event triggers which populate the WAL, are located in
//! `ddl_replication.sql`,
//!in the same directory as this module - this file is executed on the database by the [
//!`setup_ddl_replication`] function when first starting up the postgresql
//! replicator.
//!
//! [pglogical]: https://github.com/2ndQuadrant/pglogical
//! [event triggers]: https://www.postgresql.org/docs/current/event-triggers.html
//!
//! # Specifics of different DDL events
//!
//! Due to details about how PostgreSQL exposes metadata about the schema of different objects,
//! different DDL events are replicated in different ways:
//!
//! * `DROP TABLE` and `DROP VIEW` don't have any extra information we have to convey, this is why
//!   they are encoded simply with a schema and object information.
//! * For `CREATE TABLE` and `CREATE VIEW`, the event trigger constructs a postgresql-dialect
//!   statement *in PL/PGSQL*, which we then convert to the noria-native dialect by parsing it and
//!   re-converting it to a string. This is necessary because the information provided to us by the
//!   postgresql catalog (`pg_views.definition` for views, and numerous things for tables) has
//!   information that's already formatted in the postgresql dialect, which we would have to convert
//!   anyway.
//! * Since the information available to an event trigger for an `ALTER TABLE` event is insufficient
//!   to construct a full `ALTER TABLE` statement, `ALTER TABLE` events are replicated as a `CREATE
//!   TABLE` statement - ReadySet will then know that a `CREATE TABLE` statement for a table that
//!   already exists should be treated as an alter table.
//!
//! [dialect]: nom_sql::Dialect

use nom_sql::{parse_query, Dialect, DropTableStatement, DropViewStatement, SqlQuery, Table};
use pgsql::tls::MakeTlsConnect;
use readyset_errors::ReadySetResult;
use serde::{Deserialize, Deserializer};
use tokio_postgres as pgsql;
use tracing::debug;

/// Setup everything in the database that's necessary for DDL replication.
///
/// This makes a new connection to the database, since the main connection created for the
/// replicator has the `replication` flag set, and postgres disallows schema change queries from
/// being run on replication connections
pub(crate) async fn setup_ddl_replication<T>(config: pgsql::Config, tls: T) -> ReadySetResult<()>
where
    T: MakeTlsConnect<pgsql::Socket> + Send,
    <T as MakeTlsConnect<pgsql::Socket>>::Stream: Send + 'static,
{
    let (client, conn) = config.connect(tls).await?;
    let conn_handle = tokio::spawn(conn);
    debug!("Setting up DDL replication");
    client
        .batch_execute(include_str!("./ddl_replication.sql"))
        .await?;
    debug!("Set up DDL replication");
    conn_handle.abort();

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DdlEventOperation {
    CreateTable,
    DropTable,
    AlterTable,
    CreateView,
    DropView,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DdlEvent {
    operation: DdlEventOperation,
    schema: String,
    object: String,
    statement: Option<ParsedStatement>,
}

/// This is just an ugly wrapper because `deserialize_with` doesn't play well with Options
#[derive(Debug, Deserialize, Clone)]
struct ParsedStatement(#[serde(deserialize_with = "parse_pgsql")] SqlQuery);

fn parse_pgsql<'de, D>(deserializer: D) -> Result<SqlQuery, D::Error>
where
    D: Deserializer<'de>,
{
    let query = String::deserialize(deserializer)?;
    parse_query(Dialect::PostgreSQL, query).map_err(serde::de::Error::custom)
}

impl DdlEvent {
    /// Convert this [`DdlEvent`] into a SQL DDL statement that can be sent to ReadySet directly
    /// (using the ReadySet-native SQL dialect, not the postgresql dialect!)
    pub(crate) fn to_ddl(&self) -> String {
        match self.operation {
            DdlEventOperation::CreateTable
            | DdlEventOperation::CreateView
            | DdlEventOperation::AlterTable => self.statement.clone().unwrap().0.to_string(),
            DdlEventOperation::DropTable => DropTableStatement {
                tables: vec![Table {
                    schema: Some(self.schema.as_str().into()),
                    name: self.object.as_str().into(),
                }],
                // We might be getting a drop table event for a table we don't have, eg if the table
                // originally failed to parse
                if_exists: true,
            }
            .to_string(),
            DdlEventOperation::DropView => DropViewStatement {
                views: vec![self.object.as_str().into()],
                // We might be getting a drop view event for a view we don't have, eg if the view
                // originally failed to parse
                if_exists: true,
            }
            .to_string(),
        }
    }

    pub(crate) fn schema(&self) -> &str {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::ops::{Deref, DerefMut};
    use std::time::Duration;

    use nom_sql::{
        ColumnConstraint, ColumnSpecification, CreateViewStatement, Expr, FieldDefinitionExpr,
        SelectSpecification, SqlQuery, SqlType, TableExpr, TableKey,
    };
    use pgsql::NoTls;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tracing::error;

    use super::*;

    struct Context {
        client: pgsql::Client,
        conn_handle: JoinHandle<Result<(), pgsql::Error>>,
    }

    impl Drop for Context {
        fn drop(&mut self) {
            self.conn_handle.abort();
        }
    }

    impl Deref for Context {
        type Target = pgsql::Client;

        fn deref(&self) -> &Self::Target {
            &self.client
        }
    }

    impl DerefMut for Context {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.client
        }
    }

    fn config() -> tokio_postgres::Config {
        let mut config = tokio_postgres::Config::new();
        config
            .user(&env::var("PGUSER").unwrap_or_else(|_| "postgres".into()))
            .password(
                env::var("PGPASSWORD")
                    .unwrap_or_else(|_| "noria".into())
                    .as_bytes(),
            )
            .host(&env::var("PGHOST").unwrap_or_else(|_| "localhost".into()))
            .port(
                env::var("PGPORT")
                    .unwrap_or_else(|_| "5432".into())
                    .parse()
                    .unwrap(),
            );
        config
    }

    async fn setup(dbname: &str) -> Context {
        let (client, conn) = config().dbname("postgres").connect(NoTls).await.unwrap();
        let handle = tokio::spawn(conn);
        while let Err(error) = client
            .simple_query(&format!("DROP DATABASE IF EXISTS {dbname}"))
            .await
        {
            error!(%error, "Error dropping database");
            sleep(Duration::from_millis(200)).await;
        }
        client
            .simple_query(&format!("CREATE DATABASE {dbname}"))
            .await
            .unwrap();

        handle.abort();

        let mut config = config();
        config.dbname(dbname);
        setup_ddl_replication(config.clone(), NoTls).await.unwrap();

        let (client, conn) = config.connect(NoTls).await.unwrap();

        let conn_handle = tokio::spawn(conn);

        let _ = client
            .simple_query(&format!("SELECT pg_drop_replication_slot('{dbname}');"))
            .await;
        client
            .simple_query(&format!(
                "SELECT pg_create_logical_replication_slot('{dbname}', 'pgoutput');"
            ))
            .await
            .unwrap();

        Context {
            client,
            conn_handle,
        }
    }

    async fn get_last_ddl(client: &Context, dbname: &str) -> anyhow::Result<DdlEvent> {
        let ddl: Vec<u8> = client
            .query_one(
                format!(
                    "SELECT substring(data, 24)
                     FROM pg_logical_slot_get_binary_changes('{dbname}',
                     NULL, NULL, 'proto_version', '1', 'publication_names', '{dbname}', 'messages', 'true')
                     OFFSET 1 LIMIT 1;"
                )
                .as_str(),
                &[],
            )
            .await?
            .get(0);
        Ok(serde_json::from_slice(&ddl)?)
    }

    #[tokio::test]
    async fn create_table() {
        let client = setup("create_table").await;

        client
            .simple_query("create table t1 (id integer primary key, value text, unique(value))")
            .await
            .unwrap();

        let ddl = get_last_ddl(&client, "create_table").await.unwrap();
        assert_eq!(ddl.operation, DdlEventOperation::CreateTable);
        assert_eq!(ddl.schema, "public");
        assert_eq!(ddl.object, "t1");

        match ddl.statement.unwrap().0 {
            SqlQuery::CreateTable(stmt) => {
                assert_eq!(stmt.table.name, "t1");
                assert_eq!(
                    stmt.fields,
                    vec![
                        ColumnSpecification {
                            column: "id".into(),
                            sql_type: SqlType::Int(None),
                            constraints: vec![ColumnConstraint::NotNull],
                            comment: None
                        },
                        ColumnSpecification {
                            column: "value".into(),
                            sql_type: SqlType::Text,
                            constraints: vec![],
                            comment: None
                        },
                    ]
                );
                assert_eq!(
                    stmt.keys.unwrap(),
                    vec![
                        TableKey::PrimaryKey {
                            name: None,
                            columns: vec!["id".into()]
                        },
                        TableKey::UniqueKey {
                            name: None,
                            columns: vec!["value".into()],
                            index_type: None
                        }
                    ]
                );
            }
            _ => panic!("Unexpected query type: {:?}", ddl.operation),
        }
    }

    #[tokio::test]
    async fn create_table_with_reserved_keyword_as_name() {
        readyset_tracing::init_test_logging();
        let client = setup("create_table_with_reserved_keyword_as_name").await;
        client
            .simple_query("create table \"table\" (x int)")
            .await
            .unwrap();

        let ddl = get_last_ddl(&client, "create_table_with_reserved_keyword_as_name")
            .await
            .unwrap();

        match ddl.statement.unwrap().0 {
            SqlQuery::CreateTable(stmt) => {
                assert_eq!(stmt.table.name, "table");
            }
            _ => panic!("Unexpected query type: {:?}", ddl.operation),
        }
    }

    #[tokio::test]
    async fn alter_table() {
        let client = setup("alter_table").await;
        client.simple_query("create table t (x int)").await.unwrap();

        let _ = get_last_ddl(&client, "alter_table").await;

        client
            .simple_query("alter table t add column y int")
            .await
            .unwrap();

        let ddl = get_last_ddl(&client, "alter_table").await.unwrap();
        assert_eq!(ddl.operation, DdlEventOperation::AlterTable);
        assert_eq!(ddl.schema, "public");
        assert_eq!(ddl.object, "t");

        match ddl.statement.unwrap().0 {
            SqlQuery::AlterTable(stmt) => {
                assert_eq!(stmt.table.name, "t");
                assert_eq!(
                    stmt.definitions,
                    vec![nom_sql::AlterTableDefinition::AddColumn(
                        ColumnSpecification::new("y".into(), SqlType::Int(None),)
                    ),]
                );
            }
            _ => panic!("Unexpected query type: {:?}", ddl.operation),
        }
    }

    #[tokio::test]
    async fn create_view() {
        let client = setup("create_view").await;
        client
            .simple_query("create table t (x int);")
            .await
            .unwrap();

        let _ = get_last_ddl(&client, "create_view").await;

        client
            .simple_query("create view v as select * from t;")
            .await
            .unwrap();

        let ddl = get_last_ddl(&client, "create_view").await.unwrap();
        assert_eq!(ddl.operation, DdlEventOperation::CreateView);
        assert_eq!(ddl.schema, "public");
        assert_eq!(ddl.object, "v");

        match ddl.statement.unwrap().0 {
            SqlQuery::CreateView(CreateViewStatement {
                name, definition, ..
            }) => {
                assert_eq!(name, "v".into());
                match *definition {
                    SelectSpecification::Simple(select_stmt) => {
                        assert_eq!(
                            select_stmt.fields,
                            vec![FieldDefinitionExpr::Expr {
                                expr: Expr::Column("t.x".into()),
                                alias: None
                            }]
                        );
                        assert_eq!(select_stmt.tables, vec![TableExpr::from(Table::from("t"))]);
                    }
                    _ => panic!(),
                }
            }
            _ => panic!("Unexpected query type {:?}", ddl.operation),
        }
    }

    #[tokio::test]
    async fn rollback_no_ddl() {
        readyset_tracing::init_test_logging();
        let client = setup("rollback_no_ddl").await;

        client
            .simple_query(
                "begin;
                 create table v1 (x int);
                 rollback;",
            )
            .await
            .unwrap();
        get_last_ddl(&client, "rollback_no_ddl").await.unwrap_err();

        client
            .simple_query(
                "begin;
                 create table v1 (x int);
                 commit;",
            )
            .await
            .unwrap();
        get_last_ddl(&client, "rollback_no_ddl").await.unwrap();
    }
}
