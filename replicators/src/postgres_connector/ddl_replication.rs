//! DDL streaming replication for PostgreSQL
//!
//! Postgresql's native streaming logical replication protocol doesn't support replicating schema
//! changes. There's a [plugin][pglogical] that adds support for replicating DDL, but we want
//! ReadySet to be deployable in situations where users might not be able to install arbitrary
//! plugins (such as AWS RDS). The method we use to implement streaming replication of DDL for
//! PostgreSQL under these constraints is to use [event triggers][], which allow registering
//! arbitrary functions written in PL/PGSQL to be run on DDL change events, to write information
//! about changes to DDL to a special [`ddl_replication_log` table][table]. Those events are then
//! replicated as normal, and we [special-case them][handle] in the replication adapter to convert
//! them to schema change events, which are then handled normally.
//!
//! The definition of the `ddl_replication_log` table, and the functions and event triggers which
//! populate it, are located in `ddl_replication.sql`, in the same directory as this module - this
//! file is executed on the database by the [`setup_ddl_replication`] function when first starting
//! up the postgresql replicator.
//!
//! [pglogical]: https://github.com/2ndQuadrant/pglogical
//! [event triggers]: https://www.postgresql.org/docs/current/event-triggers.html
//! [table]: DDL_REPLICATION_LOG_TABLE
//! [handle]: DdlEvent::from_wal_event
//!
//! # Specifics of different DDL events
//!
//! Due to details about how PostgreSQL exposes metadata about the schema of different objects,
//! different DDL events are replicated in different ways:
//!
//! * `DROP TABLE` and `DROP VIEW` are replicated directly as rows in the `ddl_replication_log`
//!   table, then [`DdlEvent::to_ddl`] constructs the statement directly by constructing an AST
//!   in-place and converting that to a string. Since neither of these statements have any extra
//!   information we have to convey, this is the simplest way to avoid having to do any SQL
//!   [dialect] conversion
//! * For `CREATE TABLE` and `CREATE VIEW`, the event trigger constructs a postgresql-dialect
//!   statement *in PL/PGSQL*, which we then convert to the noria-native dialect by parsing it and
//!   re-converting it to a string. This is necessary because the information provided to us by the
//!   postgresql catalog (`pg_views.definition` for views, and numerous things for tables) has
//!   information that's already formatted in the postgresql dialect, which we would have to convert
//!   anyway.
//! * Since the information available to an event trigger for an `ALTER TABLE` event is insufficient
//!   to construct a full `ALTER TABLE` statement, `ALTER TABLE` events are replicated as a `CREATE
//!   TABLE` statement - Noria will then know that a `CREATE TABLE` statement for a table that
//!   already exists should be treated as an alter table.
//!
//! [dialect]: nom_sql::Dialect

use std::str::FromStr;

use nom_sql::{parse_query, Dialect, DropTableStatement, DropViewStatement, Table};
use noria_errors::{internal, internal_err, ReadySetError, ReadySetResult};
use pgsql::tls::MakeTlsConnect;
use tokio_postgres as pgsql;
use tracing::debug;

use super::wal_reader::WalEvent;

/// The name of the table that DDL replication logs will be written to
const DDL_REPLICATION_LOG_TABLE: &str = "ddl_replication_log";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DdlEventKind {
    CreateTable,
    DropTable,
    AlterTable,
    CreateView,
    DropView,
}

impl FromStr for DdlEventKind {
    type Err = ReadySetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "create_table" => Ok(Self::CreateTable),
            "drop_table" => Ok(Self::DropTable),
            "alter_table" => Ok(Self::AlterTable),
            "create_view" => Ok(Self::CreateView),
            "drop_view" => Ok(Self::DropView),
            s => internal!("Unknown DDL event kind `{}`", s),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DdlEvent<'a> {
    kind: DdlEventKind,
    schema_name: &'a str,
    object_name: &'a str,
    statement: Option<String>,
}

impl<'a> DdlEvent<'a> {
    /// If the given WAL event represents a DDL change event, attempt to convert that to a
    /// [`DdlEvent`] and return it, otherwise returns `Ok(None)`. Returns an error if the WAL event
    /// represents an invalid [`DdlEvent`]
    pub(crate) fn from_wal_event(wal_event: &'a WalEvent) -> ReadySetResult<Option<Self>> {
        let tuple = if let WalEvent::Insert { table, tuple, .. } = wal_event {
            if table != DDL_REPLICATION_LOG_TABLE {
                return Ok(None);
            }
            tuple
        } else {
            return Ok(None);
        };

        // Schema is:
        // CREATE TABLE IF NOT EXISTS readyset.ddl_replication_log (
        //     "id" SERIAL PRIMARY KEY,
        //     "event_type" TEXT NOT NULL,
        //     "schema_name" TEXT,
        //     "object_name" TEXT NOT NULL,
        //     "statement" TEXT,
        //     "created_at" TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
        // );

        if tuple.len() != 6 {
            internal!("Invalid event received from DDL replication log");
        }

        let kind = DdlEventKind::from_str((&tuple[1]).try_into()?)
            .map_err(|_| internal_err!("Invalid DDL event kind"))?;
        let schema_name = (&tuple[2]).try_into()?;
        let object_name = (&tuple[3]).try_into()?;
        let statement = if [DdlEventKind::CreateTable, DdlEventKind::CreateView].contains(&kind) {
            let query = <&str>::try_from(&tuple[4])?;
            Some(
                parse_query(Dialect::PostgreSQL, query)
                    .map_err(|_| ReadySetError::UnparseableQuery {
                        query: query.into(),
                    })?
                    .to_string(),
            )
        } else {
            None
        };

        Ok(Some(Self {
            kind,
            schema_name,
            object_name,
            statement,
        }))
    }

    /// Convert this [`DdlEvent`] into a SQL DDL statement that can be sent to Noria directly (using
    /// the Noria-native SQL dialect, not the postgresql dialect!)
    pub(crate) fn to_ddl(&self) -> String {
        match self.kind {
            DdlEventKind::CreateTable | DdlEventKind::CreateView | DdlEventKind::AlterTable => {
                self.statement.clone().unwrap()
            }
            DdlEventKind::DropTable => DropTableStatement {
                tables: vec![Table {
                    schema: Some(self.schema_name.into()),
                    name: self.object_name.into(),
                }],
                // We might be getting a drop table event for a table we don't have, eg if the table
                // originally failed to parse
                if_exists: true,
            }
            .to_string(),
            DdlEventKind::DropView => DropViewStatement {
                views: vec![self.object_name.into()],
                // We might be getting a drop view event for a view we don't have, eg if the view
                // originally failed to parse
                if_exists: true,
            }
            .to_string(),
        }
    }

    pub(crate) fn schema(&self) -> &str {
        self.schema_name
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::ops::{Deref, DerefMut};
    use std::time::Duration;

    use nom_sql::{
        parse_query, ColumnConstraint, ColumnSpecification, CreateViewStatement, Dialect, Expr,
        FieldDefinitionExpr, SelectSpecification, SqlQuery, SqlType, TableExpr, TableKey,
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
        Context {
            client,
            conn_handle,
        }
    }

    #[tokio::test]
    async fn create_table() {
        let client = setup("create_table").await;

        client
            .simple_query("create table t1 (id integer primary key, value text, unique(value))")
            .await
            .unwrap();
        let ddl = client
            .query_one(
                "select *
             from readyset.ddl_replication_log
             where event_type = 'create_table'
             order by id desc
             limit 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(ddl.get::<_, String>("event_type"), "create_table");
        assert_eq!(ddl.get::<_, String>("schema_name"), "public");
        assert_eq!(ddl.get::<_, String>("object_name"), "t1");
        let statement = ddl.get::<_, String>("statement");
        let statement_parsed = parse_query(Dialect::PostgreSQL, &statement).unwrap();
        match statement_parsed {
            SqlQuery::CreateTable(stmt) => {
                assert_eq!(stmt.table.name, "t1");
                assert_eq!(
                    stmt.fields,
                    vec![
                        ColumnSpecification {
                            column: "t1.id".into(),
                            sql_type: SqlType::Int(None),
                            constraints: vec![ColumnConstraint::NotNull],
                            comment: None
                        },
                        ColumnSpecification {
                            column: "t1.value".into(),
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
                            columns: vec!["t1.id".into()]
                        },
                        TableKey::UniqueKey {
                            name: None,
                            columns: vec!["t1.value".into()],
                            index_type: None
                        }
                    ]
                );
            }
            _ => panic!("Unexpected query type: {:?}", statement_parsed),
        }
    }

    #[tokio::test]
    async fn create_table_with_reserved_keyword_as_name() {
        let client = setup("create_table_with_reserved_keyword_as_name").await;
        client
            .simple_query("create table \"table\" (x int)")
            .await
            .unwrap();
        let ddl = client
            .query_one(
                "select *
             from readyset.ddl_replication_log
             where event_type = 'create_table'
             order by id desc
             limit 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(ddl.get::<_, String>("object_name"), "table");
        let ddl_parsed =
            parse_query(Dialect::PostgreSQL, &ddl.get::<_, String>("statement")).unwrap();
        match ddl_parsed {
            SqlQuery::CreateTable(stmt) => {
                assert_eq!(stmt.table.name, "table");
            }
            _ => panic!("Unexpected query type: {:?}", ddl_parsed),
        }
    }

    #[tokio::test]
    async fn alter_table_has_create_table_statement() {
        let client = setup("alter_table_has_create_table_statement").await;
        client.simple_query("create table t (x int)").await.unwrap();
        client
            .simple_query("alter table t add column y int")
            .await
            .unwrap();
        let ddl = client
            .query_one(
                "select *
             from readyset.ddl_replication_log
             where event_type = 'alter_table'
             order by id desc
             limit 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(ddl.get::<_, String>("event_type"), "alter_table");
        assert_eq!(ddl.get::<_, String>("schema_name"), "public");
        assert_eq!(ddl.get::<_, String>("object_name"), "t");
        let statement = ddl.get::<_, String>("statement");
        let statement_parsed = parse_query(Dialect::PostgreSQL, &statement).unwrap();
        match statement_parsed {
            SqlQuery::CreateTable(stmt) => {
                assert_eq!(stmt.table.name, "t");
                assert_eq!(
                    stmt.fields,
                    vec![
                        ColumnSpecification {
                            column: "t.x".into(),
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None
                        },
                        ColumnSpecification {
                            column: "t.y".into(),
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None
                        },
                    ]
                );
            }
            _ => panic!("Unexpected query type: {:?}", statement_parsed),
        }
    }

    #[tokio::test]
    async fn create_view() {
        let client = setup("create_view").await;
        client
            .simple_query("create table t (x int);")
            .await
            .unwrap();
        client
            .simple_query("create view v as select * from t;")
            .await
            .unwrap();
        let ddl = client
            .query_one(
                "select *
             from readyset.ddl_replication_log
             where event_type = 'create_view'
             order by id desc
             limit 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(ddl.get::<_, String>("event_type"), "create_view");
        assert_eq!(ddl.get::<_, String>("schema_name"), "public");
        assert_eq!(ddl.get::<_, String>("object_name"), "v");
        let statement = ddl.get::<_, String>("statement");
        let statement_parsed = parse_query(Dialect::PostgreSQL, &statement).unwrap();
        match statement_parsed {
            SqlQuery::CreateView(CreateViewStatement {
                name, definition, ..
            }) => {
                assert_eq!(name, "v");
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
            _ => panic!("Unexpected query type {:?}", statement_parsed),
        }
    }
}
