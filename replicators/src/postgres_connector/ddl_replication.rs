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

use nom_sql::{
    parse_query, AlterTableStatement, Column, ColumnConstraint, ColumnSpecification,
    CreateTableBody, CreateTableStatement, CreateViewStatement, Dialect, Relation, SqlQuery,
    SqlType, TableKey,
};
use pgsql::tls::MakeTlsConnect;
use readyset_client::recipe::changelist::{AlterTypeChange, Change};
use readyset_data::{DfType, PgEnumMetadata};
use readyset_errors::ReadySetResult;
use readyset_tracing::info;
use serde::{Deserialize, Deserializer};
use tokio_postgres as pgsql;

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
    info!("Setting up DDL replication");
    client
        .batch_execute(include_str!("./ddl_replication.sql"))
        .await?;
    info!("Set up DDL replication");
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

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct DdlCreateTableColumn {
    name: String,
    #[serde(deserialize_with = "parse_sql_type")]
    column_type: Result<SqlType, String>,
    not_null: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct DdlCreateTableConstraint {
    #[serde(deserialize_with = "parse_table_key")]
    definition: Result<TableKey, String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct DdlEnumVariant {
    oid: u32,
    pub(crate) label: String,
}

macro_rules! make_parse_deserialize_with {
    ($name: ident -> $res: ty) => {
        make_parse_deserialize_with!($name -> $res, $name);
    };
    ($name: ident -> $res: ty, $parser: ident) => {
        fn $name<'de, D>(deserializer: D) -> Result<$res, D::Error>
        where
            D: Deserializer<'de>,
        {
            let ty = String::deserialize(deserializer)?;
            nom_sql::$parser(Dialect::PostgreSQL, ty).map_err(serde::de::Error::custom)
        }
    };
}

macro_rules! make_fallible_parse_deserialize_with {
    ($name: ident -> $res: ty) => {
        make_fallible_parse_deserialize_with!($name -> $res, $name);
    };
    ($name: ident -> $res: ty, $parser: ident) => {
        fn $name<'de, D>(deserializer: D) -> Result<Result<$res, String>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let ty = String::deserialize(deserializer)?;
            Ok(nom_sql::$parser(Dialect::PostgreSQL, ty))
        }
    };
}

make_fallible_parse_deserialize_with!(parse_sql_type -> SqlType);
make_fallible_parse_deserialize_with!(parse_table_key -> TableKey, parse_key_specification_string);
make_parse_deserialize_with!(parse_alter_table_statement -> AlterTableStatement, parse_alter_table);
make_parse_deserialize_with!(parse_create_view_statement -> CreateViewStatement, parse_create_view);

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub(crate) enum DdlEventData {
    CreateTable {
        name: String,
        columns: Vec<DdlCreateTableColumn>,
        constraints: Vec<DdlCreateTableConstraint>,
    },
    AlterTable(#[serde(deserialize_with = "parse_alter_table_statement")] AlterTableStatement),
    CreateView(#[serde(deserialize_with = "parse_create_view_statement")] CreateViewStatement),
    Drop(String),
    CreateType {
        oid: u32,
        array_oid: u32,
        name: String,
        variants: Vec<DdlEnumVariant>,
    },
    AlterType {
        oid: u32,
        name: String,
        variants: Vec<DdlEnumVariant>,
        original_variants: Option<Vec<DdlEnumVariant>>,
    },
}

#[derive(Debug, Deserialize)]
pub(crate) struct DdlEvent {
    pub(crate) schema: String,
    pub(crate) data: DdlEventData,
}

/// This is just an ugly wrapper because `deserialize_with` doesn't play well with Options
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct ParsedStatement(#[serde(deserialize_with = "parse_pgsql")] SqlQuery);

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
    pub(crate) fn into_change(self) -> Change {
        match self.data {
            DdlEventData::CreateTable {
                name,
                columns,
                constraints,
            } => {
                let table = Relation {
                    schema: Some(self.schema.into()),
                    name: name.into(),
                };

                let create_table_body: Result<_, String> = columns
                    .into_iter()
                    .map(|col| {
                        Ok(ColumnSpecification {
                            column: Column {
                                name: col.name.into(),
                                table: Some(table.clone()),
                            },
                            sql_type: col.column_type?,
                            constraints: if col.not_null {
                                vec![ColumnConstraint::NotNull]
                            } else {
                                vec![]
                            },
                            comment: None,
                        })
                    })
                    .collect::<Result<_, _>>()
                    .and_then(|fields| {
                        Ok(CreateTableBody {
                            fields,
                            keys: if constraints.is_empty() {
                                None
                            } else {
                                Some(
                                    constraints
                                        .into_iter()
                                        .map(|c| c.definition)
                                        .collect::<Result<_, _>>()?,
                                )
                            },
                        })
                    });

                match create_table_body {
                    Ok(body) => Change::CreateTable(CreateTableStatement {
                        if_not_exists: false,
                        table,
                        body: Ok(body),
                        options: Ok(vec![]),
                    }),
                    Err(_) => Change::AddNonReplicatedRelation(table),
                }
            }
            DdlEventData::AlterTable(stmt) => Change::AlterTable(stmt),
            DdlEventData::CreateView(stmt) => Change::CreateView(stmt),
            DdlEventData::Drop(name) => Change::Drop {
                name: name.into(),
                if_exists: false,
            },
            DdlEventData::CreateType {
                oid,
                array_oid,
                name,
                variants,
            } => Change::CreateType {
                name: Relation {
                    schema: Some(self.schema.clone().into()),
                    name: name.clone().into(),
                },
                ty: DfType::from_enum_variants(
                    variants.into_iter().map(|v| v.label),
                    Some(PgEnumMetadata {
                        name: name.into(),
                        schema: self.schema.into(),
                        oid,
                        array_oid,
                    }),
                ),
            },
            DdlEventData::AlterType {
                name,
                oid,
                variants,
                original_variants,
            } => Change::AlterType {
                oid,
                name: Relation {
                    schema: Some(self.schema.clone().into()),
                    name: name.into(),
                },
                change: AlterTypeChange::SetVariants {
                    new_variants: variants.into_iter().map(|v| v.label).collect(),
                    original_variants: original_variants
                        .map(|vs| vs.into_iter().map(|v| v.label).collect()),
                },
            },
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
        ColumnSpecification, CreateViewStatement, Expr, FieldDefinitionExpr, Relation,
        SelectSpecification, SqlType, TableExpr, TableKey,
    };
    use pgsql::NoTls;
    use readyset_tracing::error;
    use test_utils::{parallel_group, AsyncParallelGroup};
    use tokio::task::JoinHandle;
    use tokio::time::sleep;

    use super::*;

    static GROUP: AsyncParallelGroup = AsyncParallelGroup::new(8);

    struct Context {
        client: pgsql::Client,
        dbname: &'static str,
        conn_handle: JoinHandle<Result<(), pgsql::Error>>,
    }

    impl Context {
        async fn teardown(&self) {
            self.simple_query(&format!(
                "SELECT pg_drop_replication_slot('{}')",
                self.dbname
            ))
            .await
            .unwrap();
        }
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

    async fn setup(dbname: &'static str) -> Context {
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
            dbname,
            conn_handle,
        }
    }

    async fn get_last_ddl(client: &Context, dbname: &str) -> anyhow::Result<DdlEvent> {
        use crate::postgres_connector::wal_reader::{
            DDL_REPLICATION_LOG_SCHEMA, DDL_REPLICATION_LOG_TABLE,
        };

        let version: u32 = client
            .client
            .query_one("SHOW server_version_num", &[])
            .await?
            .get::<_, String>(0)
            .parse()
            .unwrap();

        let ddl: Vec<u8> = if version >= 140000 {
            // If Postgres version is 14 and up we replicate using pg_logical messages
            client
            .query_one(
                format!(
                    "SELECT substring(data, 24)
                     FROM pg_logical_slot_get_binary_changes('{dbname}',
                     NULL, NULL, 'proto_version', '1', 'publication_names', '{dbname}', 'messages', 'true')
                     OFFSET 1 LIMIT 1;"
                ).as_str(),
                &[],
            )
            .await?
            .get(0)
        } else {
            // If Postgres version is 13 or below we replicate by updating a row in a special table
            let ddl_text: String = client
                .query_one(
                    format!(
                        "SELECT ddl FROM {DDL_REPLICATION_LOG_SCHEMA}.{DDL_REPLICATION_LOG_TABLE}"
                    )
                    .as_str(),
                    &[],
                )
                .await?
                .get(0);
            ddl_text.into()
        };
        Ok(serde_json::from_slice(&ddl)?)
    }

    #[parallel_group(GROUP)]
    #[tokio::test]
    async fn create_table() {
        let client = setup("create_table").await;

        client
            .simple_query("create table t1 (id integer primary key, value text, unique(value))")
            .await
            .unwrap();

        let ddl = get_last_ddl(&client, "create_table").await.unwrap();
        assert_eq!(ddl.schema, "public");

        match ddl.data {
            DdlEventData::CreateTable {
                name,
                columns,
                constraints,
            } => {
                assert_eq!(name, "t1");
                assert_eq!(
                    columns,
                    vec![
                        DdlCreateTableColumn {
                            name: "id".into(),
                            column_type: Ok(SqlType::Int(None)),
                            not_null: true
                        },
                        DdlCreateTableColumn {
                            name: "value".into(),
                            column_type: Ok(SqlType::Text),
                            not_null: false
                        },
                    ]
                );
                assert_eq!(
                    constraints,
                    vec![
                        DdlCreateTableConstraint {
                            definition: Ok(TableKey::PrimaryKey {
                                constraint_name: None,
                                index_name: None,
                                columns: vec!["id".into()]
                            }),
                        },
                        DdlCreateTableConstraint {
                            definition: Ok(TableKey::UniqueKey {
                                constraint_name: None,
                                index_name: None,
                                columns: vec!["value".into()],
                                index_type: None
                            })
                        }
                    ]
                );
            }
            _ => panic!(),
        }

        client.teardown().await;
    }

    #[parallel_group(GROUP)]
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

        match ddl.data {
            DdlEventData::CreateTable { name, .. } => assert_eq!(name, "table"),
            _ => panic!(),
        }

        client.teardown().await;
    }

    #[parallel_group(GROUP)]
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
        assert_eq!(ddl.schema, "public");

        match ddl.data {
            DdlEventData::AlterTable(stmt) => {
                assert_eq!(stmt.table.name, "t");
                assert_eq!(
                    stmt.definitions.unwrap(),
                    vec![nom_sql::AlterTableDefinition::AddColumn(
                        ColumnSpecification::new("y".into(), SqlType::Int(None),)
                    ),]
                );
            }
            _ => panic!("Unexpected DDL event data: {:?}", ddl.data),
        }

        client.teardown().await;
    }

    #[parallel_group(GROUP)]
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
        assert_eq!(ddl.schema, "public");

        match ddl.data {
            DdlEventData::CreateView(CreateViewStatement {
                name, definition, ..
            }) => {
                assert_eq!(
                    name,
                    Relation {
                        schema: Some("public".into()),
                        name: "v".into()
                    }
                );
                match *definition.unwrap() {
                    SelectSpecification::Simple(select_stmt) => {
                        assert_eq!(
                            select_stmt.fields,
                            vec![FieldDefinitionExpr::Expr {
                                expr: Expr::Column("t.x".into()),
                                alias: None
                            }]
                        );
                        assert_eq!(
                            select_stmt.tables,
                            vec![TableExpr::from(Relation::from("t"))]
                        );
                    }
                    _ => panic!(),
                }
            }
            _ => panic!("Unexpected query type {:?}", ddl.data),
        }

        client.teardown().await;
    }

    #[parallel_group(GROUP)]
    #[tokio::test]
    async fn drop_table() {
        let client = setup("drop_table").await;

        client
            .simple_query("create table t (x int);")
            .await
            .unwrap();

        let _ = get_last_ddl(&client, "drop_table").await;

        client.simple_query("drop table t;").await.unwrap();

        let ddl = get_last_ddl(&client, "drop_table").await.unwrap();
        assert_eq!(ddl.data, DdlEventData::Drop("t".into()));

        client.teardown().await;
    }

    #[parallel_group(GROUP)]
    #[tokio::test]
    async fn create_type() {
        let client = setup("create_type").await;
        client
            .simple_query("create type abc as enum ('a', 'b', 'c')")
            .await
            .unwrap();

        let ddl = get_last_ddl(&client, "create_type").await.unwrap();

        match ddl.data {
            DdlEventData::CreateType { name, variants, .. } => {
                assert_eq!(name, "abc");
                assert_eq!(
                    variants.into_iter().map(|v| v.label).collect::<Vec<_>>(),
                    vec!["a", "b", "c"]
                );
            }
            _ => panic!(),
        }

        client.teardown().await;
    }

    #[parallel_group(GROUP)]
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

        client.teardown().await;
    }

    #[parallel_group(GROUP)]
    #[tokio::test]
    async fn alter_type_add_value_before() {
        readyset_tracing::init_test_logging();
        let client = setup("alter_type_add_value_before").await;

        client
            .simple_query("create type my_type as enum ('x', 'y');")
            .await
            .unwrap();
        get_last_ddl(&client, "alter_type_add_value_before")
            .await
            .unwrap();

        client
            .simple_query("alter type my_type add value 'xy' before 'y';")
            .await
            .unwrap();
        let ddl = get_last_ddl(&client, "alter_type_add_value_before")
            .await
            .unwrap();

        assert_eq!(ddl.schema(), "public");
        match ddl.data {
            DdlEventData::AlterType {
                name,
                variants,
                original_variants,
                ..
            } => {
                assert_eq!(name, "my_type");
                assert_eq!(
                    variants.into_iter().map(|v| v.label).collect::<Vec<_>>(),
                    vec!["x", "xy", "y"]
                );
                assert_eq!(
                    original_variants
                        .unwrap()
                        .into_iter()
                        .map(|v| v.label)
                        .collect::<Vec<_>>(),
                    vec!["x", "y"]
                );
            }
            ev => panic!("{ev:?}"),
        }

        client.teardown().await;
    }

    #[parallel_group(GROUP)]
    #[tokio::test]
    async fn drop_type() {
        let client = setup("drop_type").await;

        client
            .simple_query("create type type_to_drop as enum ('x', 'y');")
            .await
            .unwrap();
        get_last_ddl(&client, "drop_type").await.unwrap();

        client
            .simple_query("drop type type_to_drop;")
            .await
            .unwrap();
        let ddl = get_last_ddl(&client, "drop_type").await.unwrap();

        assert_eq!(ddl.schema(), "public");
        assert_eq!(ddl.data, DdlEventData::Drop("type_to_drop".into()));

        client.teardown().await;
    }

    #[parallel_group(GROUP)]
    #[tokio::test]
    async fn rename_type() {
        let client = setup("rename_type").await;

        client
            .simple_query("create type t as enum ('x', 'y')")
            .await
            .unwrap();
        get_last_ddl(&client, "rename_type").await.unwrap();

        let type_oid: u32 = client
            .query_one("select oid from pg_type where typname = 't'", &[])
            .await
            .unwrap()
            .get(0);

        client
            .simple_query("alter type t rename to t_new")
            .await
            .unwrap();

        let ddl = get_last_ddl(&client, "rename_type").await.unwrap();

        assert_eq!(ddl.schema(), "public");
        assert!(matches!(
            ddl.data,
            DdlEventData::AlterType {
                oid,
                name,
                ..
            } if name == "t_new" && oid == type_oid
        ));

        client.teardown().await;
    }
}
