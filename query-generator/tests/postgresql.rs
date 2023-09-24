use std::env;

use nom_sql::{CreateTableStatement, Dialect, DialectDisplay};
use query_generator::{GeneratorState, QueryDialect, QuerySeed};
use test_strategy::proptest;
use tokio_postgres::NoTls;

fn config() -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::new();
    config
        .host(env::var("PGHOST").as_deref().unwrap_or("localhost"))
        .port(
            env::var("PGPORT")
                .unwrap_or_else(|_| "5432".into())
                .parse()
                .unwrap(),
        )
        .user(env::var("PGUSER").as_deref().unwrap_or("postgres"))
        .password(env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into()))
        .dbname(env::var("PGDATABASE").as_deref().unwrap_or("noria"));
    config
}

async fn recreate_test_database(mut config: tokio_postgres::Config) {
    let dbname = config.get_dbname().unwrap().to_owned();
    let (admin_client, admin_conn) = config.dbname("postgres").connect(NoTls).await.unwrap();
    tokio::spawn(admin_conn);
    admin_client
        .simple_query(&format!("DROP DATABASE IF EXISTS {dbname};"))
        .await
        .unwrap();
    admin_client
        .simple_query(&format!("CREATE DATABASE {dbname};"))
        .await
        .unwrap();
}

#[proptest]
#[ignore = "Currently failing"]
fn queries_work_in_postgresql(#[any(dialect = QueryDialect(Dialect::PostgreSQL))] seed: QuerySeed) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let config = config();
        recreate_test_database(config.clone()).await;
        let (client, conn) = config.connect(NoTls).await.unwrap();
        tokio::spawn(conn);

        let mut gen = GeneratorState::default();
        let query = gen.generate_query(seed);
        let statement = query.statement.display(Dialect::PostgreSQL).to_string();
        let params = query.state.key();

        eprintln!("==========");
        for table in gen.tables().values() {
            if table.columns.is_empty() {
                continue;
            }
            let create_table = CreateTableStatement::from(table.clone())
                .display(Dialect::PostgreSQL)
                .to_string();
            eprintln!("Table: {create_table}");
            client.simple_query(&create_table).await.unwrap();
        }
        eprintln!("Query: {statement}\nParams: {params:?}");

        let res = client.prepare(&statement).await;
        match res {
            Ok(stmt) => client
                .execute(&stmt, &params.iter().map(|p| p as _).collect::<Vec<_>>())
                .await
                .unwrap(),
            Err(e) => panic!("Error preparing query against postgresql: {e}"),
        };
    })
}
