use std::env;

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Params};
use nom_sql::{CreateTableStatement, Dialect};
use query_generator::{GeneratorState, QuerySeed};
use serial_test::serial;
use test_strategy::proptest;

async fn mysql_connection() -> mysql_async::Conn {
    let db_name = env::var("MYSQL_DB").unwrap_or_else(|_| "test".to_owned());
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()))
        .user(Some(
            env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_owned()),
        ))
        .pass(Some(
            env::var("MYSQL_PWD").unwrap_or_else(|_| "noria".to_owned()),
        ))
        .db_name(Some(db_name.clone()));
    let admin_url = OptsBuilder::from_opts(opts.clone()).db_name::<String>(None);
    let mut admin_conn = mysql_async::Conn::new(admin_url).await.unwrap();

    admin_conn
        .query_drop(format!("DROP DATABASE IF EXISTS {db_name}"))
        .await
        .unwrap();

    admin_conn
        .query_drop(format!(
            "CREATE DATABASE {db_name} CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_0900_bin'"
        ))
        .await
        .unwrap();

    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    conn.query_drop("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_bin'")
        .await
        .unwrap();

    conn
}

#[proptest]
#[serial]
#[ignore = "Currently failing"]
fn queries_work_in_mysql(seed: QuerySeed) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let mut conn = mysql_connection().await;
        let mut gen = GeneratorState::default();
        let query = gen.generate_query(seed);
        let statement = query.statement.display(Dialect::MySQL).to_string();
        let params = query
            .state
            .key()
            .into_iter()
            .map(|v| v.try_into().unwrap())
            .collect::<Vec<_>>();
        let params = if params.is_empty() {
            Params::Empty
        } else {
            Params::Positional(params)
        };

        eprintln!("==========");
        for table in gen.tables().values() {
            if table.columns.is_empty() {
                continue;
            }
            let create_table = CreateTableStatement::from(table.clone())
                .display(Dialect::MySQL)
                .to_string();
            eprintln!("Table: {create_table}");
            conn.query_drop(create_table).await.unwrap()
        }
        eprintln!("Query: {statement}\nParams: {params:?}");

        let res = conn.prep(statement).await;
        match res {
            Ok(stmt) => conn.exec_drop(stmt, params).await.unwrap(),
            // Err(mysql_async::Error::Driver(DriverError::MixedParams)) => {}
            Err(e) => panic!("Error preparing query against mysql: {e}"),
        };
    });
}
