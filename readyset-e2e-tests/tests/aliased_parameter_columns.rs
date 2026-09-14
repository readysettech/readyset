//! A parameter on a join key projected only under an alias is typed from the key it looks up,
//! and the statement is served.

use std::panic::AssertUnwindSafe;

use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_tracing::init_test_logging;
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Client;

type Param<'a> = &'a (dyn ToSql + Sync);

const QUERY: &str = "SELECT users.name AS who, users.id AS uid FROM orders \
                     INNER JOIN users ON orders.user_id = users.id \
                     WHERE users.name = $1 AND users.id = $2";

/// The LIMIT is the client's first parameter and the statement's last.
const LIMIT_FIRST: &str = "SELECT users.name AS who, users.id AS uid FROM orders \
                           INNER JOIN users ON orders.user_id = users.id \
                           WHERE users.name = $2 ORDER BY users.id LIMIT $1";

async fn prepare_and_query(
    rs: &Client,
    query: &str,
    params: &[Param<'_>],
) -> Result<(Vec<Type>, Vec<(String, i32)>), tokio_postgres::Error> {
    let statement = rs.prepare(query).await?;
    let rows = rs.query(&statement, params).await?;
    let rows = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    Ok((statement.params().to_vec(), rows))
}

#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn aliased_parameter_columns_postgres() {
    init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default().build::<PostgreSQLAdapter>().await;
    let rs = psql_helpers::connect(rs_opts).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream = psql_helpers::connect(upstream_config).await;
    for statement in [
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT)",
        "INSERT INTO users VALUES (1, 'ann'), (2, 'bob'), (3, 'cat')",
        "INSERT INTO orders VALUES (10, 1, 5), (11, 2, 7), (12, 2, 9), (13, 3, 4)",
    ] {
        upstream.simple_query(statement).await.unwrap();
    }

    let bob = || ("bob".to_owned(), 2);
    let cases = [
        (QUERY, [&"bob" as Param, &2i32], [Type::TEXT, Type::INT4], vec![bob(), bob()]),
        (LIMIT_FIRST, [&1i64, &"bob"], [Type::INT8, Type::TEXT], vec![bob()]),
    ];
    for (query, params, types, rows) in cases {
        eventually!(run_test: {
            let result = prepare_and_query(&rs, query, &params).await;
            AssertUnwindSafe(move || result)
        }, then_assert: |result| {
            assert_eq!(result().unwrap(), (types.to_vec(), rows.clone()));
        });
    }

    shutdown_tx.shutdown().await;
}
