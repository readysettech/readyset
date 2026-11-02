//! Checks that `LIMIT ? OFFSET ?` gives the same rows, or the same error, through Readyset as on
//! MySQL, for counts of each type a client can bind: integers at both ends of the range, floats,
//! strings, negatives and NULL.

use std::assert_matches;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Error, Row, Value};
use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, derive_test_name, sleep};
use test_utils::{tags, upstream};

const SQL: &str = "SELECT v FROM t ORDER BY id LIMIT ? OFFSET ?";

/// Runs the query and returns its rows, or the MySQL error code if it fails.
async fn read(conn: &mut Conn, limit: &Value, offset: &Value) -> Result<Vec<i32>, u16> {
    match conn
        .exec::<Row, _, _>(SQL, vec![limit.clone(), offset.clone()])
        .await
    {
        Ok(rows) => Ok(rows.iter().map(|row| row.get(0).unwrap()).collect()),
        Err(Error::Server(err)) => Err(err.code),
        Err(err) => panic!("{err}"),
    }
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_count_reads_as_mysql_reads_it() {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    mysql_helpers::recreate_database(&db).await;
    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some(&db)))
        .await
        .unwrap();
    upstream
        .query_drop("CREATE TABLE t (id int PRIMARY KEY, v int)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .await
        .unwrap();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(&db)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = Conn::new(opts).await.unwrap();
    readyset
        .query_drop(format!("CREATE CACHE FROM {SQL}"))
        .await
        .unwrap();
    sleep().await;

    for (limit, offset) in [
        (Value::Int(2), Value::Int(1)),
        (Value::Double(2.5), Value::Int(0)),
        (Value::from("2"), Value::Int(0)),
        (Value::Int(2), Value::from("1")),
        (Value::NULL, Value::Int(0)),
        (Value::Int(2), Value::NULL),
        (Value::Int(-1), Value::Int(0)),
        (Value::Int(i64::MIN), Value::Int(0)),
        (Value::Int(i64::MAX), Value::Int(1)),
        (Value::UInt(u64::MAX), Value::Int(0)),
        (Value::Int(2), Value::UInt(u64::MAX)),
        (Value::Int(0), Value::Int(0)),
    ] {
        assert_eq!(
            read(&mut readyset, &limit, &offset).await,
            read(&mut upstream, &limit, &offset).await,
            "LIMIT {limit:?} OFFSET {offset:?}"
        );
    }
    assert_eq!(
        read(&mut readyset, &Value::Int(2), &Value::Int(1)).await,
        Ok(vec![20, 30])
    );
    assert_matches!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Readyset(_)
    );

    shutdown_tx.shutdown().await;
}
