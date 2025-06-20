use mysql_async::consts::MAX_PAYLOAD_LEN;
use mysql_async::prelude::Queryable;
use mysql_common::Value;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::TestBuilder;
use test_utils::tags;

#[tags(serial, slow, mysql8_upstream)]
#[test]
fn mysql_send_long_data() {
    readyset_tracing::init_test_logging();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(mysql_send_long_data_inner())
}

async fn mysql_send_long_data_inner() {
    let (rs_opts, _rs_handle, shutdown_tx) = TestBuilder::default()
        .replicate_db("mysql_long_data".to_string())
        .fallback(true)
        .migration_mode(readyset_adapter::backend::MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    rs_conn
        .query_drop("CREATE TABLE t (x LONGBLOB)")
        .await
        .unwrap();
    rs_conn
        .exec_drop(
            "INSERT INTO t VALUES (?)",
            vec![Value::Bytes(Vec::from_iter(std::iter::repeat_n(
                0,
                MAX_PAYLOAD_LEN * 2,
            )))],
        )
        .await
        .unwrap();
    let row: (usize,) = rs_conn
        .query_first("SELECT length(x) FROM t")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(row.0, MAX_PAYLOAD_LEN * 2);

    shutdown_tx.shutdown().await;
}
