use mysql_async::prelude::Queryable;
use pretty_assertions::assert_eq;
use readyset_client_test_helpers::{
    mysql_helpers::{self, MySQLAdapter},
    sleep, TestBuilder,
};
use std::fmt::{Display, UpperHex};
use test_utils::{serial, slow};

/// At present, this tests that snapshotting and streaming replication of a varchar column with the
/// specified character set results in the same utf8 encoded version of the data in Readyset. This
/// means that we connect with relevant session variables configured for utf8 (i.e. `SET NAMES
/// utf8mb4;`) so that MySQL will convert the data to utf8 before returning it to the client. This
/// causes Readyset to return matching data iff it decodes the replicated (e.g. latin1) data to utf8
/// before storing it.
///
/// In the future, this test can be extended for other boundary interfaces, e.g. also checking that
/// `HEX(text)` returns the same values on both MySQL and Readyset; or that connecting to Readyset
/// and requesting the original character set results in it being re-encoded from utf8 to the
/// original before being returned to the client.
#[cfg(test)]
async fn test_encoding_replication_inner<T>(
    charset: &str,
    hex_len: usize,
    range: impl IntoIterator<Item = T>,
) where
    T: UpperHex + Display,
{
    readyset_tracing::init_test_logging();

    println!("starting");

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some("encoding_test"));
    mysql_helpers::recreate_database("encoding_test").await;

    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    let create_snapshot_table = format!(
        r#"
            SET NAMES utf8mb4;
            DROP TABLE IF EXISTS encoding_snapshot;
            CREATE TABLE encoding_snapshot (
                id INT NOT NULL PRIMARY KEY,
                text VARCHAR(255) CHARACTER SET {}
            );
        "#,
        charset
    );
    upstream_conn
        .query_drop(create_snapshot_table)
        .await
        .unwrap();

    println!("created table");

    // Generate all latin1 characters (0x00-0xFF); characters 128-255 aren't valid UTF-8
    let insert_values: String = range
        .into_iter()
        .map(|i| format!("({i}, UNHEX('{i:0width$X}'))", width = hex_len))
        .collect::<Vec<String>>()
        .join(",");
    upstream_conn
        .query_drop(format!(
            "INSERT INTO encoding_snapshot (id, text) VALUES {insert_values}"
        ))
        .await
        .unwrap();

    println!("inserted data");

    // Verify the data was inserted correctly
    let my_rows: Vec<(i64, Vec<u8>)> = upstream_conn
        .query("SELECT id, text FROM encoding_snapshot ORDER BY id")
        .await
        .unwrap();
    dbg!(&my_rows);

    println!("starting readyset");

    // Test snapshot replication
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback_db("encoding_test".to_string())
        .build::<MySQLAdapter>()
        .await;

    println!("started readyset");

    sleep().await;

    println!("slept");

    // Query the snapshotted table to verify all 255 characters were replicated correctly
    let mut rs_conn = mysql_async::Conn::new(mysql_async::OptsBuilder::from_opts(rs_opts))
        .await
        .unwrap();

    println!("connected");

    let rs_snapshot_rows: Vec<(i64, Vec<u8>)> = rs_conn
        .query("SELECT id, text FROM encoding_snapshot ORDER BY id")
        .await
        .unwrap();

    dbg!(&rs_snapshot_rows);

    assert_eq!(my_rows, rs_snapshot_rows);

    // Test streaming replication
    let create_streaming_table = format!(
        r#"
            DROP TABLE IF EXISTS encoding_streaming;
            CREATE TABLE encoding_streaming (
                id INT NOT NULL PRIMARY KEY,
                text VARCHAR(255) CHARACTER SET {}
            );
        "#,
        charset
    );
    upstream_conn
        .query_drop(create_streaming_table)
        .await
        .unwrap();

    println!("created streaming table");

    upstream_conn
        .query_drop(format!(
            "INSERT INTO encoding_streaming (id, text) VALUES {insert_values}"
        ))
        .await
        .unwrap();

    println!("inserted streaming data");

    sleep().await;

    println!("slept");

    let rs_streaming_rows: Vec<(i64, Vec<u8>)> = rs_conn
        .query("SELECT id, text FROM encoding_streaming ORDER BY id")
        .await
        .unwrap();

    dbg!(&rs_streaming_rows);

    assert_eq!(my_rows, rs_streaming_rows);

    println!("shutting down");

    shutdown_tx.shutdown().await;

    println!("shutdown complete");
}

macro_rules! test_encoding_replication {
    ($name:ident, $charset:expr, $hex_len:expr, $range:expr) => {
        #[tokio::test]
        #[serial(mysql)]
        #[slow]
        async fn $name() {
            test_encoding_replication_inner($charset, $hex_len, $range).await;
        }
    };
}

test_encoding_replication!(ascii, "ascii", 2, 0..=127);
test_encoding_replication!(test_latin1, "latin1", 2, 0..=255);
test_encoding_replication!(test_utf8mb4, "utf8mb4", 2, 0..=127);
test_encoding_replication!(test_utf8mb3, "utf8mb3", 2, 0..=127);
