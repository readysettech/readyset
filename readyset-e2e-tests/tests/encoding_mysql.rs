use itertools::Itertools;
use mysql_async::prelude::Queryable;
use pretty_assertions::assert_eq;
use readyset_client_test_helpers::{
    mysql_helpers::{self, MySQLAdapter},
    sleep, TestBuilder,
};
use test_utils::{serial, slow};

macro_rules! check_rows {
    ($my_rows:expr, $rs_rows:expr) => {
        for row in $my_rows.iter().zip($rs_rows.iter()) {
            assert_eq!(row.0, row.1, "mysql (left) differed from readyset (right)");
        }
    };
}

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
async fn test_encoding_replication_inner<I>(charset: &str, range: I)
where
    I: IntoIterator<Item = (u32, String)>,
{
    readyset_tracing::init_test_logging();

    mysql_helpers::recreate_database("encoding_test").await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some("encoding_test"));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    let create_snapshot_table = format!(
        r#"
            SET NAMES utf8mb4;
            DROP TABLE IF EXISTS encoding_snapshot;
            CREATE TABLE encoding_snapshot (
                id INT NOT NULL PRIMARY KEY,
                hex VARCHAR(255) CHARACTER SET utf8mb4,
                text VARCHAR(255) CHARACTER SET {}
            );
        "#,
        charset
    );
    upstream_conn
        .query_drop(create_snapshot_table)
        .await
        .unwrap();

    let values: Vec<_> = range.into_iter().collect();

    for chunk in values.iter().chunks(1000).into_iter() {
        let insert_values: String = chunk
            .map(|(i, h)| format!("({i}, '{h}', UNHEX('{h}'))"))
            .collect::<Vec<String>>()
            .join(",");
        upstream_conn
            .query_drop(format!(
                "INSERT INTO encoding_snapshot (id, hex, text) VALUES {insert_values}"
            ))
            .await
            .unwrap();
    }

    // Verify the data was inserted correctly
    let my_rows: Vec<(i64, String, Vec<u8>)> = upstream_conn
        .query("SELECT id, hex, text FROM encoding_snapshot ORDER BY id")
        .await
        .unwrap();

    // Test snapshot replication
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback_db("encoding_test".to_string())
        .build::<MySQLAdapter>()
        .await;

    sleep().await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    let rs_snapshot_rows: Vec<(i64, String, Vec<u8>)> = rs_conn
        .query("SELECT id, hex, text FROM encoding_snapshot ORDER BY id")
        .await
        .unwrap();

    check_rows!(my_rows, rs_snapshot_rows);

    // Test streaming replication
    let create_streaming_table = format!(
        r#"
            DROP TABLE IF EXISTS encoding_streaming;
            CREATE TABLE encoding_streaming (
                id INT NOT NULL PRIMARY KEY,
                hex VARCHAR(255) CHARACTER SET utf8mb4,
                text VARCHAR(255) CHARACTER SET {}
            );
        "#,
        charset
    );
    upstream_conn
        .query_drop(create_streaming_table)
        .await
        .unwrap();

    for chunk in values.iter().chunks(1000).into_iter() {
        let insert_values: String = chunk
            .map(|(i, h)| format!("({i}, '{h}', UNHEX('{h}'))"))
            .collect::<Vec<String>>()
            .join(",");
        upstream_conn
            .query_drop(format!(
                "INSERT INTO encoding_streaming (id, hex, text) VALUES {insert_values}"
            ))
            .await
            .unwrap();
    }

    let rs_streaming_rows: Vec<(i64, String, Vec<u8>)> = rs_conn
        .query("SELECT id, hex, text FROM encoding_streaming ORDER BY id")
        .await
        .unwrap();

    check_rows!(my_rows, rs_streaming_rows);

    shutdown_tx.shutdown().await;
}

macro_rules! test_encoding_replication {
    ($name:ident, $charset:expr, $range:expr) => {
        #[tokio::test]
        #[serial(mysql)]
        #[slow]
        async fn $name() {
            test_encoding_replication_inner($charset, $range).await;
        }
    };
}

fn format_u32s<I>(width: usize, range: I) -> impl Iterator<Item = (u32, String)>
where
    I: IntoIterator<Item = u32>,
{
    range
        .into_iter()
        .map(move |value| (value, format!("{value:0width$X}", width = width)))
}

test_encoding_replication!(ascii, "ascii", format_u32s(2, 0..=127));
test_encoding_replication!(test_latin1, "latin1", format_u32s(2, 0..=255));
test_encoding_replication!(test_utf8mb4, "utf8mb4", format_u32s(2, 0..=127));
test_encoding_replication!(test_utf8mb3, "utf8mb3", format_u32s(2, 0..=127));
