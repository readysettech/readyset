use itertools::Itertools;
use mysql_async::prelude::Queryable;
use pretty_assertions::assert_eq;
use readyset_client_test_helpers::{
    mysql_helpers::{self, MySQLAdapter},
    TestBuilder,
};
use readyset_util::eventually;
use std::time::Duration;
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
async fn test_encoding_replication_inner<I>(collation: &str, range: I)
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
                text VARCHAR(255) COLLATE {}
            );
        "#,
        collation
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
        .replicate_db("encoding_test".to_string())
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Smoke test to ensure snapshotting has finished
    eventually!(attempts: 5, sleep: Duration::from_secs(5), {
        let count: usize = rs_conn
            .query_first("SELECT count(*) FROM encoding_snapshot")
            .await
            .unwrap()
            .unwrap();
        my_rows.len() == count
    });

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
                text VARCHAR(255) COLLATE {}
            );
        "#,
        collation
    );
    upstream_conn
        .query_drop(create_streaming_table)
        .await
        .unwrap();

    for chunk in values.iter().chunks(1000).into_iter() {
        let chunk: Vec<_> = chunk.collect();
        let first_id = chunk.first().unwrap().0;
        let last_id = chunk.last().unwrap().0;
        let insert_values: String = chunk
            .iter()
            .map(|(i, h)| format!("({i}, '{h}', UNHEX('{h}'))"))
            .collect::<Vec<String>>()
            .join(",");
        upstream_conn
            .query_drop(format!(
                "INSERT INTO encoding_streaming (id, hex, text) VALUES {insert_values}"
            ))
            .await
            .unwrap();

        // Smoke test to ensure streaming replication has caught up
        eventually!(sleep: Duration::from_millis(50), {
            let count: usize = rs_conn
                .exec_first("SELECT count(*) FROM encoding_streaming WHERE id >= ? AND id <= ?", (first_id, last_id))
                .await
                .unwrap()
                .unwrap();
            count == chunk.len()
        });

        let my_streaming_rows_chunk: Vec<(i64, String, Vec<u8>)> = upstream_conn
            .exec("SELECT id, hex, text FROM encoding_streaming WHERE id >= ? AND id <= ? ORDER BY id", (first_id, last_id))
            .await
            .unwrap();

        let rs_streaming_rows_chunk: Vec<(i64, String, Vec<u8>)> = rs_conn
            .exec("SELECT id, hex, text FROM encoding_streaming WHERE id >= ? AND id <= ? ORDER BY id", (first_id, last_id))
            .await
            .unwrap();

        check_rows!(my_streaming_rows_chunk, rs_streaming_rows_chunk);
    }

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

test_encoding_replication!(
    test_ascii_general_ci,
    "ascii_general_ci",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(test_ascii_bin, "ascii_bin", format_u32s(2, 0..=127));
test_encoding_replication!(
    test_latin1_german1_ci,
    "latin1_german1_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    test_latin1_swedish_ci,
    "latin1_swedish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    test_latin1_danish_ci,
    "latin1_danish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    test_latin1_german2_ci,
    "latin1_german2_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(test_latin1_bin, "latin1_bin", format_u32s(2, 0..=255));
test_encoding_replication!(
    test_latin1_general_ci,
    "latin1_general_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    test_latin1_general_cs,
    "latin1_general_cs",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    test_latin1_spanish_ci,
    "latin1_spanish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    test_utf8mb4_bin_ascii,
    "utf8mb4_bin",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    test_utf8mb3_bin_ascii,
    "utf8mb3_bin",
    format_u32s(2, 0..=127)
);

fn format_utf8_chars<I>(range: I) -> impl Iterator<Item = (u32, String)>
where
    I: IntoIterator<Item = char>,
{
    range.into_iter().map(|c| {
        let mut utf8 = vec![0; c.len_utf8()];
        c.encode_utf8(&mut utf8);
        let mut out = String::new();
        for byte in &utf8 {
            out.push_str(&format!("{byte:02X}"));
        }
        (c as u32, out)
    })
}

test_encoding_replication!(
    test_utf8mb4_all_codepoints,
    "utf8mb4_general_ci",
    format_utf8_chars(char::MIN..=char::MAX)
);
test_encoding_replication!(
    test_utf8mb3_all_codepoints,
    "utf8mb3_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
