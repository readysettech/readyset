use itertools::Itertools;
use mysql_async::prelude::Queryable;
use pretty_assertions::assert_eq;
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter},
};
use readyset_util::eventually;
use std::time::Duration;
use test_utils::tags;

macro_rules! check_rows {
    ($my_rows:expr_2021, $rs_rows:expr_2021, $($format_args:tt)*) => {
        for row in $my_rows.iter().zip($rs_rows.iter()) {
            assert_eq!(row.0, row.1, $($format_args)*);
        }
    };
}

const CHUNK_SIZE: usize = 1000;
const CHARACTER_SETS: [&str; 4] = ["latin1", "cp850", "utf8mb3", "utf8mb4"];

/// Tests snapshotting replication of a varchar column with the specified character set.
/// Verifies that the same utf8 encoded version of the data is stored in Readyset.
/// Also tests that updates and deletes work correctly without a primary key.
#[cfg(test)]
async fn test_snapshot_encoding<I>(test_name: &str, column_type: &str, collation: &str, range: I)
where
    I: IntoIterator<Item = (u32, String)>,
{
    readyset_tracing::init_test_logging();
    let db_name = format!("encoding_snapshot_{test_name}");
    mysql_helpers::recreate_database(&db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let values: Vec<_> = range.into_iter().collect();

    let collation_clause = if collation.is_empty() {
        String::new()
    } else {
        format!(" COLLATE '{collation}'")
    };

    let create_table = format!(
        r#"
            SET NAMES utf8mb4;
            DROP TABLE IF EXISTS encoding_table;
            CREATE TABLE encoding_table (
                id INT NOT NULL,
                hex VARCHAR(255) CHARACTER SET utf8mb4,
                text {column_type} {collation_clause},
                counter INT NOT NULL DEFAULT 0
            );
        "#
    );
    upstream_conn.query_drop(create_table).await.unwrap();

    for chunk in values.iter().chunks(CHUNK_SIZE).into_iter() {
        let insert_values: String = chunk
            .map(|(i, h)| format!("({i}, '{h}', UNHEX('{h}'), 0)"))
            .collect::<Vec<String>>()
            .join(",");
        upstream_conn
            .query_drop(format!(
                "INSERT INTO encoding_table (id, hex, text, counter) VALUES {insert_values}"
            ))
            .await
            .unwrap();
    }

    // Verify the data was inserted correctly
    let mut my_rows: Vec<(i64, String, Vec<u8>, i32)> = upstream_conn
        .query("SELECT id, hex, text, counter FROM encoding_table")
        .await
        .unwrap();

    my_rows.sort();

    // Test snapshot replication
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.clone())
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Smoke test to ensure snapshotting has finished
    eventually!(attempts: 5, sleep: Duration::from_secs(5), {
        let count: usize = rs_conn
            .query_first("SELECT count(*) FROM encoding_table")
            .await
            .unwrap()
            .unwrap();
        my_rows.len() == count
    });

    let mut rs_snapshot_rows: Vec<(i64, String, Vec<u8>, i32)> = rs_conn
        .query("SELECT id, hex, text, counter FROM encoding_table")
        .await
        .unwrap();

    rs_snapshot_rows.sort();

    check_rows!(
        my_rows,
        rs_snapshot_rows,
        "mysql (left) differed from readyset (right) for snapshot replication; column type {column_type}, collation {collation}"
    );

    // Test updating rows to verify encoding consistency in chunks
    for chunk in values.iter().chunks(CHUNK_SIZE).into_iter() {
        let chunk: Vec<_> = chunk.collect();
        let first_id = chunk.first().unwrap().0;
        let last_id = chunk.last().unwrap().0;

        // Update this chunk
        upstream_conn
            .exec_drop(
                "UPDATE encoding_table SET counter = 1 WHERE id >= ? AND id <= ?",
                (first_id, last_id),
            )
            .await
            .unwrap();

        // Wait for updates to propagate
        eventually!(
            sleep: Duration::from_millis(50),
            message: format!("snapshot update: waiting for updates to rows {first_id}-{last_id} to propagate"),
            {
                let updated_count: usize = rs_conn
                    .exec_first(
                        "SELECT COUNT(*) FROM encoding_table WHERE counter = 1 AND id >= ? AND id <= ?",
                        (first_id, last_id)
                    )
                    .await
                    .unwrap()
                    .unwrap_or(0);

                updated_count == chunk.len()
            }
        );

        // Verify this chunk after updates in all supported character sets
        for character_set in CHARACTER_SETS {
            upstream_conn
                .query_drop(format!(
                    "SET @@session.character_set_results = {character_set}"
                ))
                .await
                .unwrap();

            let mut my_chunk: Vec<(i64, String, Vec<u8>, i32)> = upstream_conn
                .exec(
                    "SELECT id, hex, text, counter FROM encoding_table WHERE id >= ? AND id <= ?",
                    (first_id, last_id),
                )
                .await
                .unwrap();
            my_chunk.sort();

            rs_conn
                .query_drop(format!(
                    "SET @@session.character_set_results = {character_set}"
                ))
                .await
                .unwrap();

            let mut rs_chunk: Vec<(i64, String, Vec<u8>, i32)> = rs_conn
                .exec(
                    "SELECT id, hex, text, counter FROM encoding_table WHERE id >= ? AND id <= ?",
                    (first_id, last_id),
                )
                .await
                .unwrap();
            rs_chunk.sort();

            check_rows!(
                my_chunk,
                rs_chunk,
                "mysql (left) differed from readyset (right) after updates for snapshot update chunk {first_id}-{last_id} with character set {character_set}",
            );
        }
    }

    shutdown_tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

/// Tests streaming replication of a varchar column with the specified character set.
/// Verifies that the same utf8 encoded version of the data is stored in Readyset.
/// Also tests that updates and deletes work correctly without a primary key.
#[cfg(test)]
async fn test_streaming_encoding<I>(test_name: &str, column_type: &str, collation: &str, range: I)
where
    I: IntoIterator<Item = (u32, String)>,
{
    readyset_tracing::init_test_logging();
    let db_name = format!("encoding_streaming_{test_name}",);
    mysql_helpers::recreate_database(&db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let values: Vec<_> = range.into_iter().collect();

    // Test streaming replication
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.clone())
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    let collation_clause = if collation.is_empty() {
        String::new()
    } else {
        format!(" COLLATE '{collation}'")
    };

    let create_table = format!(
        r#"
            SET NAMES utf8mb4;
            DROP TABLE IF EXISTS encoding_table;
            CREATE TABLE encoding_table (
                id INT NOT NULL,
                hex VARCHAR(255) CHARACTER SET utf8mb4,
                text {column_type} {collation_clause}
            );
        "#
    );
    upstream_conn.query_drop(create_table).await.unwrap();

    for chunk in values.iter().chunks(CHUNK_SIZE).into_iter() {
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
                "INSERT INTO encoding_table (id, hex, text) VALUES {insert_values}"
            ))
            .await
            .unwrap();

        // Smoke test to ensure streaming replication has caught up
        eventually!(sleep: Duration::from_millis(50), {
            let count: usize = rs_conn
                .exec_first("SELECT count(*) FROM encoding_table WHERE id >= ? AND id <= ?", (first_id, last_id))
                .await
                .unwrap()
                .unwrap();
            count == chunk.len()
        });

        for character_set in CHARACTER_SETS {
            upstream_conn
                .query_drop(format!(
                    "SET @@session.character_set_results = {character_set}"
                ))
                .await
                .unwrap();

            let mut my_chunk: Vec<(i64, String, Vec<u8>)> = upstream_conn
                .exec(
                    "SELECT id, hex, text FROM encoding_table WHERE id >= ? AND id <= ?",
                    (first_id, last_id),
                )
                .await
                .unwrap();
            my_chunk.sort();

            rs_conn
                .query_drop(format!(
                    "SET @@session.character_set_results = {character_set}"
                ))
                .await
                .unwrap();

            let mut rs_chunk: Vec<(i64, String, Vec<u8>)> = rs_conn
                .exec(
                    "SELECT id, hex, text FROM encoding_table WHERE id >= ? AND id <= ?",
                    (first_id, last_id),
                )
                .await
                .unwrap();
            rs_chunk.sort();

            check_rows!(
                my_chunk,
                rs_chunk,
                "mysql (left) differed from readyset (right) for streaming replication chunk {first_id}-{last_id} with character set {character_set}",
            );
        }
    }

    shutdown_tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

macro_rules! test_encoding_replication {
    ($name:ident, $coltype:expr_2021, $charset:expr_2021, $range:expr_2021) => {
        paste::paste! {
            #[tokio::test]
            #[tags(serial, slow, mysql_upstream)]
            async fn [<test_ $name _snapshot>]() {
                test_snapshot_encoding(stringify!($name), $coltype, $charset, $range).await;
            }

            #[tokio::test]
            #[tags(serial, slow, mysql_upstream)]
            async fn [<test_ $name _streaming>]() {
                test_streaming_encoding(stringify!($name), $coltype, $charset, $range).await;
            }
        }
    };
}

fn format_u32s<I>(width: usize, range: I) -> impl Iterator<Item = (u32, String)>
where
    I: IntoIterator<Item = u32>,
{
    range
        .into_iter()
        .map(move |value| (value, format!("{value:0width$X}")))
}

test_encoding_replication!(
    ascii_general_ci_varchar,
    "VARCHAR(255)",
    "ascii_general_ci",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    ascii_general_ci_char,
    "CHAR(10)",
    "ascii_general_ci",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    ascii_general_ci_text,
    "TEXT",
    "ascii_general_ci",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    ascii_bin_varchar,
    "VARCHAR(255)",
    "ascii_bin",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    ascii_bin_char,
    "CHAR(10)",
    "ascii_bin",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(ascii_bin_text, "TEXT", "ascii_bin", format_u32s(2, 0..=127));
test_encoding_replication!(
    latin1_german1_ci_varchar,
    "VARCHAR(255)",
    "latin1_german1_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_german1_ci_char,
    "CHAR(10)",
    "latin1_german1_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_german1_ci_text,
    "TEXT",
    "latin1_german1_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_swedish_ci_varchar,
    "VARCHAR(255)",
    "latin1_swedish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_swedish_ci_char,
    "CHAR(10)",
    "latin1_swedish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_swedish_ci_text,
    "TEXT",
    "latin1_swedish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_danish_ci_varchar,
    "VARCHAR(255)",
    "latin1_danish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_danish_ci_char,
    "CHAR(10)",
    "latin1_danish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_danish_ci_text,
    "TEXT",
    "latin1_danish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_german2_ci_varchar,
    "VARCHAR(255)",
    "latin1_german2_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_german2_ci_char,
    "CHAR(10)",
    "latin1_german2_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_german2_ci_text,
    "TEXT",
    "latin1_german2_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_bin_varchar,
    "VARCHAR(255)",
    "latin1_bin",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_bin_char,
    "CHAR(10)",
    "latin1_bin",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_bin_text,
    "TEXT",
    "latin1_bin",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_general_ci_varchar,
    "VARCHAR(255)",
    "latin1_general_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_general_ci_char,
    "CHAR(10)",
    "latin1_general_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_general_ci_text,
    "TEXT",
    "latin1_general_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_general_cs_varchar,
    "VARCHAR(255)",
    "latin1_general_cs",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_general_cs_char,
    "CHAR(10)",
    "latin1_general_cs",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_general_cs_text,
    "TEXT",
    "latin1_general_cs",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_spanish_ci_varchar,
    "VARCHAR(255)",
    "latin1_spanish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_spanish_ci_char,
    "CHAR(10)",
    "latin1_spanish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    latin1_spanish_ci_text,
    "TEXT",
    "latin1_spanish_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    cp850_general_ci_varchar,
    "VARCHAR(255)",
    "cp850_general_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    cp850_general_ci_char,
    "CHAR(10)",
    "cp850_general_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    cp850_general_ci_text,
    "TEXT",
    "cp850_general_ci",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    utf8mb4_bin_ascii_varchar,
    "VARCHAR(255)",
    "utf8mb4_bin",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    utf8mb4_bin_ascii_char,
    "CHAR(10)",
    "utf8mb4_bin",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    utf8mb4_bin_ascii_text,
    "TEXT",
    "utf8mb4_bin",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    utf8mb3_bin_ascii_varchar,
    "VARCHAR(255)",
    "utf8mb3_bin",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    utf8mb3_bin_ascii_char,
    "CHAR(10)",
    "utf8mb3_bin",
    format_u32s(2, 0..=127)
);
test_encoding_replication!(
    utf8mb3_bin_ascii_text,
    "TEXT",
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
    utf8mb3_bmp_codepoints_varchar,
    "VARCHAR(255)",
    "utf8mb3_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
test_encoding_replication!(
    utf8mb3_bmp_codepoints_char,
    "CHAR(10)",
    "utf8mb3_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
test_encoding_replication!(
    utf8mb3_bmp_codepoints_text,
    "TEXT",
    "utf8mb3_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);

test_encoding_replication!(
    utf8mb4_bmp_codepoints_varchar,
    "VARCHAR(255)",
    "utf8mb4_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
test_encoding_replication!(
    utf8mb4_bmp_codepoints_char,
    "CHAR(10)",
    "utf8mb4_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
test_encoding_replication!(
    utf8mb4_bmp_codepoints_text,
    "TEXT",
    "utf8mb4_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
// These tests for the *entire* range are excessively long and not that valuable. We could replace
// these with a proptest, add a separate CI pipeline, or else just run these manually as needed.
#[cfg(feature = "utf8mb4_all_codepoints_test")]
test_encoding_replication!(
    utf8mb4_all_codepoints_varchar,
    "VARCHAR(255)",
    "utf8mb4_general_ci",
    format_utf8_chars(char::MIN..=char::MAX)
);
#[cfg(feature = "utf8mb4_all_codepoints_test")]
test_encoding_replication!(
    utf8mb4_all_codepoints_char,
    "CHAR(10)",
    "utf8mb4_general_ci",
    format_utf8_chars(char::MIN..=char::MAX)
);
#[cfg(feature = "utf8mb4_all_codepoints_test")]
test_encoding_replication!(
    utf8mb4_all_codepoints_text,
    "TEXT",
    "utf8mb4_general_ci",
    format_utf8_chars(char::MIN..=char::MAX)
);

// Doesn't really do any encoding, obviously, but protects against mistakes in the conversion
// codepaths where blob and binary string column types overlap with text column types.
test_encoding_replication!(blob, "BLOB", "binary", format_u32s(2, 0..=255));
test_encoding_replication!(blob_no_collate, "BLOB", "", format_u32s(2, 0..=255));
test_encoding_replication!(binary, "BINARY", "binary", format_u32s(2, 0..=255));
test_encoding_replication!(binary_no_collate, "BINARY", "", format_u32s(2, 0..=255));
test_encoding_replication!(
    binary_padded,
    "BINARY(10)",
    "binary",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    binary_padded_no_collate,
    "BINARY(10)",
    "",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    varbinary,
    "VARBINARY(255)",
    "binary",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    varbinary_no_collate,
    "VARBINARY(255)",
    "",
    format_u32s(2, 0..=255)
);
test_encoding_replication!(
    char_binary_padded,
    "CHAR(10)",
    "binary",
    format_u32s(2, 0..=255)
);
