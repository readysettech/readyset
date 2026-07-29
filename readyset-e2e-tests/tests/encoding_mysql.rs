use itertools::Itertools;
use mysql_async::consts::Command;
use mysql_async::prelude::Queryable;
use pretty_assertions::assert_eq;
use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter, last_query_info},
};
use std::panic::AssertUnwindSafe;
use readyset_util::eventually;
use std::time::Duration;
use test_utils::{tags, upstream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
        .replicate_db(&db_name)
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
        .replicate_db(&db_name)
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
        eventually!(
            sleep: Duration::from_millis(50),
            run_test: {
                match rs_conn
                    .exec_first::<usize, _, _>(
                        "SELECT count(*) FROM encoding_table WHERE id >= ? AND id <= ?",
                        (first_id, last_id),
                    )
                    .await
                {
                    Ok(Some(count)) => Ok(count),
                    Ok(None) => Ok(0),
                    Err(mysql_async::Error::Server(ref e))
                        if e.message.contains("Schema generation mismatch") =>
                    {
                        Ok(0)
                    }
                    Err(e) => Err(e.to_string()),
                }
            },
            then_assert: |res| {
                let res: Result<usize, String> = res;
                match res {
                    Ok(count) => assert_eq!(count, chunk.len(), "streaming catch-up count mismatch"),
                    Err(msg) => panic!("Unexpected error while waiting for streaming catch-up: {msg}"),
                }
            }
        );

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
            #[tags(serial, slow)]
            #[upstream(mysql)]
            async fn [<test_ $name _snapshot>]() {
                test_snapshot_encoding(stringify!($name), $coltype, $charset, $range).await;
            }

            #[tokio::test]
            #[tags(serial, slow)]
            #[upstream(mysql)]
            async fn [<test_ $name _streaming>]() {
                test_streaming_encoding(stringify!($name), $coltype, $charset, $range).await;
            }
        }
    };
}

macro_rules! test_encoding_replication_very_slow {
    ($name:ident, $coltype:expr_2021, $charset:expr_2021, $range:expr_2021) => {
        paste::paste! {
            #[tokio::test]
            #[tags(serial, very_slow)]
            #[upstream(mysql)]
            async fn [<test_ $name _snapshot>]() {
                test_snapshot_encoding(stringify!($name), $coltype, $charset, $range).await;
            }

            #[tokio::test]
            #[tags(serial, very_slow)]
            #[upstream(mysql)]
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

test_encoding_replication_very_slow!(
    utf8mb3_bmp_codepoints_varchar,
    "VARCHAR(255)",
    "utf8mb3_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
test_encoding_replication_very_slow!(
    utf8mb3_bmp_codepoints_char,
    "CHAR(10)",
    "utf8mb3_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
test_encoding_replication_very_slow!(
    utf8mb3_bmp_codepoints_text,
    "TEXT",
    "utf8mb3_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);

test_encoding_replication_very_slow!(
    utf8mb4_bmp_codepoints_varchar,
    "VARCHAR(255)",
    "utf8mb4_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
test_encoding_replication_very_slow!(
    utf8mb4_bmp_codepoints_char,
    "CHAR(10)",
    "utf8mb4_general_ci",
    format_utf8_chars((char::MIN..=char::MAX).filter(|c| c.len_utf8() <= 3))
);
test_encoding_replication_very_slow!(
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

/// A minimal raw MySQL client for exercising handshake charsets mysql_async cannot negotiate (it
/// always sends utf8mb4) and statements containing non-UTF-8 bytes.
struct RawConn {
    stream: TcpStream,
}

impl RawConn {
    async fn read_packet(&mut self) -> (u8, Vec<u8>) {
        let mut header = [0u8; 4];
        self.stream.read_exact(&mut header).await.unwrap();
        let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).await.unwrap();
        (header[3], payload)
    }

    async fn write_packet(&mut self, seq: u8, payload: &[u8]) {
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes()[..3]);
        buf.push(seq);
        buf.extend_from_slice(payload);
        self.stream.write_all(&buf).await.unwrap();
    }

    /// Connect and complete a handshake advertising the given charset (a collation id).
    async fn connect_with_charset(opts: &mysql_async::Opts, charset: u8) -> Self {
        const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
        const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
        const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
        const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

        let stream = TcpStream::connect((opts.ip_or_hostname(), opts.tcp_port()))
            .await
            .unwrap();
        let mut conn = RawConn { stream };
        let (seq, _server_handshake) = conn.read_packet().await;

        let mut capabilities =
            CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        if opts.db_name().is_some() {
            capabilities |= CLIENT_CONNECT_WITH_DB;
        }
        let mut payload = Vec::new();
        payload.extend_from_slice(&capabilities.to_le_bytes());
        payload.extend_from_slice(&(16u32 << 20).to_le_bytes()); // max packet size
        payload.push(charset);
        payload.extend_from_slice(&[0u8; 23]);
        payload.extend_from_slice(opts.user().unwrap_or("root").as_bytes());
        payload.push(0);
        // Authentication is disabled in the test harness; any non-empty scramble is accepted.
        payload.push(20);
        payload.extend_from_slice(&[1u8; 20]);
        if let Some(db) = opts.db_name() {
            payload.extend_from_slice(db.as_bytes());
            payload.push(0);
        }
        payload.extend_from_slice(b"mysql_native_password\0");
        conn.write_packet(seq + 1, &payload).await;

        let (_, response) = conn.read_packet().await;
        assert_eq!(
            response.first(),
            Some(&0x00),
            "handshake should succeed: {response:?}"
        );
        conn
    }

    /// Send a COM_QUERY with the given raw statement bytes and return the column names from the
    /// result-set metadata along with the raw payloads of any row packets (both empty for an OK
    /// response).
    async fn query_with_metadata(&mut self, statement: &[u8]) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let mut payload = Vec::with_capacity(statement.len() + 1);
        payload.push(0x03); // COM_QUERY
        payload.extend_from_slice(statement);
        self.write_packet(0, &payload).await;

        let (_, first) = self.read_packet().await;
        match first.first() {
            Some(0x00) => return (Vec::new(), Vec::new()),
            Some(0xFF) => panic!("query failed: {}", String::from_utf8_lossy(&first[9..])),
            _ => {}
        }
        // A result set: `first` holds the column count; column definitions follow, terminated by
        // EOF, then row packets, terminated by EOF.
        let mut names = Vec::new();
        for _ in 0..first[0] {
            let (_, def) = self.read_packet().await;
            names.push(column_def_name(&def));
        }
        let (_, eof) = self.read_packet().await;
        assert_eq!(eof.first(), Some(&0xFE), "expected EOF after column defs");
        let mut rows = Vec::new();
        loop {
            let (_, packet) = self.read_packet().await;
            if packet.first() == Some(&0xFE) && packet.len() < 9 {
                return (names, rows);
            }
            rows.push(packet);
        }
    }

    /// Send a COM_QUERY with the given raw statement bytes and return the raw payloads of any
    /// result row packets (empty for an OK response).
    async fn query_raw(&mut self, statement: &[u8]) -> Vec<Vec<u8>> {
        self.query_with_metadata(statement).await.1
    }

    /// The Query_destination column of EXPLAIN LAST STATEMENT.
    async fn last_destination(&mut self) -> String {
        let (_, rows) = self.query_with_metadata(b"EXPLAIN LAST STATEMENT").await;
        assert_eq!(rows.len(), 1, "expected a single row: {rows:?}");
        String::from_utf8(first_text_column(&rows[0]).to_vec()).unwrap()
    }
}

/// Extract the column name (alias) from a column definition payload, assuming its length-encoded
/// strings are under 251 bytes.
fn column_def_name(payload: &[u8]) -> Vec<u8> {
    // The name is the fifth length-encoded string, after catalog, schema, table, and org_table.
    let mut pos = 0;
    for _ in 0..4 {
        pos += 1 + payload[pos] as usize;
    }
    payload[pos + 1..][..payload[pos] as usize].to_vec()
}

/// Extract the first column's value from a text-protocol row, assuming a value under 251 bytes.
fn first_text_column(row: &[u8]) -> &[u8] {
    &row[1..1 + row[0] as usize]
}

/// Extract the value of a single-column text-protocol row, assuming a value under 251 bytes.
fn single_text_column(row: &[u8]) -> &[u8] {
    let len = row[0] as usize;
    assert_eq!(row.len(), len + 1, "expected a one-column row: {row:?}");
    &row[1..]
}

/// The latin1_swedish_ci collation id, latin1's default.
const LATIN1_COLLATION: u8 = 8;

/// A latin1 handshake must make the adapter decode inbound query bytes as latin1 (instead of
/// dropping the connection on invalid UTF-8) and return proxied result rows re-encoded as latin1
/// via the upstream session's character_set_results.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn latin1_handshake_roundtrip() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;

    let mut utf8_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    utf8_conn
        .query_drop(
            "CREATE TABLE charset_t (id INT PRIMARY KEY, t VARCHAR(32) CHARACTER SET latin1)",
        )
        .await
        .unwrap();

    let mut raw = RawConn::connect_with_charset(&opts, LATIN1_COLLATION).await;
    // 'Não' with the ã as the single latin1 byte 0xE3
    raw.query_raw(b"INSERT INTO charset_t (id, t) VALUES (1, 'N\xE3o')")
        .await;

    // The inbound byte was decoded as latin1, so the upstream received well-formed UTF-8 and the
    // latin1 column holds the single byte 0xE3.
    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let hex: String = upstream_conn
        .query_first("SELECT hex(t) FROM charset_t WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hex, "4EE36F");

    // Proxied result rows come back in the client's charset.
    let rows = raw
        .query_raw(b"SELECT t FROM charset_t WHERE id = 1")
        .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(single_text_column(&rows[0]), b"N\xE3o");

    // A utf8mb4 session reading the same row gets UTF-8 bytes.
    let value: Vec<u8> = utf8_conn
        .query_first("SELECT t FROM charset_t WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, "Não".as_bytes());

    shutdown_tx.shutdown().await;
}

/// Column names in result-set metadata arrive in the session's charset, matching MySQL: a latin1
/// session gets latin1 name bytes for both proxied and readyset-cached results, while a utf8mb4
/// session gets UTF-8 name bytes.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn latin1_column_names_roundtrip() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;

    let mut utf8_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    utf8_conn
        .query_drop("CREATE TABLE charset_colname (id INT PRIMARY KEY, x INT)")
        .await
        .unwrap();
    utf8_conn
        .query_drop("INSERT INTO charset_colname (id, x) VALUES (1, 42)")
        .await
        .unwrap();

    let mut raw = RawConn::connect_with_charset(&opts, LATIN1_COLLATION).await;

    // Proxied: 'situação' aliased with ç and ã as the latin1 bytes 0xE7 and 0xE3 comes back with
    // the same latin1 name bytes.
    let (names, rows) = raw
        .query_with_metadata(b"SELECT 'x' AS `situa\xE7\xE3o`")
        .await;
    assert_eq!(raw.last_destination().await, "upstream");
    assert_eq!(names, vec![b"situa\xE7\xE3o".to_vec()]);
    assert_eq!(rows.len(), 1);

    // The same proxied query in a utf8mb4 session names the column in UTF-8.
    let result = utf8_conn
        .query_iter("SELECT 'x' AS `situação`")
        .await
        .unwrap();
    assert_eq!(result.columns_ref()[0].name_ref(), "situação".as_bytes());
    drop(result);

    utf8_conn
        .query_drop("CREATE CACHE FROM SELECT x AS `situação` FROM charset_colname WHERE id = ?")
        .await
        .unwrap();

    // Readyset-cached results also name the column in the session's charset.
    eventually!(run_test: {
        let (names, rows) = raw
            .query_with_metadata(b"SELECT x AS `situa\xE7\xE3o` FROM charset_colname WHERE id = 1")
            .await;
        let destination = raw.last_destination().await;
        AssertUnwindSafe(move || (names, rows, destination))
    }, then_assert: |result| {
        let (names, rows, destination) = result();
        // A readyset destination includes the cache name, e.g. "readyset(q_...)".
        assert_eq!(destination.split('(').next().unwrap(), "readyset");
        assert_eq!(names, vec![b"situa\xE7\xE3o".to_vec()]);
        assert_eq!(rows.len(), 1);
    });

    let result = utf8_conn
        .query_iter("SELECT x AS `situação` FROM charset_colname WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(result.columns_ref()[0].name_ref(), "situação".as_bytes());
    drop(result);
    assert!(matches!(
        last_query_info(&mut utf8_conn).await.destination,
        QueryDestination::Readyset(_)
    ));

    shutdown_tx.shutdown().await;
}

/// A shallow cache entry filled under a lossy results charset must not leak its conversion loss
/// into other charsets. A latin1 session caching a row containing a character latin1 can't
/// represent stores MySQL's '?' substitution under its own key. A utf8mb4 session then misses
/// and fills its own entry with the original character.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn shallow_cache_lossy_charset_not_shared() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop(
        "CREATE TABLE charset_lossy (id INT PRIMARY KEY, t VARCHAR(32) CHARACTER SET utf8mb4)",
    )
    .await
    .unwrap();
    conn.query_drop("INSERT INTO charset_lossy (id, t) VALUES (1, '日')")
        .await
        .unwrap();
    conn.query_drop("CREATE SHALLOW CACHE FROM SELECT t FROM charset_lossy WHERE id = ?")
        .await
        .unwrap();

    conn.query_drop("SET NAMES latin1").await.unwrap();

    // The miss proxies to upstream, whose latin1 conversion substitutes '?' for 日.
    let value: Vec<u8> = conn
        .query_first("SELECT t FROM charset_lossy WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, b"?".to_vec());
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    // The latin1 entry serves the same substitution back, matching MySQL for a latin1 reader.
    eventually!(run_test: {
        let value: Vec<u8> = conn
            .query_first("SELECT t FROM charset_lossy WHERE id = 1")
            .await
            .unwrap()
            .unwrap();
        let info = last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (info, value))
    }, then_assert: |result| {
        let (info, value) = result();
        assert_eq!(info.destination, QueryDestination::ReadysetShallow);
        assert_eq!(value, b"?".to_vec());
    });

    conn.query_drop("SET NAMES utf8mb4").await.unwrap();

    // The utf8mb4 session keys its own entry, so it misses and gets the original character.
    let value: Vec<u8> = conn
        .query_first("SELECT t FROM charset_lossy WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, "日".as_bytes());
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    eventually!(run_test: {
        let value: Vec<u8> = conn
            .query_first("SELECT t FROM charset_lossy WHERE id = 1")
            .await
            .unwrap()
            .unwrap();
        let info = last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (info, value))
    }, then_assert: |result| {
        let (info, value) = result();
        assert_eq!(info.destination, QueryDestination::ReadysetShallow);
        assert_eq!(value, "日".as_bytes());
    });

    shutdown_tx.shutdown().await;
}

/// Shallow cache entries are partitioned by the session's results charset. A latin1 session and
/// a utf8mb4 session each fill and hit their own entry, and each receives bytes in its own
/// charset.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn shallow_cache_cross_charset() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut utf8_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    utf8_conn
        .query_drop(
            "CREATE TABLE charset_shallow (id INT PRIMARY KEY, t VARCHAR(32) CHARACTER SET latin1)",
        )
        .await
        .unwrap();
    utf8_conn
        .query_drop("INSERT INTO charset_shallow (id, t) VALUES (1, 'Não'), (2, 'Sim ã')")
        .await
        .unwrap();
    utf8_conn
        .query_drop("CREATE SHALLOW CACHE FROM SELECT t FROM charset_shallow WHERE id = ?")
        .await
        .unwrap();

    let mut latin1_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    latin1_conn.query_drop("SET NAMES latin1").await.unwrap();

    // The first execution misses, proxies to upstream (latin1 bytes for this session), and
    // fills the cache with the canonical UTF-8 decoding.
    let value: Vec<u8> = latin1_conn
        .query_first("SELECT t FROM charset_shallow WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, b"N\xE3o".to_vec());
    assert_eq!(
        last_query_info(&mut latin1_conn).await.destination,
        QueryDestination::Upstream
    );

    // A shallow hit in the latin1 session returns latin1 bytes.
    eventually!(run_test: {
        let value: Vec<u8> = latin1_conn
            .query_first("SELECT t FROM charset_shallow WHERE id = 1")
            .await
            .unwrap()
            .unwrap();
        let info = last_query_info(&mut latin1_conn).await;
        AssertUnwindSafe(move || (info, value))
    }, then_assert: |result| {
        let (info, value) = result();
        assert_eq!(info.destination, QueryDestination::ReadysetShallow);
        assert_eq!(value, b"N\xE3o".to_vec());
    });

    // The latin1 entry is not shared with the utf8mb4 session. Its first query for the same
    // params misses and proxies to upstream in UTF-8.
    let value: Vec<u8> = utf8_conn
        .query_first("SELECT t FROM charset_shallow WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        last_query_info(&mut utf8_conn).await.destination,
        QueryDestination::Upstream
    );
    assert_eq!(value, "Não".as_bytes());

    // A shallow hit in the utf8mb4 session returns UTF-8 bytes from its own entry.
    eventually!(run_test: {
        let value: Vec<u8> = utf8_conn
            .query_first("SELECT t FROM charset_shallow WHERE id = 1")
            .await
            .unwrap()
            .unwrap();
        let info = last_query_info(&mut utf8_conn).await;
        AssertUnwindSafe(move || (info, value))
    }, then_assert: |result| {
        let (info, value) = result();
        assert_eq!(info.destination, QueryDestination::ReadysetShallow);
        assert_eq!(value, "Não".as_bytes());
    });

    // The reverse direction partitions the same way. The utf8mb4 session fills id 2, and the
    // latin1 session still misses on it before filling its own entry.
    let value: Vec<u8> = utf8_conn
        .query_first("SELECT t FROM charset_shallow WHERE id = 2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, "Sim ã".as_bytes());
    assert_eq!(
        last_query_info(&mut utf8_conn).await.destination,
        QueryDestination::Upstream
    );

    let value: Vec<u8> = latin1_conn
        .query_first("SELECT t FROM charset_shallow WHERE id = 2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        last_query_info(&mut latin1_conn).await.destination,
        QueryDestination::Upstream
    );
    assert_eq!(value, b"Sim \xE3".to_vec());

    eventually!(run_test: {
        let value: Vec<u8> = latin1_conn
            .query_first("SELECT t FROM charset_shallow WHERE id = 2")
            .await
            .unwrap()
            .unwrap();
        let info = last_query_info(&mut latin1_conn).await;
        AssertUnwindSafe(move || (info, value))
    }, then_assert: |result| {
        let (info, value) = result();
        assert_eq!(info.destination, QueryDestination::ReadysetShallow);
        assert_eq!(value, b"Sim \xE3".to_vec());
    });

    shutdown_tx.shutdown().await;
}

/// Shallow-cache column names arrive in the session's charset. Entries are partitioned per
/// results charset, so each session fills and hits its own entry with its own name bytes.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn shallow_cross_charset_column_names() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut utf8_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    utf8_conn
        .query_drop("CREATE TABLE charset_shallow_name (id INT PRIMARY KEY, x INT)")
        .await
        .unwrap();
    utf8_conn
        .query_drop("INSERT INTO charset_shallow_name (id, x) VALUES (1, 1), (2, 2)")
        .await
        .unwrap();
    utf8_conn
        .query_drop(
            "CREATE SHALLOW CACHE FROM SELECT x AS `situação` FROM charset_shallow_name WHERE id = ?",
        )
        .await
        .unwrap();

    let mut raw = RawConn::connect_with_charset(&opts, LATIN1_COLLATION).await;

    // Fill the entry from the latin1 session: the miss proxies to upstream, whose mirrored
    // charset names the column in latin1.
    let (names, _) = raw
        .query_with_metadata(b"SELECT x AS `situa\xE7\xE3o` FROM charset_shallow_name WHERE id = 1")
        .await;
    assert_eq!(raw.last_destination().await, "upstream");
    assert_eq!(names, vec![b"situa\xE7\xE3o".to_vec()]);

    // A shallow hit in the latin1 session keeps the latin1 name bytes.
    eventually!(run_test: {
        let (names, _) = raw
            .query_with_metadata(b"SELECT x AS `situa\xE7\xE3o` FROM charset_shallow_name WHERE id = 1")
            .await;
        let destination = raw.last_destination().await;
        AssertUnwindSafe(move || (names, destination))
    }, then_assert: |result| {
        let (names, destination) = result();
        assert_eq!(destination, "readyset_shallow");
        assert_eq!(names, vec![b"situa\xE7\xE3o".to_vec()]);
    });

    // The latin1 entry is not shared with the utf8mb4 session. Its first query misses, then its
    // own entry serves the name in UTF-8.
    let result = utf8_conn
        .query_iter("SELECT x AS `situação` FROM charset_shallow_name WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(result.columns_ref()[0].name_ref(), "situação".as_bytes());
    drop(result);
    assert_eq!(
        last_query_info(&mut utf8_conn).await.destination,
        QueryDestination::Upstream
    );

    eventually!(run_test: {
        let result = utf8_conn
            .query_iter("SELECT x AS `situação` FROM charset_shallow_name WHERE id = 1")
            .await
            .unwrap();
        let name = result.columns_ref()[0].name_ref().to_vec();
        drop(result);
        let info = last_query_info(&mut utf8_conn).await;
        AssertUnwindSafe(move || (info, name))
    }, then_assert: |result| {
        let (info, name) = result();
        assert_eq!(info.destination, QueryDestination::ReadysetShallow);
        assert_eq!(name, "situação".as_bytes());
    });

    // The reverse direction partitions the same way. The utf8mb4 session fills id 2, the latin1
    // session misses on it, and its own entry then serves latin1 name bytes.
    utf8_conn
        .query_drop("SELECT x AS `situação` FROM charset_shallow_name WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut utf8_conn).await.destination,
        QueryDestination::Upstream
    );

    let (names, _) = raw
        .query_with_metadata(b"SELECT x AS `situa\xE7\xE3o` FROM charset_shallow_name WHERE id = 2")
        .await;
    assert_eq!(raw.last_destination().await, "upstream");
    assert_eq!(names, vec![b"situa\xE7\xE3o".to_vec()]);

    eventually!(run_test: {
        let (names, _) = raw
            .query_with_metadata(b"SELECT x AS `situa\xE7\xE3o` FROM charset_shallow_name WHERE id = 2")
            .await;
        let destination = raw.last_destination().await;
        AssertUnwindSafe(move || (names, destination))
    }, then_assert: |result| {
        let (names, destination) = result();
        assert_eq!(destination, "readyset_shallow");
        assert_eq!(names, vec![b"situa\xE7\xE3o".to_vec()]);
    });

    shutdown_tx.shutdown().await;
}

/// A scheduled refresh runs in the entry's charset. An entry filled by a latin1 session keeps
/// returning latin1 bytes after the refresh picks up a new value.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn shallow_refresh_in_entry_charset() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut utf8_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    utf8_conn
        .query_drop(
            "CREATE TABLE charset_refresh (id INT PRIMARY KEY, t VARCHAR(32) CHARACTER SET latin1)",
        )
        .await
        .unwrap();
    utf8_conn
        .query_drop("INSERT INTO charset_refresh (id, t) VALUES (1, 'Não')")
        .await
        .unwrap();
    utf8_conn
        .query_drop(
            "CREATE SHALLOW CACHE
               POLICY TTL 60 SECONDS
               REFRESH EVERY 2 SECONDS
               FROM SELECT t FROM charset_refresh WHERE id = ?",
        )
        .await
        .unwrap();

    let mut latin1_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    latin1_conn.query_drop("SET NAMES latin1").await.unwrap();

    // Fill the latin1 entry.
    let value: Vec<u8> = latin1_conn
        .query_first("SELECT t FROM charset_refresh WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, b"N\xE3o".to_vec());
    assert_eq!(
        last_query_info(&mut latin1_conn).await.destination,
        QueryDestination::Upstream
    );

    // Update the row upstream so the scheduled refresh picks up a new value.
    utf8_conn
        .query_drop("UPDATE charset_refresh SET t = 'Sim ã' WHERE id = 1")
        .await
        .unwrap();

    // The refreshed hit returns latin1 bytes for the new value.
    eventually!(run_test: {
        let value: Vec<u8> = latin1_conn
            .query_first("SELECT t FROM charset_refresh WHERE id = 1")
            .await
            .unwrap()
            .unwrap();
        let info = last_query_info(&mut latin1_conn).await;
        AssertUnwindSafe(move || (info, value))
    }, then_assert: |result| {
        let (info, value) = result();
        assert_eq!(info.destination, QueryDestination::ReadysetShallow);
        assert_eq!(value, b"Sim \xE3".to_vec());
    });

    shutdown_tx.shutdown().await;
}

/// A latin1 session's string parameters over the binary protocol are transcoded to UTF-8 before
/// reaching the utf8mb4 upstream session, storing the same bytes a native latin1 MySQL session
/// stores.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn latin1_execute_string_param() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;

    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop(
        "CREATE TABLE charset_exec (id INT PRIMARY KEY, t VARCHAR(32) CHARACTER SET latin1)",
    )
    .await
    .unwrap();
    conn.query_drop("SET NAMES latin1").await.unwrap();

    // 'Não' as latin1 bytes; mysql_async sends Value::Bytes parameters typed as VAR_STRING
    let latin1_bytes = mysql_async::Value::Bytes(b"N\xE3o".to_vec());
    conn.exec_drop(
        "INSERT INTO charset_exec (id, t) VALUES (?, ?)",
        (1, latin1_bytes.clone()),
    )
    .await
    .unwrap();

    // The same insert through a native latin1 MySQL session must store the same bytes
    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn.query_drop("SET NAMES latin1").await.unwrap();
    upstream_conn
        .exec_drop(
            "INSERT INTO charset_exec (id, t) VALUES (?, ?)",
            (2, latin1_bytes),
        )
        .await
        .unwrap();

    let hexes: Vec<(i64, String)> = upstream_conn
        .query("SELECT id, hex(t) FROM charset_exec ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        hexes,
        vec![(1, "4EE36F".to_string()), (2, "4EE36F".to_string())]
    );

    shutdown_tx.shutdown().await;
}

/// SET NAMES latin1 mid-session must make the adapter decode inbound query bytes as latin1 and
/// return proxied result rows re-encoded as latin1.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn set_names_latin1_roundtrip() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;

    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop(
        "CREATE TABLE charset_names (id INT PRIMARY KEY, t VARCHAR(32) CHARACTER SET latin1)",
    )
    .await
    .unwrap();

    conn.query_drop("SET NAMES latin1").await.unwrap();

    // mysql_async can't send non-UTF-8 statement text through its typed API, so write the
    // COM_QUERY payload directly: 'Não' with the ã as the single latin1 byte 0xE3.
    conn.write_command_data(
        Command::COM_QUERY,
        b"INSERT INTO charset_names (id, t) VALUES (1, 'N\xE3o')",
    )
    .await
    .unwrap();
    let ok = conn.read_packet().await.unwrap();
    assert_eq!(ok[0], 0x00, "INSERT should return an OK packet");

    // The inbound byte was decoded as latin1 and stored as the latin1 byte 0xE3.
    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let hex: String = upstream_conn
        .query_first("SELECT hex(t) FROM charset_names WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hex, "4EE36F");

    // Proxied result rows come back in the session's charset.
    let value: Vec<u8> = conn
        .query_first("SELECT t FROM charset_names WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, b"N\xE3o".to_vec());

    shutdown_tx.shutdown().await;
}
