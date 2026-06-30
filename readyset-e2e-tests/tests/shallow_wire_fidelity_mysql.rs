//! Wire-fidelity tests for shallow caching, MySQL.
//!
//! Shallow caching does not replay raw upstream bytes. On a miss the upstream result is
//! decoded into `DfValue`s and stored (`CacheEntry::Text` / `CacheEntry::Binary`), and on a
//! hit those values are re-encoded back into wire format. A bug in that re-encode would
//! silently serve wrong bytes.
//!
//! `mysql_async::Row` exposes no raw-bytes getter (unlike `tokio_postgres`'s
//! `row.body().buffer()`), so we hand-drive the protocol over the public low-level `Conn`
//! methods `write_command_data` and `read_packet` and collect the raw result-set packets.
//!
//! `mysql-srv` advertises `CLIENT_DEPRECATE_EOF` and emits the full column-definition metadata
//! (schema, original table, original column), so a `DEPRECATE_EOF`-negotiating client like
//! `mysql_async` sees the same framing and bytes from Readyset as it does from real MySQL: a
//! column-count packet, the column definitions, the rows, and an OK-style terminator, with no
//! EOF after the column definitions. On the fallback and shallow-hit paths the column
//! definitions are the stored upstream columns re-emitted through the same conversion, so the
//! whole stream, packet for packet, must match real MySQL. Each test asserts that both the
//! fallback (which fills the cache) and the shallow hit reproduce upstream's bytes exactly.
//!
//! The terminator's status flags would otherwise diverge: real MySQL sets
//! `SERVER_QUERY_NO_INDEX_USED` when a query retrieves rows without an index, while Readyset
//! synthesizes its own session-status flags and never sets that bit. Every query here is
//! therefore index-backed (the lookup key is a primary key or indexed column), so MySQL's
//! terminator carries only `SERVER_STATUS_AUTOCOMMIT`, matching Readyset's.
//!
//! The legacy framing for a client that did not negotiate `CLIENT_DEPRECATE_EOF` (a
//! post-column EOF and an EOF terminator) is covered by the `mysql-srv` unit tests;
//! `mysql_async` always negotiates the capability, so it is not exercised here.

use std::panic::AssertUnwindSafe;

use mysql_async::consts::{ColumnType, Command};
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, last_query_info, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// A typed table whose first row is fully populated and whose second row is all NULLs, so
/// every per-column query exercises both a value and a NULL. `id` is the primary key so the
/// `WHERE id = ?` lookups are index-backed (see the module docs on terminator status flags).
const CREATE_TYPED_TABLE: &str = "
    CREATE TABLE shallow_wire (
        id INT NOT NULL PRIMARY KEY,
        c_tinyint TINYINT,
        c_smallint SMALLINT,
        c_int INT,
        c_bigint BIGINT,
        c_decimal DECIMAL(20,4),
        c_float FLOAT,
        c_double DOUBLE,
        c_text TEXT,
        c_varchar VARCHAR(50),
        c_blob BLOB,
        c_bool TINYINT(1),
        c_date DATE,
        c_datetime DATETIME(6),
        c_time TIME(6)
    )";

const INSERT_ROWS: &str = "
    INSERT INTO shallow_wire VALUES
        (1, 127, 32000, 2000000000, 9223372036854775807, 12345.6789, 3.5,
         2.718281828459045, 'hello world', 'varchar value', x'DEADBEEF', 1,
         '2021-03-14', '2021-03-14 09:26:53.589793', '09:26:53.500000'),
        (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL)";

/// One projection per type, each fetched in isolation so a byte mismatch points at a single
/// re-encode path.
const COLUMN_CASES: &[(&str, &str)] = &[
    ("tinyint", "c_tinyint"),
    ("smallint", "c_smallint"),
    ("int", "c_int"),
    ("bigint", "c_bigint"),
    ("decimal", "c_decimal"),
    ("float", "c_float"),
    ("double", "c_double"),
    ("text", "c_text"),
    ("varchar", "c_varchar"),
    ("blob", "c_blob"),
    ("bool", "c_bool"),
    ("date", "c_date"),
    ("datetime", "c_datetime"),
    ("time", "c_time"),
];

/// A result-set EOF or terminating OK packet. Row packets never collide with this: text rows
/// begin with a NULL marker (`0xFB`) or a short length prefix, binary rows begin with
/// `0x00`, and only a terminator begins with `0xFE` in a packet shorter than 9 bytes.
fn is_terminator(packet: &[u8]) -> bool {
    matches!(packet.first(), Some(0xFE)) && packet.len() < 9
}

/// The column count carried by the first packet of a result-set response. These queries
/// select few columns, so it is always a single byte below the length-encoding escape
/// values.
fn column_count(first_packet: &[u8]) -> u8 {
    assert_ne!(
        first_packet.first(),
        Some(&0xFF),
        "query returned an error packet"
    );
    let n = first_packet[0];
    assert!(
        (1..0xFB).contains(&n),
        "unexpected column-count packet: {first_packet:?}"
    );
    n
}

/// Capture a complete result set as its ordered payloads. Both Readyset and real MySQL
/// negotiate `CLIENT_DEPRECATE_EOF` with `mysql_async`, so the framing is identical: a
/// column-count packet, the column definitions, the rows, and an OK-style terminator, with no
/// EOF after the column definitions. `read_packet` strips the 4-byte packet header.
async fn read_rs_response(conn: &mut Conn) -> Vec<Vec<u8>> {
    let first = conn.read_packet().await.unwrap().to_vec();
    let n = column_count(&first);

    let mut packets = vec![first];
    for _ in 0..n {
        packets.push(conn.read_packet().await.unwrap().to_vec());
    }

    loop {
        let packet = conn.read_packet().await.unwrap().to_vec();
        let terminator = is_terminator(&packet);
        packets.push(packet);
        if terminator {
            break;
        }
    }
    packets
}

async fn send_text(conn: &mut Conn, sql: &str) {
    conn.write_command_data(Command::COM_QUERY, sql)
        .await
        .unwrap();
}

/// A hand-built `COM_STMT_EXECUTE` body binding a single signed `i32` parameter.
fn execute_body(statement_id: u32, param: i32) -> Vec<u8> {
    let mut body = Vec::with_capacity(18);
    body.extend_from_slice(&statement_id.to_le_bytes());
    body.push(0x00); // flags: CURSOR_TYPE_NO_CURSOR
    body.extend_from_slice(&1u32.to_le_bytes()); // iteration count
    body.push(0x00); // NULL bitmap for one non-NULL param
    body.push(0x01); // new-params-bound flag
    body.push(ColumnType::MYSQL_TYPE_LONG as u8);
    body.push(0x00); // parameter flags (signed)
    body.extend_from_slice(&param.to_le_bytes());
    body
}

/// Issue a prepared statement over the binary protocol binding `param`. The prepare is
/// delegated to `mysql_async` so its response framing is consumed cleanly; only the execute
/// and its result set are driven by hand.
async fn send_binary(conn: &mut Conn, sql: &str, param: i32) {
    let statement = conn.prep(sql).await.unwrap();
    conn.write_command_data(
        Command::COM_STMT_EXECUTE,
        execute_body(statement.id(), param),
    )
    .await
    .unwrap();
}

async fn setup_upstream() -> Conn {
    let upstream_opts = mysql_helpers::upstream_config().db_name(Some("noria"));
    let mut conn = Conn::new(upstream_opts).await.unwrap();
    conn.query_drop("DROP TABLE IF EXISTS shallow_wire")
        .await
        .unwrap();
    conn.query_drop(CREATE_TYPED_TABLE).await.unwrap();
    conn.query_drop(INSERT_ROWS).await.unwrap();
    conn
}

/// Each per-type text-protocol query, served from a shallow cache hit, must reproduce the
/// complete byte stream real MySQL sends, as must the fallback that fills the cache.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn mysql_shallow_text_protocol_matches_upstream_bytes() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut upstream = setup_upstream().await;
    let mut rs = Conn::new(rs_opts).await.unwrap();

    for (label, projection) in COLUMN_CASES {
        let q1 = format!("SELECT {projection} FROM shallow_wire WHERE id = 1");
        let q2 = format!("SELECT {projection} FROM shallow_wire WHERE id = 2");

        send_text(&mut upstream, &q1).await;
        let upstream1 = read_rs_response(&mut upstream).await;
        send_text(&mut upstream, &q2).await;
        let upstream2 = read_rs_response(&mut upstream).await;

        let create =
            format!("CREATE SHALLOW CACHE FROM SELECT {projection} FROM shallow_wire WHERE id = ?");
        rs.query_drop(&create)
            .await
            .expect("CREATE SHALLOW CACHE should succeed");

        // The first execution of each key misses and proxies to upstream, filling the cache.
        send_text(&mut rs, &q1).await;
        let fallback1 = read_rs_response(&mut rs).await;
        assert_eq!(
            last_query_info(&mut rs).await.destination,
            QueryDestination::Upstream
        );
        send_text(&mut rs, &q2).await;
        let fallback2 = read_rs_response(&mut rs).await;
        assert_eq!(
            last_query_info(&mut rs).await.destination,
            QueryDestination::Upstream
        );

        assert_eq!(fallback1, upstream1, "[{label}] key=1 fallback vs upstream");
        assert_eq!(fallback2, upstream2, "[{label}] key=2 fallback vs upstream");

        eventually!(run_test: {
            send_text(&mut rs, &q1).await;
            let hit1 = read_rs_response(&mut rs).await;
            let info1 = last_query_info(&mut rs).await;
            send_text(&mut rs, &q2).await;
            let hit2 = read_rs_response(&mut rs).await;
            let info2 = last_query_info(&mut rs).await;
            AssertUnwindSafe(move || (info1, info2, hit1, hit2))
        }, then_assert: |result| {
            let (info1, info2, hit1, hit2) = result();
            assert_eq!(info1.destination, QueryDestination::ReadysetShallow, "[{label}] key=1");
            assert_eq!(info2.destination, QueryDestination::ReadysetShallow, "[{label}] key=2");
            assert_eq!(hit1, upstream1, "[{label}] key=1 shallow hit vs upstream");
            assert_eq!(hit2, upstream2, "[{label}] key=2 shallow hit vs upstream");
        });
    }

    shutdown_tx.shutdown().await;
}

/// As above, but over the binary protocol (prepared statements), exercising the
/// `to_mysql_bin` re-encode path.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn mysql_shallow_binary_protocol_matches_upstream_bytes() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut upstream = setup_upstream().await;
    let mut rs = Conn::new(rs_opts).await.unwrap();

    for (label, projection) in COLUMN_CASES {
        let query = format!("SELECT {projection} FROM shallow_wire WHERE id = ?");

        send_binary(&mut upstream, &query, 1).await;
        let upstream1 = read_rs_response(&mut upstream).await;
        send_binary(&mut upstream, &query, 2).await;
        let upstream2 = read_rs_response(&mut upstream).await;

        let create = format!("CREATE SHALLOW CACHE FROM {query}");
        rs.query_drop(&create)
            .await
            .expect("CREATE SHALLOW CACHE should succeed");

        send_binary(&mut rs, &query, 1).await;
        let fallback1 = read_rs_response(&mut rs).await;
        assert_eq!(
            last_query_info(&mut rs).await.destination,
            QueryDestination::Upstream
        );
        send_binary(&mut rs, &query, 2).await;
        let fallback2 = read_rs_response(&mut rs).await;
        assert_eq!(
            last_query_info(&mut rs).await.destination,
            QueryDestination::Upstream
        );

        assert_eq!(fallback1, upstream1, "[{label}] key=1 fallback vs upstream");
        assert_eq!(fallback2, upstream2, "[{label}] key=2 fallback vs upstream");

        eventually!(run_test: {
            send_binary(&mut rs, &query, 1).await;
            let hit1 = read_rs_response(&mut rs).await;
            let info1 = last_query_info(&mut rs).await;
            send_binary(&mut rs, &query, 2).await;
            let hit2 = read_rs_response(&mut rs).await;
            let info2 = last_query_info(&mut rs).await;
            AssertUnwindSafe(move || (info1, info2, hit1, hit2))
        }, then_assert: |result| {
            let (info1, info2, hit1, hit2) = result();
            assert_eq!(info1.destination, QueryDestination::ReadysetShallow, "[{label}] key=1");
            assert_eq!(info2.destination, QueryDestination::ReadysetShallow, "[{label}] key=2");
            assert_eq!(hit1, upstream1, "[{label}] key=1 shallow hit vs upstream");
            assert_eq!(hit2, upstream2, "[{label}] key=2 shallow hit vs upstream");
        });
    }

    shutdown_tx.shutdown().await;
}

/// Aggregates cached shallowly must re-encode to the complete byte stream real MySQL sends.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn mysql_shallow_aggregate_matches_upstream_bytes() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some("noria"));
    let mut upstream = Conn::new(upstream_opts).await.unwrap();
    upstream
        .query_drop("DROP TABLE IF EXISTS shallow_wire_agg")
        .await
        .unwrap();
    // `grp` is indexed so the `WHERE grp = ?` lookup is index-backed (see the module docs on
    // terminator status flags).
    upstream
        .query_drop(
            "CREATE TABLE shallow_wire_agg (grp INT NOT NULL, val BIGINT NOT NULL, KEY (grp))",
        )
        .await
        .unwrap();
    upstream
        .query_drop(
            "INSERT INTO shallow_wire_agg (grp, val) VALUES
               (1, 3002399751580331), (1, 3002399751580331), (1, 3002399751580331),
               (2, 4503599627370497), (2, 4503599627370496)",
        )
        .await
        .unwrap();

    let mut rs = Conn::new(rs_opts).await.unwrap();

    let query = "SELECT grp, AVG(val), SUM(val), COUNT(*) \
                 FROM shallow_wire_agg WHERE grp = ? GROUP BY grp";

    send_binary(&mut upstream, query, 1).await;
    let upstream1 = read_rs_response(&mut upstream).await;
    send_binary(&mut upstream, query, 2).await;
    let upstream2 = read_rs_response(&mut upstream).await;

    let create = format!("CREATE SHALLOW CACHE FROM {query}");
    rs.query_drop(&create)
        .await
        .expect("CREATE SHALLOW CACHE should succeed");

    send_binary(&mut rs, query, 1).await;
    let fallback1 = read_rs_response(&mut rs).await;
    assert_eq!(
        last_query_info(&mut rs).await.destination,
        QueryDestination::Upstream
    );
    send_binary(&mut rs, query, 2).await;
    let fallback2 = read_rs_response(&mut rs).await;
    assert_eq!(
        last_query_info(&mut rs).await.destination,
        QueryDestination::Upstream
    );

    assert_eq!(fallback1, upstream1, "group 1 fallback vs upstream");
    assert_eq!(fallback2, upstream2, "group 2 fallback vs upstream");

    eventually!(run_test: {
        send_binary(&mut rs, query, 1).await;
        let hit1 = read_rs_response(&mut rs).await;
        let info1 = last_query_info(&mut rs).await;
        send_binary(&mut rs, query, 2).await;
        let hit2 = read_rs_response(&mut rs).await;
        let info2 = last_query_info(&mut rs).await;
        AssertUnwindSafe(move || (info1, info2, hit1, hit2))
    }, then_assert: |result| {
        let (info1, info2, hit1, hit2) = result();
        assert_eq!(info1.destination, QueryDestination::ReadysetShallow);
        assert_eq!(info2.destination, QueryDestination::ReadysetShallow);
        assert_eq!(hit1, upstream1, "group 1 shallow hit vs upstream");
        assert_eq!(hit2, upstream2, "group 2 shallow hit vs upstream");
    });

    shutdown_tx.shutdown().await;
}
