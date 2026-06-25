//! Wire-fidelity tests for shallow caching.
//!
//! Shallow caching does not replay raw upstream bytes.  For Postgres, with the simple protocol,
//! we copy bytes, but with the extended protocol, on a miss we decode the upstream result into
//! `DfValue`s and store them, and on a hit we re-encode them back into wire format.
//!
//! PostgreSQL only: `tokio_postgres` exposes the raw DataRow body bytes for extended-protocol
//! results (`row.body().buffer()`). For the simple-query protocol there is no public getter for
//! row bytes, but the `Debug` rendering of each `SimpleQueryMessage` includes the raw
//! `DataRowBody`, so that case compares the full message stream byte-for-byte.

use std::assert_matches;
use std::panic::AssertUnwindSafe;

use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder,
    psql_helpers::{self, PostgreSQLAdapter},
};
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::Client;

/// A typed table whose first row is fully populated and whose second row is all
/// NULLs, so every per-column query exercises both a value and a NULL.
const CREATE_TYPED_TABLE: &str = "
    CREATE TABLE shallow_wire (
        id INT NOT NULL,
        c_int2 SMALLINT,
        c_int4 INT,
        c_int8 BIGINT,
        c_numeric NUMERIC(20,4),
        c_float4 REAL,
        c_float8 DOUBLE PRECISION,
        c_text TEXT,
        c_varchar VARCHAR(50),
        c_bytea BYTEA,
        c_bool BOOLEAN,
        c_date DATE,
        c_timestamp TIMESTAMP,
        c_timestamptz TIMESTAMPTZ,
        c_time TIME
    );
    INSERT INTO shallow_wire VALUES
        (1, 32000, 2000000000, 9223372036854775807, 12345.6789, 3.5,
         2.718281828459045, 'hello world', 'varchar value', '\\xDEADBEEF', true,
         '2021-03-14', '2021-03-14 09:26:53.589793',
         '2021-03-14 09:26:53.589793+00', '09:26:53.5'),
        (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL);
";

/// One projection per type, each fetched in isolation so a byte mismatch points
/// at a single re-encode path.
const COLUMN_CASES: &[(&str, &str)] = &[
    ("int2", "c_int2"),
    ("int4", "c_int4"),
    ("int8", "c_int8"),
    ("numeric", "c_numeric"),
    ("float4", "c_float4"),
    ("float8", "c_float8"),
    ("text", "c_text"),
    ("varchar", "c_varchar"),
    ("bytea", "c_bytea"),
    ("bool", "c_bool"),
    ("date", "c_date"),
    ("timestamp", "c_timestamp"),
    ("timestamptz", "c_timestamptz"),
    ("time", "c_time"),
];

/// Raw DataRow body bytes for an extended-protocol query at a single key.
async fn extended_row_bytes(conn: &Client, query: &str, id: i32) -> Vec<Vec<u8>> {
    conn.query(query, &[&id])
        .await
        .unwrap()
        .iter()
        .map(|r| r.body().buffer().to_vec())
        .collect()
}

/// Debug rendering of every simple-query message. For `Row` this includes the raw `DataRowBody`
/// bytes, so the comparison is byte-level; it also covers the `RowDescription` and
/// `CommandComplete` messages.
async fn simple_messages(conn: &Client, query: &str) -> Vec<String> {
    conn.simple_query(query)
        .await
        .unwrap()
        .iter()
        .map(|m| format!("{m:?}"))
        .collect()
}

/// Each per-type extended-protocol query, served from a shallow cache hit, must
/// emit byte-for-byte what upstream emitted, for both the populated and the
/// all-NULL row.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn postgres_shallow_extended_protocol_matches_upstream_bytes() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;
    upstream_conn.simple_query(CREATE_TYPED_TABLE).await.unwrap();

    let rs_conn = psql_helpers::connect(rs_opts).await;

    for (label, projection) in COLUMN_CASES {
        let query = format!("SELECT {projection} FROM shallow_wire WHERE id = $1");

        let mut upstream_bodies = extended_row_bytes(&upstream_conn, &query, 1).await;
        upstream_bodies.extend(extended_row_bytes(&upstream_conn, &query, 2).await);

        let create = format!("CREATE SHALLOW CACHE FROM {query}");
        rs_conn
            .simple_query(&create)
            .await
            .expect("CREATE SHALLOW CACHE should succeed");

        // Populate both keys (these miss and fall through to upstream).
        rs_conn.query(&query, &[&1i32]).await.unwrap();
        rs_conn.query(&query, &[&2i32]).await.unwrap();

        eventually!(run_test: {
            let mut bodies = extended_row_bytes(&rs_conn, &query, 1).await;
            let info1 = psql_helpers::last_query_info(&rs_conn).await;
            bodies.extend(extended_row_bytes(&rs_conn, &query, 2).await);
            let info2 = psql_helpers::last_query_info(&rs_conn).await;
            AssertUnwindSafe(move || (info1, info2, bodies))
        }, then_assert: |result| {
            let (info1, info2, rs_bodies) = result();
            // A fallback to upstream would compare upstream against itself and
            // pass vacuously; require the rows to come from the shallow cache.
            assert_matches!(
                &info1.destination, QueryDestination::ReadysetShallow,
                "[{label}] key=1",
            );
            assert_matches!(
                &info2.destination, QueryDestination::ReadysetShallow,
                "[{label}] key=2",
            );
            assert_eq!(
                upstream_bodies.len(), 2,
                "[{label}] expected one row per key, not an empty comparison",
            );
            assert_eq!(
                upstream_bodies, rs_bodies,
                "[{label}] shallow wire bytes diverged from upstream",
            );
        });
    }

    shutdown_tx.shutdown().await;
}

/// Aggregates computed by upstream and cached shallowly must re-encode to the
/// same bytes upstream sent.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn postgres_shallow_aggregate_matches_upstream_bytes() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;
    upstream_conn
        .simple_query(
            "CREATE TABLE shallow_wire_agg (grp INT NOT NULL, val BIGINT NOT NULL);
             INSERT INTO shallow_wire_agg (grp, val) VALUES
               (1, 3002399751580331), (1, 3002399751580331), (1, 3002399751580331),
               (2, 4503599627370497), (2, 4503599627370496);",
        )
        .await
        .unwrap();

    let rs_conn = psql_helpers::connect(rs_opts).await;

    let query = "SELECT grp, AVG(val), SUM(val), COUNT(*) \
                 FROM shallow_wire_agg WHERE grp = $1 GROUP BY grp";

    let mut upstream_bodies = extended_row_bytes(&upstream_conn, query, 1).await;
    upstream_bodies.extend(extended_row_bytes(&upstream_conn, query, 2).await);

    let create = format!("CREATE SHALLOW CACHE FROM {query}");
    rs_conn
        .simple_query(&create)
        .await
        .expect("CREATE SHALLOW CACHE should succeed");

    rs_conn.query(query, &[&1i32]).await.unwrap();
    rs_conn.query(query, &[&2i32]).await.unwrap();

    eventually!(run_test: {
        let mut bodies = extended_row_bytes(&rs_conn, query, 1).await;
        let info1 = psql_helpers::last_query_info(&rs_conn).await;
        bodies.extend(extended_row_bytes(&rs_conn, query, 2).await);
        let info2 = psql_helpers::last_query_info(&rs_conn).await;
        AssertUnwindSafe(move || (info1, info2, bodies))
    }, then_assert: |result| {
        let (info1, info2, rs_bodies) = result();
        assert_matches!(&info1.destination, QueryDestination::ReadysetShallow);
        assert_matches!(&info2.destination, QueryDestination::ReadysetShallow);
        assert_eq!(
            upstream_bodies.len(), 2,
            "expected one row per group, not an empty comparison",
        );
        assert_eq!(
            upstream_bodies, rs_bodies,
            "shallow aggregate wire bytes diverged from upstream",
        );
    });

    shutdown_tx.shutdown().await;
}

/// The simple-query protocol stores and serves a distinct `CacheEntry::Simple`
/// representation. Compare the full message stream served from a shallow hit
/// against upstream; the `Debug` rendering includes the raw row bytes.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn postgres_shallow_simple_protocol_matches_upstream() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;
    upstream_conn.simple_query(CREATE_TYPED_TABLE).await.unwrap();

    let rs_conn = psql_helpers::connect(rs_opts).await;

    // Align the session time zone so timestamptz text rendering is comparable.
    upstream_conn.simple_query("SET TimeZone = 'UTC'").await.unwrap();
    rs_conn.simple_query("SET TimeZone = 'UTC'").await.unwrap();

    let columns = COLUMN_CASES.iter().map(|(_, col)| *col).collect::<Vec<_>>().join(", ");
    let query = format!("SELECT id, {columns} FROM shallow_wire ORDER BY id");

    let upstream_messages = simple_messages(&upstream_conn, &query).await;

    let create = format!("CREATE SHALLOW CACHE FROM {query}");
    rs_conn
        .simple_query(&create)
        .await
        .expect("CREATE SHALLOW CACHE should succeed");

    // Populate the cache through the simple-query protocol so the served entry
    // is a `CacheEntry::Simple`.
    rs_conn.simple_query(&query).await.unwrap();

    eventually!(run_test: {
        let messages = simple_messages(&rs_conn, &query).await;
        let info = psql_helpers::last_query_info(&rs_conn).await;
        AssertUnwindSafe(move || (info, messages))
    }, then_assert: |result| {
        let (info, rs_messages) = result();
        assert_matches!(&info.destination, QueryDestination::ReadysetShallow);
        let row_count = upstream_messages.iter().filter(|m| m.starts_with("Row(")).count();
        assert_eq!(row_count, 2, "expected both rows, not an empty comparison");
        assert_eq!(
            upstream_messages, rs_messages,
            "shallow simple-query messages diverged from upstream",
        );
    });

    shutdown_tx.shutdown().await;
}
