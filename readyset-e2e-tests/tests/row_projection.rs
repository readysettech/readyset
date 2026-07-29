//! End-to-end wire checks for `ROW` constructors in the select list.
//!
//! PostgreSQL reports an anonymous `ROW(...)` as the `record` pseudo-type, and encodes it
//! field-wise: text format is `record_out`'s parenthesized, selectively-quoted form, and binary
//! format prefixes each field with its own type OID. Readyset computes these rows in its own
//! engine rather than copying upstream bytes, so each case here compares Readyset's bytes against
//! what upstream emits for the identical query, in both protocols.
//!
//! MySQL has no projectable row type at all — `SELECT ROW(1,2)` is error 1241, "Operand should
//! contain 1 column(s)" — so the MySQL test asserts the query is refused rather than cached.

use std::assert_matches;
use std::panic::AssertUnwindSafe;

use mysql_async::prelude::Queryable;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder, sleep,
    mysql_helpers::{self, MySQLAdapter},
    psql_helpers::{self, PostgreSQLAdapter},
};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::Client;

/// A typed table whose first row is fully populated and whose second row is all NULLs, so every
/// case exercises both real values and the NULL field encoding.
///
/// `c_quoted` holds every character that forces `record_out` to quote a field: a comma, a double
/// quote, a backslash, parentheses, and trailing whitespace.
const CREATE_TYPED_TABLE: &str = r#"
    CREATE TABLE row_wire (
        id INT NOT NULL,
        c_int2 SMALLINT,
        c_int4 INT,
        c_int8 BIGINT,
        c_numeric NUMERIC(20,4),
        c_float4 REAL,
        c_float8 DOUBLE PRECISION,
        c_text TEXT,
        c_varchar VARCHAR(50),
        c_bool BOOLEAN,
        c_date DATE,
        c_timestamp TIMESTAMP,
        c_time TIME,
        c_quoted TEXT,
        c_empty TEXT
    );
    INSERT INTO row_wire VALUES
        (1, 32000, 2000000000, 9223372036854775807, 12345.6789, 3.5,
         2.718281828459045, 'hello world', 'varchar value', true,
         '2021-03-14', '2021-03-14 09:26:53.589793', '09:26:53.5',
         'a,b"c\d(e) ', ''),
        (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL);
"#;

/// One `ROW` shape per case, each fetched in isolation so a byte mismatch points at a single
/// field type rather than at "some record somewhere".
const ROW_CASES: &[(&str, &str)] = &[
    ("int2", "ROW(c_int2)"),
    ("int4", "ROW(c_int4)"),
    ("int8", "ROW(c_int8)"),
    ("numeric", "ROW(c_numeric)"),
    ("float4", "ROW(c_float4)"),
    ("float8", "ROW(c_float8)"),
    ("text", "ROW(c_text)"),
    ("varchar", "ROW(c_varchar)"),
    ("bool", "ROW(c_bool)"),
    ("date", "ROW(c_date)"),
    ("timestamp", "ROW(c_timestamp)"),
    ("time", "ROW(c_time)"),
    ("ints", "ROW(c_int2, c_int4, c_int8)"),
    ("mixed", "ROW(c_int4, c_text, c_bool, c_float8)"),
    ("quoting", "ROW(c_quoted)"),
    ("empty_vs_null", "ROW(c_empty, c_text)"),
    ("nested", "ROW(c_int4, ROW(c_text, c_int8))"),
    ("alongside_scalar", "c_int4, ROW(c_text, c_int8)"),
];

/// Raw DataRow body bytes for an extended-protocol query at a single key. `tokio_postgres`
/// requests binary results, so this exercises the binary `record` encoding.
async fn extended_row_bytes(conn: &Client, query: &str, id: i32) -> Vec<Vec<u8>> {
    conn.query(query, &[&id])
        .await
        .unwrap()
        .iter()
        .map(|r| r.body().buffer().to_vec())
        .collect()
}

/// The column type OIDs and raw `DataRow` bytes of each simple-query row, which is what the text
/// format has to get right.
///
/// The column *name* is deliberately excluded: Readyset labels every expression column with the
/// expression's own text where upstream uses `?column?` or a function name. That difference is
/// unrelated to `ROW` — `SELECT c + 1` shows it too — and it is the name only, not the type or the
/// bytes.
async fn simple_row_signatures(conn: &Client, query: &str) -> Vec<(Vec<u32>, Vec<u8>)> {
    conn.simple_query(query)
        .await
        .unwrap()
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.fields().iter().map(|f| f.type_oid()).collect(),
                row.body().buffer().to_vec(),
            )),
            _ => None,
        })
        .collect()
}

/// Creates a cache, retrying while the schema generation is still catching up with the DDL that
/// set the fixture up.
async fn create_cache(conn: &Client, label: &str, query: &str) {
    let create = format!("CREATE CACHE FROM {query}");
    eventually!(run_test: {
        conn.simple_query(&create)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }, then_assert: |result| {
        result.unwrap_or_else(|e| panic!("[{label}] CREATE CACHE should succeed: {e}"));
    });
}

/// Every `ROW` projection, served from a Readyset cache, must emit byte-for-byte what upstream
/// emitted — in binary format, where each field carries its own type OID.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn postgres_row_projection_binary_matches_upstream_bytes() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;
    upstream_conn.simple_query(CREATE_TYPED_TABLE).await.unwrap();

    let rs_conn = psql_helpers::connect(rs_opts).await;

    for (label, projection) in ROW_CASES {
        let query = format!("SELECT {projection} FROM row_wire WHERE id = $1");

        let mut upstream_bodies = extended_row_bytes(&upstream_conn, &query, 1).await;
        upstream_bodies.extend(extended_row_bytes(&upstream_conn, &query, 2).await);

        create_cache(&rs_conn, label, &query).await;

        eventually!(run_test: {
            let mut bodies = extended_row_bytes(&rs_conn, &query, 1).await;
            let info1 = psql_helpers::last_query_info(&rs_conn).await;
            bodies.extend(extended_row_bytes(&rs_conn, &query, 2).await);
            let info2 = psql_helpers::last_query_info(&rs_conn).await;
            AssertUnwindSafe(move || (info1, info2, bodies))
        }, then_assert: |result| {
            let (info1, info2, rs_bodies) = result();
            // A fallback to upstream would compare upstream against itself and pass vacuously.
            assert_matches!(
                &info1.destination, QueryDestination::Readyset(_), "[{label}] key=1",
            );
            assert_matches!(
                &info2.destination, QueryDestination::Readyset(_), "[{label}] key=2",
            );
            assert_eq!(
                upstream_bodies.len(), 2,
                "[{label}] expected one row per key, not an empty comparison",
            );
            assert_eq!(
                upstream_bodies, rs_bodies,
                "[{label}] record binary encoding diverged from upstream",
            );
        });
    }

    shutdown_tx.shutdown().await;
}

/// The same projections over the simple-query protocol, which exercises `record_out`'s text
/// format and its quoting rules.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn postgres_row_projection_text_matches_upstream_bytes() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;
    upstream_conn.simple_query(CREATE_TYPED_TABLE).await.unwrap();

    let rs_conn = psql_helpers::connect(rs_opts).await;

    for (label, projection) in ROW_CASES {
        // The simple protocol has no parameters, so pin the key in the query text.
        let query = format!("SELECT {projection} FROM row_wire WHERE id = 1");

        let upstream_messages = simple_row_signatures(&upstream_conn, &query).await;

        create_cache(&rs_conn, label, &query).await;

        eventually!(run_test: {
            let messages = simple_row_signatures(&rs_conn, &query).await;
            let info = psql_helpers::last_query_info(&rs_conn).await;
            AssertUnwindSafe(move || (info, messages))
        }, then_assert: |result| {
            let (info, rs_messages) = result();
            assert_matches!(
                &info.destination, QueryDestination::Readyset(_), "[{label}]",
            );
            assert_eq!(
                upstream_messages, rs_messages,
                "[{label}] record text encoding diverged from upstream",
            );
        });
    }

    shutdown_tx.shutdown().await;
}

/// The positions other than a plain select list where a `ROW` can reach Postgres.
///
/// A row projected through a derived table is served from the cache and must match upstream. The
/// other two produce a row-typed value the engine has no encoding for, and each is already refused
/// at migration time for its own reason — an array of records cannot have its element type
/// determined, and a `VALUES` clause takes only constant expressions. Being refused up front is
/// what matters: the query proxies and the client still gets upstream's answer, rather than the
/// cache being built and the failure surfacing later at row-description time.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn postgres_row_in_other_positions() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;
    upstream_conn.simple_query(CREATE_TYPED_TABLE).await.unwrap();

    let rs_conn = psql_helpers::connect(rs_opts).await;

    // Cacheable: a row projected out of a derived table.
    let derived = "SELECT sub.r FROM \
                   (SELECT ROW(c_int4, c_text) AS r FROM row_wire WHERE id = 1) sub";
    let upstream_derived = simple_row_signatures(&upstream_conn, derived).await;
    create_cache(&rs_conn, "derived_table", derived).await;
    eventually!(run_test: {
        let rows = simple_row_signatures(&rs_conn, derived).await;
        let info = psql_helpers::last_query_info(&rs_conn).await;
        AssertUnwindSafe(move || (info, rows))
    }, then_assert: |result| {
        let (info, rs_rows) = result();
        assert_matches!(&info.destination, QueryDestination::Readyset(_), "[derived_table]");
        assert_eq!(
            upstream_derived, rs_rows,
            "[derived_table] record encoding diverged from upstream",
        );
    });

    // Not cacheable: must be refused at migration time, then proxy to a correct answer.
    let declined = [
        (
            "array_of_records",
            "SELECT ARRAY(SELECT ROW(c_int4, c_text) FROM row_wire WHERE id = 1)",
        ),
        (
            "values_row_column",
            "SELECT c FROM (VALUES((1,1)),((1,2))) AS v(c)",
        ),
    ];
    for (label, query) in declined {
        let create = rs_conn
            .simple_query(&format!("CREATE CACHE FROM {query}"))
            .await;
        assert!(
            create.is_err(),
            "[{label}] CREATE CACHE should be refused rather than build an unencodable cache",
        );

        let upstream_rows = simple_row_signatures(&upstream_conn, query).await;
        let rs_rows = simple_row_signatures(&rs_conn, query).await;
        let info = psql_helpers::last_query_info(&rs_conn).await;
        // Either destination means upstream answered: `ReadysetThenUpstream` is the fallback after
        // the migration on this connection failed, `Upstream` once the query is known to be
        // unsupported.
        assert_matches!(
            &info.destination,
            QueryDestination::Upstream | QueryDestination::ReadysetThenUpstream(_),
            "[{label}] should not be served from a cache",
        );
        assert_eq!(
            upstream_rows, rs_rows,
            "[{label}] proxied result diverged from upstream",
        );
    }

    shutdown_tx.shutdown().await;
}

/// MySQL cannot return a row value, so Readyset must refuse to cache the query and let it proxy —
/// which is what makes the client see MySQL's own error 1241 rather than one of ours. `ROW` in a
/// predicate stays usable.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn mysql_row_projection_is_refused_and_proxied() {
    readyset_tracing::init_test_logging();

    let db_name = "row_projection_mysql";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn
        .query_drop(
            "CREATE TABLE row_wire (id INT NOT NULL, c_int4 INT, c_text TEXT);
             INSERT INTO row_wire VALUES (1, 2000000000, 'hello world');",
        )
        .await
        .unwrap();

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    sleep().await;

    let query = "SELECT ROW(c_int4, c_text) FROM row_wire WHERE id = 1";

    // Caching must be refused: were it cached, serving it would surface a Readyset error instead
    // of MySQL's.
    let create = rs_conn
        .query_drop(format!("CREATE CACHE FROM {query}"))
        .await;
    assert!(
        create.is_err(),
        "CREATE CACHE over a projected ROW should be refused on MySQL",
    );

    // Proxied, so the client sees upstream's error verbatim.
    let upstream_err = upstream_conn
        .query_drop(query)
        .await
        .expect_err("MySQL rejects a projected ROW")
        .to_string();
    let rs_err = rs_conn
        .query_drop(query)
        .await
        .expect_err("Readyset should proxy the rejection")
        .to_string();
    assert_eq!(
        upstream_err, rs_err,
        "Readyset should report MySQL's own error for a projected ROW",
    );
    assert!(
        rs_err.contains("Operand should contain 1 column"),
        "expected MySQL error 1241, got: {rs_err}",
    );

    // A ROW in a predicate is unaffected and still cacheable.
    let predicate_query = "SELECT c_int4 FROM row_wire WHERE (id, c_int4) = (1, 2000000000)";
    rs_conn
        .query_drop(format!("CREATE CACHE FROM {predicate_query}"))
        .await
        .expect("CREATE CACHE over a ROW predicate should succeed");
    eventually!(run_test: {
        let rows: Vec<i32> = rs_conn.query(predicate_query).await.unwrap();
        let info = mysql_helpers::last_query_info(&mut rs_conn).await;
        AssertUnwindSafe(move || (info, rows))
    }, then_assert: |result| {
        let (info, rows) = result();
        assert_matches!(&info.destination, QueryDestination::Readyset(_));
        assert_eq!(rows, vec![2000000000]);
    });

    shutdown_tx.shutdown().await;
}
