use std::mem::discriminant;
use std::panic::AssertUnwindSafe;

use mysql_async::consts::ColumnFlags;
use mysql_async::prelude::Queryable;
use mysql_async::{Column, Conn, Row};
use readyset_adapter::backend::BackendBuilder;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, TestShutdownSender, derive_test_name};
use readyset_server::{CacheMode, Handle};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// `id` is unsigned only through ZEROFILL; `id`, `b` and `t` contain values above the signed
/// range.
const SETUP: &[&str] = &[
    "CREATE TABLE t_zf (
        id int(10) zerofill NOT NULL PRIMARY KEY,
        m mediumint(8) unsigned zerofill,
        d decimal(8,2) zerofill,
        f float zerofill,
        dbl double zerofill,
        s smallint(3) zerofill,
        b bigint zerofill,
        t tinyint zerofill,
        name varchar(30),
        grp int
    )",
    "INSERT INTO t_zf VALUES
        (1, 7, 12.5, 3.25, 3.25, 6, 5, 9, 'a', 1),
        (4294967295, 1, 4.25, 5, 6, 9999, 9223372036854775808, 200, 'b', 1),
        (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'c', 1),
        (5, 2, 1.5, 1, 1, 1, 1, 1, 'd', 2)",
    "CREATE TABLE ev (ev_id int PRIMARY KEY, id int unsigned NOT NULL)",
    "INSERT INTO ev VALUES (1, 1), (2, 4294967295), (3, 3), (4, 4294967294), (5, 77)",
    "CREATE VIEW v_zf AS SELECT id, name, grp FROM t_zf",
    "CREATE VIEW v_concat AS SELECT CONCAT(id) AS c, grp FROM t_zf",
    "CREATE VIEW v_union AS
        SELECT id, grp FROM t_zf WHERE id < 100 UNION ALL SELECT id, grp FROM t_zf WHERE id >= 100",
    "CREATE TABLE t_float (id int PRIMARY KEY, f float(7,2) zerofill)",
    "CREATE TABLE t_year (id int PRIMARY KEY, y year)",
    "INSERT INTO t_year VALUES (1, 0)",
];

const REPLICATED_DDL: &[&str] = &[
    "CREATE TABLE t_alter (id int PRIMARY KEY, label varchar(10))",
    "INSERT INTO t_alter VALUES (1, 'y')",
    "ALTER TABLE t_alter ADD COLUMN w smallint zerofill",
    "UPDATE t_alter SET w = 1 WHERE id = 1",
    "CREATE TABLE t_ddl (
        id int(6) zerofill NOT NULL PRIMARY KEY,
        v decimal zerofill,
        label varchar(10)
    )",
    "INSERT INTO t_ddl VALUES (1, 1, 'x')",
];

const BINLOG_WRITES: &[&str] = &[
    "INSERT INTO t_zf VALUES
        (4294967294, 8, 99.99, 1.5, 2.5, 12, 18446744073709551615, 255, 'a', 1),
        (4294967293, 3, 1.25, 1, 1, 1, 1, 1, 'z', 1)",
    "UPDATE t_zf SET name = 'bb' WHERE id = 4294967295",
    "DELETE FROM t_zf WHERE id = 4294967293",
];

const ALL_COLUMNS: &str = "SELECT id, m, d, f, dbl, s, b, t, name FROM t_zf WHERE grp = ?";

/// The value of every placeholder is 1.
const DEEP: &[&str] = &[
    "SELECT name FROM t_zf WHERE id = ?",
    "SELECT ev.ev_id FROM ev JOIN t_zf ON ev.id = t_zf.id WHERE t_zf.grp = ?",
    "SELECT ev.ev_id, t_zf.name FROM ev LEFT JOIN t_zf ON ev.id = t_zf.id WHERE ev.ev_id >= ?",
    "SELECT t_zf.grp, ev.id FROM t_zf LEFT JOIN ev ON t_zf.id = ev.id WHERE t_zf.grp = ?",
    "SELECT name FROM t_zf WHERE (id > 100 OR d > 10) AND grp = ?",
    "SELECT name FROM t_zf WHERE id IN (1, 3) AND grp = ?",
    "SELECT name FROM t_zf WHERE id NOT IN (1, 3) AND grp = ?",
    "SELECT name FROM t_zf WHERE id BETWEEN 2 AND 100 AND grp = ?",
    "SELECT name FROM t_zf WHERE id <> 1 AND grp = ?",
    "SELECT name FROM t_zf WHERE m IS NULL AND grp = ?",
    "SELECT name FROM t_zf WHERE id AND grp = ?",
    "SELECT count(*) FROM t_zf WHERE grp = ?",
    "SELECT COALESCE(m, 0) AS c FROM t_zf WHERE grp = ?",
    "SELECT name FROM t_zf WHERE grp = ? ORDER BY id LIMIT 2",
    "SELECT name FROM t_zf WHERE grp = ? ORDER BY id LIMIT 2 OFFSET 1",
    "SELECT name, row_number() OVER (PARTITION BY grp ORDER BY id) AS rn FROM t_zf WHERE grp = ?",
    "SELECT name FROM v_zf WHERE id = ?",
    "SELECT grp FROM v_union WHERE id = ?",
    "SELECT label FROM t_ddl WHERE v = ?",
    "SELECT label FROM t_alter WHERE w = ?",
];

/// A deep cache is disallowed for each of these, and the query is proxied.
const REFUSED: &[&str] = &[
    ALL_COLUMNS,
    "SELECT id AS m FROM t_zf WHERE grp = ?",
    "SELECT id AS name FROM t_zf WHERE grp = ?",
    "SELECT concat(id) FROM t_zf WHERE grp = ?",
    "SELECT CAST(id AS CHAR) FROM t_zf WHERE grp = ?",
    "SELECT substring(id, 1, 4) FROM t_zf WHERE grp = ?",
    "SELECT name FROM t_zf WHERE id LIKE '00%' AND grp = ?",
    "SELECT group_concat(id) FROM t_zf WHERE grp = ?",
    "SELECT max(id) FROM t_zf WHERE grp = ?",
    "SELECT count(id) FROM t_zf WHERE grp = ?",
    "SELECT id + 1 FROM t_zf WHERE grp = ?",
    "SELECT c FROM v_concat WHERE grp = ?",
    "SELECT grp FROM v_concat WHERE grp = ?",
    "SELECT id FROM v_union WHERE grp = ?",
    "SELECT v_union.id AS ev_id FROM v_union JOIN ev ON v_union.grp = ev.ev_id WHERE ev.ev_id = ?",
    "SELECT t_zf.id FROM ev JOIN t_zf ON ev.id = t_zf.id WHERE t_zf.grp = ?",
    "SELECT t_zf.m AS ev_id FROM ev JOIN t_zf ON ev.id = t_zf.id WHERE ev.ev_id = ?",
    "SELECT ev.ev_id, t_zf.id FROM ev LEFT JOIN t_zf ON ev.id = t_zf.id WHERE ev.ev_id = ?",
    "SELECT ev.ev_id, (SELECT m FROM t_zf WHERE t_zf.id = ev.id) AS m FROM ev WHERE ev.ev_id = ?",
    "SELECT (SELECT m FROM t_zf WHERE t_zf.id = ev.id) AS ev_id FROM ev WHERE ev.ev_id = ?",
];

async fn setup(test_name: &str) -> (Conn, Conn, Handle, TestShutdownSender<MySQLAdapter>) {
    readyset_tracing::init_test_logging();
    mysql_helpers::recreate_database(test_name).await;
    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some(test_name)))
        .await
        .unwrap();
    for stmt in SETUP {
        upstream.query_drop(*stmt).await.unwrap();
    }
    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Deep);
    let (opts, handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate_db(test_name)
        .recreate_database(false)
        .fallback(true)
        .set_topk(true)
        .parsing_preset(ParsingPreset::for_prod())
        .build::<MySQLAdapter>()
        .await;
    let mut rs = Conn::new(opts).await.unwrap();
    rs.query_drop(format!("USE {test_name}")).await.unwrap();
    (upstream, rs, handle, shutdown_tx)
}

/// Assert that the text and the binary form of the query are served as expected.
async fn assert_served(
    upstream: &mut Conn,
    rs: &mut Conn,
    query: &str,
    destination: QueryDestination,
) {
    let text_query = query.replace('?', "1");
    let expected_text: Vec<Row> = upstream.query(&text_query).await.unwrap();
    let expected_bin: Vec<Row> = upstream.exec(query, (1,)).await.unwrap();
    let ordered = query
        .rsplit(')')
        .next()
        .is_some_and(|tail| tail.contains("ORDER BY"));
    let values = |rows: &[Row]| {
        let mut values = rows
            .iter()
            .map(|row| row.clone().unwrap())
            .collect::<Vec<_>>();
        if !ordered {
            values.sort_by_key(|row| row.iter().map(|v| v.as_sql(false)).collect::<Vec<_>>());
        }
        values
    };
    let definition = |column: &Column| {
        let flags =
            ColumnFlags::UNSIGNED_FLAG | ColumnFlags::ZEROFILL_FLAG | ColumnFlags::NOT_NULL_FLAG;
        (
            column.column_type(),
            column.column_length(),
            column.decimals(),
            column.flags() & flags,
        )
    };
    eventually!(run_test: {
        let text: Vec<Row> = rs.query(&text_query).await.unwrap();
        let text_info = last_query_info(rs).await;
        let bin: Vec<Row> = rs.exec(query, (1,)).await.unwrap();
        let bin_info = last_query_info(rs).await;
        AssertUnwindSafe(move || [(text, text_info), (bin, bin_info)])
    }, then_assert: |result| {
        let expected = [&expected_text, &expected_bin];
        for ((actual, info), expected) in result().into_iter().zip(expected) {
            assert_eq!(
                discriminant(&info.destination),
                discriminant(&destination),
                "{query}: {info:?}"
            );
            if let (Some(e), Some(a)) = (expected.first(), actual.first()) {
                for (e, a) in e.columns_ref().iter().zip(a.columns_ref()) {
                    if (e.flags() | a.flags()).contains(ColumnFlags::ZEROFILL_FLAG) {
                        assert_eq!(definition(e), definition(a), "{query}: {}", e.name_str());
                    }
                }
            }
            assert_eq!(values(expected), values(&actual), "{query}");
        }
    });
}

#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql, modern)]
async fn zerofill_column_uses() {
    let test_name = derive_test_name();
    let (mut upstream, mut rs, handle, shutdown_tx) = setup(&test_name).await;
    let tables: Vec<(String, String, Option<String>)> =
        rs.query("SHOW READYSET ALL TABLES").await.unwrap();
    let t_float = tables
        .iter()
        .find(|(table, _, _)| *table == format!("`{test_name}`.`t_float`"))
        .unwrap();
    assert_eq!(t_float.1, "Not replicated", "{t_float:?}");
    assert!(
        t_float.2.as_deref().is_some_and(|d| d.contains("ZEROFILL")),
        "{t_float:?}"
    );

    for stmt in REPLICATED_DDL {
        upstream.query_drop(*stmt).await.unwrap();
    }
    for query in DEEP {
        eventually!(run_test: {
            rs.query_drop(format!("CREATE CACHE FROM {query}")).await.map_err(|e| e.to_string())
        }, then_assert: |result| result.unwrap_or_else(|e| panic!("{query}: {e}")));
    }
    for query in REFUSED {
        let error = rs
            .query_drop(format!("CREATE CACHE FROM {query}"))
            .await
            .expect_err(query);
        assert!(error.to_string().contains("ZEROFILL"), "{query}: {error}");
    }
    for stmt in BINLOG_WRITES {
        upstream.query_drop(*stmt).await.unwrap();
    }
    for query in DEEP {
        assert_served(
            &mut upstream,
            &mut rs,
            query,
            QueryDestination::Readyset(None),
        )
        .await;
    }
    for query in REFUSED {
        assert_served(&mut upstream, &mut rs, query, QueryDestination::Upstream).await;
    }
    // YEAR is unsupported, so this is proxied.
    assert_served(
        &mut upstream,
        &mut rs,
        "SELECT y FROM t_year WHERE id = ?",
        QueryDestination::Upstream,
    )
    .await;

    drop(rs);
    let (opts, _handle, shutdown_tx) = shutdown_tx.restart(handle).await;
    let mut rs = Conn::new(opts).await.unwrap();
    rs.query_drop(format!("USE {test_name}")).await.unwrap();
    for query in DEEP {
        assert_served(
            &mut upstream,
            &mut rs,
            query,
            QueryDestination::Readyset(None),
        )
        .await;
    }
    for query in REFUSED {
        assert_served(&mut upstream, &mut rs, query, QueryDestination::Upstream).await;
    }
    shutdown_tx.shutdown().await;
}
