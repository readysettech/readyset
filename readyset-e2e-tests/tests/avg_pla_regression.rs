//! REA-6568: AVG over BIGINT through a cached query with range/`WHERE IN`
//! parameters (post-lookup aggregation) must match upstream byte-for-byte.

use std::panic::AssertUnwindSafe;

use assert_matches::assert_matches;
use mysql_async::prelude::Queryable;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter, last_query_info as mysql_last_query_info},
    psql_helpers::{self, last_query_info as psql_last_query_info},
};
use readyset_util::eventually;
use test_utils::{tags, upstream};

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn postgres_bigint_avg_pla_matches_upstream_bytes() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<psql_helpers::PostgreSQLAdapter>()
        .await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .simple_query(
            "CREATE TABLE avg_pla_test_pg (grp INT NOT NULL, val BIGINT NOT NULL);\
             INSERT INTO avg_pla_test_pg (grp, val) VALUES \
               (1, 3002399751580331), (1, 3002399751580331), (1, 3002399751580331), \
               (2, 4503599627370497), (2, 4503599627370496);",
        )
        .await
        .unwrap();

    let query =
        "SELECT grp, AVG(val) FROM avg_pla_test_pg WHERE grp > $1 GROUP BY grp ORDER BY grp";

    let upstream_rows = upstream_conn.query(query, &[&0_i32]).await.unwrap();
    let upstream_bodies: Vec<Vec<u8>> = upstream_rows
        .iter()
        .map(|row| row.body().buffer().to_vec())
        .collect();
    assert_eq!(upstream_bodies.len(), 2);

    let rs_conn = psql_helpers::connect(rs_opts).await;
    eventually!(
        run_test: {
            rs_conn
                .simple_query(
                    "CREATE CACHE FROM SELECT grp, AVG(val) FROM avg_pla_test_pg \
                     WHERE grp > $1 GROUP BY grp",
                )
                .await
                .map_err(|e| e.to_string())
        },
        then_assert: |result| {
            result.expect("CREATE CACHE should succeed once replication catches up");
        }
    );

    eventually!(run_test: {
        let rs_rows = rs_conn.query(query, &[&0_i32]).await.unwrap();
        let info = psql_last_query_info(&rs_conn).await;
        let bodies: Vec<Vec<u8>> = rs_rows.iter().map(|r| r.body().buffer().to_vec()).collect();
        AssertUnwindSafe(move || (info, bodies))
    }, then_assert: |result| {
        let (info, rs_bodies) = result();
        // Fail loudly if the query fell back to upstream — the byte
        // comparison is meaningless in that case (we'd be comparing
        // upstream's output against itself).
        assert_matches!(
            info.destination,
            QueryDestination::Readyset(..),
            "expected PLA query to be served by Readyset, got {info:?}",
        );
        assert_eq!(upstream_bodies, rs_bodies);
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn mysql_bigint_avg_pla_matches_upstream_text() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some("noria"));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE avg_pla_test_my (grp INT NOT NULL, val BIGINT NOT NULL)")
        .await
        .unwrap();
    upstream_conn
        .query_drop(
            "INSERT INTO avg_pla_test_my (grp, val) VALUES \
               (1, 3002399751580331), (1, 3002399751580331), (1, 3002399751580331), \
               (2, 4503599627370497), (2, 4503599627370496)",
        )
        .await
        .unwrap();

    // MySQL AVG of BIGINT renders as DECIMAL with scale 4 (input_scale + 4).
    let query =
        "SELECT grp, AVG(val) FROM avg_pla_test_my WHERE grp > ? GROUP BY grp ORDER BY grp";

    let upstream_rows: Vec<mysql_async::Row> =
        upstream_conn.exec(query, (0_i32,)).await.unwrap();
    let upstream_values: Vec<(i32, String)> = upstream_rows
        .iter()
        .map(|row| {
            let g: i32 = row.get(0).unwrap();
            let v: String = row.get(1).unwrap();
            (g, v)
        })
        .collect();
    assert_eq!(upstream_values.len(), 2);

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    eventually!(
        run_test: {
            rs_conn
                .query_drop(
                    "CREATE CACHE FROM SELECT grp, AVG(val) FROM avg_pla_test_my \
                     WHERE grp > ? GROUP BY grp",
                )
                .await
                .map_err(|e| e.to_string())
        },
        then_assert: |result| {
            result.expect("CREATE CACHE should succeed once replication catches up");
        }
    );

    eventually!(run_test: {
        let rs_rows: Vec<mysql_async::Row> = rs_conn.exec(query, (0_i32,)).await.unwrap();
        let info = mysql_last_query_info(&mut rs_conn).await;
        let values: Vec<(i32, String)> = rs_rows
            .iter()
            .map(|row| {
                let g: i32 = row.get(0).unwrap();
                let v: String = row.get(1).unwrap();
                (g, v)
            })
            .collect();
        AssertUnwindSafe(move || (info, values))
    }, then_assert: |result| {
        let (info, rs_values) = result();
        assert_matches!(
            info.destination,
            QueryDestination::Readyset(..),
            "expected PLA query to be served by Readyset, got {info:?}",
        );
        assert_eq!(upstream_values, rs_values);
    });

    shutdown_tx.shutdown().await;
}
