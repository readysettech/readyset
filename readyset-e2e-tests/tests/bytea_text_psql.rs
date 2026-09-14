//! Text-format bytea parameters must decode the way Postgres decodes them, in both the `\x` hex
//! form libpq sends and the legacy escape form.

use readyset_client_test_helpers::{
    TestBuilder,
    psql_helpers::{self, PostgreSQLAdapter, TextParam},
};
use test_utils::{tags, upstream};
use tokio_postgres::Client;

const SETUP: &str = "
    CREATE TABLE bytea_text (id INT PRIMARY KEY, b BYTEA);
    INSERT INTO bytea_text VALUES (1, '\\xDEADBEEF'), (2, 'abc\\000\\\\'), (3, NULL);
";

async fn ids_matching(conn: &Client, param: &'static str) -> Vec<i32> {
    conn.query("SELECT id FROM bytea_text WHERE b = $1", &[&TextParam(param)])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn postgres_text_format_bytea_param_matches_upstream() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;
    upstream_conn.simple_query(SETUP).await.unwrap();

    let rs_conn = psql_helpers::connect(rs_opts).await;

    for (param, expected) in [
        ("\\xdeadbeef", vec![1]),
        ("\\xDEADBEEF", vec![1]),
        ("\\xde ad\tbe\nef", vec![1]),
        ("\\336\\255\\276\\357", vec![1]),
        ("abc\\000\\\\", vec![2]),
        ("\\x", vec![]),
    ] {
        let upstream_ids = ids_matching(&upstream_conn, param).await;
        assert_eq!(upstream_ids, expected, "[{param:?}] upstream");
        let rs_ids = ids_matching(&rs_conn, param).await;
        assert_eq!(rs_ids, upstream_ids, "[{param:?}] readyset diverged from upstream");
    }

    shutdown_tx.shutdown().await;
}
