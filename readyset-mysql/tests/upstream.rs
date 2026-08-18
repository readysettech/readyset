//! Tests that drive [`MySqlUpstream`] directly as a library against a live upstream.

use mysql_async::prelude::Queryable;
use readyset_adapter::{UpstreamConfig, UpstreamDatabase};
use readyset_client_test_helpers::mysql_helpers;
use readyset_mysql::MySqlUpstream;
use test_utils::{tags, upstream};

async fn connect() -> MySqlUpstream {
    readyset_tracing::init_test_logging();
    let opts = mysql_async::Opts::from(mysql_helpers::upstream_config());
    let url = format!(
        "mysql://{}:{}@{}:{}",
        opts.user().unwrap(),
        opts.pass().unwrap(),
        opts.ip_or_hostname(),
        opts.tcp_port(),
    );
    MySqlUpstream::connect(UpstreamConfig::from_url(url), None, None, false)
        .await
        .unwrap()
}

/// A collation the upstream supports applies verbatim.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn supported_collation_applies_verbatim() {
    let mut upstream = connect().await;
    let applied = upstream
        .set_connection_charset("utf8mb4", "utf8mb4_bin")
        .await
        .unwrap();
    assert_eq!(applied, None);
}

/// A collation the upstream rejects falls back to the upstream's default collation for the
/// requested charset, and the applied collation is reported back.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn unknown_collation_falls_back_to_charset_default() {
    let mut upstream = connect().await;
    let applied = upstream
        .set_connection_charset("utf8mb4", "utf8mb4_nonexistent_ci")
        .await
        .unwrap()
        .expect("fallback should report the applied collation");
    assert_eq!(applied.character_set_name, "utf8mb4");

    let mut conn = mysql_async::Conn::new(mysql_helpers::upstream_config())
        .await
        .unwrap();
    let expected: String = conn
        .query_first(
            "SELECT COLLATION_NAME FROM information_schema.COLLATIONS \
             WHERE CHARACTER_SET_NAME = 'utf8mb4' AND IS_DEFAULT = 'Yes'",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied.collation_name, expected);
}

/// A charset the upstream rejects falls back to the upstream's server default charset and
/// collation.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn unknown_charset_falls_back_to_server_default() {
    let mut upstream = connect().await;
    let applied = upstream
        .set_connection_charset("utf9", "utf9_general_ci")
        .await
        .unwrap()
        .expect("fallback should report the applied collation");

    let mut conn = mysql_async::Conn::new(mysql_helpers::upstream_config())
        .await
        .unwrap();
    let expected: String = conn
        .query_first("SELECT @@collation_server")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied.collation_name, expected);
}
