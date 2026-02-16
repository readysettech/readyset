use mysql_async::prelude::Queryable;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_util::shutdown::ShutdownSender;
use test_utils::tags;

/// Verify that specifying a database in the connection URL works: the adapter should route
/// subsequent unqualified queries to the chosen database.
#[tokio::test]
#[tags(serial, mysql_upstream)]
async fn non_default_db_in_connection_opts() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx): (_, _, ShutdownSender) =
        TestBuilder::default().build::<MySQLAdapter>().await;

    let db_name = rs_opts.db_name().unwrap().to_string();
    let opts_with_db = mysql_async::OptsBuilder::from_opts(rs_opts).db_name(Some(db_name.clone()));

    let mut conn = mysql_async::Conn::new(opts_with_db).await.unwrap();

    conn.query_drop("CREATE TABLE db_sel_test (a INT)")
        .await
        .unwrap();
    conn.query_drop(format!("SELECT a FROM {db_name}.db_sel_test"))
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

/// When the `set-database` failpoint is active, the handshake database selection should fail
/// and the client connection should be rejected (not silently proceed with no database).
#[cfg(feature = "failure_injection")]
mod failure_injection {
    use fail::FailScenario;
    use mysql_async::prelude::Queryable;
    use readyset_client_test_helpers::TestBuilder;
    use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
    use readyset_util::failpoints;
    use test_utils::tags;

    #[tokio::test]
    #[tags(serial, mysql_upstream)]
    async fn handshake_db_selection_failure_rejects_connection() {
        readyset_tracing::init_test_logging();

        let failpoint_guard = FailScenario::setup();

        let (rs_opts, _handle, shutdown_tx) =
            TestBuilder::default().build::<MySQLAdapter>().await;

        let db_name = rs_opts.db_name().unwrap().to_string();

        // Arm the failpoint so the next set_database call returns an error.
        fail::cfg(failpoints::SET_DATABASE, "return").expect("failed to set failpoint");

        // Connecting with a database name should fail because the handshake cannot set the
        // database when the failpoint is active.
        let opts_with_db =
            mysql_async::OptsBuilder::from_opts(rs_opts.clone()).db_name(Some(db_name));
        let result = mysql_async::Conn::new(opts_with_db).await;
        assert!(
            result.is_err(),
            "expected connection to fail when set_database errors during handshake"
        );

        // Disable the failpoint and verify that a connection without a database still works.
        fail::cfg(failpoints::SET_DATABASE, "off").expect("failed to disable failpoint");
        let mut conn = mysql_async::Conn::new(rs_opts).await.unwrap();
        conn.query_drop("CREATE TABLE db_sel_test (a INT)")
            .await
            .unwrap();
        conn.query_drop("SELECT a FROM db_sel_test")
            .await
            .unwrap();
        //conn.query_drop("SELECT 1").await.unwrap();

        shutdown_tx.shutdown().await;
        failpoint_guard.teardown();
    }
}
