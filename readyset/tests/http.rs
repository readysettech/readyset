use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use clap::Parser;
use database_utils::DatabaseType;
use readyset::psql::PsqlHandler;
use readyset::{NoriaAdapter, Options};
use readyset_adapter::ReadySetStatus;
use readyset_psql::AuthenticationMethod;

/// Start a test instance of the ReadySet adapter in standalone mode
fn start_adapter(test_db: &str) -> anyhow::Result<()> {
    let temp_dir = temp_dir::TempDir::new().unwrap();
    let options = vec![
        "adapter_test", // This is equivalent to the program name in argv, ignored
        "--deployment",
        test_db,
        "--database-type",
        "postgresql",
        "--deployment-mode",
        "standalone",
        "--authority",
        "standalone",
        "--authority-address",
        temp_dir.path().to_str().unwrap(),
        "--allow-unauthenticated-connections",
        "--allow-full-materialization",
        "--upstream-db-url",
        "postgresql://postgres:noria@postgres:5432/noria",
        "--eviction-policy",
        "lru",
        "--noria-metrics",
    ];

    let adapter_options = Options::parse_from(options);

    let mut adapter = NoriaAdapter {
        description: "ReadySet test adapter",
        default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 54351),
        connection_handler: PsqlHandler {
            authentication_method: AuthenticationMethod::Cleartext,
            tls_acceptor: None,
            enable_statement_logging: false,
        },
        database_type: DatabaseType::PostgreSQL,
        parse_dialect: nom_sql::Dialect::PostgreSQL,
        expr_dialect: readyset_data::Dialect::DEFAULT_POSTGRESQL,
    };

    adapter.run(adapter_options).unwrap();
    Ok(())
}

use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn http_tests() {
    let _jh = std::thread::spawn(|| start_adapter("http_tests"));
    // Wait for startup
    tokio::time::sleep(Duration::from_secs(1)).await;

    let response = reqwest::get("http://localhost:6034/health").await.unwrap();
    assert!(response.status().is_success(), "Health check failed");

    let payload = response.text().await.unwrap();
    let expected_payload = "Adapter is in healthy state";
    assert_eq!(payload, expected_payload, "Payload did not match expected");

    let response = reqwest::get("http://localhost:6034/readyset_status")
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "/readyset_status GET failed"
    );

    let payload = response.text().await.unwrap();

    let status: ReadySetStatus = serde_json::from_str(&payload).unwrap();
    assert_eq!(status.connection_count, 0);
    assert_eq!(status.connected, Some(true));
    assert!(status.persistent_stats.is_some());
    assert!(status.controller_status.is_some());
}
