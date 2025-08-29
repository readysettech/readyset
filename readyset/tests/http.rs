use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use clap::Parser;
use database_utils::{DatabaseType, TlsMode};
use readyset::psql::PsqlHandler;
use readyset::{NoriaAdapter, Options};
use readyset_adapter::ReadySetStatus;
use readyset_psql::AuthenticationMethod;
use readyset_util::eventually;
use test_utils::tags;

const TEST_METRICS_ADDRESS: &str = "127.0.0.1:6035";

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
        "--upstream-db-url",
        "postgresql://postgres:noria@postgres:5432/noria",
        "--eviction-policy",
        "lru",
        "--noria-metrics",
        "--metrics-address",
        TEST_METRICS_ADDRESS,
    ];

    let adapter_options = Options::parse_from(options);

    let mut adapter = NoriaAdapter {
        description: "Readyset test adapter",
        default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 54351),
        connection_handler: PsqlHandler {
            authentication_method: AuthenticationMethod::Cleartext,
            tls_acceptor: None,
            enable_statement_logging: false,
            tls_mode: TlsMode::Optional,
        },
        database_type: DatabaseType::PostgreSQL,
        parse_dialect: readyset_sql::Dialect::PostgreSQL,
        expr_dialect: readyset_data::Dialect::DEFAULT_POSTGRESQL,
    };

    adapter.run(adapter_options).unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, postgres_upstream)]
async fn http_tests() {
    let _jh = std::thread::spawn(|| start_adapter("http_tests"));

    eventually!(attempts: 5, sleep: Duration::from_millis(500), run_test: {
        if let Ok(response) = reqwest::get(format!("http://{TEST_METRICS_ADDRESS}/health")).await {
            Some((response.status(), response.text().await.ok()))
        } else {
            None
        }
    }, then_assert: |result| {
        let (status, text) = result.expect("Could not perform health check");
        assert!(status.is_success(), "Health check failed");
        let payload = text.expect("Could not read health check response text");
        let expected_payload = "Adapter is in healthy state";
        assert_eq!(payload, expected_payload, "Health check succeeded but returned unexpected payload");
    });

    let response = reqwest::get(format!("http://{TEST_METRICS_ADDRESS}/readyset_status"))
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "/readyset_status GET failed"
    );

    let payload = response.text().await.unwrap();

    let status: ReadySetStatus = serde_json::from_str(&payload).unwrap();
    assert_eq!(status.connection_count, 0);
    assert_eq!(status.upstream_reachable, Some(true));
    assert!(status.persistent_stats.is_some());
    assert!(status.controller_status.is_some());
}
