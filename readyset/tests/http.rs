use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use clap::Parser;
use database_utils::{DatabaseType, TlsMode};
use readyset::psql::PsqlHandler;
use readyset::{init_adapter_runtime, init_adapter_tracing, NoriaAdapter, Options};
use readyset_adapter::ReadySetStatus;
use readyset_psql::AuthenticationMethod;
use readyset_util::eventually;
use test_utils::tags;

const TEST_METRICS_ADDRESS: &str = "127.0.0.1:6035";

fn upstream_db_url() -> String {
    format!(
        "postgresql://{user}:{password}@{host}:{port}/noria",
        host = env::var("PGHOST").as_deref().unwrap_or("localhost"),
        port = env::var("PGPORT").as_deref().unwrap_or("5432"),
        user = env::var("PGUSER").as_deref().unwrap_or("postgres"),
        password = env::var("PGPASSWORD").as_deref().unwrap_or("noria"),
    )
}

/// Start a test instance of the ReadySet adapter in standalone mode
fn start_adapter(test_db: &str) -> anyhow::Result<()> {
    let temp_dir = temp_dir::TempDir::new().unwrap();
    let upstream_db_url = upstream_db_url();
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
        &upstream_db_url,
        "--eviction-policy",
        "lru",
        "--noria-metrics",
        "--metrics-address",
        TEST_METRICS_ADDRESS,
    ];

    let options = Options::parse_from(options);

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

    let rt = init_adapter_runtime()?;
    let _tracing_guard = init_adapter_tracing(&rt, &options)?;
    adapter.run(rt, options)
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, postgres_upstream)]
async fn http_tests() {
    std::thread::spawn(|| {
        if let Err(err) = start_adapter("http_tests") {
            eprintln!("Failed to start adapter: {err}");
        }
    });

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
