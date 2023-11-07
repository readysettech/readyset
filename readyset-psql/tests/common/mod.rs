use std::sync::Arc;

use readyset_client::consensus::{Authority, StandaloneAuthority};
use readyset_client_test_helpers::psql_helpers::PostgreSQLAdapter;
use readyset_client_test_helpers::TestBuilder;
use readyset_server::Handle;
use readyset_util::shutdown::ShutdownSender;
use tokio_postgres::{Client, Config, NoTls};

pub async fn connect(config: Config) -> Client {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

// This is used in integration.rs, but for some reason clippy isn't detecting that.
#[allow(dead_code)]
pub async fn setup_standalone_with_authority(
    prefix: &str,
    authority: Option<Arc<Authority>>,
    upstream: bool,
    recreate: bool,
) -> (Config, Handle, Arc<Authority>, ShutdownSender) {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_str().unwrap();
    let authority = authority.unwrap_or_else(|| {
        Arc::new(Authority::from(
            StandaloneAuthority::new(dir_path, prefix).unwrap(),
        ))
    });
    let (config, handle, shutdown_tx) = TestBuilder::default()
        .fallback(upstream)
        .persistent(true)
        .recreate_database(recreate)
        .authority(authority.clone())
        .build::<PostgreSQLAdapter>()
        .await;

    (config, handle, authority, shutdown_tx)
}
