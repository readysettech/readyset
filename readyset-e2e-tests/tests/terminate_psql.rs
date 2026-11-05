//! Regression test for REA-7214: Postgres ends the session when it reads Terminate. Some clients
//! (asyncpg's `close()`) send Terminate on a socket they keep open, then wait for the server to
//! close it.

use std::time::Duration;

use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::psql_helpers::PostgreSQLAdapter;
use test_utils::{tags, upstream};
use tokio::io;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_postgres::NoTls;

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn terminate_closes_connection() {
    readyset_tracing::init_test_logging();

    let (mut opts, _handle, shutdown_tx) =
        TestBuilder::new(BackendBuilder::new().require_authentication(false))
            .fallback(true)
            .build::<PostgreSQLAdapter>()
            .await;
    // Without a user, `connect_raw` looks up the OS user name, which a test sandbox may lack.
    opts.user("postgres");

    // Relay between tokio-postgres and Readyset. tokio-postgres shuts down its write side after
    // it sends Terminate; asyncpg does not. The relay forwards the client's bytes but holds the
    // write side to Readyset open, so only Terminate can end the session.
    let server = TcpStream::connect(("127.0.0.1", opts.get_ports()[0]))
        .await
        .unwrap();
    let (mut server_read, mut server_write) = server.into_split();
    let (client_end, relay_end) = io::duplex(64 * 1024);
    let (mut relay_read, mut relay_write) = io::split(relay_end);
    let client_to_server = tokio::spawn(async move {
        io::copy(&mut relay_read, &mut server_write).await.unwrap();
        server_write
    });
    let server_to_client =
        tokio::spawn(async move { io::copy(&mut server_read, &mut relay_write).await });

    let (client, connection) = opts.connect_raw(client_end, NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    client.simple_query("SELECT 1").await.unwrap();

    // Dropping the client makes the connection send Terminate.
    drop(client);
    connection.await.unwrap().unwrap();
    let _server_write = client_to_server.await.unwrap();

    timeout(Duration::from_secs(10), server_to_client)
        .await
        .expect("Readyset did not close the connection after Terminate")
        .unwrap()
        .expect("Readyset sent data after Terminate");

    shutdown_tx.shutdown().await;
}
