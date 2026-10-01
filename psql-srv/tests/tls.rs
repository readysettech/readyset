use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use futures::stream;
use postgres_protocol::Oid;
use postgres_types::Type;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_native_tls::{native_tls, TlsAcceptor};
use tokio_postgres::config::SslMode;
use tokio_postgres::Config;

use database_utils::tls::ServerCertVerification;
use database_utils::{DatabaseURL, QueryableConnection, TlsMode};
use psql_srv::{
    run_backend, Credentials, CredentialsNeeded, Error, PsqlBackend, PsqlSrvRow, TransferFormat,
    DEFAULT_CONNECT_TIMEOUT,
};
use readyset_adapter_types::{DeallocateId, PreparedStatementType};
use readyset_util::redacted::RedactedString;

struct TestBackend;
struct TestValue;

impl TryFrom<TestValue> for psql_srv::PsqlValue {
    type Error = Error;

    fn try_from(_: TestValue) -> Result<Self, Self::Error> {
        panic!() // never called
    }
}

impl PsqlBackend for TestBackend {
    type Resultset = stream::Iter<vec::IntoIter<Result<PsqlSrvRow, Error>>>;

    fn credentials_for_user(&self, _user: &str) -> Option<Credentials> {
        Some(Credentials::Any)
    }

    async fn set_auth_info(
        &mut self,
        _user: &str,
        _password: Option<RedactedString>,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn on_init(
        &mut self,
        _database: &str,
    ) -> Result<psql_srv::CredentialsNeeded, psql_srv::Error> {
        Ok(CredentialsNeeded::None)
    }

    async fn on_query(
        &mut self,
        _query: &str,
    ) -> Result<psql_srv::QueryResponse<Self::Resultset>, psql_srv::Error> {
        // Dummy response
        Ok(psql_srv::QueryResponse::Insert(1))
    }

    async fn on_prepare(
        &mut self,
        _query: &str,
        _parameter_data_types: &[Type],
        _statement_type: PreparedStatementType,
    ) -> Result<psql_srv::PrepareResponse, psql_srv::Error> {
        panic!() // never called
    }

    async fn on_execute(
        &mut self,
        _statement_id: u32,
        _params: &[psql_srv::PsqlValue],
        _result_transfer_formats: &[TransferFormat],
    ) -> Result<psql_srv::QueryResponse<Self::Resultset>, psql_srv::Error> {
        panic!() // never called
    }

    async fn on_close(&mut self, _statement_id: DeallocateId) -> Result<(), Error> {
        Ok(())
    }

    fn version(&self) -> String {
        "Readyset".to_string()
    }

    fn in_transaction(&self) -> bool {
        false
    }

    async fn load_extended_types(&mut self) -> Result<HashMap<Oid, i16>, psql_srv::Error> {
        Ok(HashMap::default())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn connect() {
    // Load the identity file as bytes (using relative path)
    let identity_file = include_bytes!("tls_certs/keyStore.p12");
    // Test identify file does not require password
    let identity = native_tls::Identity::from_pkcs12(identity_file, "password").unwrap();
    let tls_acceptor = Some(Arc::new(TlsAcceptor::from(
        native_tls::TlsAcceptor::new(identity).unwrap(),
    )));

    let (send_port, recv_port) = oneshot::channel();

    // The test certificate is self signed, which by default the TlsConnector rejects
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        send_port
            .send(listener.local_addr().unwrap().port())
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        run_backend(
            TestBackend,
            socket,
            false,
            tls_acceptor,
            TlsMode::Optional,
            DEFAULT_CONNECT_TIMEOUT,
        )
        .await;
    });

    let port = recv_port.await.unwrap();
    let mut config = Config::default();
    config
        .host("127.0.0.1")
        .port(port)
        .dbname("foo")
        .ssl_mode(SslMode::Require);

    // With SslMode::Require, connect() errors if the server does not support TLS.
    let mut conn = DatabaseURL::PostgreSQL(config)
        .connect(&ServerCertVerification::None)
        .await
        .unwrap();
    // The Runner should then accept queries.
    conn.query_drop("FAKE QUERY").await.unwrap();
}

/// Serves one connection with a short connect timeout, returning the port and the server task.
async fn serve_with_connect_timeout(
    tls_acceptor: Option<Arc<TlsAcceptor>>,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let timeout = Duration::from_millis(200);
        run_backend(
            TestBackend,
            socket,
            false,
            tls_acceptor,
            TlsMode::Optional,
            timeout,
        )
        .await;
    });
    (port, server)
}

/// Asserts that the server ends the connection: the task finishes and the socket reads EOF.
async fn assert_closed(server: tokio::task::JoinHandle<()>, mut stream: TcpStream) {
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("server did not close the stalled connection")
        .unwrap();
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut [0; 1]))
        .await
        .expect("socket was not closed");
    assert_eq!(read.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_times_out() {
    let (port, server) = serve_with_connect_timeout(None).await;
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_closed(server, stream).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_handshake_times_out() {
    let identity =
        native_tls::Identity::from_pkcs12(include_bytes!("tls_certs/keyStore.p12"), "password")
            .unwrap();
    let tls_acceptor = Arc::new(TlsAcceptor::from(
        native_tls::TlsAcceptor::new(identity).unwrap(),
    ));
    let (port, server) = serve_with_connect_timeout(Some(tls_acceptor)).await;

    // Send an SSLRequest, read the server's agreement, then never start the TLS handshake.
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream
        .write_all(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])
        .await
        .unwrap();
    let mut answer = [0; 1];
    stream.read_exact(&mut answer).await.unwrap();
    assert_eq!(&answer, b"S");
    assert_closed(server, stream).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_session_outlives_connect_timeout() {
    let (port, _server) = serve_with_connect_timeout(None).await;
    let mut config = Config::default();
    config
        .host("127.0.0.1")
        .port(port)
        .dbname("foo")
        .ssl_mode(SslMode::Disable);
    let mut conn = DatabaseURL::PostgreSQL(config)
        .connect(&ServerCertVerification::None)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;
    conn.query_drop("FAKE QUERY").await.unwrap();
}
