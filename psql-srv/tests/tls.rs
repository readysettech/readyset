use std::sync::Arc;
use std::vec;

use async_trait::async_trait;
use database_utils::{DatabaseURL, QueryableConnection};
use futures::stream;
use postgres_types::Type;
use psql_srv::{run_backend, Backend, Credentials, CredentialsNeeded, Error};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_native_tls::{native_tls, TlsAcceptor};
use tokio_postgres::config::SslMode;
use tokio_postgres::Config;

struct TestBackend;
struct TestValue;

impl TryFrom<TestValue> for psql_srv::Value {
    type Error = Error;

    fn try_from(_: TestValue) -> Result<Self, Self::Error> {
        panic!() // never called
    }
}

#[async_trait]
impl Backend for TestBackend {
    type Value = TestValue;
    type Row = Vec<Self::Value>;
    type Resultset = stream::Iter<vec::IntoIter<Result<Self::Row, Error>>>;

    fn credentials_for_user(&self, _user: &str) -> Option<Credentials> {
        Some(Credentials::Any)
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
    ) -> Result<psql_srv::PrepareResponse, psql_srv::Error> {
        panic!() // never called
    }

    async fn on_execute(
        &mut self,
        _statement_id: u32,
        _params: &[psql_srv::Value],
    ) -> Result<psql_srv::QueryResponse<Self::Resultset>, psql_srv::Error> {
        panic!() // never called
    }

    async fn on_close(&mut self, _statement_id: u32) -> Result<(), Error> {
        Ok(())
    }

    fn version(&self) -> String {
        "ReadySet".to_string()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn connect() {
    // Load the identity file as bytes (using relative path)
    let identity_file = include_bytes!("tls_certs/keyStore.p12");
    // Test identify file does not require password
    let identity = native_tls::Identity::from_pkcs12(identity_file, "").unwrap();
    let tls_acceptor = Some(Arc::new(TlsAcceptor::from(
        native_tls::TlsAcceptor::new(identity).unwrap(),
    )));

    let (send_port, recv_port) = oneshot::channel();

    let mut tls_connector_builder = native_tls::TlsConnector::builder();
    // The test certificate is self signed, which by default the TlsConnector rejects
    tls_connector_builder.danger_accept_invalid_certs(true);
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        send_port
            .send(listener.local_addr().unwrap().port())
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        run_backend(TestBackend, socket, false, tls_acceptor).await;
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
        .connect(Some(tls_connector_builder))
        .await
        .unwrap();
    // The Runner should then accept queries.
    conn.query_drop("FAKE QUERY").await.unwrap();
}
