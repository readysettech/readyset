use std::sync::Arc;
use std::vec;

use async_trait::async_trait;
use futures::stream;
use postgres::config::{ChannelBinding, SslMode};
use postgres::error::SqlState;
use postgres::NoTls;
use postgres_types::Type;
use psql_srv::{
    run_backend, Credentials, CredentialsNeeded, Error, PrepareResponse, PsqlBackend, PsqlSrvRow,
    PsqlValue, QueryResponse,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_native_tls::{native_tls, TlsAcceptor};

struct Value(Result<PsqlValue, Error>);

impl TryFrom<Value> for PsqlValue {
    type Error = Error;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        v.0
    }
}

#[derive(Debug, Clone, Copy)]
struct ScramSha256Backend {
    username: &'static str,
    password: &'static str,
}

#[async_trait]
impl PsqlBackend for ScramSha256Backend {
    type Resultset = stream::Iter<vec::IntoIter<Result<PsqlSrvRow, psql_srv::Error>>>;

    fn version(&self) -> String {
        "13.4 ReadySet".to_string()
    }

    fn credentials_for_user(&self, user: &str) -> Option<Credentials> {
        if self.username == user {
            Some(Credentials::CleartextPassword(self.password))
        } else {
            None
        }
    }

    async fn on_init(&mut self, _database: &str) -> Result<CredentialsNeeded, Error> {
        Ok(CredentialsNeeded::ScramSha256)
    }

    async fn on_query(&mut self, _query: &str) -> Result<QueryResponse<Self::Resultset>, Error> {
        unreachable!()
    }

    async fn on_prepare(
        &mut self,
        _query: &str,
        _parameter_data_types: &[Type],
    ) -> Result<PrepareResponse, Error> {
        unreachable!()
    }

    async fn on_execute(
        &mut self,
        _statement_id: u32,
        _params: &[PsqlValue],
    ) -> Result<QueryResponse<Self::Resultset>, Error> {
        unreachable!()
    }

    async fn on_close(&mut self, _statement_id: u32) -> Result<(), Error> {
        Ok(())
    }
}

async fn run_server(backend: ScramSha256Backend) -> u16 {
    let identity_file = include_bytes!("tls_certs/keyStore.p12");
    let identity = native_tls::Identity::from_pkcs12(identity_file, "").unwrap();
    let tls_acceptor = Some(Arc::new(TlsAcceptor::from(
        native_tls::TlsAcceptor::new(identity).unwrap(),
    )));

    let (send_port, recv_port) = oneshot::channel();
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        send_port
            .send(listener.local_addr().unwrap().port())
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        run_backend(backend, socket, false, tls_acceptor).await;
    });
    recv_port.await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_scram_sha256_valid_password() {
    readyset_tracing::init_test_logging();
    let username = "user";
    let password = "password";
    let port = run_server(ScramSha256Backend { username, password }).await;

    let (_, _) = tokio_postgres::Config::default()
        .host("localhost")
        .port(port)
        .dbname("noria")
        .user(username)
        .password(password)
        .connect(NoTls)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_scram_sha256_invalid_password() {
    readyset_tracing::init_test_logging();
    let username = "user";
    let password = "password";
    let port = run_server(ScramSha256Backend { username, password }).await;

    let res = tokio_postgres::Config::default()
        .host("localhost")
        .port(port)
        .dbname("noria")
        .user(username)
        .password("wrong password")
        .connect(NoTls)
        .await;
    let err = res.err().unwrap();
    assert_eq!(*err.code().unwrap(), SqlState::INVALID_PASSWORD);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_scram_sha256_with_escapes_in_username_and_password() {
    readyset_tracing::init_test_logging();
    let username = "user=,=,=,user";
    let password = "password,=,=,=password";
    let port = run_server(ScramSha256Backend { username, password }).await;

    let (_, _) = tokio_postgres::Config::default()
        .host("localhost")
        .port(port)
        .dbname("noria")
        .user(username)
        .password(password)
        .connect(NoTls)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_scram_sha256_over_tls_with_channel_binding_required() {
    readyset_tracing::init_test_logging();
    let username = "user";
    let password = "password";
    let port = run_server(ScramSha256Backend { username, password }).await;

    let (_, _) = tokio_postgres::Config::default()
        .host("localhost")
        .port(port)
        .dbname("noria")
        .user(username)
        .password(password)
        .ssl_mode(SslMode::Require)
        .channel_binding(ChannelBinding::Require)
        .connect(postgres_native_tls::MakeTlsConnector::new(
            native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
        ))
        .await
        .unwrap();
}
