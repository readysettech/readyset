use std::vec;

use async_trait::async_trait;
use futures::{stream, Future};
use postgres::NoTls;
use postgres_types::Type;
use psql_srv::{
    run_backend, Column, Credentials, CredentialsNeeded, Error, PrepareResponse, PsqlBackend,
    PsqlSrvRow, PsqlValue, QueryResponse, TransferFormat,
};
use readyset_adapter_types::DeallocateId;
use tokio::join;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_postgres::Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorPosition {
    Query,
    Prepare,
    Execute,
    Serialize,
}

#[derive(Clone, Copy)]
struct ErrorBackend(ErrorPosition);

#[async_trait]
impl PsqlBackend for ErrorBackend {
    type Resultset = stream::Iter<vec::IntoIter<Result<PsqlSrvRow, psql_srv::Error>>>;

    fn credentials_for_user(&self, _user: &str) -> Option<Credentials> {
        Some(Credentials::Any)
    }

    async fn on_init(&mut self, _database: &str) -> Result<CredentialsNeeded, Error> {
        Ok(CredentialsNeeded::None)
    }

    async fn on_query(&mut self, _query: &str) -> Result<QueryResponse<Self::Resultset>, Error> {
        if self.0 == ErrorPosition::Query {
            Err(Error::InternalError("help I'm".to_owned()))
        } else {
            Ok(QueryResponse::Select {
                schema: vec![],
                resultset: stream::iter(vec![]),
            })
        }
    }

    async fn on_prepare(
        &mut self,
        _query: &str,
        _parameter_data_types: &[Type],
    ) -> Result<PrepareResponse, Error> {
        if self.0 == ErrorPosition::Prepare {
            Err(Error::InternalError("trapped in".to_owned()))
        } else {
            Ok(PrepareResponse {
                prepared_statement_id: 1,
                param_schema: vec![],
                row_schema: vec![Column::Column {
                    name: "x".into(),
                    table_oid: None,
                    attnum: None,
                    col_type: Type::BOOL,
                }],
            })
        }
    }

    async fn on_execute(
        &mut self,
        _statement_id: u32,
        _params: &[PsqlValue],
        _result_transfer_formats: &[TransferFormat],
    ) -> Result<QueryResponse<Self::Resultset>, Error> {
        match self.0 {
            ErrorPosition::Execute => Err(Error::InternalError("a database".to_owned())),
            ErrorPosition::Serialize => Ok(QueryResponse::Select {
                schema: vec![Column::Column {
                    name: "x".into(),
                    table_oid: None,
                    attnum: None,
                    col_type: Type::BOOL,
                }],
                resultset: stream::iter(vec![Err(Error::InternalError("factory".to_owned()))]),
            }),
            _ => Ok(QueryResponse::Select {
                schema: vec![],
                resultset: stream::iter(vec![]),
            }),
        }
    }

    async fn on_close(&mut self, _statement_id: DeallocateId) -> Result<(), Error> {
        Ok(())
    }

    fn version(&self) -> String {
        "13.4 ReadySet".to_string()
    }

    fn in_transaction(&self) -> bool {
        false
    }
}

async fn error_test<F, R>(error_pos: ErrorPosition, inner: F)
where
    F: Fn(Client) -> R + Send + 'static,
    R: Future + Send,
{
    let (send_port, recv_port) = oneshot::channel();
    let server = tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        send_port
            .send(listener.local_addr().unwrap().port())
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        run_backend(ErrorBackend(error_pos), socket, false, None).await;
    });
    let client = tokio::spawn(async move {
        let port = recv_port.await.unwrap();
        let (client, conn) = tokio_postgres::Config::default()
            .host("localhost")
            .port(port)
            .dbname("noria")
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        let inner_fut = inner(client);
        inner_fut.await;
    });

    let (client, server) = join!(client, server);
    client.unwrap();
    server.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_error() {
    error_test(ErrorPosition::Query, |client| async move {
        let res = client.simple_query("SELECT 1 as one").await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().as_db_error().unwrap().message(),
            "internal error: help I'm"
        );
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_error() {
    error_test(ErrorPosition::Prepare, |client| async move {
        let res = client.execute("SELECT 1 as one", &[]).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().as_db_error().unwrap().message(),
            "internal error: trapped in"
        );
    })
    .await
}

// The synchronous postgres client specifically doesn't like receiving a ReadyForQuery message
// immediately after an error message, before it gets a chance to send a Sync message
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_error_sync() {
    let (send_port, recv_port) = oneshot::channel();
    let server = tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        send_port
            .send(listener.local_addr().unwrap().port())
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        run_backend(ErrorBackend(ErrorPosition::Execute), socket, false, None).await;
    });
    let port = recv_port.await.unwrap();
    tokio::task::spawn_blocking(move || {
        let mut client = postgres::Config::default()
            .host("localhost")
            .port(port)
            .dbname("noria")
            .connect(NoTls)
            .unwrap();
        let res = client.execute("SELECT 1 as one", &[]);
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(err.as_db_error().is_some(), "err = {}", err);
        assert_eq!(
            err.as_db_error().unwrap().message(),
            "internal error: a database"
        );
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_error() {
    error_test(ErrorPosition::Execute, |client| async move {
        let res = client.execute("SELECT 1 as one", &[]).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().as_db_error().unwrap().message(),
            "internal error: a database"
        );
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serialize_error() {
    error_test(ErrorPosition::Serialize, |client| async move {
        let res = client.execute("SELECT 1 as one", &[]).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().as_db_error().unwrap().message(),
            "internal error: factory"
        );
    })
    .await
}
