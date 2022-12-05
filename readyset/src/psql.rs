use async_trait::async_trait;
use readyset_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};
use readyset_tracing::error;
use tokio::net;
use tracing::instrument;

use crate::ConnectionHandler;

#[derive(Clone, Copy)]
pub struct PsqlHandler;

#[async_trait]
impl ConnectionHandler for PsqlHandler {
    type UpstreamDatabase = PostgreSqlUpstream;
    type Handler = PostgreSqlQueryHandler;

    #[instrument(level = "debug", "connection", skip_all, fields(addr = ?stream.peer_addr().unwrap()))]
    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: readyset_adapter::Backend<PostgreSqlUpstream, PostgreSqlQueryHandler>,
    ) {
        psql_srv::run_backend(readyset_psql::Backend(backend), stream).await;
    }

    async fn immediate_error(self, stream: net::TcpStream, error_message: String) {
        if let Err(error) = psql_srv::send_immediate_err::<readyset_psql::Backend, _>(
            stream,
            psql_srv::Error::InternalError(error_message),
        )
        .await
        {
            error!(%error, "Could not send immediate error packet")
        }
    }
}
