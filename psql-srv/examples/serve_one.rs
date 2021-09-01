extern crate psql_srv;

use async_trait::async_trait;
use postgres_types::Type;
use psql_srv::{run_backend, Backend, Column, Error, PrepareResponse, QueryResponse};
use std::convert::TryFrom;
use std::io;
use tokio::net::TcpListener;

struct Value(psql_srv::Value);

impl TryFrom<Value> for psql_srv::Value {
    type Error = Error;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        Ok(v.0)
    }
}

struct ServeOneBackend;

#[async_trait]
impl Backend for ServeOneBackend {
    type Value = Value;
    type Row = Vec<Value>;
    type Resultset = Vec<Self::Row>;

    async fn on_init(&mut self, _database: &str) -> Result<(), Error> {
        Ok(())
    }

    async fn on_query(&mut self, _query: &str) -> Result<QueryResponse<Self::Resultset>, Error> {
        Ok(QueryResponse::Select {
            schema: vec![Column {
                name: "bar".to_string(),
                col_type: Type::INT4,
            }],
            resultset: vec![vec![Value(psql_srv::Value::Int(100))]],
        })
    }

    async fn on_prepare(&mut self, _query: &str) -> Result<PrepareResponse, Error> {
        Ok(PrepareResponse {
            prepared_statement_id: 42,
            param_schema: vec![],
            row_schema: vec![Column {
                name: "bar".to_string(),
                col_type: Type::INT4,
            }],
        })
    }

    async fn on_execute(
        &mut self,
        _id: u32,
        _params: &[psql_srv::Value],
    ) -> Result<QueryResponse<Self::Resultset>, Error> {
        Ok(QueryResponse::Select {
            schema: vec![Column {
                name: "bar".to_string(),
                col_type: Type::INT4,
            }],
            resultset: vec![vec![Value(psql_srv::Value::Int(100))]],
        })
    }

    async fn on_close(&mut self, _stmt: u32) -> Result<(), Error> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:5432").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        run_backend(ServeOneBackend, socket).await;
    }
}
