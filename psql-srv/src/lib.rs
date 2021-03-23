#![feature(associated_type_bounds)]

mod bytes;
mod channel;
mod codec;
mod error;
mod message;
mod protocol;
mod runner;
mod r#type;
mod value;

pub use crate::error::Error;
pub use crate::r#type::ColType;
pub use crate::value::Value;

use async_trait::async_trait;
use std::convert::TryInto;
use tokio::io::{AsyncRead, AsyncWrite};

#[async_trait]
pub trait Backend {
    type Value: TryInto<Value, Error = Error>;
    type Row: IntoIterator<Item = Self::Value>;
    type Resultset: IntoIterator<Item = Self::Row>;

    async fn on_init(&mut self, database: &str) -> Result<(), Error>;

    async fn on_query(&mut self, query: &str) -> Result<QueryResponse<Self::Resultset>, Error>;

    async fn on_prepare(&mut self, query: &str) -> Result<PrepareResponse, Error>;

    async fn on_execute(
        &mut self,
        statement_id: u32,
        params: &[Value],
    ) -> Result<QueryResponse<Self::Resultset>, Error>;

    async fn on_close(&mut self, statement_id: u32) -> Result<(), Error>;
}

pub type Schema = Vec<(String, ColType)>;

pub struct PrepareResponse {
    pub prepared_statement_id: u32,
    pub param_schema: Schema,
    pub row_schema: Schema,
}

pub enum QueryResponse<S> {
    Select { schema: Schema, resultset: S },
    Insert(u64),
    Update(u64),
    Delete(u64),
    Command,
}

pub async fn run_backend<B: Backend, C: AsyncRead + AsyncWrite + Unpin>(backend: B, channel: C) {
    runner::Runner::run(backend, channel).await
}
