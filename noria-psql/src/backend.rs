use crate::error::Error;
use crate::response::{PrepareResponse, QueryResponse};
use crate::resultset::Resultset;
use crate::row::Row;
use crate::value::Value;
use async_trait::async_trait;
use noria::DataType;
use noria_client::backend as cl;
use psql_srv as ps;
use std::convert::{TryFrom, TryInto};

/// A `noria_client` `Backend` wrapper that implements `psql_srv::Backend`. PostgreSQL client
/// requests provided to `psql_srv::Backend` trait function implementations are forwared to the
/// wrapped `noria_client` `Backend`. All request parameters and response results are forwarded
/// using type conversion.
pub struct Backend(pub cl::Backend);

impl Backend {
    async fn query(&mut self, query: &str) -> Result<QueryResponse, Error> {
        Ok(QueryResponse(self.0.query(query).await?))
    }

    async fn prepare(&mut self, query: &str) -> Result<PrepareResponse, Error> {
        Ok(PrepareResponse(self.0.prepare(query).await?))
    }

    async fn execute(&mut self, id: u32, params: Vec<DataType>) -> Result<QueryResponse, Error> {
        Ok(QueryResponse(self.0.execute(id, params).await?))
    }
}

#[async_trait]
impl ps::Backend for Backend {
    type Value = Value;
    type Row = Row;
    type Resultset = Resultset;

    async fn on_init(&mut self, _database: &str) -> Result<(), ps::Error> {
        Ok(())
    }

    async fn on_query(&mut self, query: &str) -> Result<ps::QueryResponse<Resultset>, ps::Error> {
        self.query(query).await?.try_into()
    }

    async fn on_prepare(&mut self, query: &str) -> Result<ps::PrepareResponse, ps::Error> {
        self.prepare(query).await?.try_into()
    }

    async fn on_execute(
        &mut self,
        statement_id: u32,
        params: &[ps::Value],
    ) -> Result<ps::QueryResponse<Resultset>, ps::Error> {
        let params = params
            .iter()
            .map(|p| ParamRef(p).try_into())
            .collect::<Result<Vec<DataType>, ps::Error>>()?;
        self.execute(statement_id, params).await?.try_into()
    }

    async fn on_close(&mut self, _statement_id: u32) -> Result<(), ps::Error> {
        Ok(())
    }
}

/// A simple wrapper around a request parameter `psql_srv::Value` reference, facilitiating
/// conversion to `DataType`.
struct ParamRef<'a>(&'a ps::Value);

impl TryFrom<ParamRef<'_>> for DataType {
    type Error = ps::Error;

    fn try_from(v: ParamRef) -> Result<Self, Self::Error> {
        match v.0 {
            ps::Value::Null => Ok(DataType::None),
            ps::Value::Bool(_) => Err(ps::Error::Unsupported("Value Bool".to_string())),
            ps::Value::Char(v) => Ok(DataType::Text(v.clone())),
            ps::Value::Varchar(v) => Ok(DataType::Text(v.clone())),
            ps::Value::Int(v) => Ok((*v).into()),
            ps::Value::Bigint(v) => Ok((*v).into()),
            ps::Value::Smallint(_) => Err(ps::Error::Unsupported("Value Smallint".to_string())),
            ps::Value::Double(v) => Ok((*v).into()),
            ps::Value::Real(v) => Ok((*v).into()),
            ps::Value::Text(v) => Ok(DataType::Text(v.clone())),
            ps::Value::Timestamp(v) => Ok((*v).into()),
        }
    }
}
