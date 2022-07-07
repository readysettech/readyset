use std::convert::{TryFrom, TryInto};
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use eui48::MacAddressFormat;
use noria_client::backend as cl;
use noria_data::DataType;
use psql_srv as ps;

use crate::error::Error;
use crate::query_handler::PostgreSqlQueryHandler;
use crate::response::{PrepareResponse, QueryResponse};
use crate::resultset::Resultset;
use crate::row::Row;
use crate::value::Value;
use crate::PostgreSqlUpstream;

/// A `noria_client` `Backend` wrapper that implements `psql_srv::Backend`. PostgreSQL client
/// requests provided to `psql_srv::Backend` trait function implementations are forwared to the
/// wrapped `noria_client` `Backend`. All request parameters and response results are forwarded
/// using type conversion.
pub struct Backend(pub cl::Backend<PostgreSqlUpstream, PostgreSqlQueryHandler>);

impl Deref for Backend {
    type Target = cl::Backend<PostgreSqlUpstream, PostgreSqlQueryHandler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Backend {
    async fn query(&mut self, query: &str) -> Result<QueryResponse<'_>, Error> {
        Ok(QueryResponse(self.0.query(query).await?))
    }

    async fn prepare(&mut self, query: &str) -> Result<PrepareResponse<'_>, Error> {
        Ok(PrepareResponse(self.0.prepare(query).await?))
    }

    async fn execute(&mut self, id: u32, params: &[DataType]) -> Result<QueryResponse<'_>, Error> {
        Ok(QueryResponse(self.0.execute(id, params).await?))
    }
}

#[async_trait]
impl ps::Backend for Backend {
    type Value = Value;
    type Row = Row;
    type Resultset = Resultset;

    const SERVER_VERSION: &'static str = "13.4 (ReadySet)";

    async fn on_init(&mut self, _database: &str) -> Result<(), ps::Error> {
        Ok(())
    }

    async fn on_query(&mut self, query: &str) -> Result<ps::QueryResponse<Resultset>, ps::Error> {
        self.query(query).await?.try_into()
    }

    async fn on_prepare(&mut self, query: &str) -> Result<ps::PrepareResponse, ps::Error> {
        let statement_id = self.next_prepared_id(); // If prepare succeeds it will get this id
        self.prepare(query).await?.try_into_ps(statement_id)
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
        self.execute(statement_id, &params).await?.try_into()
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
            ps::Value::Bool(b) => Ok(DataType::from(*b)),
            ps::Value::Varchar(v) | ps::Value::Name(v) | ps::Value::Text(v) => {
                Ok(v.as_str().into())
            }
            ps::Value::Char(v) => Ok((*v).into()),
            ps::Value::Int(v) => Ok((*v).into()),
            ps::Value::Bigint(v) => Ok((*v).into()),
            ps::Value::Smallint(v) => Ok((*v).into()),
            ps::Value::Oid(v) => Ok((*v).into()),
            ps::Value::Double(v) => DataType::try_from(*v)
                .map_err(|_| ps::Error::Unsupported(format!("f64 with value `{}`", v))),
            ps::Value::Float(v) => DataType::try_from(*v)
                .map_err(|_| ps::Error::Unsupported(format!("f32 with value `{}`", v))),
            ps::Value::Numeric(d) => Ok(DataType::from(*d)),
            ps::Value::Timestamp(v) => Ok((*v).into()),
            ps::Value::TimestampTz(v) => Ok(DataType::from(*v)),
            ps::Value::Date(v) => Ok((*v).into()),
            ps::Value::Time(v) => Ok((*v).into()),
            ps::Value::ByteArray(b) => Ok(DataType::ByteArray(Arc::new(b.clone()))),
            ps::Value::MacAddress(m) => {
                Ok(DataType::from(m.to_string(MacAddressFormat::HexString)))
            }
            ps::Value::Inet(ip) => Ok(DataType::from(ip.to_string())),
            ps::Value::Uuid(uuid) => Ok(DataType::from(uuid.to_string())),
            ps::Value::Json(v) | ps::Value::Jsonb(v) => Ok(DataType::from(v.to_string())),
            ps::Value::Bit(bits) | ps::Value::VarBit(bits) => Ok(DataType::from(bits.clone())),
            ps::Value::Array(arr, _) => Ok(DataType::from(arr.clone())),
        }
    }
}
