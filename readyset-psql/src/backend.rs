use std::convert::{TryFrom, TryInto};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use clap::ValueEnum;
use eui48::MacAddressFormat;
use postgres_types::Type;
use psql_srv as ps;
use readyset_adapter::backend as cl;
use readyset_data::DfValue;
use thiserror::Error;

use crate::error::Error;
use crate::query_handler::PostgreSqlQueryHandler;
use crate::response::{PrepareResponse, QueryResponse};
use crate::resultset::Resultset;
use crate::row::Row;
use crate::value::TypedDfValue;
use crate::PostgreSqlUpstream;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum AuthenticationMethod {
    Cleartext,

    #[value(name = "scram-sha-256")]
    #[default]
    ScramSha256,
}

#[derive(Debug, Error, Clone, Copy)]
#[error("Invalid authentication method")]
pub struct InvalidAuthenticationMethod;

impl FromStr for AuthenticationMethod {
    type Err = InvalidAuthenticationMethod;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cleartext" => Ok(Self::Cleartext),
            "scram-sha-256" => Ok(Self::ScramSha256),
            _ => Err(InvalidAuthenticationMethod),
        }
    }
}

/// A `noria_client` `Backend` wrapper that implements `psql_srv::Backend`. PostgreSQL client
/// requests provided to `psql_srv::Backend` trait function implementations are forwared to the
/// wrapped `noria_client` `Backend`. All request parameters and response results are forwarded
/// using type conversion.
pub struct Backend {
    inner: cl::Backend<PostgreSqlUpstream, PostgreSqlQueryHandler>,
    authentication_method: AuthenticationMethod,
}

impl Backend {
    pub fn new(inner: cl::Backend<PostgreSqlUpstream, PostgreSqlQueryHandler>) -> Self {
        Self {
            inner,
            authentication_method: Default::default(),
        }
    }

    pub fn with_authentication_method(self, authentication_method: AuthenticationMethod) -> Self {
        Self {
            authentication_method,
            ..self
        }
    }
}

impl Deref for Backend {
    type Target = cl::Backend<PostgreSqlUpstream, PostgreSqlQueryHandler>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Backend {
    async fn query<'a>(&'a mut self, query: &'a str) -> Result<QueryResponse<'_>, Error> {
        Ok(QueryResponse(self.inner.query(query).await?))
    }

    async fn prepare(
        &mut self,
        query: &str,
        parameter_data_types: &[Type],
    ) -> Result<PrepareResponse<'_>, Error> {
        Ok(PrepareResponse(
            self.inner.prepare(query, parameter_data_types).await?,
        ))
    }

    async fn execute(&mut self, id: u32, params: &[DfValue]) -> Result<QueryResponse<'_>, Error> {
        Ok(QueryResponse(self.inner.execute(id, params).await?))
    }
}

#[async_trait]
impl ps::PsqlBackend for Backend {
    type Value = TypedDfValue;
    type Row = Row;
    type Resultset = Resultset;

    fn version(&self) -> String {
        self.inner.version()
    }

    fn credentials_for_user(&self, user: &str) -> Option<ps::Credentials> {
        self.users
            .get(user)
            .map(|pw| ps::Credentials::CleartextPassword(pw))
    }

    async fn on_init(&mut self, _database: &str) -> Result<ps::CredentialsNeeded, ps::Error> {
        if self.does_require_authentication() {
            match self.authentication_method {
                AuthenticationMethod::Cleartext => Ok(ps::CredentialsNeeded::Cleartext),
                AuthenticationMethod::ScramSha256 => Ok(ps::CredentialsNeeded::ScramSha256),
            }
        } else {
            Ok(ps::CredentialsNeeded::None)
        }
    }

    async fn on_query(&mut self, query: &str) -> Result<ps::QueryResponse<Resultset>, ps::Error> {
        self.query(query).await?.try_into()
    }

    async fn on_prepare(
        &mut self,
        query: &str,
        parameter_data_types: &[Type],
    ) -> Result<ps::PrepareResponse, ps::Error> {
        let statement_id = self.next_prepared_id(); // If prepare succeeds it will get this id
        self.prepare(query, parameter_data_types)
            .await?
            .try_into_ps(statement_id)
    }

    async fn on_execute(
        &mut self,
        statement_id: u32,
        params: &[ps::Value],
    ) -> Result<ps::QueryResponse<Resultset>, ps::Error> {
        let params = params
            .iter()
            .map(|p| ParamRef(p).try_into())
            .collect::<Result<Vec<DfValue>, ps::Error>>()?;
        self.execute(statement_id, &params).await?.try_into()
    }

    async fn on_close(&mut self, _statement_id: u32) -> Result<(), ps::Error> {
        Ok(())
    }
}

/// A simple wrapper around a request parameter `psql_srv::Value` reference, facilitiating
/// conversion to `DfValue`.
pub struct ParamRef<'a>(pub &'a ps::Value);

impl TryFrom<ParamRef<'_>> for DfValue {
    type Error = ps::Error;

    fn try_from(v: ParamRef) -> Result<Self, Self::Error> {
        match v.0 {
            ps::Value::Null => Ok(DfValue::None),
            ps::Value::Bool(b) => Ok(DfValue::from(*b)),
            ps::Value::BpChar(v)
            | ps::Value::VarChar(v)
            | ps::Value::Name(v)
            | ps::Value::Text(v) => Ok(v.as_str().into()),
            ps::Value::Char(v) => Ok((*v).into()),
            ps::Value::Int(v) => Ok((*v).into()),
            ps::Value::BigInt(v) => Ok((*v).into()),
            ps::Value::SmallInt(v) => Ok((*v).into()),
            ps::Value::Oid(v) => Ok((*v).into()),
            ps::Value::Double(v) => DfValue::try_from(*v)
                .map_err(|_| ps::Error::Unsupported(format!("f64 with value `{}`", v))),
            ps::Value::Float(v) => DfValue::try_from(*v)
                .map_err(|_| ps::Error::Unsupported(format!("f32 with value `{}`", v))),
            ps::Value::Numeric(d) => Ok(DfValue::from(*d)),
            ps::Value::Timestamp(v) => Ok((*v).into()),
            ps::Value::TimestampTz(v) => Ok(DfValue::from(*v)),
            ps::Value::Date(v) => Ok((*v).into()),
            ps::Value::Time(v) => Ok((*v).into()),
            ps::Value::ByteArray(b) => Ok(DfValue::ByteArray(Arc::new(b.clone()))),
            ps::Value::MacAddress(m) => Ok(DfValue::from(m.to_string(MacAddressFormat::HexString))),
            ps::Value::Inet(ip) => Ok(DfValue::from(ip.to_string())),
            ps::Value::Uuid(uuid) => Ok(DfValue::from(uuid.to_string())),
            ps::Value::Json(v) | ps::Value::Jsonb(v) => Ok(DfValue::from(v.to_string())),
            ps::Value::Bit(bits) | ps::Value::VarBit(bits) => Ok(DfValue::from(bits.clone())),
            ps::Value::Array(arr, _) => Ok(DfValue::from(arr.clone())),
            ps::Value::PassThrough(p) => Ok(DfValue::PassThrough(Arc::new(p.clone()))),
        }
    }
}
