use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use clap::ValueEnum;
use eui48::MacAddressFormat;
use postgres_types::{Oid, Type};
use ps::{PsqlValue, TransferFormat};
use psql_srv as ps;
use readyset_adapter::backend as cl;
use readyset_adapter::upstream_database::LazyUpstream;
use readyset_adapter_types::{DeallocateId, PreparedStatementType};
use readyset_data::DfValue;
use readyset_util::redacted::RedactedString;
use thiserror::Error;
use tokio_postgres::SimpleQueryMessage;

use crate::error::Error;
use crate::query_handler::PostgreSqlQueryHandler;
use crate::response::{PrepareResponse, QueryResponse};
use crate::resultset::Resultset;
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
    inner: cl::Backend<LazyUpstream<PostgreSqlUpstream>, PostgreSqlQueryHandler>,
    authentication_method: AuthenticationMethod,
}

impl Backend {
    pub fn new(
        inner: cl::Backend<LazyUpstream<PostgreSqlUpstream>, PostgreSqlQueryHandler>,
    ) -> Self {
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
    type Target = cl::Backend<LazyUpstream<PostgreSqlUpstream>, PostgreSqlQueryHandler>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Backend {
    async fn query<'a>(&'a mut self, query: &'a str) -> Result<QueryResponse<'a>, Error> {
        Ok(QueryResponse(self.inner.query(query).await?))
    }

    async fn simple_query_upstream<'a>(
        &'a mut self,
        query: &'a str,
    ) -> Result<QueryResponse<'a>, Error> {
        Ok(QueryResponse(
            self.inner.simple_query_upstream(query).await?,
        ))
    }

    async fn prepare(
        &mut self,
        query: &str,
        parameter_data_types: &[Type],
        statement_type: PreparedStatementType,
    ) -> Result<PrepareResponse<'_>, Error> {
        Ok(PrepareResponse(
            self.inner
                .prepare(query, parameter_data_types, statement_type)
                .await?,
        ))
    }

    async fn execute(
        &mut self,
        id: u32,
        params: &[DfValue],
        result_transfer_formats: &[TransferFormat],
    ) -> Result<QueryResponse<'_>, Error> {
        Ok(QueryResponse(
            self.inner
                .execute(id, params, result_transfer_formats)
                .await?,
        ))
    }
}

impl ps::PsqlBackend for Backend {
    type Resultset = Resultset;

    fn version(&self) -> String {
        self.inner.version()
    }

    fn credentials_for_user(&self, user: &str) -> Option<ps::Credentials> {
        self.users
            .get(user)
            .map(|pw| ps::Credentials::CleartextPassword(pw))
    }

    async fn set_auth_info(&mut self, user: &str, password: Option<RedactedString>) {
        if let Some(password) = password {
            let _ = self.inner.set_user(user, password).await;
        }
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
        statement_type: PreparedStatementType,
    ) -> Result<ps::PrepareResponse, ps::Error> {
        self.prepare(query, parameter_data_types, statement_type)
            .await?
            .try_into_ps()
    }

    async fn on_execute(
        &mut self,
        statement_id: u32,
        params: &[PsqlValue],
        result_transfer_formats: &[TransferFormat],
    ) -> Result<ps::QueryResponse<Resultset>, ps::Error> {
        let params = params
            .iter()
            .map(|p| ParamRef(p).try_into())
            .collect::<Result<Vec<DfValue>, ps::Error>>()?;
        self.execute(statement_id, &params, result_transfer_formats)
            .await?
            .try_into()
    }

    async fn on_close(&mut self, statement_id: DeallocateId) -> Result<(), ps::Error> {
        self.inner.remove_statement(statement_id).await?;
        Ok(())
    }

    fn in_transaction(&self) -> bool {
        self.inner.in_transaction()
    }

    /// Loads any extended types from the upstream postgres, returning a map of Oid to typelen
    async fn load_extended_types(&mut self) -> Result<HashMap<Oid, i16>, ps::Error> {
        let err = |m| {
            ps::Error::InternalError(format!(
                "failed while loading extended type information: {m}"
            ))
        };

        let response = self
            .simple_query_upstream("select oid, typlen from pg_catalog.pg_type")
            .await?
            .try_into()?;

        match response {
            ps::QueryResponse::SimpleQuery(r) => r
                .into_iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(row) => Some(row),
                    _ => None,
                })
                .map(|row| match (row.get(0), row.get(1)) {
                    (Some(oid), Some(typlen)) => Ok((
                        oid.parse().map_err(|_| err("could not parse oid"))?,
                        typlen.parse().map_err(|_| err("could not parse typlen"))?,
                    )),
                    _ => Err(err("wrong number of columns returned from upstream")),
                })
                .collect(),
            _ => Err(err("wrong query response type")),
        }
    }
}

/// A simple wrapper around a request parameter `psql_srv::PsqlValue` reference, facilitiating
/// conversion to `DfValue`.
pub struct ParamRef<'a>(pub &'a PsqlValue);

impl TryFrom<ParamRef<'_>> for DfValue {
    type Error = ps::Error;

    fn try_from(v: ParamRef) -> Result<Self, Self::Error> {
        match v.0 {
            PsqlValue::Null => Ok(DfValue::None),
            PsqlValue::Bool(b) => Ok(DfValue::from(*b)),
            PsqlValue::BpChar(v)
            | PsqlValue::VarChar(v)
            | PsqlValue::Name(v)
            | PsqlValue::Text(v) => Ok(v.as_str().into()),
            PsqlValue::Char(v) => Ok((*v).into()),
            PsqlValue::Int(v) => Ok((*v).into()),
            PsqlValue::BigInt(v) => Ok((*v).into()),
            PsqlValue::SmallInt(v) => Ok((*v).into()),
            PsqlValue::Oid(v) => Ok((*v).into()),
            PsqlValue::Double(v) => DfValue::try_from(*v)
                .map_err(|_| ps::Error::Unsupported(format!("f64 with value `{v}`"))),
            PsqlValue::Float(v) => DfValue::try_from(*v)
                .map_err(|_| ps::Error::Unsupported(format!("f32 with value `{v}`"))),
            PsqlValue::Numeric(d) => Ok(DfValue::from(d.clone())),
            PsqlValue::Timestamp(v) => Ok((*v).into()),
            PsqlValue::TimestampTz(v) => Ok(DfValue::from(*v)),
            PsqlValue::Date(v) => Ok((*v).into()),
            PsqlValue::Time(v) => Ok((*v).into()),
            PsqlValue::TinyText(v) => Ok(v.as_str().into()),
            PsqlValue::ByteArray(b) => Ok(DfValue::ByteArray(Arc::new(b.clone()))),
            PsqlValue::MacAddress(m) => Ok(DfValue::from(m.to_string(MacAddressFormat::HexString))),
            PsqlValue::Inet(ip) => Ok(DfValue::from(ip.to_string())),
            PsqlValue::Uuid(uuid) => Ok(DfValue::from(uuid.to_string())),
            PsqlValue::Json(v) | PsqlValue::Jsonb(v) => Ok(DfValue::from(v.to_string())),
            PsqlValue::Bit(bits) | PsqlValue::VarBit(bits) => Ok(DfValue::from(bits.clone())),
            PsqlValue::Array(arr, _) => Ok(DfValue::from(arr.clone())),
            PsqlValue::PassThrough(p) => Ok(DfValue::PassThrough(Arc::new(p.clone()))),
        }
    }
}
