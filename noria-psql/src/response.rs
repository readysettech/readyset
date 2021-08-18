use crate::resultset::Resultset;
use crate::schema::{MysqlSchema, SelectSchema};
use noria_client::backend::noria_connector;
use noria_client::backend::postgresql_connector::PostgreSqlConnector;
use noria_client::backend::{self as cl, UpstreamPrepare};
use psql_srv as ps;
use std::convert::{TryFrom, TryInto};

/// A simple wrapper around `noria_client`'s `PrepareResult`, facilitating conversion to
/// `psql_srv::PrepareResponse`.
pub struct PrepareResponse(pub cl::PrepareResult);

impl TryFrom<PrepareResponse> for ps::PrepareResponse {
    type Error = ps::Error;

    fn try_from(r: PrepareResponse) -> Result<Self, Self::Error> {
        use cl::PrepareResult::*;
        match r.0 {
            NoriaPrepareSelect {
                statement_id,
                params,
                schema,
            } => Ok(ps::PrepareResponse {
                prepared_statement_id: statement_id,
                param_schema: MysqlSchema(params).try_into()?,
                row_schema: MysqlSchema(schema).try_into()?,
            }),
            NoriaPrepareInsert {
                statement_id,
                params,
                schema,
            } => Ok(ps::PrepareResponse {
                prepared_statement_id: statement_id,
                param_schema: MysqlSchema(params).try_into()?,
                row_schema: MysqlSchema(schema).try_into()?,
            }),
            NoriaPrepareUpdate {
                statement_id,
                params,
            } => Ok(ps::PrepareResponse {
                // NOTE u32::try_from is used because NoriaPrepareUpdate's statement_id has a
                // non-standard u64 data type.
                prepared_statement_id: u32::try_from(statement_id)?,
                param_schema: MysqlSchema(params).try_into()?,
                row_schema: vec![],
            }),
            Upstream(UpstreamPrepare { statement_id, .. }) => Ok(ps::PrepareResponse {
                prepared_statement_id: statement_id,
                // TODO(grfn): Fill these in
                param_schema: vec![],
                row_schema: vec![],
            }),
        }
    }
}

/// A simple wrapper around `noria_client`'s `QueryResult`, facilitating conversion to
/// `psql_srv::QueryResponse`.
pub struct QueryResponse(pub cl::QueryResult<PostgreSqlConnector>);

impl TryFrom<QueryResponse> for ps::QueryResponse<Resultset> {
    type Error = ps::Error;

    fn try_from(r: QueryResponse) -> Result<Self, Self::Error> {
        use cl::QueryResult::*;
        use noria_connector::QueryResult as NoriaResult;
        use ps::QueryResponse::*;

        match r.0 {
            Noria(NoriaResult::CreateTable) => Ok(Command),
            Noria(NoriaResult::CreateView) => Ok(Command),
            Noria(NoriaResult::Insert {
                num_rows_inserted, ..
            }) => Ok(Insert(num_rows_inserted)),
            Noria(NoriaResult::Select {
                data,
                select_schema,
            }) => {
                let select_schema = SelectSchema(select_schema);
                let resultset = Resultset::try_new(data, &select_schema)?;
                Ok(Select {
                    schema: select_schema.try_into()?,
                    resultset,
                })
            }
            Noria(NoriaResult::Update {
                num_rows_updated, ..
            }) => Ok(Update(num_rows_updated)),
            Noria(NoriaResult::Delete { num_rows_deleted }) => Ok(Delete(num_rows_deleted)),
            UpstreamRead(_rows) => {
                // TODO(grfn): Implement this
                Err(ps::Error::Unimplemented(
                    "Handling of pgsql select results not yet implemented".to_string(),
                ))
            }
            UpstreamWrite(num_rows_affected) => Ok(Insert(num_rows_affected)),
        }
    }
}
