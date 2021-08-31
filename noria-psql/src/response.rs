use crate::resultset::Resultset;
use crate::schema::{NoriaSchema, SelectSchema};
use crate::upstream;
use noria_client::backend::noria_connector;
use noria_client::backend::{self as cl, UpstreamPrepare};
use psql_srv as ps;
use std::convert::TryFrom;

use crate::PostgreSqlUpstream;

/// A simple wrapper around `noria_client`'s `PrepareResult`, facilitating conversion to
/// `psql_srv::PrepareResponse`.
pub struct PrepareResponse(pub cl::PrepareResult<PostgreSqlUpstream>);

impl TryFrom<PrepareResponse> for ps::PrepareResponse {
    type Error = ps::Error;

    fn try_from(r: PrepareResponse) -> Result<Self, Self::Error> {
        use cl::PrepareResult::*;
        use noria_client::backend::noria_connector::PrepareResult::*;

        match r.0 {
            Noria(Select {
                statement_id,
                params,
                schema,
            }) => Ok(ps::PrepareResponse {
                prepared_statement_id: statement_id,
                param_schema: NoriaSchema(params).into(),
                row_schema: NoriaSchema(schema).into(),
            }),
            Noria(Insert {
                statement_id,
                params,
                schema,
            }) => Ok(ps::PrepareResponse {
                prepared_statement_id: statement_id,
                param_schema: NoriaSchema(params).into(),
                row_schema: NoriaSchema(schema).into(),
            }),
            Noria(Update {
                statement_id,
                params,
            }) => Ok(ps::PrepareResponse {
                // NOTE u32::try_from is used because NoriaPrepareUpdate's statement_id has a
                // non-standard u64 data type.
                prepared_statement_id: u32::try_from(statement_id)?,
                param_schema: NoriaSchema(params).into(),
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
pub struct QueryResponse(pub cl::QueryResult<PostgreSqlUpstream>);

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
                    schema: select_schema.into(),
                    resultset,
                })
            }
            Noria(NoriaResult::Update {
                num_rows_updated, ..
            }) => Ok(Update(num_rows_updated)),
            Noria(NoriaResult::Delete { num_rows_deleted }) => Ok(Delete(num_rows_deleted)),
            Upstream(upstream::QueryResult::ReadResult { data: _rows }) => {
                // TODO(grfn): Implement this
                Err(ps::Error::Unimplemented(
                    "Handling of pgsql select results not yet implemented".to_string(),
                ))
            }
            Upstream(upstream::QueryResult::WriteResult { num_rows_affected }) => {
                Ok(Insert(num_rows_affected))
            }
            Upstream(upstream::QueryResult::None) => Ok(Command),
        }
    }
}
