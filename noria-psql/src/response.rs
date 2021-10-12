use noria_client::backend::noria_connector;
use noria_client::backend::{self as cl, UpstreamPrepare};
use psql_srv as ps;
use std::convert::{TryFrom, TryInto};
use upstream::StatementMeta;

use crate::resultset::Resultset;
use crate::schema::{NoriaSchema, SelectSchema};
use crate::upstream;
use crate::PostgreSqlUpstream;
use psql_srv::Column;

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
                param_schema: NoriaSchema(params).try_into()?,
                row_schema: NoriaSchema(schema).try_into()?,
            }),
            Noria(Insert {
                statement_id,
                params,
                schema,
            }) => Ok(ps::PrepareResponse {
                prepared_statement_id: statement_id,
                param_schema: NoriaSchema(params).try_into()?,
                row_schema: NoriaSchema(schema).try_into()?,
            }),
            Noria(
                Update {
                    statement_id,
                    params,
                }
                | Delete {
                    statement_id,
                    params,
                },
            ) => Ok(ps::PrepareResponse {
                prepared_statement_id: statement_id,
                param_schema: NoriaSchema(params).try_into()?,
                row_schema: vec![],
            }),
            Upstream(UpstreamPrepare {
                statement_id,
                meta: StatementMeta { params, schema },
                ..
            }) => Ok(ps::PrepareResponse {
                prepared_statement_id: statement_id,
                param_schema: params,
                row_schema: schema,
            }),
        }
    }
}

/// A simple wrapper around `noria_client`'s `QueryResult`, facilitating conversion to
/// `psql_srv::QueryResponse`.
pub struct QueryResponse<'a>(pub cl::QueryResult<'a, PostgreSqlUpstream>);

impl<'a> TryFrom<QueryResponse<'a>> for ps::QueryResponse<Resultset> {
    type Error = ps::Error;

    fn try_from(r: QueryResponse<'a>) -> Result<Self, Self::Error> {
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
            Upstream(upstream::QueryResult::Read { data: rows }) => {
                let schema = match rows.first() {
                    None => vec![],
                    Some(row) => row
                        .columns()
                        .iter()
                        .map(|c| Column {
                            name: c.name().to_owned(),
                            col_type: c.type_().clone(),
                        })
                        .collect(),
                };
                Ok(ps::QueryResponse::Select {
                    schema,
                    resultset: Resultset::try_from(rows)?,
                })
            }
            Upstream(upstream::QueryResult::Write { num_rows_affected }) => {
                Ok(Insert(num_rows_affected))
            }
            Upstream(upstream::QueryResult::Command) => Ok(Command),
        }
    }
}
