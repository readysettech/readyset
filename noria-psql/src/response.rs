use crate::resultset::Resultset;
use crate::schema::{MysqlSchema, SelectSchema};
use noria_client::backend as cl;
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
            MySqlPrepareWrite { statement_id } => Ok(ps::PrepareResponse {
                prepared_statement_id: statement_id,
                param_schema: vec![],
                row_schema: vec![],
            }),
        }
    }
}

/// A simple wrapper around `noria_client`'s `QueryResult`, facilitating conversion to
/// `psql_srv::QueryResponse`.
pub struct QueryResponse(pub cl::QueryResult);

impl TryFrom<QueryResponse> for ps::QueryResponse<Resultset> {
    type Error = ps::Error;

    fn try_from(r: QueryResponse) -> Result<Self, Self::Error> {
        use cl::QueryResult::*;
        use ps::QueryResponse::*;
        match r.0 {
            NoriaCreateTable => Ok(Command),
            NoriaCreateView => Ok(Command),
            NoriaInsert {
                num_rows_inserted, ..
            } => Ok(Insert(num_rows_inserted)),
            NoriaSelect {
                data,
                select_schema,
            } => {
                let select_schema = SelectSchema(select_schema);
                let resultset = Resultset::try_new(data, &select_schema)?;
                Ok(Select {
                    schema: select_schema.try_into()?,
                    resultset,
                })
            }
            NoriaUpdate {
                num_rows_updated, ..
            } => Ok(Update(num_rows_updated)),
            NoriaDelete { num_rows_deleted } => Ok(Delete(num_rows_deleted)),
            MySqlWrite { .. } => Err(ps::Error::Unimplemented(
                "QueryResult::MySqlWrite not handled in psql_backend".to_string(),
            )),
            MySqlSelect { .. } => Err(ps::Error::Unimplemented(
                "QueryResult::MySqlSelect not handled in psql_backend".to_string(),
            )),
        }
    }
}
