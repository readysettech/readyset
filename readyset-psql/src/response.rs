use std::borrow::Cow;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use psql_srv as ps;
use readyset_adapter::backend::{
    self as cl, SinglePrepareResult, UpstreamPrepare, noria_connector,
};
use readyset_adapter::upstream_database::LazyUpstream;
use readyset_adapter_types::ParsedCommand;
use readyset_client::ColumnSchema;
use readyset_client::results::{ResultIterator, Results};
use readyset_data::DfType;
use readyset_shallow::QueryMetadata;
use readyset_sql::ast::{self, SqlIdentifier};
use upstream::{CacheEntry, StatementMeta};

use crate::resultset::{Resultset, copy_simple_query_message};
use crate::schema::{NoriaSchema, SelectSchema};
use crate::{PostgreSqlUpstream, upstream};

/// A simple wrapper around `noria_client`'s `PrepareResult`, facilitating conversion to
/// `psql_srv::PrepareResponse`.
pub struct PrepareResponse<'a>(pub &'a cl::PrepareResult<LazyUpstream<PostgreSqlUpstream>>);

impl PrepareResponse<'_> {
    pub fn try_into_ps(self) -> Result<ps::PrepareResponse, ps::Error> {
        use readyset_adapter::backend::noria_connector::PrepareResult::*;
        use readyset_adapter::backend::noria_connector::{
            PreparedSelectTypes, SelectPrepareResultInner,
        };

        let prepared_statement_id = self.0.statement_id;
        match self.0.upstream_biased() {
            SinglePrepareResult::Noria(Select {
                types: PreparedSelectTypes::Schema(SelectPrepareResultInner { params, schema, .. }),
                ..
            }) => Ok(ps::PrepareResponse {
                prepared_statement_id,
                param_schema: NoriaSchema(params).try_into()?,
                row_schema: NoriaSchema(schema).try_into()?,
            }),
            SinglePrepareResult::Noria(Select {
                types: PreparedSelectTypes::NoSchema,
                ..
            }) => Err(ps::Error::InternalError("Unreachable".into())),
            SinglePrepareResult::Noria(Insert { params, schema, .. }) => Ok(ps::PrepareResponse {
                prepared_statement_id,
                param_schema: NoriaSchema(params).try_into()?,
                row_schema: NoriaSchema(schema).try_into()?,
            }),
            SinglePrepareResult::Noria(Update { params, .. } | Delete { params, .. }) => {
                Ok(ps::PrepareResponse {
                    prepared_statement_id,
                    param_schema: NoriaSchema(params).try_into()?,
                    row_schema: vec![],
                })
            }
            SinglePrepareResult::Noria(Set { .. }) => Ok(ps::PrepareResponse {
                prepared_statement_id,
                param_schema: vec![],
                row_schema: vec![],
            }),
            SinglePrepareResult::Upstream(UpstreamPrepare {
                meta: StatementMeta { params, schema },
                ..
            }) => Ok(ps::PrepareResponse {
                prepared_statement_id,
                param_schema: params.to_vec(),
                row_schema: schema.to_vec(),
            }),
        }
    }
}

/// A simple wrapper around `noria_client`'s `QueryResult`, facilitating conversion to
/// `psql_srv::QueryResponse`.
pub struct QueryResponse<'a>(pub cl::QueryResult<'a, LazyUpstream<PostgreSqlUpstream>>);

impl<'a> TryFrom<QueryResponse<'a>> for ps::QueryResponse<Resultset> {
    type Error = ps::Error;

    fn try_from(r: QueryResponse<'a>) -> Result<Self, Self::Error> {
        use cl::QueryResult::*;
        use noria_connector::QueryResult as NoriaResult;
        use ps::QueryResponse::*;

        match r.0 {
            Noria(NoriaResult::Empty) => Ok(Command("".to_string())),
            Noria(NoriaResult::Insert {
                num_rows_inserted, ..
            }) => Ok(Insert(num_rows_inserted)),
            Noria(NoriaResult::Select { rows, schema }) => {
                let select_schema = SelectSchema(schema);
                let resultset = Resultset::from_readyset(rows, &select_schema)?;
                Ok(Select {
                    schema: select_schema.try_into()?,
                    resultset,
                })
            }
            Noria(NoriaResult::Update {
                num_rows_updated, ..
            }) => Ok(Update(num_rows_updated)),
            Noria(NoriaResult::Delete { num_rows_deleted }) => Ok(Delete(num_rows_deleted)),
            Noria(NoriaResult::Meta(vars)) => {
                let columns = vars.iter().map(|v| v.name.clone()).collect::<Vec<_>>();

                let select_schema = SelectSchema(readyset_adapter::backend::SelectSchema {
                    schema: Cow::Owned(
                        vars.iter()
                            .map(|v| ColumnSchema {
                                column: ast::Column {
                                    name: v.name.clone(),
                                    table: None,
                                },
                                column_type: DfType::DEFAULT_TEXT,
                                base: None,
                            })
                            .collect(),
                    ),
                    columns: Cow::Owned(columns),
                });

                let resultset = Resultset::from_readyset(
                    ResultIterator::owned(vec![Results::new(vec![vars
                        .into_iter()
                        .map(|v| readyset_data::DfValue::from(v.value))
                        .collect()])]),
                    &select_schema,
                )?;
                Ok(Select {
                    schema: select_schema.try_into()?,
                    resultset,
                })
            }
            Noria(NoriaResult::MetaVariables(vars)) => {
                let select_schema = SelectSchema(readyset_adapter::backend::SelectSchema {
                    schema: Cow::Owned(vec![
                        ColumnSchema {
                            column: ast::Column {
                                name: "name".into(),
                                table: None,
                            },
                            column_type: DfType::DEFAULT_TEXT,
                            base: None,
                        },
                        ColumnSchema {
                            column: ast::Column {
                                name: "value".into(),
                                table: None,
                            },
                            column_type: DfType::DEFAULT_TEXT,
                            base: None,
                        },
                    ]),
                    columns: Cow::Owned(vec!["name".into(), "value".into()]),
                });
                let mut rows: Vec<Vec<readyset_data::DfValue>> = Vec::new();
                for v in vars {
                    rows.push(vec![v.name.as_str().into(), v.value.into()]);
                }

                let resultset = Resultset::from_readyset(
                    ResultIterator::owned(vec![Results::new(rows)]),
                    &select_schema,
                )?;
                Ok(Select {
                    schema: select_schema.try_into()?,
                    resultset,
                })
            }
            Noria(NoriaResult::MetaWithHeader(vars)) => {
                let (col1_header, col2_header): (SqlIdentifier, SqlIdentifier) =
                    (vars[0].name.clone(), vars[0].value.clone().into());
                let select_schema = SelectSchema(readyset_adapter::backend::SelectSchema {
                    schema: Cow::Owned(vec![
                        ColumnSchema {
                            column: ast::Column {
                                name: col1_header.clone(),
                                table: None,
                            },
                            column_type: DfType::DEFAULT_TEXT,
                            base: None,
                        },
                        ColumnSchema {
                            column: ast::Column {
                                name: col2_header.clone(),
                                table: None,
                            },
                            column_type: DfType::DEFAULT_TEXT,
                            base: None,
                        },
                    ]),
                    columns: Cow::Owned(vec![col1_header, col2_header]),
                });
                let mut rows: Vec<Vec<readyset_data::DfValue>> = Vec::new();
                for v in vars.into_iter().skip(1) {
                    rows.push(vec![v.name.as_str().into(), v.value.into()]);
                }

                let resultset = Resultset::from_readyset(
                    ResultIterator::owned(vec![Results::new(rows)]),
                    &select_schema,
                )?;
                Ok(Select {
                    schema: select_schema.try_into()?,
                    resultset,
                })
            }
            Shallow(result) => {
                let QueryMetadata::PostgreSql(metadata) = result.metadata.as_ref() else {
                    return Err(ps::Error::InternalError("wrong metadata for psql".to_string()));
                };

                if let Some(first) = result.values.first() {
                    match first {
                        CacheEntry::DfValue(_) => {
                            let rows = result
                                .values
                                .iter()
                                .map(|entry| match entry {
                                    CacheEntry::DfValue(vals) => vals.clone(),
                                    _ => unreachable!("mixed psql result format"),
                                })
                                .collect();

                            Ok(Select {
                                schema: metadata.schema.clone(),
                                resultset: Resultset::from_shallow(
                                    Arc::new(rows),
                                    metadata.types.clone(),
                                ),
                            })
                        }
                        CacheEntry::Simple(_) => {
                            let messages: Result<Vec<_>, _> = result
                                .values
                                .iter()
                                .map(|entry| match entry {
                                    CacheEntry::Simple(msg) => copy_simple_query_message(msg),
                                    _ => unreachable!("mixed psql result format"),
                                })
                                .collect();

                            Ok(SimpleQuery(messages?))
                        }
                    }
                } else {
                    Ok(Select {
                        schema: metadata.schema.clone(),
                        resultset: Resultset::empty(),
                    })
                }
            }
            Upstream(upstream::QueryResult::EmptyRead, cache) => {
                if let Some(mut cache) = cache {
                    let meta = QueryMetadata::PostgreSql(Default::default());
                    cache.set_metadata(meta);
                    cache.filled();
                }
                Ok(Select {
                    schema: Vec::new(),
                    resultset: Resultset::empty(),
                })
            }
            Upstream(upstream::QueryResult::Stream { first_row, stream }, cache) => {
                let field_types = first_row
                    .columns()
                    .iter()
                    .map(|c| c.type_().clone())
                    .collect();

                Ok(ps::QueryResponse::Select {
                    schema: vec![], // Schema isn't necessary for upstream execute results
                    resultset: Resultset::from_stream(stream, first_row, field_types, cache),
                })
            }
            Upstream(upstream::QueryResult::RowStream { first_row, stream }, cache) => {
                let field_types = first_row
                    .columns()
                    .iter()
                    .map(|c| c.type_().clone())
                    .collect();

                Ok(ps::QueryResponse::Select {
                    schema: vec![], // Schema isn't necessary for upstream execute results
                    resultset: Resultset::from_row_stream(stream, first_row, field_types, cache),
                })
            }
            Upstream(upstream::QueryResult::Write { num_rows_affected }, _) => {
                Ok(Insert(num_rows_affected))
            }
            Upstream(upstream::QueryResult::Command { tag }, _) => Ok(Command(tag)),
            Upstream(upstream::QueryResult::SimpleQueryStream {
                first_message,
                stream,
            }, cache) => Ok(ps::QueryResponse::Stream {
                resultset: Resultset::from_simple_query_stream(stream, first_message, cache),
            }),
            // We still use the SimpleQuery response for some upstream responses that are not
            // Selects
            Upstream(upstream::QueryResult::SimpleQuery(resp), _) => Ok(SimpleQuery(resp)),
            UpstreamBufferedInMemory(upstream::QueryResult::SimpleQuery(resp)) => Ok(SimpleQuery(resp)),
            UpstreamBufferedInMemory(..) => Err(ps::Error::InternalError(
                "Mismatched QueryResult for UpstreamBufferedInMemory response type: Expected SimpleQuery".to_string(),
            )),
            Parser(p) => match p {
                ParsedCommand::Deallocate(name) => Ok(ps::QueryResponse::Deallocate(name)),
            },
        }
    }
}
