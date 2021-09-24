use crate::utils;
use chrono::NaiveDateTime;
use neon::{prelude::*, types::JsDate};
use noria::ColumnSchema;
use noria::{results::Results, DataType};
use noria_client::backend::{PrepareResult, QueryResult, SelectSchema, UpstreamPrepare};
use noria_mysql::{Error, MySqlUpstream};
use std::convert::TryFrom;

pub(crate) fn convert_error<'a, C>(cx: &mut C, e: Error) -> NeonResult<Handle<'a, JsError>>
where
    C: Context<'a>,
{
    cx.error(e.to_string())
}

fn convert_cols<'a, C>(cx: &mut C, cols: Vec<ColumnSchema>) -> NeonResult<Handle<'a, JsArray>>
where
    C: Context<'a>,
{
    let js_cols = cx.empty_array();
    for (i, col) in cols.iter().enumerate() {
        let js_col = cx.empty_object();
        if let Some(table) = &col.spec.column.table {
            // TODO: this table name is NOT always the expected user-friendly name....
            utils::set_str_field(cx, &js_col, "tableName", table)?;
        }
        utils::set_str_field(cx, &js_col, "columnName", &col.spec.column.name)?;
        // TODO: deal with colType and flags?
        js_cols.set(cx, i as u32, js_col)?;
    }
    Ok(js_cols)
}

pub(crate) fn convert_prepare_result<'a, C>(
    cx: &mut C,
    raw_prepare_result: PrepareResult<MySqlUpstream>,
) -> NeonResult<Handle<'a, JsObject>>
where
    C: Context<'a>,
{
    use noria_client::backend::noria_connector::PrepareResult::*;

    let js_prepare_result = cx.empty_object();
    match raw_prepare_result {
        PrepareResult::Noria(Select {
            statement_id,
            params,
            schema,
        })
        | PrepareResult::Noria(Insert {
            statement_id,
            params,
            schema,
        }) => {
            utils::set_num_field(cx, &js_prepare_result, "statementId", statement_id as f64)?;
            let js_params = convert_cols(cx, params)?;
            utils::set_jsval_field(
                cx,
                &js_prepare_result,
                "params",
                js_params.upcast::<JsValue>(),
            )?;
            let js_schema = convert_cols(cx, schema)?;
            utils::set_jsval_field(
                cx,
                &js_prepare_result,
                "schema",
                js_schema.upcast::<JsValue>(),
            )?;
        }
        PrepareResult::Noria(Update {
            statement_id,
            params,
        }) => {
            utils::set_num_field(cx, &js_prepare_result, "statementId", statement_id as f64)?;
            let js_params = convert_cols(cx, params)?;
            utils::set_jsval_field(
                cx,
                &js_prepare_result,
                "params",
                js_params.upcast::<JsValue>(),
            )?;
        }
        PrepareResult::Upstream(UpstreamPrepare { statement_id, .. }) => {
            utils::set_num_field(cx, &js_prepare_result, "statementId", statement_id as f64)?;
        }
    }
    Ok(js_prepare_result)
}

fn convert_datum<'a, C>(cx: &mut C, d: &DataType) -> NeonResult<Handle<'a, JsValue>>
where
    C: Context<'a>,
{
    match d {
        DataType::None => Ok(cx.null().upcast::<JsValue>()),
        DataType::Int(n) => Ok(cx.number(*n).upcast::<JsValue>()),
        DataType::UnsignedInt(n) => Ok(cx.number(*n).upcast::<JsValue>()),
        DataType::BigInt(n) => Ok(cx.number(*n as f64).upcast::<JsValue>()),
        DataType::UnsignedBigInt(n) => Ok(cx.number(*n as f64).upcast::<JsValue>()),
        DataType::Float(float, _) => Ok(cx.number(*float).upcast::<JsValue>()),
        DataType::Double(double, _) => Ok(cx.number(*double).upcast::<JsValue>()),
        DataType::Text(_) => {
            let s = String::try_from(d).or_else(|e| cx.throw_error(e.to_string()))?;
            Ok(cx.string(s).upcast::<JsValue>())
        }
        DataType::TinyText(_) => {
            let s = String::try_from(d).or_else(|e| cx.throw_error(e.to_string()))?;
            Ok(cx.string(s).upcast::<JsValue>())
        }
        // NOTE: making a js date object from the naive timestamp *assuming* it was created in the *UTC* timezone
        DataType::Timestamp(t) => Ok(cx
            .date(t.timestamp_millis() as f64)
            .or_else(|e| cx.throw_error(e.to_string()))?
            .upcast::<JsValue>()),
        DataType::Time(_) => unimplemented!("Time conversion to JS type"),
        DataType::ByteArray(bytes) => {
            Ok(JsArrayBuffer::external(cx, bytes.as_ref().clone()).upcast::<JsValue>())
        }
    }
}

fn convert_data<'a, C>(
    cx: &mut C,
    data: Vec<Results>,
    select_schema: &SelectSchema,
) -> NeonResult<Handle<'a, JsArray>>
where
    C: Context<'a>,
{
    let js_data = cx.empty_array();
    let col_names = &select_schema.columns;
    for resultsets in data {
        let mut row_num = 0;
        for row_struct in resultsets {
            let row_vec: Vec<_> = row_struct.into();
            let js_current_row = cx.empty_object();
            for (i, col_name) in col_names.iter().enumerate() {
                if select_schema.use_bogo && col_name == "bogokey" {
                    continue;
                }
                let js_datum = convert_datum(cx, &row_vec[i])?;
                utils::set_jsval_field(cx, &js_current_row, col_name, js_datum)?;
                js_data.set(cx, row_num as u32, js_current_row)?;
            }
            row_num += 1;
        }
    }
    Ok(js_data)
}

pub(crate) fn convert_query_result<'a, C>(
    cx: &mut C,
    raw_query_result: QueryResult<MySqlUpstream>,
) -> NeonResult<Handle<'a, JsObject>>
where
    C: Context<'a>,
{
    use noria_client::backend::noria_connector::QueryResult as NoriaResult;

    let js_query_result = cx.empty_object();
    match raw_query_result {
        QueryResult::Noria(NoriaResult::CreateTable | NoriaResult::CreateView) => (),
        QueryResult::Noria(NoriaResult::Insert {
            num_rows_inserted,
            first_inserted_id,
        }) => {
            utils::set_num_field(
                cx,
                &js_query_result,
                "numRowsInserted",
                num_rows_inserted as f64,
            )?;
            utils::set_num_field(
                cx,
                &js_query_result,
                "firstInsertedId",
                first_inserted_id as f64,
            )?;
        }
        QueryResult::Noria(NoriaResult::Select {
            data,
            select_schema,
        }) => {
            let js_data = convert_data(cx, data, &select_schema)?;
            utils::set_jsval_field(cx, &js_query_result, "data", js_data.upcast::<JsValue>())?;
            // TODO: convert select_schema?
        }
        QueryResult::Noria(NoriaResult::Update {
            num_rows_updated,
            last_inserted_id,
        }) => {
            utils::set_num_field(
                cx,
                &js_query_result,
                "numRowsUpdated",
                num_rows_updated as f64,
            )?;
            utils::set_num_field(
                cx,
                &js_query_result,
                "lastInsertedId",
                last_inserted_id as f64,
            )?;
        }
        QueryResult::Noria(NoriaResult::Delete { num_rows_deleted }) => {
            utils::set_num_field(
                cx,
                &js_query_result,
                "numRowsDeleted",
                num_rows_deleted as f64,
            )?;
        }
        QueryResult::Upstream(noria_mysql::QueryResult::WriteResult {
            num_rows_affected,
            last_inserted_id,
            ..
        }) => {
            utils::set_num_field(
                cx,
                &js_query_result,
                "numRowsAffected",
                num_rows_affected as f64,
            )?;
            utils::set_num_field(
                cx,
                &js_query_result,
                "lastInsertedId",
                last_inserted_id as f64,
            )?;
        }
        // TODO(Dan): Implement
        QueryResult::Upstream(..) => {
            unimplemented!("Query fallback processing not implemented for js-client")
        }
    }
    Ok(js_query_result)
}

pub(crate) fn convert_param<'a, C>(cx: &mut C, js_param: &Handle<JsValue>) -> NeonResult<DataType>
where
    C: Context<'a>,
{
    if js_param.is_a::<JsNull, _>(cx) {
        Ok(DataType::None)
    } else if js_param.is_a::<JsString, _>(cx) {
        DataType::try_from(js_param.downcast_or_throw::<JsString, _>(cx)?.value(cx))
            .or_else(|e| cx.throw_error(e.to_string()))
    } else if js_param.is_a::<JsNumber, _>(cx) {
        DataType::try_from(js_param.downcast_or_throw::<JsNumber, _>(cx)?.value(cx))
            .or_else(|e| cx.throw_error(e.to_string()))
    } else if js_param.is_a::<JsBoolean, _>(cx) {
        Ok(DataType::from(
            js_param.downcast_or_throw::<JsBoolean, _>(cx)?.value(cx),
        ))
    } else if js_param.is_a::<JsDate, _>(cx) {
        // NOTE: a neon developer confirmed the `value` function on a JsDate always gives milliseconds in UTC
        // because it returns the ECMAScript *time value* defined here: https://262.ecma-international.org/11.0/#sec-time-values-and-time-range
        let millis = js_param.downcast_or_throw::<JsDate, _>(cx)?.value(cx);
        if f64::is_nan(millis) {
            return cx.throw_error("Date input must be a valid javascript date");
        }
        Ok(DataType::Timestamp(NaiveDateTime::from_timestamp(
            millis as i64 / 1000i64,
            ((millis as i64 % 1000i64) * 1000000i64) as u32,
        )))
    } else {
        cx.throw_error("Unknown parameter type")
    }
}
