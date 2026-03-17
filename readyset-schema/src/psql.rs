use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use tokio_postgres::types::Type;

use psql_srv::{Column, PsqlValue};
use readyset_data::Text;
use readyset_sql::ast::SqlIdentifier;

use crate::ReadysetSchemaResult;

/// Convert an Arrow `DataType` to the corresponding PostgreSQL `Type`.
fn arrow_to_psql_type(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::UInt32 => Type::INT8,
        DataType::UInt64 => Type::INT8,
        DataType::Binary => Type::BYTEA,
        DataType::Utf8 => Type::TEXT,
        _ => Type::TEXT,
    }
}

pub fn extract_columns(result: &ReadysetSchemaResult) -> Vec<Column> {
    result
        .schema()
        .iter()
        .map(|field| Column::Column {
            name: SqlIdentifier::from(field.name().as_str()),
            col_type: arrow_to_psql_type(field.data_type()),
            table_oid: None,
            attnum: None,
        })
        .collect()
}

pub fn extract_types(result: &ReadysetSchemaResult) -> Vec<Type> {
    result
        .schema()
        .iter()
        .map(|col| arrow_to_psql_type(col.data_type()))
        .collect()
}

/// Convert a single cell from an Arrow array at the given row index into a `PsqlValue`.
pub fn to_psql_value(array: &dyn Array, row: usize) -> PsqlValue {
    if array.is_null(row) {
        return PsqlValue::Null;
    }

    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        PsqlValue::Bool(a.value(row))
    } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        PsqlValue::Int(a.value(row))
    } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        PsqlValue::BigInt(a.value(row))
    } else if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
        PsqlValue::BigInt(i64::from(a.value(row)))
    } else if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        let v = a.value(row);
        match i64::try_from(v) {
            Ok(i) => PsqlValue::BigInt(i),
            Err(_) => PsqlValue::Text(Text::from(v.to_string().as_str())),
        }
    } else if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        PsqlValue::Float(a.value(row))
    } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        PsqlValue::Double(a.value(row))
    } else if let Some(a) = array.as_any().downcast_ref::<BinaryArray>() {
        PsqlValue::ByteArray(a.value(row).to_vec())
    } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        PsqlValue::Text(Text::from(a.value(row)))
    } else {
        // Fallback: render via Arrow's Display as text
        let s = array_value_to_string(array, row).unwrap_or_else(|_| "?".to_string());
        PsqlValue::Text(Text::from(s.as_str()))
    }
}
