use std::fmt::Write;

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;

use mysql_srv::{Column, ColumnFlags, ColumnType};

use crate::ReadysetSchemaResult;

/// Convert an Arrow `DataType` to the corresponding MySQL `ColumnType`.
fn arrow_to_mysql_type(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Boolean => ColumnType::MYSQL_TYPE_BIT,
        DataType::Int32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::UInt32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::UInt64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Binary => ColumnType::MYSQL_TYPE_BLOB,
        DataType::Utf8 => ColumnType::MYSQL_TYPE_VAR_STRING,
        _ => ColumnType::MYSQL_TYPE_VAR_STRING,
    }
}

pub fn extract_columns(result: &ReadysetSchemaResult) -> Vec<Column> {
    result
        .schema()
        .iter()
        .map(|field| Column {
            table: String::new(),
            column: field.name().clone(),
            coltype: arrow_to_mysql_type(field.data_type()),
            column_length: 0,
            character_set: 0,
            colflags: ColumnFlags::empty(),
            decimals: 0,
        })
        .collect()
}

/// Convert a single cell from an Arrow array at the given row index into a value
/// that can be written via `RowWriter::write_col`.
///
/// Returns an `Option<String>` for null-safe writing: `None` maps to SQL NULL,
/// `Some(s)` maps to the string-encoded value. This works because `RowWriter::write_col`
/// accepts `Option<T>` where `T: ToMySqlValue`, and `String` implements `ToMySqlValue`.
pub fn to_mysql_value(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }

    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        Some(if a.value(row) { "1" } else { "0" }.to_string())
    } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        Some(a.value(row).to_string())
    } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(row).to_string())
    } else if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
        Some(a.value(row).to_string())
    } else if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        Some(a.value(row).to_string())
    } else if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        Some(a.value(row).to_string())
    } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        Some(a.value(row).to_string())
    } else if let Some(a) = array.as_any().downcast_ref::<BinaryArray>() {
        Some(a.value(row).iter().fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        }))
    } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        Some(a.value(row).to_string())
    } else {
        // Fallback: render via Arrow's Display
        Some(
            datafusion::arrow::util::display::array_value_to_string(array, row)
                .unwrap_or_else(|_| "?".to_string()),
        )
    }
}
