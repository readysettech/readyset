use std::string::FromUtf8Error;
use std::sync::Arc;

use mysql_common::collations::{self, Collation, CollationId};
use mysql_srv::ColumnType;
use readyset_data::DfValue;

//TODO(marce): Make this a configuration parameter or dynamically adjust based on the table size
#[allow(dead_code)]
pub const MYSQL_BATCH_SIZE: usize = 100_000; // How many rows to fetch at a time from MySQL

/// Pad a MYSQL_TYPE_STRING (CHAR / BINARY) column value to the correct length for the given column
/// type and charset.
///
/// Parameters:
/// - `val`: The current column value as a vector of bytes.
/// - `col`: The column type.
/// - `collation`: The collation ID of the column.
/// - `col_len`: The length of the column in bytes.
///
/// Returns:
/// - A `DfValue` representing the padded column value - `CHAR` will return a `TinyText` or `Text`
///   and `BINARY` will return a `ByteArray`.
pub fn mysql_pad_collation_column(
    val: &[u8],
    col: ColumnType,
    collation: u16,
    col_len: usize,
) -> Result<DfValue, FromUtf8Error> {
    assert_eq!(col, ColumnType::MYSQL_TYPE_STRING);
    let collation: Collation = collations::CollationId::from(collation).into();
    match collation.id() {
        CollationId::BINARY => {
            if val.len() < col_len {
                let mut padded = val.to_owned();
                padded.extend(std::iter::repeat(0).take(col_len - val.len()));
                return Ok(DfValue::ByteArray(Arc::new(padded)));
            }
            Ok(DfValue::ByteArray(Arc::new(val.to_vec())))
        }
        _ => {
            let column_length_characters = col_len / collation.max_len() as usize;
            let mut str = String::from_utf8(val.to_vec())?;
            let str_len = str.chars().count();
            if str_len < column_length_characters {
                str.extend(std::iter::repeat(' ').take(column_length_characters - str_len));
            }
            Ok(DfValue::from(str))
        }
    }
}
