use mysql_async::{self as mysql, prelude::Queryable};
use mysql_common::collations::{self, Collation, CollationId};
use mysql_srv::ColumnType;
use readyset_data::DfValue;
use std::string::FromUtf8Error;
use std::sync::Arc;

//TODO(marce): Make this a configuration parameter or dynamically adjust based on the table size
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

pub fn parse_mysql_version(version: &str) -> mysql_async::Result<u32> {
    let version_parts: Vec<&str> = version.split('.').collect();
    let major = version_parts[0].parse::<u32>().unwrap_or(8);
    let minor = version_parts[1].parse::<u32>().unwrap_or(0);
    let patch_parts: Vec<&str> = version_parts[2].split('-').collect();
    let patch = patch_parts[0].parse::<u32>().unwrap_or(0);
    Ok(major * 10000 + minor * 100 + patch)
}

/// Get MySQL Server Version
pub async fn get_mysql_version(conn: &mut mysql_async::Conn) -> mysql::Result<u32> {
    let version: mysql::Row = conn.query_first("SELECT VERSION()").await?.unwrap();
    let version: String = version.get(0).expect("MySQL version");
    parse_mysql_version(&version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mysql_version() {
        let version = "8.0.23";
        let version_number = parse_mysql_version(version).unwrap();
        assert_eq!(version_number, 80023);

        let version = "8.0.23-0ubuntu0.18.04.1";
        let version_number = parse_mysql_version(version).unwrap();
        assert_eq!(version_number, 80023);

        let version = "8.0.23-rds.20240529-log";
        let version_number = parse_mysql_version(version).unwrap();
        assert_eq!(version_number, 80023);
    }
}
