use mysql_async::{self as mysql, prelude::Queryable};
use mysql_common::collations::{Collation as MyCollation, CollationId};
use readyset_data::encoding::Encoding;
use readyset_data::{Collation as RsCollation, DfValue, Dialect};
use readyset_errors::{internal, replication_failed, replication_failed_err, ReadySetResult};
use serde_json::Value as JsonValue;
use std::sync::Arc;

//TODO(marce): Make this a configuration parameter or dynamically adjust based on the table size
pub const MYSQL_BATCH_SIZE: usize = 100_000; // How many rows to fetch at a time from MySQL

/// Pad a MYSQL_TYPE_STRING CHAR column value to the correct length for the given charset.
///
/// Parameters:
/// - `val`: The current column value as a vector of bytes.
/// - `collation`: The collation ID of the column.
/// - `already_utf8_encoded`: Whether the column value is already UTF-8 encoded (i.e. during snapshot,
///   but not during binlog streaming replication)
/// - `col_len`: The length of the column in bytes.
///
/// Returns:
/// - A `DfValue::TinyText` or `DfValue::Text` encoded as UTF-8 padded with spaces on the right.
pub(crate) fn mysql_pad_char_column(
    val: &[u8],
    collation: u16,
    col_len: usize,
    already_utf8_encoded: bool,
) -> ReadySetResult<DfValue> {
    let collation: MyCollation = match CollationId::from(collation) {
        CollationId::UNKNOWN_COLLATION_ID => internal!("Unknown collation id {collation}"),
        collation_id => collation_id.into(),
    };
    // We calculate the length *in characters* to pad to based on the column length; but this is
    // given in terms of bytes in the result set encoding (collation). In snapshot, the `collation`
    // we have here is the stored collation of the column, but `val` is encoded as utf8mb4. When
    // converting values from the binlog, `val` and `collation` match, and we infer the number of
    // characters based on that.
    let column_length_characters = if already_utf8_encoded {
        col_len / 4
    } else {
        col_len / collation.max_len() as usize
    };
    let encoding = if already_utf8_encoded {
        Encoding::Utf8
    } else {
        Encoding::from_mysql_collation_id(collation.id() as u16)
    };
    let mut str = encoding.decode(val)?;
    let str_len = str.chars().count();
    let rs_collation = RsCollation::get_or_default(
        Dialect::DEFAULT_MYSQL,
        MyCollation::resolve(collation.id()).collation(),
    );
    if str_len < column_length_characters {
        str.extend(std::iter::repeat_n(' ', column_length_characters - str_len));
    }
    Ok(DfValue::from_str_and_collation(str.as_str(), rs_collation))
}

/// Pad a MYSQL_TYPE_STRING BINARY column or MYSQL_TYPE_STRING CHAR column with the `binary`
/// character set/collation.
///
/// Parameters:
/// - `val`: The value to pad.
/// - `col_len`: The length to pad to.
///
/// Returns:
/// - A `DfValue::ByteArray` padded to `col_len` with the zero byte.
pub(crate) fn mysql_pad_binary_column(mut val: Vec<u8>, col_len: usize) -> ReadySetResult<DfValue> {
    if val.len() < col_len {
        val.extend(std::iter::repeat_n(0, col_len - val.len()));
    }
    Ok(DfValue::ByteArray(Arc::new(val)))
}

/// Check if GTID mode is enabled on the MySQL server.
///
/// Returns:
/// - `Ok(true)` if gtid_mode is ON (fully enabled)
/// - `Ok(false)` if gtid_mode is OFF
/// - `Err` if gtid_mode is in a permissive mode (not yet supported) or query fails
#[allow(dead_code)] // Will be used when GTID mode is integrated into noria_adapter
pub async fn is_gtid_mode_enabled(conn: &mut mysql_async::Conn) -> ReadySetResult<bool> {
    let mode: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'gtid_mode'")
        .await
        .map_err(|e| replication_failed_err!("Failed to query gtid_mode: {e}"))?;

    let value = mode
        .ok_or_else(|| replication_failed_err!("MySQL did not return gtid_mode"))?
        .1;

    match value.as_str() {
        "OFF" => Ok(false),
        "ON" => Ok(true),
        other if other.contains("PERMISSIVE") => {
            replication_failed!("GTID permissive modes are not yet supported (gtid_mode={other})")
        }
        other => replication_failed!("Unexpected gtid_mode value: {other}"),
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

/// Format JSON the same way MySQL renders a JSON column in a result set.
pub(crate) fn mysql_json_print(json: &JsonValue) -> String {
    fn write_json_string(output: &mut String, string: &str) {
        match serde_json::to_string(string) {
            Ok(string) => output.push_str(&string),
            Err(_) => unreachable!("serializing a JSON string cannot fail"),
        }
    }

    fn write_json(output: &mut String, json: &JsonValue) {
        match json {
            JsonValue::Object(object) => {
                let mut fields = object.iter().collect::<Vec<_>>();
                fields.sort_unstable_by(|(left, _), (right, _)| {
                    left.len()
                        .cmp(&right.len())
                        .then_with(|| left.as_bytes().cmp(right.as_bytes()))
                });

                output.push('{');
                for (index, (key, value)) in fields.into_iter().enumerate() {
                    if index != 0 {
                        output.push_str(", ");
                    }
                    write_json_string(output, key);
                    output.push_str(": ");
                    write_json(output, value);
                }
                output.push('}');
            }
            JsonValue::Array(array) => {
                output.push('[');
                for (index, value) in array.iter().enumerate() {
                    if index != 0 {
                        output.push_str(", ");
                    }
                    write_json(output, value);
                }
                output.push(']');
            }
            JsonValue::String(string) => write_json_string(output, string),
            JsonValue::Number(number) => output.push_str(&number.to_string()),
            JsonValue::Bool(boolean) => output.push_str(if *boolean { "true" } else { "false" }),
            JsonValue::Null => output.push_str("null"),
        }
    }

    let mut output = String::new();
    write_json(&mut output, json);
    output
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

    #[test]
    fn mysql_json_print_matches_mysql_key_order_and_spacing() {
        let json = serde_json::json!({
            "zeta": 1,
            "alpha": 2,
            "middle": {
                "z": 1,
                "a": 2,
                "m": 3
            },
            "numeric": {
                "10": "ten",
                "2": "two",
                "1": "one",
                "20": "twenty"
            }
        });

        assert_eq!(
            mysql_json_print(&json),
            r#"{"zeta": 1, "alpha": 2, "middle": {"a": 2, "m": 3, "z": 1}, "numeric": {"1": "one", "2": "two", "10": "ten", "20": "twenty"}}"#
        );
    }

    #[test]
    fn mysql_json_print_escapes_strings() {
        let json = serde_json::json!({
            "quote\"": "line\nslash\\tab\t"
        });

        assert_eq!(
            mysql_json_print(&json),
            r#"{"quote\"": "line\nslash\\tab\t"}"#
        );
    }
}
