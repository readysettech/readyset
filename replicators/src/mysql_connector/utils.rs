use mysql_async::{self as mysql, prelude::Queryable};
use mysql_common::collations::{Collation as MyCollation, CollationId};
use readyset_data::encoding::Encoding;
use readyset_data::{Collation as RsCollation, DfValue, Dialect};
use readyset_errors::{internal, replication_failed, replication_failed_err, ReadySetResult};
use std::io::Write as _;
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

/// Write a double the way MySQL renders one inside a JSON value: `my_gcvt()` with the widest
/// field width, plus a trailing `.0` when the result reads as an integer.
pub(crate) fn write_mysql_json_double(out: &mut String, x: f64) {
    // `my_gcvt()` keeps fixed notation while the decimal point lands within `DBL_DIG` places
    // of the first digit, or while some digit falls after it.
    const MAX_DECPT_FOR_F_FORMAT: i32 = 15;

    // LowerExp prints the shortest digits that round-trip, as `dtoa()` does for `my_gcvt()`, and
    // its exponent form (`1.5e-7`, `1e15`) is the one MySQL uses. The longest f64 output,
    // `-2.2250738585072014e-308`, fits in the buffer.
    let mut buf = [0u8; 32];
    let mut cursor = std::io::Cursor::new(&mut buf[..]);
    write!(cursor, "{:e}", x.abs()).expect("LowerExp output fits in 32 bytes");
    let len = cursor.position() as usize;
    let sci = str::from_utf8(&buf[..len]).expect("LowerExp output is ASCII");
    let (mantissa, exp) = sci
        .split_once('e')
        .expect("LowerExp output has an exponent");
    let exp: i32 = exp.parse().expect("LowerExp exponent is an integer");
    // `-0.0` keeps its sign, as in MySQL.
    if x.is_sign_negative() {
        out.push('-');
    }

    let digits = || mantissa.chars().filter(|c| *c != '.');
    let len = digits().count() as i32;
    // Position of the decimal point relative to the first digit, as `dtoa()` reports it.
    let decpt = exp + 1;
    let zeros = |out: &mut String, n: i32| out.extend(std::iter::repeat_n('0', n as usize));

    if decpt >= 1 - MAX_DECPT_FOR_F_FORMAT && (decpt <= MAX_DECPT_FOR_F_FORMAT || len > decpt) {
        if decpt <= 0 {
            out.push_str("0.");
            zeros(out, -decpt);
            out.extend(digits());
        } else {
            for (i, d) in digits().enumerate() {
                if i as i32 == decpt {
                    out.push('.');
                }
                out.push(d);
            }
            if decpt >= len {
                zeros(out, decpt - len);
                out.push_str(".0");
            }
        }
    } else {
        out.push_str(sci);
    }
}

/// Write `s` as a JSON string literal escaped the way MySQL escapes it.
pub(crate) fn write_mysql_json_string(out: &mut String, s: &str) {
    out.push('"');
    let mut start = 0;
    for (i, b) in s.bytes().enumerate() {
        let escape = match b {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            b'\n' => "\\n",
            b'\r' => "\\r",
            b'\t' => "\\t",
            0x08 => "\\b",
            0x0c => "\\f",
            0x00..=0x1f => "",
            _ => continue,
        };
        // Escaped bytes are ASCII, so `i` is always a char boundary.
        out.push_str(&s[start..i]);
        if escape.is_empty() {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            out.push_str("\\u00");
            out.push(HEX[usize::from(b >> 4)].into());
            out.push(HEX[usize::from(b & 0xf)].into());
        } else {
            out.push_str(escape);
        }
        start = i + 1;
    }
    out.push_str(&s[start..]);
    out.push('"');
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
    fn mysql_json_double_matches_mysql() {
        // Expected strings are MySQL 8.4 output for `JSON_ARRAY()` of each value as a DOUBLE.
        let cases = [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1e14, "100000000000000.0"),
            (1e15, "1e15"),
            (1e-14, "0.00000000000001"),
            (1e-15, "0.000000000000001"),
            (1e-16, "1e-16"),
            (0.0000001, "0.0000001"),
            (1234567890123456.7, "1234567890123456.8"),
            (123456789012345678.0, "1.2345678901234568e17"),
            (-1.5e300, "-1.5e300"),
            (1e30, "1e30"),
            (2.5e-200, "2.5e-200"),
            (100.0, "100.0"),
            (0.1, "0.1"),
            (1e100, "1e100"),
        ];
        for (x, expected) in cases {
            let mut out = String::new();
            write_mysql_json_double(&mut out, x);
            assert_eq!(out, expected, "{x:e}");
        }
    }

    #[test]
    fn write_mysql_json_string_matches_mysql() {
        // Expected string is MySQL 8.4 output for the same characters in `JSON_ARRAY()`.
        let mut out = String::new();
        write_mysql_json_string(&mut out, "a\u{1f}\u{1}\u{7f}/\u{8}\u{c}\n\r\t\"\\é");
        assert_eq!(out, "\"a\\u001f\\u0001\u{7f}/\\b\\f\\n\\r\\t\\\"\\\\é\"");
    }
}
