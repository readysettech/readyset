use std::borrow::Cow;
use std::fmt;

use readyset_errors::ReadySetError;
use readyset_errors::ReadySetResult;

macro_rules! decoding_err {
    ($encoding:expr, $($format_args:tt)*) => {
        ReadySetError::DecodingError {
            encoding: $encoding.to_string(),
            message: format!($($format_args)*),
        }
    };
}

macro_rules! encoding_err {
    ($encoding:expr, $($format_args:tt)*) => {
        ReadySetError::EncodingError {
            encoding: $encoding.to_string(),
            message: format!($($format_args)*),
        }
    };
}

/// Supported character encodings for string data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// UTF-8
    ///
    /// Note, we don't distinguish between MySQL's default utf8mb4 and deprecated utf8mb3 (which
    /// only supports the BMP).
    Utf8,
    /// latin1 (CP1252/ISO-8859-1)
    Latin1,
    /// cp850
    Cp850,
    /// Binary data (not interpreted as text)
    Binary,
    /// Unsupported encoding
    OtherMySql(u16),
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Encoding::Utf8 => write!(f, "utf8"),
            Encoding::Latin1 => write!(f, "latin1"),
            Encoding::Cp850 => write!(f, "cp850"),
            Encoding::Binary => write!(f, "binary"),
            Encoding::OtherMySql(id) => write!(f, "unsupported MySQL collation {id}"),
        }
    }
}

impl Encoding {
    /// For reference, see [`mysql_common::collations::CollationId`]
    pub fn from_mysql_collation_id(collation_id: u16) -> Self {
        match collation_id {
            // ascii, utf8mb3, utf8mb4
            11 | 33 | 45 | 46 | 65 | 76 | 83 | 192..=247 | 255..=323 => Self::Utf8,
            5 | 8 | 15 | 31 | 47 | 48 | 49 | 94 => Self::Latin1,
            4 | 80 => Self::Cp850,
            63 => Self::Binary,

            // Default to UTF-8 for other collations
            _ => Self::OtherMySql(collation_id),
        }
    }

    /// Get a supported encoding (or `None` if unsupported) for the given mysql character set name.
    /// This is not a collation name (e.g. we expect `latin1` not `latin1_swedish_ci`). The name
    /// must be lowercased.
    pub fn from_mysql_character_set_name(character_set_name: &str) -> Option<Self> {
        // This is mostly because we already lowercase this for use in metrics at the caller.
        debug_assert!(
            character_set_name
                .chars()
                .all(|c| c.is_ascii() && !c.is_uppercase()),
            "character set names should be lowercase ascii, got {character_set_name:?}"
        );
        match character_set_name {
            "utf8" | "utf8mb3" | "utf8mb4" => Some(Self::Utf8),
            "latin1" => Some(Self::Latin1),
            "cp850" => Some(Self::Cp850),
            "binary" => Some(Self::Binary),
            _ => None,
        }
    }

    pub fn decode(&self, bytes: &[u8]) -> ReadySetResult<String> {
        match self {
            Self::Utf8 => core::str::from_utf8(bytes)
                .map(|s| s.to_string())
                .map_err(|e| decoding_err!(self, "Invalid bytes: {e}")),
            Self::Latin1 => Ok(yore::code_pages::CP1252.decode(bytes).into_owned()),
            Self::Cp850 => Ok(yore::code_pages::CP850.decode(bytes).into_owned()),
            Self::Binary | Self::OtherMySql(_) => Err(decoding_err!(self, "Unsupported encoding")),
        }
    }

    pub fn encode<'a>(&self, string: &'a str) -> ReadySetResult<Cow<'a, [u8]>> {
        match self {
            Self::Utf8 => Ok(string.as_bytes().into()),
            Self::Latin1 => Ok(yore::code_pages::CP1252.encode_lossy(string, b"?"[0])),
            Self::Cp850 => Ok(yore::code_pages::CP850.encode_lossy(string, b"?"[0])),
            Self::Binary | Self::OtherMySql(_) => Err(encoding_err!(self, "Unsupported encoding")),
        }
    }
}

/// The `mysql_common` crate currently doesn't provide a function to convert a character set name to
/// the collation ID for the default collation for that character set.
///
/// Returns 0 for unknown character sets, which matches `mysql_common`'s other behavior.
///
/// TODO: Upstream to `mysql_common`
pub fn mysql_character_set_name_to_collation_id(name: &str) -> u16 {
    match name {
        "big5" => 1,
        "dec8" => 3,
        "cp850" => 4,
        "hp8" => 6,
        "koi8r" => 7,
        "latin1" => 8,
        "latin2" => 9,
        "swe7" => 10,
        "ascii" => 11,
        "ujis" => 12,
        "sjis" => 13,
        "hebrew" => 16,
        "tis620" => 18,
        "euckr" => 19,
        "koi8u" => 22,
        "gb2312" => 24,
        "greek" => 25,
        "cp1250" => 26,
        "gbk" => 28,
        "latin5" => 30,
        "armscii8" => 32,
        // As of MySQL 5.7 and 8.4, utf8 is an alias for utf8mb3 even though it is considered deprecated in favor of utf8mb4
        "utf8" => 33,
        "utf8mb3" => 33,
        "ucs2" => 35,
        "cp866" => 36,
        "keybcs2" => 37,
        "macce" => 38,
        "macroman" => 39,
        "cp852" => 40,
        "latin7" => 41,
        "cp1251" => 51,
        "utf16" => 54,
        "utf16le" => 56,
        "cp1256" => 57,
        "cp1257" => 59,
        "utf32" => 60,
        "binary" => 63,
        "geostd8" => 92,
        "cp932" => 95,
        "eucjpms" => 97,
        "gb18030" => 248,
        "utf8mb4" => 255,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latin1_to_utf8() {
        // Test with ASCII characters (valid in both Latin1 and UTF-8)
        let latin1_bytes = b"Hello World";
        let result = Encoding::Latin1.decode(latin1_bytes).unwrap();
        assert_eq!(result, "Hello World");

        // Test with Latin1 characters that need conversion in UTF-8
        // Characters 0xA0-0xFF in Latin1 map to Unicode code points 0xA0-0xFF
        // For example, 0xE9 in Latin1 is 'é'
        let latin1_bytes = &[0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0xE9]; // "Hello é" in Latin1
        let result = Encoding::Latin1.decode(latin1_bytes).unwrap();
        assert_eq!(result, "Hello é");

        // Test with all high-bit Latin1 characters (0x80-0xFF)
        let mut latin1_high_bytes = Vec::new();
        for b in 0x80..=0xFF {
            latin1_high_bytes.push(b);
        }

        let result = Encoding::Latin1.decode(&latin1_high_bytes).unwrap();
        // Make sure all characters were decoded (should be 128 chars for bytes 0x80-0xFF)
        assert_eq!(result.chars().count(), 128);
    }

    #[test]
    fn test_utf8_to_latin1() {
        // Test with ASCII (should work fine)
        let utf8_str = "Hello World";
        let result = Encoding::Latin1.encode(utf8_str).unwrap();
        assert_eq!(result, &b"Hello World"[..]);

        // Test with Latin1 characters
        let utf8_str = "Hello é";
        let result = Encoding::Latin1.encode(utf8_str).unwrap();
        assert_eq!(result, &[0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0xE9][..]);

        // Test with characters outside Latin1 range (should fail)
        let utf8_str = "Hello 😊"; // Emoji is outside Latin1 range
        let result = Encoding::Latin1.encode(utf8_str).unwrap();
        assert_eq!(*result, b"Hello ?"[..]);
    }

    #[test]
    fn test_invalid_utf8() {
        let latin1_bytes = &[0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0xE9, 0x20]; // "Hello é " in Latin1
        let result = Encoding::Utf8.decode(latin1_bytes);
        assert!(result.is_err());
        match result.unwrap_err() {
            ReadySetError::DecodingError { encoding, message } => {
                assert_eq!(encoding, "utf8");
                assert!(
                    message.contains("index 6"),
                    "expected utf8 error message to mention index 6. Message '{message}'"
                )
            }
            e => panic!("Unexpected error type: {e:?}"),
        }
    }
}
