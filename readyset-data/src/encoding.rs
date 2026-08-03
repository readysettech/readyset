use std::borrow::Cow;
use std::fmt;

use mysql_common::collations::{Collation, CollationId};
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

/// Conversion tables for one MySQL single-byte character set, generated from a live MySQL
/// server by the `mysql_charset` binary in `database-utils`.
#[derive(Debug)]
pub struct SingleByteEncoding {
    /// MySQL character set name, e.g. "latin1".
    pub name: &'static str,
    /// The char each byte decodes to, matching MySQL's conversion to utf8mb4. Undefined byte
    /// positions hold MySQL's replacement char.
    pub decode: &'static [char; 256],
    /// MySQL's canonical byte for each encodable char, sorted by char for binary search.
    /// Chars absent from the table encode to the lossy fallback `b'?'`.
    pub encode: &'static [(char, u8)],
    /// Whether bytes 0x00-0x7F decode to the identical ASCII chars and those chars encode
    /// back to the identical bytes.
    pub ascii_transparent: bool,
}

/// Registers each supported single-byte charset. Each entry lists the enum variant, the MySQL
/// character set name, and the generated table module under `src/encoding/`.
macro_rules! single_byte_charsets {
    ($(($variant:ident, $name:literal, $module:ident),)*) => {
        $(mod $module;)*

        /// A MySQL single-byte character set with generated conversion tables.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum SingleByteCharset {
            $($variant,)*
        }

        impl SingleByteCharset {
            /// Every supported single-byte charset.
            pub const ALL: &[Self] = &[$(Self::$variant,)*];

            /// The generated conversion tables for this charset.
            pub fn spec(self) -> &'static SingleByteEncoding {
                match self {
                    $(Self::$variant => &$module::SPEC,)*
                }
            }

            /// Look up a charset by its lowercase MySQL character set name.
            pub fn from_name(name: &str) -> Option<Self> {
                match name {
                    $($name => Some(Self::$variant),)*
                    _ => None,
                }
            }
        }
    };
}

single_byte_charsets! {
    (Armscii8, "armscii8", armscii8),
    (Cp850, "cp850", cp850),
    (Cp852, "cp852", cp852),
    (Cp866, "cp866", cp866),
    (Cp1250, "cp1250", cp1250),
    (Cp1251, "cp1251", cp1251),
    (Cp1256, "cp1256", cp1256),
    (Cp1257, "cp1257", cp1257),
    (Dec8, "dec8", dec8),
    (Geostd8, "geostd8", geostd8),
    (Greek, "greek", greek),
    (Hebrew, "hebrew", hebrew),
    (Hp8, "hp8", hp8),
    (Keybcs2, "keybcs2", keybcs2),
    (Koi8r, "koi8r", koi8r),
    (Koi8u, "koi8u", koi8u),
    (Latin1, "latin1", latin1),
    (Latin2, "latin2", latin2),
    (Latin5, "latin5", latin5),
    (Latin7, "latin7", latin7),
    (Macce, "macce", macce),
    (Macroman, "macroman", macroman),
    (Swe7, "swe7", swe7),
    (Tis620, "tis620", tis620),
}

/// Supported character encodings for string data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// UTF-8
    ///
    /// Note, we don't distinguish between MySQL's default utf8mb4 and deprecated utf8mb3 (which
    /// only supports the BMP).
    Utf8,
    /// A MySQL single-byte character set, e.g. latin1 or koi8r.
    SingleByte(SingleByteCharset),
    /// Binary data (not interpreted as text)
    Binary,
    /// Unsupported encoding
    OtherMySql(u16),
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Encoding::Utf8 => write!(f, "utf8"),
            Encoding::SingleByte(charset) => write!(f, "{}", charset.spec().name),
            Encoding::Binary => write!(f, "binary"),
            Encoding::OtherMySql(id) => write!(f, "unsupported MySQL collation {id}"),
        }
    }
}

impl Encoding {
    pub const LATIN1: Self = Self::SingleByte(SingleByteCharset::Latin1);
    pub const CP850: Self = Self::SingleByte(SingleByteCharset::Cp850);

    /// The encoding for a MySQL collation id, derived from the collation's character set as
    /// known to `mysql_common`. Collation ids `mysql_common` doesn't know, and character sets
    /// without a registered conversion table, are unsupported.
    pub fn from_mysql_collation_id(collation_id: u16) -> Self {
        let collation = Collation::resolve(CollationId::from(collation_id));
        if collation.id() == CollationId::UNKNOWN_COLLATION_ID {
            return Self::OtherMySql(collation_id);
        }
        match collation.charset() {
            // ascii is a strict subset of UTF-8, so treating ascii data as UTF-8 is exact.
            "utf8mb3" | "utf8mb4" | "ascii" => Self::Utf8,
            "binary" => Self::Binary,
            name => SingleByteCharset::from_name(name)
                .map(Self::SingleByte)
                .unwrap_or(Self::OtherMySql(collation_id)),
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
            "binary" => Some(Self::Binary),
            name => SingleByteCharset::from_name(name).map(Self::SingleByte),
        }
    }

    /// The MySQL character set name for a supported encoding, used to mirror a session's charset
    /// onto an upstream connection. A fixed mapping, never derived from client input.
    pub fn mysql_character_set_name(&self) -> Option<&'static str> {
        match self {
            Self::Utf8 => Some("utf8mb4"),
            Self::SingleByte(charset) => Some(charset.spec().name),
            Self::Binary => Some("binary"),
            Self::OtherMySql(_) => None,
        }
    }

    pub fn decode(&self, bytes: &[u8]) -> ReadySetResult<String> {
        match self {
            Self::Utf8 => core::str::from_utf8(bytes)
                .map(|s| s.to_string())
                .map_err(|e| decoding_err!(self, "Invalid bytes: {e}")),
            Self::SingleByte(charset) => {
                let table = charset.spec().decode;
                Ok(bytes.iter().map(|&b| table[b as usize]).collect())
            }
            Self::Binary | Self::OtherMySql(_) => Err(decoding_err!(self, "Unsupported encoding")),
        }
    }

    pub fn encode<'a>(&self, string: &'a str) -> ReadySetResult<Cow<'a, [u8]>> {
        match self {
            Self::Utf8 => Ok(string.as_bytes().into()),
            Self::SingleByte(charset) => {
                let spec = charset.spec();
                if spec.ascii_transparent && string.is_ascii() {
                    return Ok(Cow::Borrowed(string.as_bytes()));
                }
                Ok(Cow::Owned(
                    string
                        .chars()
                        .map(|c| {
                            spec.encode
                                .binary_search_by_key(&c, |&(ec, _)| ec)
                                .map(|i| spec.encode[i].1)
                                .unwrap_or(b'?')
                        })
                        .collect(),
                ))
            }
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
    use yore::code_pages::{CP1252, CP850};
    use yore::CodePage;

    use super::*;

    #[test]
    fn test_latin1_to_utf8() {
        // Test with ASCII characters (valid in both Latin1 and UTF-8)
        let latin1_bytes = b"Hello World";
        let result = Encoding::LATIN1.decode(latin1_bytes).unwrap();
        assert_eq!(result, "Hello World");

        // Test with Latin1 characters that need conversion in UTF-8
        // Characters 0xA0-0xFF in Latin1 map to Unicode code points 0xA0-0xFF
        // For example, 0xE9 in Latin1 is 'é'
        let latin1_bytes = &[0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0xE9]; // "Hello é" in Latin1
        let result = Encoding::LATIN1.decode(latin1_bytes).unwrap();
        assert_eq!(result, "Hello é");

        // Test with all high-bit Latin1 characters (0x80-0xFF)
        let mut latin1_high_bytes = Vec::new();
        for b in 0x80..=0xFF {
            latin1_high_bytes.push(b);
        }

        let result = Encoding::LATIN1.decode(&latin1_high_bytes).unwrap();
        // Make sure all characters were decoded (should be 128 chars for bytes 0x80-0xFF)
        assert_eq!(result.chars().count(), 128);
    }

    #[test]
    fn test_utf8_to_latin1() {
        // Test with ASCII (should work fine)
        let utf8_str = "Hello World";
        let result = Encoding::LATIN1.encode(utf8_str).unwrap();
        assert_eq!(result, &b"Hello World"[..]);

        // Test with Latin1 characters
        let utf8_str = "Hello é";
        let result = Encoding::LATIN1.encode(utf8_str).unwrap();
        assert_eq!(result, &[0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0xE9][..]);

        // Test with characters outside Latin1 range (should fail)
        let utf8_str = "Hello 😊"; // Emoji is outside Latin1 range
        let result = Encoding::LATIN1.encode(utf8_str).unwrap();
        assert_eq!(*result, b"Hello ?"[..]);
    }

    /// The encoding for each supported collation id, spelled out explicitly. Guards against a
    /// `mysql_common` bump changing collation ids or charset naming.
    #[test]
    fn test_collation_id_mapping() {
        for id in 0u16..=1023 {
            let expected = match id {
                // Holes in the utf8 id ranges below that MySQL 8.4 does not assign
                216..=222 | 272 | 276 | 295 | 299 | 301 | 302 => Encoding::OtherMySql(id),
                // ascii, utf8mb3, utf8mb4
                11 | 33 | 45 | 46 | 65 | 76 | 83 | 192..=247 | 255..=323 => Encoding::Utf8,
                5 | 8 | 15 | 31 | 47 | 48 | 49 | 94 => Encoding::LATIN1,
                4 | 80 => Encoding::CP850,
                63 => Encoding::Binary,
                7 => Encoding::SingleByte(SingleByteCharset::Koi8r),
                25 => Encoding::SingleByte(SingleByteCharset::Greek),
                51 => Encoding::SingleByte(SingleByteCharset::Cp1251),
                _ => continue,
            };
            assert_eq!(
                Encoding::from_mysql_collation_id(id),
                expected,
                "collation id {id}"
            );
        }
    }

    /// Structural invariants of every generated charset table.
    #[test]
    fn test_single_byte_tables_consistent() {
        for &charset in SingleByteCharset::ALL {
            let spec = charset.spec();
            // The encode table is strictly sorted by char, so binary search is valid.
            for pair in spec.encode.windows(2) {
                assert!(
                    pair[0].0 < pair[1].0,
                    "{} encode table out of order at {pair:?}",
                    spec.name
                );
            }
            // Every encode entry agrees with the decode table.
            for &(c, b) in spec.encode {
                assert_eq!(
                    spec.decode[b as usize], c,
                    "{} encode entry for byte {b:#04x}",
                    spec.name
                );
            }
            // The ascii_transparent flag matches the tables in both directions.
            let transparent = (0u8..=127).all(|b| {
                spec.decode[b as usize] == b as char
                    && spec
                        .encode
                        .binary_search_by_key(&(b as char), |&(ec, _)| ec)
                        .map(|i| spec.encode[i].1)
                        == Ok(b)
            });
            assert_eq!(spec.ascii_transparent, transparent, "{}", spec.name);
        }
    }

    /// Every encodable char roundtrips through its canonical byte.
    #[test]
    fn test_single_byte_roundtrip() {
        for &charset in SingleByteCharset::ALL {
            let encoding = Encoding::SingleByte(charset);
            for &(c, b) in charset.spec().encode {
                assert_eq!(
                    *encoding.encode(&c.to_string()).unwrap(),
                    [b],
                    "{encoding} char {c:?}"
                );
                assert_eq!(
                    encoding.decode(&[b]).unwrap(),
                    c.to_string(),
                    "{encoding} byte {b:#04x}"
                );
            }
        }
    }

    /// Charset names agree across the generated spec, name lookup, Display, and the default
    /// collation id registries.
    #[test]
    fn test_single_byte_names() {
        for &charset in SingleByteCharset::ALL {
            let name = charset.spec().name;
            let encoding = Encoding::SingleByte(charset);
            assert_eq!(SingleByteCharset::from_name(name), Some(charset));
            assert_eq!(
                Encoding::from_mysql_character_set_name(name),
                Some(encoding)
            );
            assert_eq!(encoding.mysql_character_set_name(), Some(name));
            assert_eq!(encoding.to_string(), name);
            let id = mysql_character_set_name_to_collation_id(name);
            assert_ne!(id, 0, "{name} has no default collation id");
            assert_eq!(Encoding::from_mysql_collation_id(id), encoding, "{name}");
        }
    }

    /// Known codepoints decode as expected.
    #[test]
    fn test_single_byte_spot_checks() {
        for (charset, byte, expected) in [
            // CYRILLIC SMALL LETTER A
            (SingleByteCharset::Koi8r, 0xC1, '\u{0430}'),
            // GREEK SMALL LETTER ALPHA
            (SingleByteCharset::Greek, 0xE1, '\u{03b1}'),
            // LATIN SMALL LETTER A WITH RING ABOVE
            (SingleByteCharset::Swe7, 0x7D, '\u{00e5}'),
            // HEBREW LETTER ALEF
            (SingleByteCharset::Hebrew, 0xE0, '\u{05d0}'),
            // THAI CHARACTER KO KAI
            (SingleByteCharset::Tis620, 0xA1, '\u{0e01}'),
            // CYRILLIC CAPITAL LETTER A
            (SingleByteCharset::Cp866, 0x80, '\u{0410}'),
        ] {
            assert_eq!(
                Encoding::SingleByte(charset).decode(&[byte]).unwrap(),
                expected.to_string(),
                "{} byte {byte:#04x}",
                charset.spec().name
            );
        }
    }

    /// The generated latin1 and cp850 tables agree with the yore code pages that previously
    /// implemented them, in both directions, for every byte.
    #[test]
    fn test_yore_equivalence() {
        for (encoding, page) in [
            (Encoding::LATIN1, &CP1252 as &dyn CodePage),
            (Encoding::CP850, &CP850 as &dyn CodePage),
        ] {
            for b in 0u8..=255 {
                let decoded = encoding.decode(&[b]).unwrap();
                assert_eq!(
                    decoded,
                    page.decode_lossy(&[b]),
                    "{encoding} decode differs from yore at byte {b:#04x}"
                );
                assert_eq!(
                    *encoding.encode(&decoded).unwrap(),
                    *page.encode_lossy(&decoded, b'?'),
                    "{encoding} encode differs from yore for {decoded:?}"
                );
            }
        }
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
