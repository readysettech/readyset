use std::fmt;

use encoding_rs::{UTF_8, WINDOWS_1252};
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
            Encoding::Binary => write!(f, "binary"),
            Encoding::OtherMySql(id) => write!(f, "unsupported MySQL collation {}", id),
        }
    }
}

impl Encoding {
    /// For reference, see [`mysql_common::collations::CollationId`]
    pub fn from_mysql_collation_id(collation_id: u16) -> Self {
        match collation_id {
            // ascii, utf8mb3, utf8mb4
            11 | 33 | 45 | 46 | 65 | 76 | 83 | 192..=247 | 255..=323 => Self::Utf8,
            // latin1
            5 | 8 | 15 | 31 | 47 | 48 | 49 | 94 => Self::Latin1,
            // binary
            63 => Self::Binary,

            // Default to UTF-8 for other collations
            _ => Self::OtherMySql(collation_id),
        }
    }

    fn get_encoding_rs(&self) -> Option<&'static encoding_rs::Encoding> {
        match self {
            Self::Utf8 => Some(UTF_8),
            Self::Latin1 => Some(WINDOWS_1252),
            Self::Binary => None,
            Self::OtherMySql(_) => None,
        }
    }

    /// Decode bytes from this encoding to a UTF-8 String
    ///
    /// To detect errors we use a method which doesn't do replacement. By default, `encoding_rs`
    /// uses the WHATWG Encoding Standard's replacement character, which is the HTML decimal
    /// representation of the original character. MySQL by contrast uses ? as a replacement
    /// character, and we will likely want to implement such custom replacement to match MySQL in
    /// the future.
    pub fn decode(&self, bytes: &[u8]) -> ReadySetResult<String> {
        match self {
            Self::Utf8 | Self::Latin1 => {
                let Some(encoding) = self.get_encoding_rs() else {
                    return Err(decoding_err!(self, "Missing decoder"));
                };
                // XXX(mvzink): We ignore BOMs. This is only relevant for UTF-16, UTF-32, and UCS-2,
                // and the [MySQL docs] indicate there won't be a BOM. This may need to be adjusted
                // for handling those encodings on Postgres.
                //
                // [MySQL docs]: https://dev.mysql.com/doc/refman/8.4/en/charset-unicode.html
                let mut decoder = encoding.new_decoder_without_bom_handling();
                let Some(max_len) = decoder.max_utf8_buffer_length_without_replacement(bytes.len())
                else {
                    // According to docs, only happens if it would overflow usize
                    return Err(decoding_err!(self, "Worst case output too long"));
                };
                let mut out = String::with_capacity(max_len);
                let (result, bytes_read) =
                    decoder.decode_to_string_without_replacement(bytes, &mut out, true);
                match result {
                    encoding_rs::DecoderResult::InputEmpty => Ok(out),
                    encoding_rs::DecoderResult::OutputFull => {
                        Err(decoding_err!(self, "Not enough space for output"))
                    }
                    encoding_rs::DecoderResult::Malformed(len, overage) => {
                        let start = bytes_read
                            .checked_sub(overage as usize)
                            .ok_or_else(|| decoding_err!(self, "Malformed sequence overage {overage} is greater than bytes read {bytes_read}"))?
                            .checked_sub(len as usize)
                            .ok_or_else(|| decoding_err!(self, "Malformed sequence length {len} is greater than bytes read {bytes_read}"))?;
                        let end = start.checked_add(len as usize).ok_or_else(|| {
                            decoding_err!(self, "Malformed sequence length {len} overflows")
                        })?;
                        if end > bytes.len() {
                            Err(decoding_err!(
                                self,
                                "Malformed sequence length {} is past end of input length {}",
                                len,
                                bytes.len()
                            ))
                        } else {
                            Err(decoding_err!(
                                self,
                                "Malformed input from {} to {}: {:?}",
                                start,
                                end,
                                &bytes[start..end]
                            ))
                        }
                    }
                }
            }
            Self::Binary => Err(decoding_err!(
                self,
                "Binary data cannot be converted to UTF-8"
            )),
            _ => Err(decoding_err!(self, "Unsupported encoding")),
        }
    }

    /// Encode a UTF-8 string to bytes in this encoding
    pub fn encode(&self, string: &str) -> ReadySetResult<Vec<u8>> {
        match self {
            Self::Utf8 | Self::Latin1 => {
                let Some(encoding) = self.get_encoding_rs() else {
                    return Err(encoding_err!(self, "Missing encoder"));
                };
                let (cow, _encoding_used, had_errors) = encoding.encode(string);

                if had_errors {
                    return Err(encoding_err!(
                        self,
                        "Some characters couldn't be encoded properly"
                    ));
                }

                Ok(cow.into_owned())
            }
            Encoding::Binary => Err(encoding_err!(self, "Cannot encode string to binary")),
            _ => Err(encoding_err!(self, "Unsupported encoding")),
        }
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
        assert_eq!(result, b"Hello World");

        // Test with Latin1 characters
        let utf8_str = "Hello é";
        let result = Encoding::Latin1.encode(utf8_str).unwrap();
        assert_eq!(result, &[0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0xE9]);

        // Test with characters outside Latin1 range (should fail)
        let utf8_str = "Hello 😊"; // Emoji is outside Latin1 range
        let result = Encoding::Latin1.encode(utf8_str);
        assert!(result.is_err());
        match result.unwrap_err() {
            ReadySetError::EncodingError { encoding, .. } => {
                assert_eq!(encoding, "latin1");
            }
            e => panic!("Unexpected error type: {:?}", e),
        }
    }

    #[test]
    fn test_invalid_utf8() {
        // Test with invalid UTF-8 bytes
        let latin1_bytes = &[0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0xE9, 0x20]; // "Hello é" in Latin1
        let result = Encoding::Utf8.decode(latin1_bytes);
        assert!(result.is_err());
        match result.unwrap_err() {
            ReadySetError::DecodingError { encoding, message } => {
                assert_eq!(encoding, "utf8");
                assert!(message.contains("6 to 7"), "{}", message)
            }
            e => panic!("Unexpected error type: {:?}", e),
        }
    }
}
