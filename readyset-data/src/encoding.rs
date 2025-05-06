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
            5 | 8 | 15 | 31 | 47 | 48 | 49 | 94 => Self::Latin1,
            4 | 80 => Self::Cp850,
            63 => Self::Binary,

            // Default to UTF-8 for other collations
            _ => Self::OtherMySql(collation_id),
        }
    }

    pub fn decode(&self, bytes: &[u8]) -> ReadySetResult<String> {
        match self {
            Self::Utf8 => self.decode_encoding_rs(bytes, encoding_rs::UTF_8),
            Self::Latin1 => self.decode_encoding_rs(bytes, encoding_rs::WINDOWS_1252),
            Self::Cp850 => Ok(yore::code_pages::CP850.decode(bytes).into_owned()),
            Self::Binary | Self::OtherMySql(_) => Err(decoding_err!(self, "Unsupported encoding")),
        }
    }

    /// Decode bytes from this encoding to a UTF-8 String
    ///
    /// To detect errors we use a method which doesn't do replacement. By default, `encoding_rs`
    /// uses the WHATWG Encoding Standard's replacement character, which is the HTML decimal
    /// representation of the original character. MySQL by contrast uses ? as a replacement
    /// character, and we will likely want to implement such custom replacement to match MySQL in
    /// the future.
    fn decode_encoding_rs(
        &self,
        bytes: &[u8],
        encoding: &'static encoding_rs::Encoding,
    ) -> Result<String, ReadySetError> {
        // XXX(mvzink): We ignore BOMs. This is only relevant for UTF-16, UTF-32, and UCS-2,
        // and the [MySQL docs] indicate there won't be a BOM. This may need to be adjusted
        // for handling those encodings on Postgres.
        //
        // [MySQL docs]: https://dev.mysql.com/doc/refman/8.4/en/charset-unicode.html
        let mut decoder = encoding.new_decoder_without_bom_handling();
        let Some(max_len) = decoder.max_utf8_buffer_length_without_replacement(bytes.len()) else {
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

    pub fn encode<'a>(&self, string: &'a str) -> ReadySetResult<Cow<'a, [u8]>> {
        match self {
            Self::Utf8 => Ok(string.as_bytes().into()),
            Self::Latin1 => Ok(self
                .encode_encoding_rs(string, encoding_rs::WINDOWS_1252)?
                .into()),
            Self::Cp850 => Ok(yore::code_pages::CP850.encode_lossy(string, b"?"[0])),
            Self::Binary | Self::OtherMySql(_) => Err(decoding_err!(self, "Unsupported encoding")),
        }
    }

    /// Encode a UTF-8 string to bytes in this encoding
    ///
    /// As with [`Encoding::decode`], we use the more involved non-replacing method in order to try
    /// to surface a useful error message with the invalid bytes included.
    fn encode_encoding_rs(
        &self,
        string: &str,
        encoding: &'static encoding_rs::Encoding,
    ) -> ReadySetResult<Vec<u8>> {
        let mut encoder = encoding.new_encoder();
        let Some(max_len) = encoder.max_buffer_length_from_utf8_if_no_unmappables(string.len())
        else {
            // According to docs, only happens if it would overflow usize
            return Err(decoding_err!(self, "Worst case output too long"));
        };
        let mut out = Vec::with_capacity(max_len);
        let (result, _bytes_read) =
            encoder.encode_from_utf8_to_vec_without_replacement(string, &mut out, true);

        match result {
            encoding_rs::EncoderResult::InputEmpty => Ok(out),
            encoding_rs::EncoderResult::OutputFull => {
                Err(encoding_err!(self, "Not enough space for output"))
            }
            encoding_rs::EncoderResult::Unmappable(ch) => {
                Err(encoding_err!(self, "Unmappable character: {}", ch))
            }
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
        assert_eq!(result, &b"Hello World"[..]);

        // Test with Latin1 characters
        let utf8_str = "Hello é";
        let result = Encoding::Latin1.encode(utf8_str).unwrap();
        assert_eq!(result, &[0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0xE9][..]);

        // Test with characters outside Latin1 range (should fail)
        let utf8_str = "Hello 😊"; // Emoji is outside Latin1 range
        let result = Encoding::Latin1.encode(utf8_str);
        assert!(result.is_err());
        match result.unwrap_err() {
            ReadySetError::EncodingError { encoding, message } => {
                assert_eq!(encoding, "latin1");
                assert!(message.contains("Unmappable character: 😊"), "{}", message);
            }
            e => panic!("Unexpected error type: {:?}", e),
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
                assert!(message.contains("6 to 7"), "{}", message)
            }
            e => panic!("Unexpected error type: {:?}", e),
        }
    }
}
