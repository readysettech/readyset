use std::borrow::Borrow;
use std::convert::TryFrom;
use std::str;

use bit_vec::BitVec;
use bytes::{Buf, Bytes, BytesMut};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use cidr::IpInet;
use eui48::MacAddress;
use postgres_types::{FromSql, Kind, Type};
use readyset_data::{Array, Collation};
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use tokio_util::codec::Decoder;
use uuid::Uuid;

use crate::bytes::BytesStr;
use crate::codec::error::DecodeError as Error;
use crate::codec::error::DecodeError::InvalidTextByteArrayValue;
use crate::codec::{Codec, DecodeError};
use crate::message::FrontendMessage::{self, *};
use crate::message::SaslInitialResponse;
use crate::message::StatementName::*;
use crate::message::TransferFormat::{self, *};
use crate::value::PsqlValue;

const ID_AUTHENTICATE: u8 = b'p';
const ID_BIND: u8 = b'B';
const ID_CLOSE: u8 = b'C';
const ID_DESCRIBE: u8 = b'D';
const ID_EXECUTE: u8 = b'E';
const ID_FLUSH: u8 = b'H';
const ID_PARSE: u8 = b'P';
const ID_QUERY: u8 = b'Q';
const ID_SYNC: u8 = b'S';
const ID_TERMINATE: u8 = b'X';

const CLOSE_TYPE_PORTAL: u8 = b'P';
const CLOSE_TYPE_PREPARED_STATEMENT: u8 = b'S';

const DESCRIBE_TYPE_PORTAL: u8 = b'P';
const DESCRIBE_TYPE_PREPARED_STATEMENT: u8 = b'S';

const SSL_REQUEST_CODE: i32 = 80877103;

const STARTUP_MESSAGE_DATABASE_PARAMETER: &str = "database";
const STARTUP_MESSAGE_TERMINATOR: &str = "";
const STARTUP_MESSAGE_USER_PARAMETER: &str = "user";

const BOOL_TRUE_TEXT_REP: &str = "t";
const HEADER_LENGTH: usize = 5;
const LENGTH_NULL_SENTINEL: i32 = -1;
const NUL_BYTE: u8 = b'\0';
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";
const TIMESTAMP_TZ_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f %:z";

impl Decoder for Codec {
    type Item = FrontendMessage;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<FrontendMessage>, Error> {
        let msg = {
            // Try to read a complete message from `src`. Otherwise return `Ok(None)` to indicate
            // that `src` does not yet contain a complete message.

            if src.len() < HEADER_LENGTH {
                src.reserve(HEADER_LENGTH - src.len());
                return Ok(None);
            }

            let message_length = if self.is_starting_up {
                let mut length_window = src
                    .get(0..4)
                    .ok_or_else(|| Error::InternalError("missing message header".to_string()))?;
                usize::try_from(length_window.get_i32())?
            } else {
                let mut length_window = src
                    .get(1..5)
                    .ok_or_else(|| Error::InternalError("missing message header".to_string()))?;
                usize::try_from(length_window.get_i32())? + 1 // add length of message id byte
            };

            if src.len() < message_length {
                src.reserve(message_length - src.len());
                return Ok(None);
            }

            // Split off a `Bytes` buffer containing the full message.
            &mut src.split_to(message_length).freeze()
        };

        if self.is_starting_up {
            let _length = get_i32(msg)?;
            let token = get_i32(msg)?;
            let ret = match token {
                SSL_REQUEST_CODE => Ok(Some(SSLRequest)),

                // Parse StartupMessage
                protocol_version => {
                    let mut user: Option<BytesStr> = None;
                    let mut database: Option<BytesStr> = None;
                    loop {
                        let key = get_str(msg)?;
                        if key.borrow() as &str == STARTUP_MESSAGE_TERMINATOR {
                            break;
                        }
                        let val = get_str(msg)?;
                        if key.borrow() as &str == STARTUP_MESSAGE_USER_PARAMETER {
                            user = Some(val);
                        } else if key.borrow() as &str == STARTUP_MESSAGE_DATABASE_PARAMETER {
                            database = Some(val);
                        }
                    }
                    Ok(Some(StartupMessage {
                        protocol_version,
                        user,
                        database,
                    }))
                }
            };

            if msg.remaining() > 0 {
                return Err(Error::UnexpectedMessageEnd);
            }

            return ret;
        }

        let id = get_u8(msg)?;
        let _length = get_i32(msg)?;

        let ret = match id {
            ID_AUTHENTICATE => {
                let body = msg.clone();
                msg.clear(); // Take the rest of the buffer
                Ok(Some(Authenticate { body }))
            }

            ID_BIND => {
                let portal_name = get_str(msg)?;
                let prepared_statement_name = get_str(msg)?;

                let n_param_format_codes = get_i16(msg)?;
                let param_transfer_formats = (0..n_param_format_codes)
                    .map(|_| get_format(msg))
                    .collect::<Result<Vec<TransferFormat>, Error>>()?;

                let param_data_types = self
                    .statement_param_types
                    .get(prepared_statement_name.borrow() as &str)
                    .ok_or_else(|| {
                        Error::UnknownPreparedStatement(prepared_statement_name.to_string())
                    })?;
                let n_params = usize::try_from(get_i16(msg)?)?;
                if n_params != param_data_types.len() {
                    // TODO If the text transfer format is used, the frontend has the option of not
                    // pre-specifying parameter data types in the Parse message that created the
                    // prepared statement. In that case, the parameter data types must be inferred
                    // from the text representation of the values themselves. This is not currently
                    // implemented.
                    return Err(Error::IncorrectParameterCount(n_params));
                }
                let param_transfer_formats = match param_transfer_formats[..] {
                    // If no format codes are provided, use the default format (`Text`).
                    [] => vec![Text; n_params],
                    // If only one format code is provided, apply it to all parameters.
                    [f] => vec![f; n_params],
                    // Otherwise use the format codes that have been provided, as is.
                    _ => {
                        if param_transfer_formats.len() == n_params {
                            param_transfer_formats
                        } else {
                            return Err(Error::IncorrectParameterCount(n_params));
                        }
                    }
                };
                let params = param_data_types
                    .iter()
                    .zip(param_transfer_formats.iter())
                    .map(|(t, f)| match f {
                        Binary => get_binary_value(msg, t),
                        Text => get_text_value(msg, t),
                    })
                    .collect::<Result<Vec<PsqlValue>, Error>>()?;

                let n_result_format_codes = msg.get_i16();
                let result_transfer_formats = (0..n_result_format_codes)
                    .map(|_| get_format(msg))
                    .collect::<Result<Vec<TransferFormat>, Error>>()?;

                Ok(Some(Bind {
                    portal_name,
                    prepared_statement_name,
                    params,
                    result_transfer_formats,
                }))
            }

            ID_CLOSE => {
                let statement_type = get_u8(msg)?;
                let name_str = get_str(msg)?;
                let name = match statement_type {
                    CLOSE_TYPE_PORTAL => Portal(name_str),
                    CLOSE_TYPE_PREPARED_STATEMENT => PreparedStatement(name_str),
                    t => return Err(Error::UnexpectedValue(t)),
                };
                Ok(Some(Close { name }))
            }

            ID_DESCRIBE => {
                let statement_type = get_u8(msg)?;
                let name_str = get_str(msg)?;
                let name = match statement_type {
                    DESCRIBE_TYPE_PORTAL => Portal(name_str),
                    DESCRIBE_TYPE_PREPARED_STATEMENT => PreparedStatement(name_str),
                    t => return Err(Error::UnexpectedValue(t)),
                };
                Ok(Some(Describe { name }))
            }

            ID_EXECUTE => Ok(Some(Execute {
                portal_name: get_str(msg)?,
                limit: get_i32(msg)?,
            })),

            ID_PARSE => {
                let prepared_statement_name = get_str(msg)?;
                let query = get_str(msg)?;
                let n_parameter_data_types = get_i16(msg)?;
                let parameter_data_types = (0..n_parameter_data_types)
                    .map(|_| get_type(msg))
                    .collect::<Result<Vec<Type>, Error>>()?;
                Ok(Some(Parse {
                    prepared_statement_name,
                    query,
                    parameter_data_types,
                }))
            }

            ID_QUERY => Ok(Some(Query {
                query: get_str(msg)?,
            })),

            ID_SYNC => Ok(Some(Sync)),

            ID_FLUSH => Ok(Some(Flush)),

            ID_TERMINATE => Ok(Some(Terminate)),

            id => Err(Error::UnsupportedMessage(id)),
        };

        if msg.remaining() > 0 {
            return Err(Error::UnexpectedMessageEnd);
        }

        ret
    }
}

pub(crate) fn decode_password_message_body(src: &mut Bytes) -> Result<BytesStr, Error> {
    get_str(src)
}

pub(crate) fn decode_sasl_initial_response_body(
    src: &mut Bytes,
) -> Result<SaslInitialResponse, Error> {
    let authentication_mechanism = get_str(src)?;
    let scram_data = get_lenenc_bytes(src)?;
    Ok(SaslInitialResponse {
        authentication_mechanism,
        scram_data,
    })
}

fn get_u8(src: &mut Bytes) -> Result<u8, Error> {
    if src.remaining() >= 1 {
        Ok(src.get_u8())
    } else {
        Err(Error::UnexpectedMessageEnd)
    }
}

fn get_i16(src: &mut Bytes) -> Result<i16, Error> {
    if src.remaining() >= 2 {
        Ok(src.get_i16())
    } else {
        Err(Error::UnexpectedMessageEnd)
    }
}

fn get_i32(src: &mut Bytes) -> Result<i32, Error> {
    if src.remaining() >= 4 {
        Ok(src.get_i32())
    } else {
        Err(Error::UnexpectedMessageEnd)
    }
}

fn get_str(src: &mut Bytes) -> Result<BytesStr, Error> {
    let nul_pos = src
        .iter()
        .position(|&c| c == NUL_BYTE)
        .ok_or(Error::UnexpectedMessageEnd)?;
    let ret = BytesStr::try_from(src.split_to(nul_pos))?;
    src.advance(1); // skip the nul byte
    Ok(ret)
}

fn get_lenenc_bytes(src: &mut Bytes) -> Result<Bytes, Error> {
    let len = get_i32(src)?;
    if len <= 0 {
        return Ok(Bytes::new());
    }
    Ok(src.split_to(len as usize))
}

fn get_format(src: &mut Bytes) -> Result<TransferFormat, Error> {
    let format = get_i16(src)?;
    match format {
        0 => Ok(Text),
        1 => Ok(Binary),
        _ => Err(Error::InvalidFormat(format)),
    }
}

fn get_type(src: &mut Bytes) -> Result<Type, Error> {
    let oid = u32::try_from(get_i32(src)?)?;
    // Placing a zero here is equivalent to leaving the type unspecified. [1]
    // https://www.postgresql.org/docs/current/protocol-message-formats.html
    if oid == 0u32 {
        Ok(Type::UNKNOWN)
    } else {
        Type::from_oid(oid).ok_or(Error::InvalidType(oid))
    }
}

fn get_binary_value(src: &mut Bytes, t: &Type) -> Result<PsqlValue, Error> {
    let len = get_i32(src)?;
    if len == LENGTH_NULL_SENTINEL {
        return Ok(PsqlValue::Null);
    }

    let buf = &mut src.split_to(usize::try_from(len)?);

    match t.kind() {
        Kind::Array(member_type) => Ok(PsqlValue::Array(
            Array::from_sql(t, buf)?,
            member_type.clone(),
        )),
        Kind::Enum(_) => Ok(PsqlValue::Text(str::from_utf8(buf)?.into())),
        _ => match *t {
            // Postgres does not allow interior 0 bytes, even though it is valid UTF-8
            Type::BPCHAR | Type::VARCHAR | Type::TEXT | Type::NAME if buf.contains(&0) => {
                Err(Error::InvalidUtf8)
            }
            Type::BOOL => Ok(PsqlValue::Bool(bool::from_sql(t, buf)?)),
            Type::VARCHAR => Ok(PsqlValue::VarChar(<&str>::from_sql(t, buf)?.into())),
            Type::BPCHAR => Ok(PsqlValue::BpChar(<&str>::from_sql(t, buf)?.into())),
            Type::NAME => Ok(PsqlValue::Name(<&str>::from_sql(t, buf)?.into())),
            Type::CHAR => Ok(PsqlValue::Char(i8::from_sql(t, buf)?)),
            Type::INT4 => Ok(PsqlValue::Int(i32::from_sql(t, buf)?)),
            Type::INT8 => Ok(PsqlValue::BigInt(i64::from_sql(t, buf)?)),
            Type::INT2 => Ok(PsqlValue::SmallInt(i16::from_sql(t, buf)?)),
            Type::OID => Ok(PsqlValue::Oid(u32::from_sql(t, buf)?)),
            Type::FLOAT8 => Ok(PsqlValue::Double(f64::from_sql(t, buf)?)),
            Type::FLOAT4 => Ok(PsqlValue::Float(f32::from_sql(t, buf)?)),
            Type::NUMERIC => Ok(PsqlValue::Numeric(Decimal::from_sql(t, buf)?)),
            Type::TEXT => Ok(PsqlValue::Text(<&str>::from_sql(t, buf)?.into())),
            Type::DATE => Ok(PsqlValue::Date(NaiveDate::from_sql(t, buf)?)),
            Type::TIME => Ok(PsqlValue::Time(NaiveTime::from_sql(t, buf)?)),
            Type::TIMESTAMP => Ok(PsqlValue::Timestamp(NaiveDateTime::from_sql(t, buf)?)),
            Type::TIMESTAMPTZ => Ok(PsqlValue::TimestampTz(DateTime::<FixedOffset>::from_sql(
                t, buf,
            )?)),
            Type::BYTEA => Ok(PsqlValue::ByteArray(<Vec<u8>>::from_sql(t, buf)?)),
            Type::MACADDR => Ok(PsqlValue::MacAddress(MacAddress::from_sql(t, buf)?)),
            Type::INET => Ok(PsqlValue::Inet(IpInet::from_sql(t, buf)?)),
            Type::UUID => Ok(PsqlValue::Uuid(Uuid::from_sql(t, buf)?)),
            Type::JSON => Ok(PsqlValue::Json(serde_json::Value::from_sql(t, buf)?)),
            Type::JSONB => Ok(PsqlValue::Jsonb(serde_json::Value::from_sql(t, buf)?)),
            Type::BIT => Ok(PsqlValue::Bit(BitVec::from_sql(t, buf)?)),
            Type::VARBIT => Ok(PsqlValue::VarBit(BitVec::from_sql(t, buf)?)),
            ref t if t.name() == "citext" => Ok(PsqlValue::Text(
                readyset_data::Text::from_str_with_collation(
                    <&str>::from_sql(t, buf)?,
                    Collation::Citext,
                ),
            )),
            _ => Ok(PsqlValue::PassThrough(readyset_data::PassThrough {
                ty: t.clone(),
                data: buf.to_vec().into_boxed_slice(),
            })),
        },
    }
}

fn get_bitvec_from_str(bit_str: &str) -> Result<BitVec, Error> {
    let mut bits = BitVec::with_capacity(bit_str.len());
    for c in bit_str.chars() {
        match c {
            '0' => bits.push(false),
            '1' => bits.push(true),
            _ => return Err(Error::InvalidTextBitVectorValue(bit_str.to_owned())),
        }
    }
    Ok(bits)
}

fn get_text_value(src: &mut Bytes, t: &Type) -> Result<PsqlValue, Error> {
    let len = get_i32(src)?;
    if len == LENGTH_NULL_SENTINEL {
        return Ok(PsqlValue::Null);
    }

    let text = BytesStr::try_from(src.split_to(usize::try_from(len)?))?;
    let text_str: &str = text.borrow();
    match *t {
        Type::BOOL => Ok(PsqlValue::Bool(text_str == BOOL_TRUE_TEXT_REP)),
        Type::VARCHAR => Ok(PsqlValue::VarChar(text_str.into())),
        Type::NAME => Ok(PsqlValue::Name(text_str.into())),
        Type::BPCHAR => Ok(PsqlValue::BpChar(text_str.into())),
        Type::INT4 => Ok(PsqlValue::Int(text_str.parse::<i32>()?)),
        Type::INT8 => Ok(PsqlValue::BigInt(text_str.parse::<i64>()?)),
        Type::INT2 => Ok(PsqlValue::SmallInt(text_str.parse::<i16>()?)),
        Type::CHAR => Ok(PsqlValue::Char(text_str.parse::<i8>()?)),
        Type::OID => Ok(PsqlValue::Oid(text_str.parse::<u32>()?)),
        Type::FLOAT8 => {
            // TODO: Ensure all values are properly parsed, including +/-0 and +/-inf.
            Ok(PsqlValue::Double(text_str.parse::<f64>()?))
        }
        Type::FLOAT4 => {
            // TODO: Ensure all values are properly parsed, including +/-0 and +/-inf.
            Ok(PsqlValue::Float(text_str.parse::<f32>()?))
        }
        Type::NUMERIC => Ok(PsqlValue::Numeric(Decimal::from_str(text_str)?)),
        Type::TEXT => Ok(PsqlValue::Text(text_str.into())),
        Type::TIMESTAMP => {
            // TODO: Does not correctly handle all valid timestamp representations. For example,
            // 8601/SQL timestamp format is assumed; infinity/-infinity are not supported.
            Ok(PsqlValue::Timestamp(NaiveDateTime::parse_from_str(
                text_str,
                TIMESTAMP_FORMAT,
            )?))
        }
        Type::TIMESTAMPTZ => Ok(PsqlValue::TimestampTz(
            DateTime::<FixedOffset>::parse_from_str(text_str, TIMESTAMP_TZ_FORMAT)?,
        )),
        Type::BYTEA => {
            let bytes = hex::decode(text_str).map_err(InvalidTextByteArrayValue)?;
            Ok(PsqlValue::ByteArray(bytes))
        }
        Type::MACADDR => MacAddress::parse_str(text_str)
            .map_err(DecodeError::InvalidTextMacAddressValue)
            .map(PsqlValue::MacAddress),
        Type::INET => text_str
            .parse::<IpInet>()
            .map_err(DecodeError::InvalidTextIpAddressValue)
            .map(PsqlValue::Inet),
        Type::UUID => Uuid::parse_str(text_str)
            .map_err(DecodeError::InvalidTextUuidValue)
            .map(PsqlValue::Uuid),
        Type::JSON => serde_json::from_str::<serde_json::Value>(text_str)
            .map_err(DecodeError::InvalidTextJsonValue)
            .map(PsqlValue::Json),
        Type::JSONB => serde_json::from_str::<serde_json::Value>(text_str)
            .map_err(DecodeError::InvalidTextJsonValue)
            .map(PsqlValue::Jsonb),
        Type::BIT => get_bitvec_from_str(text_str).map(PsqlValue::Bit),
        Type::VARBIT => get_bitvec_from_str(text_str).map(PsqlValue::VarBit),
        ref t if t.name() == "citext" => Ok(PsqlValue::Text(text_str.into())),
        _ => Err(Error::UnsupportedType(t.clone())),
    }
}

#[cfg(test)]
mod tests {

    use bytes::{BufMut, BytesMut};
    use postgres_types::ToSql;

    use super::*;
    use crate::value::PsqlValue;

    fn bytes_str(s: &str) -> BytesStr {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        BytesStr::try_from(buf.freeze()).unwrap()
    }

    #[test]
    fn test_decode_ssl_request() {
        let mut codec = Codec::new();
        let mut buf = BytesMut::new();
        buf.put_i32(8); // size
        buf.put_i32(80877103); // ssl request code
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(SSLRequest));
    }

    #[test]
    fn test_decode_ssl_request_extra_data() {
        let mut codec = Codec::new();
        let mut buf = BytesMut::new();
        buf.put_i32(9); // size
        buf.put_i32(80877103); // ssl request code
        buf.put_u8(4); // extra byte
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_startup_message() {
        let mut codec = Codec::new();
        let mut buf = BytesMut::new();
        buf.put_i32(4 + 4 + 5 + 10 + 9 + 14 + 1); // size
        buf.put_i32(196608); // standard protocol version
        buf.extend_from_slice(b"user\0");
        buf.extend_from_slice(b"user_name\0");
        buf.extend_from_slice(b"database\0");
        buf.extend_from_slice(b"database_name\0");
        buf.put_u8(b'\0');
        let expected = Some(StartupMessage {
            protocol_version: 196608,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_startup_message_ends_early() {
        let mut codec = Codec::new();
        let mut buf = BytesMut::new();
        buf.put_i32(4 + 2); // size
        buf.put_i16(0); // incomplete protocol version
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_startup_message_missing_nul() {
        let mut codec = Codec::new();
        let mut buf = BytesMut::new();
        buf.put_i32(4 + 4 + 5 + 9); // size
        buf.put_i32(196608); // standard protocol version
        buf.extend_from_slice(b"user\0");
        buf.extend_from_slice(b"user_name"); // trailing nul missing
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_startup_message_missing_field() {
        let mut codec = Codec::new();
        let mut buf = BytesMut::new();
        buf.put_i32(4 + 4 + 5); // size
        buf.put_i32(196608); // standard protocol version
        buf.extend_from_slice(b"user\0");
        // value of user field is missing
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_startup_partial_header() {
        let mut codec = Codec::new();
        let mut buf = BytesMut::new();
        buf.put_i16(5); // partial size
                        // header incomplete
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.capacity() >= 5);
    }

    #[test]
    fn test_decode_startup_partial_message() {
        let mut codec = Codec::new();
        let mut buf = BytesMut::new();
        buf.put_i32(8); // size
        buf.put_i16(4); // data
                        // message incomplete; remaining bytes indicated in size are absent
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.capacity() >= 8);
    }

    #[test]
    fn test_decode_regular_partial_header() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'F'); // message id
        buf.put_i16(5); // partial size
                        // header incomplete
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.capacity() >= 5);
    }

    #[test]
    fn test_decode_regular_partial_message() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'F'); // message id
        buf.put_i32(8); // size
        buf.put_i16(1); // partial data
                        // message incomplete; remaining bytes indicated in size are absent
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.capacity() >= 9);
    }

    #[test]
    fn test_decode_bind_simple() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        codec.set_statement_param_types("prepared_statement_name", vec![]);
        let mut buf = BytesMut::new();
        buf.put_u8(b'B'); // message id
        buf.put_i32(4 + 12 + 24 + 2 + 2 + 2); // size
        buf.extend_from_slice(b"portal_name\0");
        buf.extend_from_slice(b"prepared_statement_name\0");
        buf.put_i16(0); // number of parameter format codes
        buf.put_i16(0); // number of parameter values
        buf.put_i16(0); // number of result format codes
        let expected = Some(Bind {
            portal_name: bytes_str("portal_name"),
            prepared_statement_name: bytes_str("prepared_statement_name"),
            params: vec![],
            result_transfer_formats: vec![],
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_bind_complex() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        codec.set_statement_param_types("prepared_statement_name", vec![Type::INT4, Type::TEXT]);
        let mut buf = BytesMut::new();
        buf.put_u8(b'B'); // message id
        buf.put_i32(4 + 12 + 24 + 2 + 2 * 2 + 2 + 4 + 4 + 4 + 9 + 2 + 3 * 2); // size
        buf.extend_from_slice(b"portal_name\0");
        buf.extend_from_slice(b"prepared_statement_name\0");
        buf.put_i16(2); // number of parameter format codes
        buf.put_i16(1); // binary
        buf.put_i16(1); // binary
        buf.put_i16(2); // number of parameter values
        buf.put_i32(4); // value length
        buf.put_i32(42); // value `42`
        buf.put_i32(9); // value length
        buf.extend_from_slice(b"some text"); // value `some text`
        buf.put_i16(3); // number of result format codes
        buf.put_i16(1); // binary
        buf.put_i16(1); // binary
        buf.put_i16(0); // text
        let expected = Some(Bind {
            portal_name: bytes_str("portal_name"),
            prepared_statement_name: bytes_str("prepared_statement_name"),
            params: vec![PsqlValue::Int(42), PsqlValue::Text("some text".into())],
            result_transfer_formats: vec![Binary, Binary, Text],
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_bind_null() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        codec.set_statement_param_types("prepared_statement_name", vec![Type::TEXT]);
        let mut buf = BytesMut::new();
        buf.put_u8(b'B'); // message id
        buf.put_i32(4 + 12 + 24 + 2 + 2 + 2 + 4 + 2); // size
        buf.extend_from_slice(b"portal_name\0");
        buf.extend_from_slice(b"prepared_statement_name\0");
        buf.put_i16(1); // number of parameter format codes
        buf.put_i16(1);
        buf.put_i16(1); // number of parameter values
        buf.put_i32(-1);
        buf.put_i16(0); // number of result format codes
        let expected = Some(Bind {
            portal_name: bytes_str("portal_name"),
            prepared_statement_name: bytes_str("prepared_statement_name"),
            params: vec![PsqlValue::Null],
            result_transfer_formats: vec![],
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_bind_invalid_value() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        codec.set_statement_param_types("prepared_statement_name", vec![Type::TEXT]);
        let mut buf = BytesMut::new();
        buf.put_u8(b'B'); // message id
        buf.put_i32(4 + 12 + 24 + 2 + 2 + 2 + 4 + 10 + 2); // size
        buf.extend_from_slice(b"portal_name\0");
        buf.extend_from_slice(b"prepared_statement_name\0");
        buf.put_i16(1); // number of parameter format codes
        buf.put_i16(1); // binary
        buf.put_i16(1); // number of parameter values
        buf.put_i32(10); // value length
        buf.extend_from_slice(b"some text\0"); // value `some text\0` - nul not allowed
        buf.put_i16(0); // number of result format codes
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_bind_incomplete_format() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        codec.set_statement_param_types("prepared_statement_name", vec![]);
        let mut buf = BytesMut::new();
        buf.put_u8(b'B'); // message id
        buf.put_i32(4 + 12 + 24 + 2 + 2 + 2); // size
        buf.extend_from_slice(b"portal_name\0");
        buf.extend_from_slice(b"prepared_statement_name\0");
        buf.put_i16(0); // number of parameter format codes
        buf.put_i16(0); // number of parameter values
        buf.put_i16(1); // number of result format codes
                        // missing format code
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_close_portal() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'C'); // message id
        buf.put_i32(4 + 1 + 5); // size
        buf.put_u8(b'P'); // name type
        buf.extend_from_slice(b"name\0");
        let expected = Some(Close {
            name: Portal(bytes_str("name")),
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_close_prepared_statement() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'C'); // message id
        buf.put_i32(4 + 1 + 5); // size
        buf.put_u8(b'S'); // name type
        buf.extend_from_slice(b"name\0");
        let expected = Some(Close {
            name: PreparedStatement(bytes_str("name")),
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_close_invalid() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'C'); // message id
        buf.put_i32(4 + 1 + 5); // size
        buf.put_u8(b'I'); // invalid name type
        buf.extend_from_slice(b"name\0");
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_close_missing_type() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'C'); // message id
        buf.put_i32(4); // size
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_describe_portal() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'D'); // message id
        buf.put_i32(4 + 1 + 5); // size
        buf.put_u8(b'P'); // name type
        buf.extend_from_slice(b"name\0");
        let expected = Some(Describe {
            name: Portal(bytes_str("name")),
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_describe_prepared_statement() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'D'); // message id
        buf.put_i32(4 + 1 + 5); // size
        buf.put_u8(b'S'); // name type
        buf.extend_from_slice(b"name\0");
        let expected = Some(Describe {
            name: PreparedStatement(bytes_str("name")),
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_describe_invalid() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'D'); // message id
        buf.put_i32(4 + 1 + 5); // size
        buf.put_u8(b'I'); // invalid name type
        buf.extend_from_slice(b"name\0");
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_execute() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'E'); // message id
        buf.put_i32(4 + 12 + 4); // size
        buf.extend_from_slice(b"portal_name\0");
        buf.put_i32(5); // limit
        let expected = Some(Execute {
            portal_name: bytes_str("portal_name"),
            limit: 5,
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_parse_no_param_types() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'P'); // message id
        buf.put_i32(4 + 15 + 18 + 2); // size
        buf.extend_from_slice(b"statement_name\0");
        buf.extend_from_slice(b"SELECT * FROM bar\0");
        buf.put_i16(0);
        let expected = Some(Parse {
            prepared_statement_name: bytes_str("statement_name"),
            query: bytes_str("SELECT * FROM bar"),
            parameter_data_types: vec![],
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_parse_with_param_types() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'P'); // message id
        buf.put_i32(4 + 15 + 42 + 2 + 4 + 4); // size
        buf.extend_from_slice(b"statement_name\0");
        buf.extend_from_slice(b"SELECT * FROM bar WHERE a = $1 AND b = $2\0");
        buf.put_i16(2); // number of param types
        buf.put_i32(16); // type oid Bool
        buf.put_i32(20); // type oid Int8
        let expected = Some(Parse {
            prepared_statement_name: bytes_str("statement_name"),
            query: bytes_str("SELECT * FROM bar WHERE a = $1 AND b = $2"),
            parameter_data_types: vec![Type::BOOL, Type::INT8],
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_parse_with_incomplete_type() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'P'); // message id
        buf.put_i32(4 + 15 + 42 + 2 + 2); // size
        buf.extend_from_slice(b"statement_name\0");
        buf.extend_from_slice(b"SELECT * FROM bar WHERE a = $1 AND b = $2\0");
        buf.put_i16(1); // number of param types
        buf.put_i16(16); // incomplete type oid
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_parse_with_missing_type() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'P'); // message id
        buf.put_i32(4 + 15 + 42 + 2 + 4); // size
        buf.extend_from_slice(b"statement_name\0");
        buf.extend_from_slice(b"SELECT * FROM bar WHERE a = $1 AND b = $2\0");
        buf.put_i16(2); // number of param types
        buf.put_i32(16); // type oid Bool
                         // missing next type oid
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_query() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q'); // message id
        buf.put_i32(4 + 18); // size
        buf.extend_from_slice(b"SELECT * FROM foo\0");
        let expected = Some(Query {
            query: bytes_str("SELECT * FROM foo"),
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_query_missing_nul() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q'); // message id
        buf.put_i32(4 + 17); // size
        buf.extend_from_slice(b"SELECT * FROM foo");
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_query_extra_data() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q'); // message id
        buf.put_i32(4 + 19); // size
        buf.extend_from_slice(b"SELECT * FROM foo\0");
        buf.put_u8(b'X'); // extra byte
        codec.decode(&mut buf).unwrap_err();
    }

    #[test]
    fn test_decode_sync() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'S'); // message id
        buf.put_i32(4); // size
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(Sync));
    }

    #[test]
    fn test_decode_sync_after_invalid_message() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();

        // Append an invalid Close message.
        buf.put_u8(b'C'); // message id
        buf.put_i32(4 + 1 + 5); // size
        buf.put_u8(b'I'); // invalid name type
        buf.extend_from_slice(b"name\0");

        // Append a valid Sync message.
        buf.put_u8(b'S'); // message id
        buf.put_i32(4); // size

        // The codec returns an error when attempting to parse the invalid Close message.
        codec.decode(&mut buf).unwrap_err();

        // The codec successfully parses the Sync message. (The invalid Close message has been
        // skipped).
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(Sync));
    }

    #[test]
    fn test_decode_terminate() {
        let mut codec = Codec::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'X'); // message id
        buf.put_i32(4); // size
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(Terminate));
    }

    #[test]
    fn test_decode_binary_null() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // size
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::INT4).unwrap(),
            PsqlValue::Null
        );
    }

    #[test]
    fn test_decode_binary_bool() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // size
        buf.put_u8(true as u8); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::BOOL).unwrap(),
            PsqlValue::Bool(true)
        );
    }

    #[test]
    fn test_decode_binary_char() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // size
        buf.put_i8(8); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::CHAR).unwrap(),
            PsqlValue::Char(8)
        );
    }

    #[test]
    fn test_decode_binary_varchar() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::VARCHAR).unwrap(),
            PsqlValue::VarChar("mighty".into())
        );
    }

    #[test]
    fn test_decode_binary_bpchar() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::BPCHAR).unwrap(),
            PsqlValue::BpChar("mighty".into())
        );
    }

    #[test]
    fn test_decode_binary_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(4); // size
        buf.put_i32(0x12345678); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::INT4).unwrap(),
            PsqlValue::Int(0x12345678)
        );
    }

    #[test]
    fn test_decode_binary_big_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(8); // size
        buf.put_i64(0x1234567890abcdef); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::INT8).unwrap(),
            PsqlValue::BigInt(0x1234567890abcdef)
        );
    }

    #[test]
    fn test_decode_binary_small_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(2); // size
        buf.put_i16(0x1234); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::INT2).unwrap(),
            PsqlValue::SmallInt(0x1234)
        );
    }

    #[test]
    fn test_decode_binary_double() {
        let mut buf = BytesMut::new();
        buf.put_i32(8); // size
        buf.put_f64(0.1234567890123456); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::FLOAT8).unwrap(),
            PsqlValue::Double(0.1234567890123456)
        );
    }

    #[test]
    fn test_decode_binary_real() {
        let mut buf = BytesMut::new();
        buf.put_i32(4); // size
        buf.put_f32(0.12345678); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::FLOAT4).unwrap(),
            PsqlValue::Float(0.12345678)
        );
    }

    #[test]
    fn test_decode_binary_numeric() {
        let mut buf = BytesMut::new();
        let decimal = Decimal::new(1234567890123456, 16);
        buf.put_i32(-1); // length (placeholder)
        decimal.to_sql(&Type::NUMERIC, &mut buf).unwrap(); // add the actual value
        let value_len = buf.len() - 4;
        let mut window = buf
            .get_mut(0..4)
            .ok_or_else(|| Error::InternalError("error writing message field".to_string()))
            .unwrap();
        window.put_i32(value_len as i32); // put the actual length
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::NUMERIC).unwrap(),
            PsqlValue::Numeric(decimal)
        );
    }

    #[test]
    fn test_decode_binary_text() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::TEXT).unwrap(),
            PsqlValue::Text("mighty".into())
        );
    }

    #[test]
    fn test_decode_binary_timestamp() {
        let dt = NaiveDateTime::from_timestamp(1_000_000_000, 42_000_000);
        let mut buf = BytesMut::new();
        buf.put_i32(8); // size
        dt.to_sql(&Type::TIMESTAMP, &mut buf).unwrap(); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::TIMESTAMP).unwrap(),
            PsqlValue::Timestamp(dt)
        );
    }

    #[test]
    fn test_decode_binary_bytes() {
        let bytes = vec![0, 8, 39, 92, 100, 128];
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // length (placeholder)
        bytes.to_sql(&Type::BYTEA, &mut buf).unwrap(); // add value
        let value_len = buf.len() - 4;
        let mut window = buf
            .get_mut(0..4)
            .ok_or_else(|| Error::InternalError("error writing message field".to_string()))
            .unwrap();
        window.put_i32(value_len as i32); // put the actual length
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::BYTEA).unwrap(),
            PsqlValue::ByteArray(bytes)
        );
    }

    #[test]
    fn test_decode_binary_macaddr() {
        let macaddr = MacAddress::new([18, 52, 86, 171, 205, 239]);
        let mut buf = BytesMut::new();
        buf.put_i32(6);
        macaddr.to_sql(&Type::MACADDR, &mut buf).unwrap(); // add value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::MACADDR).unwrap(),
            PsqlValue::MacAddress(macaddr)
        );
    }

    #[test]
    fn test_decode_binary_uuid() {
        let uuid = Uuid::from_bytes([
            85, 14, 132, 0, 226, 155, 65, 212, 167, 22, 68, 102, 85, 68, 0, 0,
        ]);
        let mut buf = BytesMut::new();
        buf.put_i32(16);
        uuid.to_sql(&Type::UUID, &mut buf).unwrap(); // add value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::UUID).unwrap(),
            PsqlValue::Uuid(uuid)
        );
    }

    #[test]
    fn test_decode_binary_json() {
        let json = serde_json::from_str::<serde_json::Value>(
            "{\"name\":\"John Doe\",\"age\":43,\"phones\":[\"+44 1234567\",\"+44 2345678\"]}",
        )
        .unwrap();
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // size (placeholder)
        json.to_sql(&Type::JSON, &mut buf).unwrap(); // add value
        let value_len = buf.len() - 4;
        let mut window = buf
            .get_mut(0..4)
            .ok_or_else(|| Error::InternalError("error writing message field".to_string()))
            .unwrap();
        window.put_i32(value_len as i32); // put the actual length
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::JSON).unwrap(),
            PsqlValue::Json(json.clone())
        );

        let mut buf = BytesMut::new();
        buf.put_i32(-1); // size (placeholder)
        json.to_sql(&Type::JSONB, &mut buf).unwrap(); // add value
        let value_len = buf.len() - 4;
        let mut window = buf
            .get_mut(0..4)
            .ok_or_else(|| Error::InternalError("error writing message field".to_string()))
            .unwrap();
        window.put_i32(value_len as i32); // put the actual length
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::JSONB).unwrap(),
            PsqlValue::Jsonb(json)
        );
    }

    #[test]
    fn test_decode_binary_bits() {
        // bits = 000000000000100000100111010111000110010010000000
        let bits = BitVec::from_bytes(&[0, 8, 39, 92, 100, 128]);
        let mut buf = BytesMut::new();
        // 48 bits divided into groups of 8 (a byte) = 6 bytes, plus one u32 (4 bytes) to hold the
        // size = 10 bytes
        buf.put_i32(10); // size
        bits.to_sql(&Type::BIT, &mut buf).unwrap(); // add value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::BIT).unwrap(),
            PsqlValue::Bit(bits.clone())
        );

        let mut buf = BytesMut::new();
        buf.put_i32(10); // size
        bits.to_sql(&Type::VARBIT, &mut buf).unwrap(); // add value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::VARBIT).unwrap(),
            PsqlValue::VarBit(bits)
        );
    }

    #[test]
    fn test_decode_binary_timestamp_tz() {
        let dt = DateTime::<FixedOffset>::from_utc(
            NaiveDateTime::new(
                NaiveDate::from_ymd(2020, 1, 2),
                NaiveTime::from_hms_milli(3, 4, 5, 660),
            ),
            FixedOffset::east(18000), // +05:00
        );
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // size (placeholder)
        dt.to_sql(&Type::TIMESTAMPTZ, &mut buf).unwrap(); // add value
        let value_len = buf.len() - 4;
        let mut window = buf
            .get_mut(0..4)
            .ok_or_else(|| Error::InternalError("error writing message field".to_string()))
            .unwrap();
        window.put_i32(value_len as i32); // put the actual length
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::TIMESTAMPTZ).unwrap(),
            PsqlValue::TimestampTz(dt)
        );
    }

    #[test]
    fn test_decode_text_null() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // size
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::INT4).unwrap(),
            PsqlValue::Null
        );
    }

    #[test]
    fn test_decode_text_bool() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // size
        buf.extend_from_slice(b"t"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::BOOL).unwrap(),
            PsqlValue::Bool(true)
        );
    }

    #[test]
    fn test_decode_text_char() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // size
        buf.extend_from_slice(b"8"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::CHAR).unwrap(),
            PsqlValue::Char(8)
        );
    }

    #[test]
    fn test_decode_text_varchar() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::VARCHAR).unwrap(),
            PsqlValue::VarChar("mighty".into())
        );
    }

    #[test]
    fn test_decode_text_bpchar() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::BPCHAR).unwrap(),
            PsqlValue::BpChar("mighty".into())
        );
    }

    #[test]
    fn test_decode_text_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(9); // size
        buf.extend_from_slice(b"305419896"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::INT4).unwrap(),
            PsqlValue::Int(0x12345678)
        );
    }

    #[test]
    fn test_decode_text_big_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(19); // size
        buf.extend_from_slice(b"1311768467294899695"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::INT8).unwrap(),
            PsqlValue::BigInt(0x1234567890abcdef)
        );
    }

    #[test]
    fn test_decode_text_small_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(4); // size
        buf.extend_from_slice(b"4660"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::INT2).unwrap(),
            PsqlValue::SmallInt(0x1234)
        );
    }

    #[test]
    fn test_decode_text_double() {
        let mut buf = BytesMut::new();
        buf.put_i32(18); // size
        buf.extend_from_slice(b"0.1234567890123456"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::FLOAT8).unwrap(),
            PsqlValue::Double(0.1234567890123456)
        );
    }

    #[test]
    fn test_decode_text_real() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // size
        buf.extend_from_slice(b"0.12345678"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::FLOAT4).unwrap(),
            PsqlValue::Float(0.12345678)
        );
    }

    #[test]
    fn test_decode_text_numeric() {
        let mut buf = BytesMut::new();
        buf.put_i32(18); // size
        buf.extend_from_slice(b"0.1234567890123456"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::NUMERIC).unwrap(),
            PsqlValue::Numeric(Decimal::new(1234567890123456, 16))
        );
    }

    #[test]
    fn test_decode_text_text() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::TEXT).unwrap(),
            PsqlValue::Text("mighty".into())
        );
    }

    #[test]
    fn test_decode_text_timestamp() {
        let mut buf = BytesMut::new();
        buf.put_i32(22); // size
        buf.extend_from_slice(b"2020-01-02 03:04:05.66"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::TIMESTAMP).unwrap(),
            PsqlValue::Timestamp(
                NaiveDateTime::parse_from_str("2020-01-02 03:04:05.66", TIMESTAMP_FORMAT).unwrap()
            )
        );
    }

    #[test]
    fn test_decode_text_bytes() {
        let mut buf = BytesMut::new();
        buf.put_i32(12);
        buf.extend_from_slice(b"0008275c6480");
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::BYTEA).unwrap(),
            PsqlValue::ByteArray(vec![0, 8, 39, 92, 100, 128])
        );
    }

    #[test]
    fn test_decode_text_macaddr() {
        let mut buf = BytesMut::new();
        buf.put_i32(17);
        buf.extend_from_slice(b"12:34:56:AB:CD:EF");
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::MACADDR).unwrap(),
            PsqlValue::MacAddress(MacAddress::new([18, 52, 86, 171, 205, 239]))
        );
    }

    #[test]
    fn test_decode_text_uuid() {
        let mut buf = BytesMut::new();
        buf.put_i32(36);
        buf.extend_from_slice(b"550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::UUID).unwrap(),
            PsqlValue::Uuid(Uuid::from_bytes([
                85, 14, 132, 0, 226, 155, 65, 212, 167, 22, 68, 102, 85, 68, 0, 0
            ]))
        );
    }

    #[test]
    fn test_decode_text_json() {
        let json_str =
            "{\"name\":\"John Doe\",\"age\":43,\"phones\":[\"+44 1234567\",\"+44 2345678\"]}";
        let expected = serde_json::from_str::<serde_json::Value>(json_str).unwrap();
        let mut buf = BytesMut::new();
        buf.put_i32(67);
        buf.extend_from_slice(json_str.as_bytes());
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::JSON).unwrap(),
            PsqlValue::Json(expected.clone())
        );

        let mut buf = BytesMut::new();
        buf.put_i32(67);
        buf.extend_from_slice(json_str.as_bytes());
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::JSONB).unwrap(),
            PsqlValue::Jsonb(expected)
        );
    }

    #[test]
    fn test_decode_text_bits() {
        let mut buf = BytesMut::new();
        buf.put_i32(48); // 48 bit characters in the text
        buf.extend_from_slice(b"000000000000100000100111010111000110010010000000");
        assert_eq!(
            get_text_value(&mut buf.clone().freeze(), &Type::BIT).unwrap(),
            PsqlValue::Bit(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128]))
        );

        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::VARBIT).unwrap(),
            PsqlValue::VarBit(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128]))
        );
    }

    #[test]
    fn test_decode_text_timestamp_tz() {
        let dt_string = "2020-01-02 08:04:05.660 +05:00";
        let expected = DateTime::<FixedOffset>::from_utc(
            NaiveDateTime::new(
                NaiveDate::from_ymd(2020, 1, 2),
                NaiveTime::from_hms_milli(3, 4, 5, 660),
            ),
            FixedOffset::east(18000), // +05:00
        );
        let mut buf = BytesMut::new();
        buf.put_i32(30); // size (placeholder)
        buf.extend_from_slice(dt_string.as_bytes());
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::TIMESTAMPTZ).unwrap(),
            PsqlValue::TimestampTz(expected)
        );
    }

    #[test]
    fn test_parse_msg_with_undefined_type() {
        let mut codec = Codec::new();
        codec.is_starting_up = false;
        let mut header = BytesMut::with_capacity(HEADER_LENGTH);
        let mut body = BytesMut::new();

        let prepared_statement_name = Bytes::from_static(b"foo");
        let query = Bytes::from_static(b"SELECT 1");

        body.put(&prepared_statement_name[..]);
        // Strings are terminated with \0 in the encoding
        body.put_u8(NUL_BYTE);
        body.put(&query[..]);
        body.put_u8(NUL_BYTE);
        body.put_i16(1); // n_parameter_data_types=1
        body.put_i32(0); // oid=0 indicating unspecified data type

        header.put_u8(ID_PARSE);

        // Length of message includes body and length itself, but not id byte of header
        header.put_i32((body.len() + HEADER_LENGTH - 1) as i32);

        let mut parse_msg = BytesMut::new();
        parse_msg.put(header);
        parse_msg.put(body);

        let frontend_msg = FrontendMessage::Parse {
            prepared_statement_name: BytesStr::try_from(prepared_statement_name).unwrap(),
            query: BytesStr::try_from(query).unwrap(),
            parameter_data_types: vec![Type::UNKNOWN],
        };

        assert_eq!(codec.decode(&mut parse_msg).unwrap(), Some(frontend_msg));
    }
}
