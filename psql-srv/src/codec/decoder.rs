use std::borrow::Borrow;
use std::convert::{TryFrom, TryInto};

use bit_vec::BitVec;
use bytes::{Buf, Bytes, BytesMut};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use eui48::MacAddress;
use postgres_types::{FromSql, Type};
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use tokio_util::codec::Decoder;
use uuid::Uuid;

use crate::bytes::BytesStr;
use crate::codec::error::DecodeError as Error;
use crate::codec::error::DecodeError::InvalidTextByteArrayValue;
use crate::codec::{Codec, DecodeError};
use crate::error::Error as BackendError;
use crate::message::FrontendMessage::{self, *};
use crate::message::StatementName::*;
use crate::message::TransferFormat::{self, *};
use crate::value::Value;

const ID_BIND: u8 = b'B';
const ID_CLOSE: u8 = b'C';
const ID_DESCRIBE: u8 = b'D';
const ID_EXECUTE: u8 = b'E';
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

impl<R: IntoIterator<Item: TryInto<Value, Error = BackendError>>> Decoder for Codec<R> {
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
                    .collect::<Result<Vec<Value>, Error>>()?;

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

            ID_TERMINATE => Ok(Some(Terminate)),

            id => Err(Error::UnsupportedMessage(id)),
        };

        if msg.remaining() > 0 {
            return Err(Error::UnexpectedMessageEnd);
        }

        ret
    }
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
    Type::from_oid(oid).ok_or(Error::InvalidType(oid))
}

fn get_binary_value(src: &mut Bytes, t: &Type) -> Result<Value, Error> {
    let len = get_i32(src)?;
    if len == LENGTH_NULL_SENTINEL {
        return Ok(Value::Null);
    }

    let buf = &mut src.split_to(usize::try_from(len)?);

    match *t {
        // Postgres does not allow interior 0 bytes, even thought is is valid UTF-8
        Type::CHAR | Type::VARCHAR | Type::TEXT | Type::NAME if buf.contains(&0) => {
            Err(Error::InvalidUtf8)
        }
        Type::BOOL => Ok(Value::Bool(bool::from_sql(t, buf)?)),
        Type::CHAR => Ok(Value::Char(<&str>::from_sql(t, buf)?.into())),
        Type::VARCHAR => Ok(Value::Varchar(<&str>::from_sql(t, buf)?.into())),
        Type::NAME => Ok(Value::Name(<&str>::from_sql(t, buf)?.into())),
        Type::INT4 => Ok(Value::Int(i32::from_sql(t, buf)?)),
        Type::INT8 => Ok(Value::Bigint(i64::from_sql(t, buf)?)),
        Type::INT2 => Ok(Value::Smallint(i16::from_sql(t, buf)?)),
        Type::OID => Ok(Value::Oid(u32::from_sql(t, buf)?)),
        Type::FLOAT8 => Ok(Value::Double(f64::from_sql(t, buf)?)),
        Type::FLOAT4 => Ok(Value::Float(f32::from_sql(t, buf)?)),
        Type::NUMERIC => Ok(Value::Numeric(Decimal::from_sql(t, buf)?)),
        Type::TEXT => Ok(Value::Text(<&str>::from_sql(t, buf)?.into())),
        Type::DATE => Ok(Value::Date(NaiveDate::from_sql(t, buf)?)),
        Type::TIME => Ok(Value::Time(NaiveTime::from_sql(t, buf)?)),
        Type::TIMESTAMP => Ok(Value::Timestamp(NaiveDateTime::from_sql(t, buf)?)),
        Type::TIMESTAMPTZ => Ok(Value::TimestampTz(DateTime::<FixedOffset>::from_sql(
            t, buf,
        )?)),
        Type::BYTEA => Ok(Value::ByteArray(<Vec<u8>>::from_sql(t, buf)?)),
        Type::MACADDR => Ok(Value::MacAddress(MacAddress::from_sql(t, buf)?)),
        Type::UUID => Ok(Value::Uuid(Uuid::from_sql(t, buf)?)),
        Type::JSON => Ok(Value::Json(serde_json::Value::from_sql(t, buf)?)),
        Type::JSONB => Ok(Value::Jsonb(serde_json::Value::from_sql(t, buf)?)),
        Type::BIT => Ok(Value::Bit(BitVec::from_sql(t, buf)?)),
        Type::VARBIT => Ok(Value::VarBit(BitVec::from_sql(t, buf)?)),
        _ => Err(Error::UnsupportedType(t.clone())),
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

fn get_text_value(src: &mut Bytes, t: &Type) -> Result<Value, Error> {
    let len = get_i32(src)?;
    if len == LENGTH_NULL_SENTINEL {
        return Ok(Value::Null);
    }

    let text = BytesStr::try_from(src.split_to(usize::try_from(len)?))?;
    let text_str: &str = text.borrow();
    match *t {
        Type::BOOL => Ok(Value::Bool(text_str == BOOL_TRUE_TEXT_REP)),
        Type::CHAR => Ok(Value::Char(text_str.into())),
        Type::VARCHAR => Ok(Value::Varchar(text_str.into())),
        Type::NAME => Ok(Value::Name(text_str.into())),
        Type::INT4 => Ok(Value::Int(text_str.parse::<i32>()?)),
        Type::INT8 => Ok(Value::Bigint(text_str.parse::<i64>()?)),
        Type::INT2 => Ok(Value::Smallint(text_str.parse::<i16>()?)),
        Type::OID => Ok(Value::Oid(text_str.parse::<u32>()?)),
        Type::FLOAT8 => {
            // TODO: Ensure all values are properly parsed, including +/-0 and +/-inf.
            Ok(Value::Double(text_str.parse::<f64>()?))
        }
        Type::FLOAT4 => {
            // TODO: Ensure all values are properly parsed, including +/-0 and +/-inf.
            Ok(Value::Float(text_str.parse::<f32>()?))
        }
        Type::NUMERIC => Ok(Value::Numeric(Decimal::from_str(text_str)?)),
        Type::TEXT => Ok(Value::Text(text_str.into())),
        Type::TIMESTAMP => {
            // TODO: Does not correctly handle all valid timestamp representations. For example,
            // 8601/SQL timestamp format is assumed; infinity/-infinity are not supported.
            Ok(Value::Timestamp(NaiveDateTime::parse_from_str(
                text_str,
                TIMESTAMP_FORMAT,
            )?))
        }
        Type::TIMESTAMPTZ => Ok(Value::TimestampTz(DateTime::<FixedOffset>::parse_from_str(
            text_str,
            TIMESTAMP_TZ_FORMAT,
        )?)),
        Type::BYTEA => {
            let bytes = hex::decode(text_str).map_err(InvalidTextByteArrayValue)?;
            Ok(Value::ByteArray(bytes))
        }
        Type::MACADDR => MacAddress::parse_str(text_str)
            .map_err(DecodeError::InvalidTextMacAddressValue)
            .map(Value::MacAddress),
        Type::UUID => Uuid::parse_str(text_str)
            .map_err(DecodeError::InvalidTextUuidValue)
            .map(Value::Uuid),
        Type::JSON => serde_json::from_str::<serde_json::Value>(text_str)
            .map_err(DecodeError::InvalidTextJsonValue)
            .map(Value::Json),
        Type::JSONB => serde_json::from_str::<serde_json::Value>(text_str)
            .map_err(DecodeError::InvalidTextJsonValue)
            .map(Value::Jsonb),
        Type::BIT => get_bitvec_from_str(text_str).map(Value::Bit),
        Type::VARBIT => get_bitvec_from_str(text_str).map(Value::VarBit),
        _ => Err(Error::UnsupportedType(t.clone())),
    }
}

#[cfg(test)]
mod tests {

    use bytes::{BufMut, BytesMut};
    use postgres_types::ToSql;

    use super::*;
    use crate::value::Value as DataValue;

    struct Value(DataValue);

    impl TryFrom<Value> for DataValue {
        type Error = BackendError;

        fn try_from(v: Value) -> Result<Self, Self::Error> {
            Ok(v.0)
        }
    }

    fn bytes_str(s: &str) -> BytesStr {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        BytesStr::try_from(buf.freeze()).unwrap()
    }

    #[test]
    fn test_decode_ssl_request() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        buf.put_i32(8); // size
        buf.put_i32(80877103); // ssl request code
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(SSLRequest));
    }

    #[test]
    fn test_decode_ssl_request_extra_data() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        buf.put_i32(9); // size
        buf.put_i32(80877103); // ssl request code
        buf.put_u8(4); // extra byte
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_startup_message() {
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        buf.put_i32(4 + 2); // size
        buf.put_i16(0); // incomplete protocol version
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_startup_message_missing_nul() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        buf.put_i32(4 + 4 + 5 + 9); // size
        buf.put_i32(196608); // standard protocol version
        buf.extend_from_slice(b"user\0");
        buf.extend_from_slice(b"user_name"); // trailing nul missing
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_startup_message_missing_field() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        buf.put_i32(4 + 4 + 5); // size
        buf.put_i32(196608); // standard protocol version
        buf.extend_from_slice(b"user\0");
        // value of user field is missing
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_startup_partial_header() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        buf.put_i16(5); // partial size
                        // header incomplete
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.capacity() >= 5);
    }

    #[test]
    fn test_decode_startup_partial_message() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        buf.put_i32(8); // size
        buf.put_i16(4); // data
                        // message incomplete; remaining bytes indicated in size are absent
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.capacity() >= 8);
    }

    #[test]
    fn test_decode_regular_partial_header() {
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
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
            params: vec![DataValue::Int(42), DataValue::Text("some text".into())],
            result_transfer_formats: vec![Binary, Binary, Text],
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_bind_null() {
        let mut codec = Codec::<Vec<Value>>::new();
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
            params: vec![DataValue::Null],
            result_transfer_formats: vec![],
        });
        assert_eq!(codec.decode(&mut buf).unwrap(), expected);
    }

    #[test]
    fn test_decode_bind_invalid_value() {
        let mut codec = Codec::<Vec<Value>>::new();
        codec.set_start_up_complete();
        codec.set_statement_param_types("prepared_statement_name", vec![Type::TEXT]);
        let mut buf = BytesMut::new();
        buf.put_u8(b'B'); // message id
        buf.put_i32(4 + 12 + 24 + 2 + 1 * 2 + 2 + 4 + 10 + 2); // size
        buf.extend_from_slice(b"portal_name\0");
        buf.extend_from_slice(b"prepared_statement_name\0");
        buf.put_i16(1); // number of parameter format codes
        buf.put_i16(1); // binary
        buf.put_i16(1); // number of parameter values
        buf.put_i32(10); // value length
        buf.extend_from_slice(b"some text\0"); // value `some text\0` - nul not allowed
        buf.put_i16(0); // number of result format codes
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_bind_incomplete_format() {
        let mut codec = Codec::<Vec<Value>>::new();
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
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_close_portal() {
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'C'); // message id
        buf.put_i32(4 + 1 + 5); // size
        buf.put_u8(b'I'); // invalid name type
        buf.extend_from_slice(b"name\0");
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_close_missing_type() {
        let mut codec = Codec::<Vec<Value>>::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'C'); // message id
        buf.put_i32(4); // size
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_describe_portal() {
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'D'); // message id
        buf.put_i32(4 + 1 + 5); // size
        buf.put_u8(b'I'); // invalid name type
        buf.extend_from_slice(b"name\0");
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_execute() {
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'P'); // message id
        buf.put_i32(4 + 15 + 42 + 2 + 2); // size
        buf.extend_from_slice(b"statement_name\0");
        buf.extend_from_slice(b"SELECT * FROM bar WHERE a = $1 AND b = $2\0");
        buf.put_i16(1); // number of param types
        buf.put_i16(16); // incomplete type oid
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_parse_with_missing_type() {
        let mut codec = Codec::<Vec<Value>>::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'P'); // message id
        buf.put_i32(4 + 15 + 42 + 2 + 4); // size
        buf.extend_from_slice(b"statement_name\0");
        buf.extend_from_slice(b"SELECT * FROM bar WHERE a = $1 AND b = $2\0");
        buf.put_i16(2); // number of param types
        buf.put_i32(16); // type oid Bool
                         // missing next type oid
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_query() {
        let mut codec = Codec::<Vec<Value>>::new();
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
        let mut codec = Codec::<Vec<Value>>::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q'); // message id
        buf.put_i32(4 + 17); // size
        buf.extend_from_slice(b"SELECT * FROM foo");
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_query_extra_data() {
        let mut codec = Codec::<Vec<Value>>::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q'); // message id
        buf.put_i32(4 + 19); // size
        buf.extend_from_slice(b"SELECT * FROM foo\0");
        buf.put_u8(b'X'); // extra byte
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_decode_sync() {
        let mut codec = Codec::<Vec<Value>>::new();
        codec.set_start_up_complete();
        let mut buf = BytesMut::new();
        buf.put_u8(b'S'); // message id
        buf.put_i32(4); // size
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(Sync));
    }

    #[test]
    fn test_decode_sync_after_invalid_message() {
        let mut codec = Codec::<Vec<Value>>::new();
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
        assert!(codec.decode(&mut buf).is_err());

        // The codec successfully parses the Sync message. (The invalid Close message has been
        // skipped).
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(Sync));
    }

    #[test]
    fn test_decode_terminate() {
        let mut codec = Codec::<Vec<Value>>::new();
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
            DataValue::Null
        );
    }

    #[test]
    fn test_decode_binary_bool() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // size
        buf.put_u8(true as u8); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::BOOL).unwrap(),
            DataValue::Bool(true)
        );
    }

    #[test]
    fn test_decode_binary_char() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::CHAR).unwrap(),
            DataValue::Char("mighty".into())
        );
    }

    #[test]
    fn test_decode_binary_varchar() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::VARCHAR).unwrap(),
            DataValue::Varchar("mighty".into())
        );
    }

    #[test]
    fn test_decode_binary_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(4); // size
        buf.put_i32(0x12345678); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::INT4).unwrap(),
            DataValue::Int(0x12345678)
        );
    }

    #[test]
    fn test_decode_binary_big_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(8); // size
        buf.put_i64(0x1234567890abcdef); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::INT8).unwrap(),
            DataValue::Bigint(0x1234567890abcdef)
        );
    }

    #[test]
    fn test_decode_binary_small_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(2); // size
        buf.put_i16(0x1234); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::INT2).unwrap(),
            DataValue::Smallint(0x1234)
        );
    }

    #[test]
    fn test_decode_binary_double() {
        let mut buf = BytesMut::new();
        buf.put_i32(8); // size
        buf.put_f64(0.1234567890123456); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::FLOAT8).unwrap(),
            DataValue::Double(0.1234567890123456)
        );
    }

    #[test]
    fn test_decode_binary_real() {
        let mut buf = BytesMut::new();
        buf.put_i32(4); // size
        buf.put_f32(0.12345678); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::FLOAT4).unwrap(),
            DataValue::Float(0.12345678)
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
            DataValue::Numeric(decimal)
        );
    }

    #[test]
    fn test_decode_binary_text() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::TEXT).unwrap(),
            DataValue::Text("mighty".into())
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
            DataValue::Timestamp(dt)
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
            DataValue::ByteArray(bytes)
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
            DataValue::MacAddress(macaddr)
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
            DataValue::Uuid(uuid)
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
            DataValue::Json(json.clone())
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
            DataValue::Jsonb(json)
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
            DataValue::Bit(bits.clone())
        );

        let mut buf = BytesMut::new();
        buf.put_i32(10); // size
        bits.to_sql(&Type::VARBIT, &mut buf).unwrap(); // add value
        assert_eq!(
            get_binary_value(&mut buf.freeze(), &Type::VARBIT).unwrap(),
            DataValue::VarBit(bits)
        );
    }

    #[test]
    fn test_decode_binary_timestamp_tz() {
        let dt = DateTime::<FixedOffset>::from_utc(
            NaiveDateTime::new(
                NaiveDate::from_ymd(2020, 01, 02),
                NaiveTime::from_hms_milli(03, 04, 05, 660),
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
            DataValue::TimestampTz(dt)
        );
    }

    #[test]
    fn test_decode_text_null() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // size
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::INT4).unwrap(),
            DataValue::Null
        );
    }

    #[test]
    fn test_decode_text_bool() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // size
        buf.extend_from_slice(b"t"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::BOOL).unwrap(),
            DataValue::Bool(true)
        );
    }

    #[test]
    fn test_decode_text_char() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::CHAR).unwrap(),
            DataValue::Char("mighty".into())
        );
    }

    #[test]
    fn test_decode_text_varchar() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::VARCHAR).unwrap(),
            DataValue::Varchar("mighty".into())
        );
    }

    #[test]
    fn test_decode_text_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(9); // size
        buf.extend_from_slice(b"305419896"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::INT4).unwrap(),
            DataValue::Int(0x12345678)
        );
    }

    #[test]
    fn test_decode_text_big_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(19); // size
        buf.extend_from_slice(b"1311768467294899695"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::INT8).unwrap(),
            DataValue::Bigint(0x1234567890abcdef)
        );
    }

    #[test]
    fn test_decode_text_small_int() {
        let mut buf = BytesMut::new();
        buf.put_i32(4); // size
        buf.extend_from_slice(b"4660"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::INT2).unwrap(),
            DataValue::Smallint(0x1234)
        );
    }

    #[test]
    fn test_decode_text_double() {
        let mut buf = BytesMut::new();
        buf.put_i32(18); // size
        buf.extend_from_slice(b"0.1234567890123456"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::FLOAT8).unwrap(),
            DataValue::Double(0.1234567890123456)
        );
    }

    #[test]
    fn test_decode_text_real() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // size
        buf.extend_from_slice(b"0.12345678"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::FLOAT4).unwrap(),
            DataValue::Float(0.12345678)
        );
    }

    #[test]
    fn test_decode_text_numeric() {
        let mut buf = BytesMut::new();
        buf.put_i32(18); // size
        buf.extend_from_slice(b"0.1234567890123456"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::NUMERIC).unwrap(),
            DataValue::Numeric(Decimal::new(1234567890123456, 16))
        );
    }

    #[test]
    fn test_decode_text_text() {
        let mut buf = BytesMut::new();
        buf.put_i32(6); // size
        buf.extend_from_slice(b"mighty"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::TEXT).unwrap(),
            DataValue::Text("mighty".into())
        );
    }

    #[test]
    fn test_decode_text_timestamp() {
        let mut buf = BytesMut::new();
        buf.put_i32(22); // size
        buf.extend_from_slice(b"2020-01-02 03:04:05.66"); // value
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::TIMESTAMP).unwrap(),
            DataValue::Timestamp(
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
            DataValue::ByteArray(vec![0, 8, 39, 92, 100, 128])
        );
    }

    #[test]
    fn test_decode_text_macaddr() {
        let mut buf = BytesMut::new();
        buf.put_i32(17);
        buf.extend_from_slice(b"12:34:56:AB:CD:EF");
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::MACADDR).unwrap(),
            DataValue::MacAddress(MacAddress::new([18, 52, 86, 171, 205, 239]))
        );
    }

    #[test]
    fn test_decode_text_uuid() {
        let mut buf = BytesMut::new();
        buf.put_i32(36);
        buf.extend_from_slice(b"550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::UUID).unwrap(),
            DataValue::Uuid(Uuid::from_bytes([
                85, 14, 132, 0, 226, 155, 65, 212, 167, 22, 68, 102, 85, 68, 0, 0
            ]))
        );
    }

    #[test]
    fn test_decode_text_json() {
        let json_str =
            "{\"name\":\"John Doe\",\"age\":43,\"phones\":[\"+44 1234567\",\"+44 2345678\"]}";
        let expected = serde_json::from_str::<serde_json::Value>(json_str.clone()).unwrap();
        let mut buf = BytesMut::new();
        buf.put_i32(67);
        buf.extend_from_slice(json_str.as_bytes());
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::JSON).unwrap(),
            DataValue::Json(expected.clone())
        );

        let mut buf = BytesMut::new();
        buf.put_i32(67);
        buf.extend_from_slice(json_str.as_bytes());
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::JSONB).unwrap(),
            DataValue::Jsonb(expected)
        );
    }

    #[test]
    fn test_decode_text_bits() {
        let mut buf = BytesMut::new();
        buf.put_i32(48); // 48 bit characters in the text
        buf.extend_from_slice(b"000000000000100000100111010111000110010010000000");
        assert_eq!(
            get_text_value(&mut buf.clone().freeze(), &Type::BIT).unwrap(),
            DataValue::Bit(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128]))
        );

        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::VARBIT).unwrap(),
            DataValue::VarBit(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128]))
        );
    }

    #[test]
    fn test_decode_text_timestamp_tz() {
        let dt_string = "2020-01-02 08:04:05.660 +05:00";
        let expected = DateTime::<FixedOffset>::from_utc(
            NaiveDateTime::new(
                NaiveDate::from_ymd(2020, 01, 02),
                NaiveTime::from_hms_milli(03, 04, 05, 660),
            ),
            FixedOffset::east(18000), // +05:00
        );
        let mut buf = BytesMut::new();
        buf.put_i32(30); // size (placeholder)
        buf.extend_from_slice(dt_string.as_bytes());
        assert_eq!(
            get_text_value(&mut buf.freeze(), &Type::TIMESTAMPTZ).unwrap(),
            DataValue::TimestampTz(expected)
        );
    }
}
