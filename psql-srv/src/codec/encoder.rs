use crate::codec::error::EncodeError as Error;
use crate::codec::Codec;
use crate::error::Error as BackendError;
use crate::message::{
    BackendMessage::{self, *},
    CommandCompleteTag::*,
    ErrorSeverity,
    TransferFormat::{self, *},
};
use crate::value::Value;
use bytes::{BufMut, BytesMut};
use postgres_types::{ToSql, Type};
use std::convert::TryFrom;
use std::convert::TryInto;
use tokio_util::codec::Encoder;

const ID_AUTHENTICATION_OK: u8 = b'R';
const ID_BIND_COMPLETE: u8 = b'2';
const ID_CLOSE_COMPLETE: u8 = b'3';
const ID_COMMAND_COMPLETE: u8 = b'C';
const ID_DATA_ROW: u8 = b'D';
const ID_ERROR_RESPONSE: u8 = b'E';
const ID_PARAMETER_DESCRIPTION: u8 = b't';
const ID_PARAMETER_STATUS: u8 = b'S';
const ID_PARSE_COMPLETE: u8 = b'1';
const ID_READY_FOR_QUERY: u8 = b'Z';
const ID_ROW_DESCRIPTION: u8 = b'T';

const AUTHENTICATION_OK_SUCCESS: i32 = 0;

const COMMAND_COMPLETE_DELETE_TAG: &str = "DELETE";
const COMMAND_COMPLETE_INSERT_TAG: &str = "INSERT";
const COMMAND_COMPLETE_INSERT_LEGACY_OID: &str = "0";
const COMMAND_COMPLETE_SELECT_TAG: &str = "SELECT";
const COMMAND_COMPLETE_UPDATE_TAG: &str = "UPDATE";
const COMMAND_COMPLETE_TAG_BUF_LEN: usize = 32;

const ERROR_RESPONSE_C_FIELD: u8 = b'C';
const ERROR_RESPONSE_M_FIELD: u8 = b'M';
const ERROR_RESPONSE_S_FIELD: u8 = b'S';
const ERROR_RESPONSE_V_FIELD: u8 = b'V';
const ERROR_RESPONSE_SEVERITY_ERROR: &str = "ERROR";
const ERROR_RESPONSE_SEVERITY_FATAL: &str = "FATAL";
const ERROR_RESPONSE_SEVERITY_PANIC: &str = "PANIC";
const ERROR_RESPONSE_TERMINATOR: u8 = b'\0';

const BOOL_FALSE_TEXT_REP: &str = "f";
const BOOL_TRUE_TEXT_REP: &str = "t";
const COUNT_PLACEHOLDER: i16 = -1;
const LENGTH_NULL_SENTINEL: i32 = -1;
const LENGTH_PLACEHOLDER: i32 = -1;
const NUL_BYTE: u8 = b'\0';
const NUL_CHAR: char = '\0';
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

impl<R> Encoder<BackendMessage<R>> for Codec<R>
where
    R: IntoIterator<Item: TryInto<Value, Error = BackendError>>,
{
    type Error = Error;

    fn encode(&mut self, message: BackendMessage<R>, dst: &mut BytesMut) -> Result<(), Error> {
        let start_ofs = dst.len();
        encode(message, dst).map_err(|e| {
            // On an encoding error, remove any partially encoded data.
            dst.truncate(start_ofs);
            e
        })
    }
}

fn encode<R>(message: BackendMessage<R>, dst: &mut BytesMut) -> Result<(), Error>
where
    R: IntoIterator<Item: TryInto<Value, Error = BackendError>>,
{
    use std::io::Write;

    // Handle SSLResponse as a special case, since it has a nonstandard message format.
    if let SSLResponse { byte } = message {
        put_u8(byte, dst);
        return Ok(());
    }

    let start_ofs = dst.len();

    match message {
        AuthenticationOk => {
            put_u8(ID_AUTHENTICATION_OK, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
            put_i32(AUTHENTICATION_OK_SUCCESS, dst);
        }

        BindComplete => {
            put_u8(ID_BIND_COMPLETE, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
        }

        CloseComplete => {
            put_u8(ID_CLOSE_COMPLETE, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
        }

        CommandComplete { tag } => {
            put_u8(ID_COMMAND_COMPLETE, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
            // Format command complete "tag" (eg "DELETE 5" to indicate 5 rows deleted).
            let mut tag_buf = [0u8; COMMAND_COMPLETE_TAG_BUF_LEN];
            match tag {
                Delete(n) => write!(&mut tag_buf[..], "{} {}", COMMAND_COMPLETE_DELETE_TAG, n)?,
                Empty => {}
                Insert(n) => write!(
                    &mut tag_buf[..],
                    "{} {} {}",
                    COMMAND_COMPLETE_INSERT_TAG,
                    COMMAND_COMPLETE_INSERT_LEGACY_OID,
                    n
                )?,
                Select(n) => write!(&mut tag_buf[..], "{} {}", COMMAND_COMPLETE_SELECT_TAG, n)?,
                Update(n) => write!(&mut tag_buf[..], "{} {}", COMMAND_COMPLETE_UPDATE_TAG, n)?,
            };
            let tag_str = std::str::from_utf8(&tag_buf)?;
            let tag_data_len = tag_str.find(NUL_CHAR).ok_or_else(|| {
                Error::InternalError("error formatting command complete tag".to_string())
            })?;
            put_str(
                tag_str.get(..tag_data_len).ok_or_else(|| {
                    Error::InternalError("Failed to index into tag_str".to_string())
                })?,
                dst,
            );
        }

        DataRow {
            values,
            explicit_transfer_formats,
        } => {
            put_u8(ID_DATA_ROW, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
            put_i16(COUNT_PLACEHOLDER, dst);
            let mut n_values = 0;
            for (i, v) in values.into_iter().enumerate() {
                let format = match explicit_transfer_formats {
                    Some(ref fs) => *fs.get(i).ok_or_else(|| {
                        Error::InternalError("incorrect DataRow transfer format length".to_string())
                    })?,
                    None => Text,
                };
                let v = v
                    .try_into()
                    .map_err(|e| Error::InternalError(e.to_string()))?;
                match format {
                    Binary => put_binary_value(v, dst)?,
                    Text => put_text_value(v, dst)?,
                };
                n_values += 1;
            }
            // Update the value count field to match the number of values just serialized.
            set_i16(i16::try_from(n_values)?, dst, start_ofs + 5)?;
        }

        ErrorResponse {
            severity,
            sqlstate,
            message,
        } => {
            let severity = match severity {
                ErrorSeverity::Error => ERROR_RESPONSE_SEVERITY_ERROR,
                ErrorSeverity::Fatal => ERROR_RESPONSE_SEVERITY_FATAL,
                ErrorSeverity::Panic => ERROR_RESPONSE_SEVERITY_PANIC,
            };
            put_u8(ID_ERROR_RESPONSE, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
            put_u8(ERROR_RESPONSE_S_FIELD, dst);
            put_str(severity, dst);
            put_u8(ERROR_RESPONSE_V_FIELD, dst);
            put_str(severity, dst);
            put_u8(ERROR_RESPONSE_C_FIELD, dst);
            put_str(sqlstate.code(), dst);
            put_u8(ERROR_RESPONSE_M_FIELD, dst);
            put_str(&message, dst);
            put_u8(ERROR_RESPONSE_TERMINATOR, dst);
        }

        ParameterDescription {
            parameter_data_types,
        } => {
            put_u8(ID_PARAMETER_DESCRIPTION, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
            put_i16(i16::try_from(parameter_data_types.len())?, dst);
            for t in parameter_data_types {
                put_type(t, dst)?;
            }
        }

        ParameterStatus {
            parameter_name,
            parameter_value,
        } => {
            put_u8(ID_PARAMETER_STATUS, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
            put_str(&parameter_name, dst);
            put_str(&parameter_value, dst);
        }

        ParseComplete => {
            put_u8(ID_PARSE_COMPLETE, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
        }

        ReadyForQuery { status } => {
            put_u8(ID_READY_FOR_QUERY, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
            put_u8(status, dst);
        }

        RowDescription { field_descriptions } => {
            put_u8(ID_ROW_DESCRIPTION, dst);
            put_i32(LENGTH_PLACEHOLDER, dst);
            put_i16(i16::try_from(field_descriptions.len())?, dst);
            for d in field_descriptions {
                put_str(&d.field_name, dst);
                put_i32(d.table_id, dst);
                put_i16(d.col_id, dst);
                put_type(d.data_type, dst)?;
                put_i16(d.data_type_size, dst);
                put_i32(d.type_modifier, dst);
                put_format(d.transfer_format, dst);
            }
        }
        #[allow(clippy::unreachable)]
        SSLResponse { .. } => {
            unreachable!("SSLResponse is handled as a special case above.")
        }
    }

    // Update the message length field to match the recently serialized data length in `dst`.
    // The one byte message identifier prefix is excluded when calculating this length.
    let message_len_without_id = dst.len() - start_ofs - 1;
    set_i32(i32::try_from(message_len_without_id)?, dst, start_ofs + 1)?;

    Ok(())
}

fn put_u8(val: u8, dst: &mut BytesMut) {
    dst.put_u8(val);
}

fn put_i16(val: i16, dst: &mut BytesMut) {
    dst.put_i16(val);
}

fn set_i16(val: i16, dst: &mut BytesMut, ofs: usize) -> Result<(), Error> {
    let mut window = dst
        .get_mut(ofs..ofs + 2)
        .ok_or_else(|| Error::InternalError("error writing message field".to_string()))?;
    window.put_i16(val);
    Ok(())
}

fn put_i32(val: i32, dst: &mut BytesMut) {
    dst.put_i32(val);
}

fn set_i32(val: i32, dst: &mut BytesMut, ofs: usize) -> Result<(), Error> {
    let mut window = dst
        .get_mut(ofs..ofs + 4)
        .ok_or_else(|| Error::InternalError("error writing message field".to_string()))?;
    window.put_i32(val);
    Ok(())
}

fn put_str(val: &str, dst: &mut BytesMut) {
    debug_assert!(!val.contains(NUL_CHAR));
    dst.put_slice(val.as_bytes());
    dst.put_u8(NUL_BYTE);
}

fn put_format(val: TransferFormat, dst: &mut BytesMut) {
    let format_code = match val {
        Binary => 1,
        Text => 0,
    };
    put_i16(format_code, dst)
}

fn put_type(val: Type, dst: &mut BytesMut) -> Result<(), Error> {
    let oid = i32::try_from(val.oid())?;
    put_i32(oid, dst);
    Ok(())
}

fn put_binary_value(val: Value, dst: &mut BytesMut) -> Result<(), Error> {
    if val == Value::Null {
        put_i32(LENGTH_NULL_SENTINEL, dst);
        return Ok(());
    }

    let start_ofs = dst.len();
    put_i32(LENGTH_PLACEHOLDER, dst);
    match val {
        #[allow(clippy::unreachable)]
        Value::Null => {
            unreachable!("Null is handled as a special case above.");
        }
        Value::Bool(v) => {
            v.to_sql(&Type::BOOL, dst)?;
        }
        Value::Char(v) => {
            v.to_bytes().to_sql(&Type::CHAR, dst)?;
        }
        Value::Varchar(v) => {
            v.to_bytes().to_sql(&Type::VARCHAR, dst)?;
        }
        Value::Int(v) => {
            v.to_sql(&Type::INT4, dst)?;
        }
        Value::Bigint(v) => {
            v.to_sql(&Type::INT8, dst)?;
        }
        Value::Smallint(v) => {
            v.to_sql(&Type::INT2, dst)?;
        }
        Value::Double(v) => {
            v.to_sql(&Type::FLOAT8, dst)?;
        }
        Value::Float(v) => {
            v.to_sql(&Type::FLOAT4, dst)?;
        }
        Value::Text(v) => {
            v.to_bytes().to_sql(&Type::TEXT, dst)?;
        }
        Value::Timestamp(v) => {
            v.to_sql(&Type::TIMESTAMP, dst)?;
        }
    };
    // Update the length field to match the recently serialized data length in `dst`. The 4 byte
    // length field itself is excluded from the length calculation.
    let value_len = dst.len() - start_ofs - 4;
    set_i32(i32::try_from(value_len)?, dst, start_ofs)?;
    Ok(())
}

fn put_text_value(val: Value, dst: &mut BytesMut) -> Result<(), Error> {
    use std::fmt::Write;

    if val == Value::Null {
        put_i32(LENGTH_NULL_SENTINEL, dst);
        return Ok(());
    }

    let start_ofs = dst.len();
    put_i32(LENGTH_PLACEHOLDER, dst);
    match val {
        #[allow(clippy::unreachable)]
        Value::Null => {
            unreachable!("Null is handled as a special case above.");
        }
        Value::Bool(v) => {
            let text = if v {
                BOOL_TRUE_TEXT_REP
            } else {
                BOOL_FALSE_TEXT_REP
            };
            write!(dst, "{}", text)?;
        }
        Value::Char(v) => {
            dst.extend_from_slice(v.to_bytes());
        }
        Value::Varchar(v) => {
            dst.extend_from_slice(v.to_bytes());
        }
        Value::Int(v) => {
            write!(dst, "{}", v)?;
        }
        Value::Bigint(v) => {
            write!(dst, "{}", v)?;
        }
        Value::Smallint(v) => {
            write!(dst, "{}", v)?;
        }
        Value::Double(v) => {
            // TODO: Ensure all values are properly serialized, including +/-0 and +/-inf.
            write!(dst, "{}", v)?;
        }
        Value::Float(v) => {
            // TODO: Ensure all values are properly serialized, including +/-0 and +/-inf.
            write!(dst, "{}", v)?;
        }
        Value::Text(v) => {
            dst.extend_from_slice(v.to_bytes());
        }
        Value::Timestamp(v) => {
            // TODO: Does not correctly handle all valid timestamp representations. For example,
            // 8601/SQL timestamp format is assumed; infinity/-infinity are not supported.
            write!(dst, "{}", v.format(TIMESTAMP_FORMAT))?;
        }
    };
    // Update the length field to match the recently serialized data length in `dst`. The 4 byte
    // length field itself is excluded from the length calculation.
    let value_len = dst.len() - start_ofs - 4;
    set_i32(i32::try_from(value_len)?, dst, start_ofs)?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::message::{FieldDescription, SqlState};
    use crate::value::Value as DataValue;
    use arccstr::ArcCStr;
    use bytes::{BufMut, BytesMut};
    use chrono::NaiveDateTime;
    use std::sync::Arc;

    struct Value(DataValue);

    impl TryFrom<Value> for DataValue {
        type Error = BackendError;

        fn try_from(v: Value) -> Result<Self, Self::Error> {
            Ok(v.0)
        }
    }

    #[test]
    fn test_encode_ssl_response() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(BackendMessage::ssl_response_n(), &mut buf)
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'N'); // byte response
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_authentication_ok() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec.encode(AuthenticationOk, &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'R'); // message id
        exp.put_i32(8); // message length
        exp.put_i32(0); // success code
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_bind_complete() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec.encode(BindComplete, &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'2'); // message id
        exp.put_i32(4); // message length
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_close_complete() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec.encode(CloseComplete, &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'3'); // message id
        exp.put_i32(4); // message length
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_command_complete_delete() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(CommandComplete { tag: Delete(0) }, &mut buf)
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'C'); // message id
        exp.put_i32(4 + 9); // message length
        exp.extend_from_slice(b"DELETE 0\0");
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_command_complete_empty() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(CommandComplete { tag: Empty }, &mut buf)
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'C'); // message id
        exp.put_i32(4 + 1); // message length
        exp.extend_from_slice(b"\0"); // empty tag
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_command_complete_insert() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(CommandComplete { tag: Insert(1) }, &mut buf)
            .unwrap();
        let mut exp = BytesMut::new();
        // message id
        exp.put_u8(b'C');
        // message length
        exp.put_i32(4 + 11);
        // NOTE: '0' is the legacy insert oid (always zero in the current PostgreSQL protocol).
        exp.extend_from_slice(b"INSERT 0 1\0");
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_command_complete_select() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(CommandComplete { tag: Select(2) }, &mut buf)
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'C'); // message id
        exp.put_i32(4 + 9); // message length
        exp.extend_from_slice(b"SELECT 2\0");
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_command_complete_update() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(CommandComplete { tag: Update(3) }, &mut buf)
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'C'); // message id
        exp.put_i32(4 + 9); // message length
        exp.extend_from_slice(b"UPDATE 3\0");
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_data_row_empty() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                DataRow {
                    values: vec![],
                    explicit_transfer_formats: Some(Arc::new(vec![])),
                },
                &mut buf,
            )
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'D'); // message id
        exp.put_i32(4 + 2); // message length
        exp.put_i16(0); // number of values
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_data_row_single() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                DataRow {
                    values: vec![Value(DataValue::Int(42))],
                    explicit_transfer_formats: Some(Arc::new(vec![Binary])),
                },
                &mut buf,
            )
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'D'); // message id
        exp.put_i32(4 + 2 + 4 + 4); // message length
        exp.put_i16(1); // number of values
        exp.put_i32(4); // length of value
        exp.put_i32(42); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_data_row_multiple() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                DataRow {
                    values: vec![
                        Value(DataValue::Int(42)),
                        Value(DataValue::Null),
                        Value(DataValue::Text(ArcCStr::try_from("some text").unwrap())),
                    ],
                    explicit_transfer_formats: Some(Arc::new(vec![Binary, Binary, Binary])),
                },
                &mut buf,
            )
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'D'); // message id
        exp.put_i32(4 + 2 + 4 + 4 + 4 + 4 + 9); // message length
        exp.put_i16(3); // number of values
        exp.put_i32(4); // length of value
        exp.put_i32(42); // value
        exp.put_i32(-1); // null value sentinel
        exp.put_i32(9); // length of value
        exp.extend_from_slice(b"some text");
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_error_response() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                ErrorResponse {
                    severity: ErrorSeverity::Error,
                    sqlstate: SqlState::FEATURE_NOT_SUPPORTED,
                    message: "unsupported kringle".to_string(),
                },
                &mut buf,
            )
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'E'); // message id
        exp.put_i32(4 + 1 + 6 + 1 + 6 + 1 + 6 + 1 + 20 + 1); // message length
        exp.put_u8(b'S'); // field id
        exp.extend_from_slice(b"ERROR\0");
        exp.put_u8(b'V'); // field id
        exp.extend_from_slice(b"ERROR\0");
        exp.put_u8(b'C'); // field id
        exp.extend_from_slice(b"0A000\0");
        exp.put_u8(b'M'); // field id
        exp.extend_from_slice(b"unsupported kringle\0");
        exp.put_u8(b'\0'); // terminator
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_error_response_after_encoding_failure() {
        struct UnserializableValue;

        impl TryFrom<UnserializableValue> for DataValue {
            type Error = BackendError;

            fn try_from(_v: UnserializableValue) -> Result<Self, Self::Error> {
                Err(BackendError::Unsupported(
                    "Unserializable value.".to_string(),
                ))
            }
        }

        let mut codec = Codec::<Vec<UnserializableValue>>::new();
        let mut buf = BytesMut::new();

        // Attempt to encode a message containing an unserializable value, resulting in an error.
        assert!(codec
            .encode(
                DataRow {
                    values: vec![UnserializableValue],
                    explicit_transfer_formats: None,
                },
                &mut buf,
            )
            .is_err());

        // Verify that the serialization buffer does not contain any partial message data from
        // the failed encode request above.
        assert_eq!(buf.len(), 0);

        // Encoding a subsequent message (an error response) works correctly after the above encode
        // failure.
        codec
            .encode(
                ErrorResponse {
                    severity: ErrorSeverity::Error,
                    sqlstate: SqlState::FEATURE_NOT_SUPPORTED,
                    message: "unsupported kringle".to_string(),
                },
                &mut buf,
            )
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'E'); // message id
        exp.put_i32(4 + 1 + 6 + 1 + 6 + 1 + 6 + 1 + 20 + 1); // message length
        exp.put_u8(b'S'); // field id
        exp.extend_from_slice(b"ERROR\0");
        exp.put_u8(b'V'); // field id
        exp.extend_from_slice(b"ERROR\0");
        exp.put_u8(b'C'); // field id
        exp.extend_from_slice(b"0A000\0");
        exp.put_u8(b'M'); // field id
        exp.extend_from_slice(b"unsupported kringle\0");
        exp.put_u8(b'\0'); // terminator
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_parameter_description() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                ParameterDescription {
                    parameter_data_types: vec![Type::BOOL, Type::INT4],
                },
                &mut buf,
            )
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b't'); // message id
        exp.put_i32(4 + 2 + 4 + 4); // message length
        exp.put_i16(2); // parameter count
        exp.put_i32(16); // BOOL oid
        exp.put_i32(23); // INT4 oid
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_parameter_description_empty() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                ParameterDescription {
                    parameter_data_types: vec![],
                },
                &mut buf,
            )
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b't'); // message id
        exp.put_i32(4 + 2); // message length
        exp.put_i16(0); // parameter count
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_parse_complete() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec.encode(ParseComplete, &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'1'); // message id
        exp.put_i32(4); // message length
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_ready_for_query() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(BackendMessage::ready_for_query_idle(), &mut buf)
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'Z'); // message id
        exp.put_i32(5); // message length
        exp.put_u8(b'I'); // idle
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_row_description() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                RowDescription {
                    field_descriptions: vec![
                        FieldDescription {
                            field_name: "one".to_string(),
                            table_id: 1,
                            col_id: 2,
                            data_type: Type::INT4,
                            data_type_size: 4,
                            type_modifier: 0,
                            transfer_format: Binary,
                        },
                        FieldDescription {
                            field_name: "two".to_string(),
                            table_id: 3,
                            col_id: 4,
                            data_type: Type::INT8,
                            data_type_size: 8,
                            type_modifier: 0,
                            transfer_format: Text,
                        },
                        FieldDescription {
                            field_name: "three".to_string(),
                            table_id: 5,
                            col_id: 6,
                            data_type: Type::TEXT,
                            data_type_size: -2,
                            type_modifier: 0,
                            transfer_format: Binary,
                        },
                    ],
                },
                &mut buf,
            )
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'T'); // message id
        exp.put_i32(4 + 2 + 4 + 18 + 4 + 18 + 6 + 18); // message length
        exp.put_i16(3); // number of fields

        exp.extend_from_slice(b"one\0");
        exp.put_i32(1);
        exp.put_i16(2);
        exp.put_i32(23);
        exp.put_i16(4);
        exp.put_i32(0);
        exp.put_i16(1);

        exp.extend_from_slice(b"two\0");
        exp.put_i32(3);
        exp.put_i16(4);
        exp.put_i32(20);
        exp.put_i16(8);
        exp.put_i32(0);
        exp.put_i16(0);

        exp.extend_from_slice(b"three\0");
        exp.put_i32(5);
        exp.put_i16(6);
        exp.put_i32(25);
        exp.put_i16(-2);
        exp.put_i32(0);
        exp.put_i16(1);

        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_row_description_empty() {
        let mut codec = Codec::<Vec<Value>>::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                RowDescription {
                    field_descriptions: vec![],
                },
                &mut buf,
            )
            .unwrap();
        let mut exp = BytesMut::new();
        exp.put_u8(b'T'); // message id
        exp.put_i32(4 + 2); // message length
        exp.put_i16(0); // number of fields
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_null() {
        let mut buf = BytesMut::new();
        put_binary_value(DataValue::Null, &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(-1); // null sentinel
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_bool() {
        let mut buf = BytesMut::new();
        put_binary_value(DataValue::Bool(true), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(1); // length
        exp.put_u8(1); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_char() {
        let mut buf = BytesMut::new();
        put_binary_value(
            DataValue::Char(ArcCStr::try_from("some stuff").unwrap()),
            &mut buf,
        )
        .unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(10); // length
        exp.extend_from_slice(b"some stuff"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_varchar() {
        let mut buf = BytesMut::new();
        put_binary_value(
            DataValue::Varchar(ArcCStr::try_from("some stuff").unwrap()),
            &mut buf,
        )
        .unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(10); // length
        exp.extend_from_slice(b"some stuff"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_int() {
        let mut buf = BytesMut::new();
        put_binary_value(DataValue::Int(0x1234567), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(4); // length
        exp.put_i32(0x1234567); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_big_int() {
        let mut buf = BytesMut::new();
        put_binary_value(DataValue::Bigint(0x1234567890abcdef), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(8); // length
        exp.put_i64(0x1234567890abcdef); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_small_int() {
        let mut buf = BytesMut::new();
        put_binary_value(DataValue::Smallint(0x1234), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(2); // length
        exp.put_i16(0x1234); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_double() {
        let mut buf = BytesMut::new();
        put_binary_value(DataValue::Double(0.1234567890123456), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(8); // length
        exp.put_f64(0.1234567890123456); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_real() {
        let mut buf = BytesMut::new();
        put_binary_value(DataValue::Float(0.12345678), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(4); // length
        exp.put_f32(0.12345678); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_text() {
        let mut buf = BytesMut::new();
        put_binary_value(
            DataValue::Text(ArcCStr::try_from("some text").unwrap()),
            &mut buf,
        )
        .unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(9); // length
        exp.extend_from_slice(b"some text"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_binary_timestamp() {
        let dt = NaiveDateTime::from_timestamp(1_000_000_000, 42_000_000);
        let mut buf = BytesMut::new();
        put_binary_value(DataValue::Timestamp(dt), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(8); // length
        dt.to_sql(&Type::TIMESTAMP, &mut exp).unwrap(); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_null() {
        let mut buf = BytesMut::new();
        put_text_value(DataValue::Null, &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(-1); // null sentinel
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_bool() {
        let mut buf = BytesMut::new();
        put_text_value(DataValue::Bool(true), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(1); // length
        exp.extend_from_slice(b"t"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_char() {
        let mut buf = BytesMut::new();
        put_text_value(
            DataValue::Char(ArcCStr::try_from("some stuff").unwrap()),
            &mut buf,
        )
        .unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(10); // length
        exp.extend_from_slice(b"some stuff"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_varchar() {
        let mut buf = BytesMut::new();
        put_text_value(
            DataValue::Varchar(ArcCStr::try_from("some stuff").unwrap()),
            &mut buf,
        )
        .unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(10); // length
        exp.extend_from_slice(b"some stuff"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_int() {
        let mut buf = BytesMut::new();
        put_text_value(DataValue::Int(0x1234567), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(8); // length
        exp.extend_from_slice(b"19088743"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_big_int() {
        let mut buf = BytesMut::new();
        put_text_value(DataValue::Bigint(0x1234567890abcdef), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(19); // length
        exp.extend_from_slice(b"1311768467294899695"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_small_int() {
        let mut buf = BytesMut::new();
        put_text_value(DataValue::Smallint(0x1234), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(4); // length
        exp.extend_from_slice(b"4660"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_double() {
        let mut buf = BytesMut::new();
        put_text_value(DataValue::Double(0.1234567890123456), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(18); // size
        exp.extend_from_slice(b"0.1234567890123456"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_real() {
        let mut buf = BytesMut::new();
        put_text_value(DataValue::Float(0.12345678), &mut buf).unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(10); // size
        exp.extend_from_slice(b"0.12345678"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_text() {
        let mut buf = BytesMut::new();
        put_text_value(
            DataValue::Text(ArcCStr::try_from("some text").unwrap()),
            &mut buf,
        )
        .unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(9); // length
        exp.extend_from_slice(b"some text"); // value
        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_text_timestamp() {
        let mut buf = BytesMut::new();
        put_text_value(
            DataValue::Timestamp(
                NaiveDateTime::parse_from_str("2020-01-02 03:04:05.660", TIMESTAMP_FORMAT).unwrap(),
            ),
            &mut buf,
        )
        .unwrap();
        let mut exp = BytesMut::new();
        exp.put_i32(23); // length
        exp.extend_from_slice(b"2020-01-02 03:04:05.660"); // value
        assert_eq!(buf, exp);
    }
}
