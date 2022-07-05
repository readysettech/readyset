use std::convert::TryFrom;

use nom_sql::SqlType;
use noria_client::backend as cl;
use noria_errors::unsupported;
use {psql_srv as ps, tokio_postgres as pgsql};

use crate::Error;

/// A simple wrapper around `noria_client`'s `SelectSchema` facilitating conversion to
/// `psql_srv::Schema`.
pub struct SelectSchema<'a>(pub cl::SelectSchema<'a>);

impl<'a> TryFrom<SelectSchema<'a>> for Vec<ps::Column> {
    type Error = Error;
    fn try_from(s: SelectSchema) -> Result<Self, Self::Error> {
        s.0.schema
            .iter()
            .map(|c| {
                Ok(ps::Column {
                    name: c.spec.column.name.to_string(),
                    col_type: type_to_pgsql(&c.spec.sql_type)?,
                })
            })
            .collect()
    }
}

pub struct NoriaSchema<'a>(pub &'a [noria::ColumnSchema]);

impl<'a> TryFrom<NoriaSchema<'a>> for Vec<pgsql::types::Type> {
    type Error = Error;

    fn try_from(value: NoriaSchema<'a>) -> Result<Self, Self::Error> {
        value
            .0
            .iter()
            .map(|c| type_to_pgsql(&c.spec.sql_type))
            .collect()
    }
}

impl<'a> TryFrom<NoriaSchema<'a>> for Vec<ps::Column> {
    type Error = Error;

    fn try_from(s: NoriaSchema<'a>) -> Result<Self, Self::Error> {
        s.0.iter()
            .map(|c| {
                Ok(ps::Column {
                    name: c.spec.column.name.to_string(),
                    col_type: type_to_pgsql(&c.spec.sql_type)?,
                })
            })
            .collect()
    }
}

pub fn type_to_pgsql(col_type: &SqlType) -> Result<pgsql::types::Type, Error> {
    use pgsql::types::Type;

    macro_rules! unsupported_type {
        () => {
            unsupported!("Unsupported type: {}", col_type)
        };
    }

    match *col_type {
        SqlType::Bool => Ok(Type::BOOL),
        SqlType::Char(_) => Ok(Type::CHAR),
        SqlType::Varchar(_) => Ok(Type::VARCHAR),
        SqlType::Int(_) => Ok(Type::INT4),
        SqlType::Bigint(_) => Ok(Type::INT8),
        SqlType::Smallint(_) => Ok(Type::INT2),
        SqlType::Real => Ok(Type::FLOAT4),
        SqlType::Double => Ok(Type::FLOAT8),
        SqlType::Text => Ok(Type::TEXT),
        SqlType::Timestamp => Ok(Type::TIMESTAMP),
        SqlType::TimestampTz => Ok(Type::TIMESTAMPTZ),
        SqlType::Json => Ok(Type::JSON),
        SqlType::Jsonb => Ok(Type::JSONB),
        SqlType::Date => Ok(Type::DATE),
        SqlType::Time => Ok(Type::TIME),
        SqlType::UnsignedInt(_) => unsupported_type!(),
        SqlType::UnsignedBigint(_) => unsupported_type!(),
        SqlType::Tinyint(_) => unsupported_type!(),
        SqlType::UnsignedTinyint(_) => unsupported_type!(),
        SqlType::UnsignedSmallint(_) => unsupported_type!(),
        SqlType::Blob => unsupported_type!(),
        SqlType::Longblob => unsupported_type!(),
        SqlType::Mediumblob => unsupported_type!(),
        SqlType::Tinyblob => unsupported_type!(),
        SqlType::Float => unsupported_type!(),
        SqlType::Tinytext => unsupported_type!(),
        SqlType::Mediumtext => unsupported_type!(),
        SqlType::Longtext => unsupported_type!(),
        SqlType::DateTime(_) => unsupported_type!(),
        SqlType::Binary(_) => unsupported_type!(),
        SqlType::Varbinary(_) => unsupported_type!(),
        SqlType::Enum(_) => unsupported_type!(),
        SqlType::Decimal(_, _) => Ok(Type::NUMERIC),
        SqlType::ByteArray => Ok(Type::BYTEA),
        SqlType::Numeric(_) => Ok(Type::NUMERIC),
        SqlType::MacAddr => Ok(Type::MACADDR),
        SqlType::Inet => Ok(Type::INET),
        SqlType::Uuid => Ok(Type::UUID),
        SqlType::Bit(_) => Ok(Type::BIT),
        SqlType::Varbit(_) => Ok(Type::VARBIT),
        SqlType::Serial => Ok(Type::INT4),
        SqlType::BigSerial => Ok(Type::INT8),
        SqlType::Array(box SqlType::Bool) => Ok(Type::BOOL_ARRAY),
        SqlType::Array(box SqlType::Char(_)) => Ok(Type::CHAR_ARRAY),
        SqlType::Array(box SqlType::Varchar(_)) => Ok(Type::VARCHAR_ARRAY),
        SqlType::Array(box SqlType::Int(_)) => Ok(Type::INT4_ARRAY),
        SqlType::Array(box SqlType::Bigint(_)) => Ok(Type::INT8_ARRAY),
        SqlType::Array(box SqlType::Smallint(_)) => Ok(Type::INT2_ARRAY),
        SqlType::Array(box SqlType::Real) => Ok(Type::FLOAT4_ARRAY),
        SqlType::Array(box SqlType::Double) => Ok(Type::FLOAT8_ARRAY),
        SqlType::Array(box SqlType::Text) => Ok(Type::TEXT_ARRAY),
        SqlType::Array(box SqlType::Timestamp) => Ok(Type::TIMESTAMP_ARRAY),
        SqlType::Array(box SqlType::TimestampTz) => Ok(Type::TIMESTAMPTZ_ARRAY),
        SqlType::Array(box SqlType::Json) => Ok(Type::JSON_ARRAY),
        SqlType::Array(box SqlType::Jsonb) => Ok(Type::JSONB_ARRAY),
        SqlType::Array(box SqlType::Date) => Ok(Type::DATE_ARRAY),
        SqlType::Array(box SqlType::Time) => Ok(Type::TIME_ARRAY),
        SqlType::Array(box SqlType::UnsignedInt(_)) => unsupported_type!(),
        SqlType::Array(box SqlType::UnsignedBigint(_)) => unsupported_type!(),
        SqlType::Array(box SqlType::Tinyint(_)) => unsupported_type!(),
        SqlType::Array(box SqlType::UnsignedTinyint(_)) => unsupported_type!(),
        SqlType::Array(box SqlType::UnsignedSmallint(_)) => unsupported_type!(),
        SqlType::Array(box SqlType::Blob) => unsupported_type!(),
        SqlType::Array(box SqlType::Longblob) => unsupported_type!(),
        SqlType::Array(box SqlType::Mediumblob) => unsupported_type!(),
        SqlType::Array(box SqlType::Tinyblob) => unsupported_type!(),
        SqlType::Array(box SqlType::Float) => unsupported_type!(),
        SqlType::Array(box SqlType::Tinytext) => unsupported_type!(),
        SqlType::Array(box SqlType::Mediumtext) => unsupported_type!(),
        SqlType::Array(box SqlType::Longtext) => unsupported_type!(),
        SqlType::Array(box SqlType::DateTime(_)) => unsupported_type!(),
        SqlType::Array(box SqlType::Binary(_)) => unsupported_type!(),
        SqlType::Array(box SqlType::Varbinary(_)) => unsupported_type!(),
        SqlType::Array(box SqlType::Enum(_)) => unsupported_type!(),
        SqlType::Array(box SqlType::Decimal(_, _)) => Ok(Type::NUMERIC_ARRAY),
        SqlType::Array(box SqlType::ByteArray) => Ok(Type::BYTEA_ARRAY),
        SqlType::Array(box SqlType::Numeric(_)) => Ok(Type::NUMERIC_ARRAY),
        SqlType::Array(box SqlType::MacAddr) => Ok(Type::MACADDR_ARRAY),
        SqlType::Array(box SqlType::Inet) => Ok(Type::INET_ARRAY),
        SqlType::Array(box SqlType::Uuid) => Ok(Type::UUID_ARRAY),
        SqlType::Array(box SqlType::Bit(_)) => Ok(Type::BIT_ARRAY),
        SqlType::Array(box SqlType::Varbit(_)) => Ok(Type::VARBIT_ARRAY),
        SqlType::Array(box SqlType::Serial) => Ok(Type::INT4_ARRAY),
        SqlType::Array(box SqlType::BigSerial) => Ok(Type::INT8_ARRAY),
        SqlType::Array(box SqlType::Array(_)) => unsupported_type!(),
    }
}
