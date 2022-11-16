use std::convert::TryFrom;

use postgres_types::Kind;
use readyset_adapter::backend as cl;
use readyset_data::{Collation, DfType, PgEnumMetadata};
use readyset_errors::unsupported;
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
                    name: c.column.name.to_string(),
                    col_type: type_to_pgsql(&c.column_type)?,
                })
            })
            .collect()
    }
}

pub struct NoriaSchema<'a>(pub &'a [readyset_client::ColumnSchema]);

impl<'a> TryFrom<NoriaSchema<'a>> for Vec<pgsql::types::Type> {
    type Error = Error;

    fn try_from(value: NoriaSchema<'a>) -> Result<Self, Self::Error> {
        value
            .0
            .iter()
            .map(|c| type_to_pgsql(&c.column_type))
            .collect()
    }
}

impl<'a> TryFrom<NoriaSchema<'a>> for Vec<ps::Column> {
    type Error = Error;

    fn try_from(s: NoriaSchema<'a>) -> Result<Self, Self::Error> {
        s.0.iter()
            .map(|c| {
                Ok(ps::Column {
                    name: c.column.name.to_string(),
                    col_type: type_to_pgsql(&c.column_type)?,
                })
            })
            .collect()
    }
}

pub fn type_to_pgsql(col_type: &DfType) -> Result<pgsql::types::Type, Error> {
    use pgsql::types::Type;

    macro_rules! unsupported_type {
        () => {
            unsupported!("Unsupported type: {}", col_type)
        };
    }

    match col_type {
        DfType::Unknown => Ok(Type::TEXT), // The default type for "unknown" in pgsql is TEXT
        DfType::Bool => Ok(Type::BOOL),
        DfType::Char(..) => Ok(Type::BPCHAR),
        DfType::VarChar(_, Collation::Utf8) => Ok(Type::VARCHAR),
        DfType::VarChar(_, Collation::Citext) => {
            // TODO: use the right CITEXT type
            Ok(Type::VARCHAR)
        }
        DfType::Int => Ok(Type::INT4),
        DfType::BigInt => Ok(Type::INT8),
        DfType::SmallInt => Ok(Type::INT2),
        DfType::Float => Ok(Type::FLOAT4),
        DfType::Double => Ok(Type::FLOAT8),
        DfType::Text(Collation::Utf8) => Ok(Type::TEXT),
        DfType::Text(Collation::Citext) => Ok(Type::TEXT), // TODO: use the right CITEXT type
        DfType::Timestamp { .. } => Ok(Type::TIMESTAMP),
        DfType::TimestampTz { .. } => Ok(Type::TIMESTAMPTZ),
        DfType::Json => Ok(Type::JSON),
        DfType::Jsonb => Ok(Type::JSONB),
        DfType::Date => Ok(Type::DATE),
        DfType::Time { .. } => Ok(Type::TIME),
        DfType::UnsignedInt => unsupported_type!(),
        DfType::UnsignedBigInt => unsupported_type!(),
        DfType::TinyInt => Ok(Type::CHAR),
        DfType::UnsignedTinyInt => unsupported_type!(),
        DfType::UnsignedSmallInt => unsupported_type!(),
        DfType::Blob => Ok(Type::BYTEA),
        DfType::DateTime { .. } => unsupported_type!(),
        DfType::Binary(_) => unsupported_type!(),
        DfType::VarBinary(_) => unsupported_type!(),
        DfType::Enum {
            metadata: Some(PgEnumMetadata {
                name, schema, oid, ..
            }),
            variants,
            ..
        } => Ok(Type::new(
            name.into(),
            *oid,
            Kind::Enum(variants.to_vec()),
            schema.into(),
        )),
        DfType::Enum { metadata: None, .. } => unsupported_type!(),
        DfType::Numeric { .. } => Ok(Type::NUMERIC),
        DfType::MacAddr => Ok(Type::MACADDR),
        DfType::Inet => Ok(Type::INET),
        DfType::Uuid => Ok(Type::UUID),
        DfType::Bit(_) => Ok(Type::BIT),
        DfType::VarBit(_) => Ok(Type::VARBIT),
        DfType::Array(box DfType::Unknown) => {
            // The default type for "unknown" in pgsql is TEXT
            Ok(Type::TEXT)
        }
        DfType::Array(box DfType::Bool) => Ok(Type::BOOL_ARRAY),
        DfType::Array(box DfType::Char(..)) => Ok(Type::BPCHAR_ARRAY),
        DfType::Array(box DfType::VarChar(_, Collation::Utf8)) => Ok(Type::VARCHAR_ARRAY),
        DfType::Array(box DfType::VarChar(_, Collation::Citext)) => {
            // TODO: use the right CITEXT type
            Ok(Type::VARCHAR_ARRAY)
        }
        DfType::Array(box DfType::Int) => Ok(Type::INT4_ARRAY),
        DfType::Array(box DfType::BigInt) => Ok(Type::INT8_ARRAY),
        DfType::Array(box DfType::SmallInt) => Ok(Type::INT2_ARRAY),
        DfType::Array(box DfType::Float) => Ok(Type::FLOAT4_ARRAY),
        DfType::Array(box DfType::Double) => Ok(Type::FLOAT8_ARRAY),
        DfType::Array(box DfType::Text(Collation::Utf8)) => Ok(Type::TEXT_ARRAY),
        DfType::Array(box DfType::Text(Collation::Citext)) => {
            // TODO: use the right CITEXT_ARRAY type
            Ok(Type::TEXT_ARRAY)
        }
        DfType::Array(box DfType::Timestamp { .. }) => Ok(Type::TIMESTAMP_ARRAY),
        DfType::Array(box DfType::TimestampTz { .. }) => Ok(Type::TIMESTAMPTZ_ARRAY),
        DfType::Array(box DfType::Json) => Ok(Type::JSON_ARRAY),
        DfType::Array(box DfType::Jsonb) => Ok(Type::JSONB_ARRAY),
        DfType::Array(box DfType::Date) => Ok(Type::DATE_ARRAY),
        DfType::Array(box DfType::Time { .. }) => Ok(Type::TIME_ARRAY),
        DfType::Array(box DfType::UnsignedInt) => unsupported_type!(),
        DfType::Array(box DfType::UnsignedBigInt) => unsupported_type!(),
        DfType::Array(box DfType::TinyInt) => Ok(Type::CHAR_ARRAY),
        DfType::Array(box DfType::UnsignedTinyInt) => unsupported_type!(),
        DfType::Array(box DfType::UnsignedSmallInt) => unsupported_type!(),
        DfType::Array(box DfType::Blob) => unsupported_type!(),
        DfType::Array(box DfType::DateTime { .. }) => unsupported_type!(),
        DfType::Array(box DfType::Binary(_)) => unsupported_type!(),
        DfType::Array(box DfType::VarBinary(_)) => unsupported_type!(),
        DfType::Array(box DfType::Enum {
            metadata:
                Some(PgEnumMetadata {
                    name,
                    schema,
                    oid,
                    array_oid,
                }),
            variants,
            ..
        }) => Ok(Type::new(
            format!("_{name}"),
            *array_oid,
            Kind::Array(Type::new(
                name.into(),
                *oid,
                Kind::Enum(variants.to_vec()),
                schema.into(),
            )),
            schema.into(),
        )),
        DfType::Array(box DfType::Enum { metadata: None, .. }) => unsupported_type!(),
        DfType::Array(box DfType::Numeric { .. }) => Ok(Type::NUMERIC_ARRAY),
        DfType::Array(box DfType::MacAddr) => Ok(Type::MACADDR_ARRAY),
        DfType::Array(box DfType::Inet) => Ok(Type::INET_ARRAY),
        DfType::Array(box DfType::Uuid) => Ok(Type::UUID_ARRAY),
        DfType::Array(box DfType::Bit(_)) => Ok(Type::BIT_ARRAY),
        DfType::Array(box DfType::VarBit(_)) => Ok(Type::VARBIT_ARRAY),
        DfType::Array(box DfType::Array(_)) => unsupported_type!(),
    }
}
