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

impl TryFrom<SelectSchema<'_>> for Vec<ps::Column> {
    type Error = Error;
    fn try_from(s: SelectSchema) -> Result<Self, Self::Error> {
        NoriaSchema(&s.0.schema).try_into()
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
                Ok(ps::Column::Column {
                    name: c.column.name.clone(),
                    col_type: type_to_pgsql(&c.column_type)?,
                    table_oid: c.base.as_ref().and_then(|b| b.table_oid),
                    attnum: c.base.as_ref().and_then(|b| b.attnum),
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
        DfType::UnsignedInt => Ok(Type::INT4),
        DfType::UnsignedBigInt => Ok(Type::INT8),
        DfType::TinyInt => Ok(Type::CHAR),
        DfType::UnsignedTinyInt => Ok(Type::INT2),
        DfType::UnsignedSmallInt => Ok(Type::INT2),
        DfType::Blob => Ok(Type::BYTEA),
        DfType::DateTime { .. } => unsupported_type!(),
        DfType::Binary(_) => unsupported_type!(),
        DfType::VarBinary(_) => unsupported_type!(),
        DfType::MediumInt => unsupported_type!(),
        DfType::UnsignedMediumInt => unsupported_type!(),
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
        DfType::Array(elem) => {
            match elem.as_ref() {
                DfType::Unknown => {
                    // The default type for "unknown" in pgsql is TEXT
                    Ok(Type::TEXT)
                }
                DfType::Bool => Ok(Type::BOOL_ARRAY),
                DfType::Char(..) => Ok(Type::BPCHAR_ARRAY),
                DfType::VarChar(_, Collation::Utf8) => Ok(Type::VARCHAR_ARRAY),
                DfType::VarChar(_, Collation::Citext) => {
                    // TODO: use the right CITEXT type
                    Ok(Type::VARCHAR_ARRAY)
                }
                DfType::Int => Ok(Type::INT4_ARRAY),
                DfType::BigInt => Ok(Type::INT8_ARRAY),
                DfType::SmallInt => Ok(Type::INT2_ARRAY),
                DfType::Float => Ok(Type::FLOAT4_ARRAY),
                DfType::Double => Ok(Type::FLOAT8_ARRAY),
                DfType::Text(Collation::Utf8) => Ok(Type::TEXT_ARRAY),
                DfType::Text(Collation::Citext) => {
                    // TODO: use the right CITEXT_ARRAY type
                    Ok(Type::TEXT_ARRAY)
                }
                DfType::Timestamp { .. } => Ok(Type::TIMESTAMP_ARRAY),
                DfType::TimestampTz { .. } => Ok(Type::TIMESTAMPTZ_ARRAY),
                DfType::Json => Ok(Type::JSON_ARRAY),
                DfType::Jsonb => Ok(Type::JSONB_ARRAY),
                DfType::Date => Ok(Type::DATE_ARRAY),
                DfType::Time { .. } => Ok(Type::TIME_ARRAY),
                DfType::UnsignedInt => unsupported_type!(),
                DfType::UnsignedBigInt => unsupported_type!(),
                DfType::TinyInt => Ok(Type::CHAR_ARRAY),
                DfType::UnsignedTinyInt => unsupported_type!(),
                DfType::UnsignedSmallInt => unsupported_type!(),
                DfType::MediumInt => unsupported_type!(),
                DfType::UnsignedMediumInt => unsupported_type!(),
                DfType::Blob => unsupported_type!(),
                DfType::DateTime { .. } => unsupported_type!(),
                DfType::Binary(_) => unsupported_type!(),
                DfType::VarBinary(_) => unsupported_type!(),
                DfType::Enum {
                    metadata:
                        Some(PgEnumMetadata {
                            name,
                            schema,
                            oid,
                            array_oid,
                        }),
                    variants,
                    ..
                } => Ok(Type::new(
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
                DfType::Enum { metadata: None, .. } => unsupported_type!(),
                DfType::Numeric { .. } => Ok(Type::NUMERIC_ARRAY),
                DfType::MacAddr => Ok(Type::MACADDR_ARRAY),
                DfType::Inet => Ok(Type::INET_ARRAY),
                DfType::Uuid => Ok(Type::UUID_ARRAY),
                DfType::Bit(_) => Ok(Type::BIT_ARRAY),
                DfType::VarBit(_) => Ok(Type::VARBIT_ARRAY),
                DfType::Array(_) => unsupported_type!(),
            }
        }
    }
}
