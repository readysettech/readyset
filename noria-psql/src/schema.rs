use std::convert::TryFrom;

use nom_sql::SqlType;
use noria::unsupported;
use noria_client::backend as cl;
use psql_srv as ps;
use tokio_postgres as pgsql;

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

pub struct NoriaSchema(pub Vec<noria::ColumnSchema>);

impl TryFrom<NoriaSchema> for Vec<pgsql::types::Type> {
    type Error = Error;

    fn try_from(value: NoriaSchema) -> Result<Self, Self::Error> {
        value
            .0
            .into_iter()
            .map(|c| type_to_pgsql(&c.spec.sql_type))
            .collect()
    }
}

impl TryFrom<NoriaSchema> for Vec<ps::Column> {
    type Error = Error;

    fn try_from(s: NoriaSchema) -> Result<Self, Self::Error> {
        s.0.into_iter()
            .map(|c| {
                Ok(ps::Column {
                    name: c.spec.column.name,
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
        SqlType::Decimal(_, _) => unsupported_type!(),
        SqlType::ByteArray => Ok(Type::BYTEA),
        SqlType::Numeric(_) => Ok(Type::NUMERIC),
        SqlType::MacAddr => Ok(Type::MACADDR),
        SqlType::Uuid => Ok(Type::UUID),
        SqlType::Bit(_) => Ok(Type::BIT),
        SqlType::Varbit(_) => Ok(Type::VARBIT),
        SqlType::Serial => Ok(Type::INT4),
        SqlType::BigSerial => Ok(Type::INT8),
    }
}
