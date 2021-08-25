use noria_client::backend as cl;
use psql_srv as ps;
use std::convert::{TryFrom, TryInto};

const TYPE_LEN_PLACEHOLDER: u16 = 0;

/// A simple wrapper around `noria_client`'s `SelectSchema` facilitating conversion to
/// `psql_srv::Schema`.
pub struct SelectSchema(pub cl::SelectSchema);

impl TryFrom<SelectSchema> for ps::Schema {
    type Error = ps::Error;

    fn try_from(s: SelectSchema) -> Result<Self, Self::Error> {
        s.0.schema
            .into_iter()
            .map(|c| -> Result<(String, ps::ColType), Self::Error> {
                Ok((c.column, MysqlType(c.coltype).try_into()?))
            })
            .collect()
    }
}

// TODO: Remove msql_srv types in PrepareResult and QueryResult to avoid Mysql type conversion.
pub struct MysqlSchema(pub Vec<msql_srv::Column>);

impl TryFrom<MysqlSchema> for ps::Schema {
    type Error = ps::Error;

    fn try_from(s: MysqlSchema) -> Result<Self, Self::Error> {
        s.0.into_iter()
            .map(|c| -> Result<(String, ps::ColType), Self::Error> {
                Ok((c.column, MysqlType(c.coltype).try_into()?))
            })
            .collect()
    }
}

// TODO: Remove msql_srv types in PrepareResult and QueryResult to avoid Mysql type conversion.
pub struct MysqlType(pub msql_srv::ColumnType);

impl TryFrom<MysqlType> for ps::ColType {
    type Error = ps::Error;

    fn try_from(t: MysqlType) -> Result<Self, Self::Error> {
        use msql_srv::ColumnType::*;
        match t.0 {
            MYSQL_TYPE_BIT => Ok(ps::ColType::Bool),
            // FIXME: The value TYPE_LEN_PLACEHOLDER is incorrect but unlikely to cause problems.
            // This entire Mysql conversion is undesirable, see TODO above.
            MYSQL_TYPE_VAR_STRING => Ok(ps::ColType::Varchar(TYPE_LEN_PLACEHOLDER)),
            MYSQL_TYPE_LONG => Ok(ps::ColType::Int(None)),
            MYSQL_TYPE_LONGLONG => Ok(ps::ColType::Bigint(None)),
            MYSQL_TYPE_SHORT => Ok(ps::ColType::Smallint(None)),
            MYSQL_TYPE_DOUBLE => Ok(ps::ColType::Double),
            MYSQL_TYPE_STRING => Ok(ps::ColType::Text),
            MYSQL_TYPE_TIMESTAMP => Ok(ps::ColType::Timestamp),
            _ => Err(ps::Error::Unsupported("Unsupported MysqlType".to_string())),
        }
    }
}
