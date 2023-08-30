#![warn(clippy::panic)]

use nom_sql::{self, ColumnConstraint, Relation};
use readyset_client::ColumnSchema;
use readyset_data::DfType;
use readyset_errors::{unsupported, ReadySetResult};

use crate::constants::DEFAULT_CHARACTER_SET;

pub(crate) fn convert_column(col: &ColumnSchema) -> ReadySetResult<mysql_srv::Column> {
    let mut colflags = mysql_srv::ColumnFlags::empty();
    use mysql_srv::ColumnType::*;

    let coltype = match col.column_type {
        DfType::Unknown => MYSQL_TYPE_UNKNOWN,
        DfType::Text(_) => MYSQL_TYPE_BLOB,
        DfType::VarChar(..) => MYSQL_TYPE_VAR_STRING,
        DfType::Int => MYSQL_TYPE_LONG,
        DfType::UnsignedInt => {
            colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
            MYSQL_TYPE_LONG
        }
        DfType::BigInt => MYSQL_TYPE_LONGLONG,
        DfType::UnsignedBigInt => {
            colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
            MYSQL_TYPE_LONGLONG
        }
        DfType::TinyInt => MYSQL_TYPE_TINY,
        DfType::UnsignedTinyInt => {
            colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
            MYSQL_TYPE_TINY
        }
        DfType::SmallInt => MYSQL_TYPE_SHORT,
        DfType::UnsignedSmallInt => {
            colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
            MYSQL_TYPE_SHORT
        }
        DfType::Bool => MYSQL_TYPE_BIT,
        DfType::DateTime { .. } => MYSQL_TYPE_DATETIME,
        DfType::Blob => MYSQL_TYPE_BLOB,
        DfType::Char(..) => {
            // TODO(aspen): I'm not sure if this is right
            MYSQL_TYPE_STRING
        }
        DfType::Float => MYSQL_TYPE_FLOAT,
        DfType::Double => MYSQL_TYPE_DOUBLE,
        DfType::Date => MYSQL_TYPE_DATE,
        DfType::Timestamp { .. } => MYSQL_TYPE_TIMESTAMP,
        DfType::TimestampTz { .. } => {
            unsupported!("MySQL does not support the timestamp with time zone type")
        }
        DfType::Binary(_) => {
            // TODO(aspen): I don't know if this is right
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            MYSQL_TYPE_STRING
        }
        DfType::VarBinary(_) => {
            // TODO(aspen): I don't know if this is right
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            MYSQL_TYPE_VAR_STRING
        }
        DfType::Enum { .. } => {
            // TODO(aspen): I don't know if this is right
            colflags |= mysql_srv::ColumnFlags::ENUM_FLAG;
            MYSQL_TYPE_VAR_STRING
        }
        DfType::Time { .. } => MYSQL_TYPE_TIME,
        DfType::Json => MYSQL_TYPE_JSON,
        DfType::Numeric { .. } => MYSQL_TYPE_DECIMAL,
        DfType::MacAddr => unsupported!("MySQL does not support the MACADDR type"),
        DfType::Inet => unsupported!("MySQL does not support the INET type"),
        DfType::Uuid => unsupported!("MySQL does not support the UUID type"),
        DfType::Jsonb => unsupported!("MySQL does not support the JSONB type"),
        DfType::Bit(size) => {
            if size < 64 {
                MYSQL_TYPE_BIT
            } else {
                unsupported!("MySQL bit type cannot have a size bigger than 64")
            }
        }
        DfType::VarBit(_) => unsupported!("MySQL does not support the bit varying type"),
        DfType::Array(_) => unsupported!("MySQL does not support arrays"),
    };

    for c in col.base.iter().flat_map(|b| &b.constraints) {
        match *c {
            ColumnConstraint::AutoIncrement => {
                colflags |= mysql_srv::ColumnFlags::AUTO_INCREMENT_FLAG;
            }
            ColumnConstraint::NotNull => {
                colflags |= mysql_srv::ColumnFlags::NOT_NULL_FLAG;
            }
            ColumnConstraint::PrimaryKey => {
                colflags |= mysql_srv::ColumnFlags::PRI_KEY_FLAG;
            }
            ColumnConstraint::Unique => {
                colflags |= mysql_srv::ColumnFlags::UNIQUE_KEY_FLAG;
            }
            _ => (),
        }
    }

    // TODO: All columns have a default display length, so `column_length` should not be an option.
    let column_length = match col.column_type {
        DfType::Char(l, ..)
        | DfType::VarChar(l, ..)
        | DfType::Binary(l)
        | DfType::VarBinary(l)
        | DfType::Bit(l) => Some(l.into()),
        _ => None,
    };

    Ok(mysql_srv::Column {
        table: col
            .column
            .table
            .clone()
            .unwrap_or_else(|| Relation {
                schema: None,
                name: "".into(),
            })
            .display(nom_sql::Dialect::MySQL)
            .to_string(),
        column: col.column.name.to_string(),
        coltype,
        column_length,
        colflags,
        character_set: DEFAULT_CHARACTER_SET,
    })
}
