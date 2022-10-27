#![warn(clippy::panic)]

use nom_sql::{self, ColumnConstraint, Relation};
use readyset::ColumnSchema;
use readyset_data::DfType;
use readyset_errors::{unsupported, ReadySetResult};

use crate::constants::DEFAULT_CHARACTER_SET;

/// Checks if `c1` is a subtype of `c2`.
pub(crate) fn is_subtype(c1: mysql_srv::ColumnType, c2: mysql_srv::ColumnType) -> bool {
    use mysql_srv::ColumnType::*;

    if c1 == c2 {
        return true;
    }

    // Handle all types that support subtypes.
    matches!(
        (c1, c2),
        (
            MYSQL_TYPE_TINY,
            MYSQL_TYPE_LONG | MYSQL_TYPE_LONGLONG | MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL,
        ) | (
            MYSQL_TYPE_LONG,
            MYSQL_TYPE_LONGLONG | MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL
        ) | (
            MYSQL_TYPE_LONGLONG,
            MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL
        ) | (MYSQL_TYPE_BLOB, MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB)
            | (MYSQL_TYPE_DOUBLE | MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE)
    )
}

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
            // TODO(grfn): I'm not sure if this is right
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
            // TODO(grfn): I don't know if this is right
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            MYSQL_TYPE_STRING
        }
        DfType::VarBinary(_) => {
            // TODO(grfn): I don't know if this is right
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            MYSQL_TYPE_VAR_STRING
        }
        DfType::Enum { .. } => {
            // TODO(grfn): I don't know if this is right
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
            .to_string(),
        column: col.column.name.to_string(),
        coltype,
        column_length,
        colflags,
        character_set: DEFAULT_CHARACTER_SET,
    })
}

#[cfg(test)]
mod tests {
    use mysql_srv::ColumnType::{self, *};
    use proptest::prelude::*;
    use test_strategy::proptest;

    use super::*;

    pub fn arbitrary_column_type() -> impl Strategy<Value = ColumnType> {
        prop_oneof![
            Just(MYSQL_TYPE_DECIMAL),
            Just(MYSQL_TYPE_TINY),
            Just(MYSQL_TYPE_SHORT),
            Just(MYSQL_TYPE_LONG),
            Just(MYSQL_TYPE_FLOAT),
            Just(MYSQL_TYPE_DOUBLE),
            Just(MYSQL_TYPE_NULL),
            Just(MYSQL_TYPE_TIMESTAMP),
            Just(MYSQL_TYPE_LONGLONG),
            Just(MYSQL_TYPE_INT24),
            Just(MYSQL_TYPE_DATE),
            Just(MYSQL_TYPE_TIME),
            Just(MYSQL_TYPE_DATETIME),
            Just(MYSQL_TYPE_YEAR),
            Just(MYSQL_TYPE_NEWDATE),
            Just(MYSQL_TYPE_VARCHAR),
            Just(MYSQL_TYPE_BIT),
            Just(MYSQL_TYPE_TIMESTAMP2),
            Just(MYSQL_TYPE_DATETIME2),
            Just(MYSQL_TYPE_TIME2),
            Just(MYSQL_TYPE_TYPED_ARRAY),
            Just(MYSQL_TYPE_UNKNOWN),
            Just(MYSQL_TYPE_JSON),
            Just(MYSQL_TYPE_NEWDECIMAL),
            Just(MYSQL_TYPE_ENUM),
            Just(MYSQL_TYPE_SET),
            Just(MYSQL_TYPE_TINY_BLOB),
            Just(MYSQL_TYPE_MEDIUM_BLOB),
            Just(MYSQL_TYPE_LONG_BLOB),
            Just(MYSQL_TYPE_BLOB),
            Just(MYSQL_TYPE_VAR_STRING),
            Just(MYSQL_TYPE_STRING),
            Just(MYSQL_TYPE_GEOMETRY)
        ]
    }

    #[proptest]
    fn subtype_reflexivity(#[strategy(arbitrary_column_type())] column_type: ColumnType) {
        assert!(is_subtype(column_type, column_type))
    }

    #[proptest]
    fn subtype_transitivity(
        #[strategy(arbitrary_column_type())] c1: ColumnType,
        #[strategy(arbitrary_column_type())] c2: ColumnType,
        #[strategy(arbitrary_column_type())] c3: ColumnType,
    ) {
        if is_subtype(c1, c2) && is_subtype(c2, c3) {
            assert!(is_subtype(c1, c3));
        }
    }

    #[proptest]
    fn subtype_antisymmetry(
        #[strategy(arbitrary_column_type())] c1: ColumnType,
        #[strategy(arbitrary_column_type())] c2: ColumnType,
    ) {
        if is_subtype(c1, c2) && is_subtype(c2, c1) {
            assert!(c1 == c2);
        }
    }
}
