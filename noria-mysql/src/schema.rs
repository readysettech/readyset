#![warn(clippy::panic)]

use nom_sql::{self, ColumnConstraint, SqlType};
use noria_errors::{unsupported, ReadySetResult};

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

pub(crate) fn convert_column(
    col: &nom_sql::ColumnSpecification,
) -> ReadySetResult<mysql_srv::Column> {
    let mut colflags = mysql_srv::ColumnFlags::empty();
    use mysql_srv::ColumnType::*;

    let coltype = match col.sql_type {
        SqlType::Mediumtext => MYSQL_TYPE_VAR_STRING,
        SqlType::Longtext => MYSQL_TYPE_BLOB,
        SqlType::Text => MYSQL_TYPE_BLOB,
        SqlType::Varchar(_) => MYSQL_TYPE_VAR_STRING,
        SqlType::Int(_) => MYSQL_TYPE_LONG,
        SqlType::UnsignedInt(_) => {
            colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
            MYSQL_TYPE_LONG
        }
        SqlType::Bigint(_) => MYSQL_TYPE_LONGLONG,
        SqlType::UnsignedBigint(_) => {
            colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
            MYSQL_TYPE_LONGLONG
        }
        SqlType::Tinyint(_) => MYSQL_TYPE_TINY,
        SqlType::UnsignedTinyint(_) => {
            colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
            MYSQL_TYPE_TINY
        }
        SqlType::Smallint(_) => MYSQL_TYPE_SHORT,
        SqlType::UnsignedSmallint(_) => {
            colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
            MYSQL_TYPE_SHORT
        }
        SqlType::Bool => MYSQL_TYPE_BIT,
        SqlType::DateTime(_) => MYSQL_TYPE_DATETIME,
        SqlType::Float => MYSQL_TYPE_FLOAT,
        SqlType::Decimal(_, _) => MYSQL_TYPE_NEWDECIMAL,
        SqlType::Char(_) => {
            // TODO(grfn): I'm not sure if this is right
            MYSQL_TYPE_STRING
        }
        SqlType::Blob => MYSQL_TYPE_BLOB,
        SqlType::Longblob => MYSQL_TYPE_LONG_BLOB,
        SqlType::Mediumblob => MYSQL_TYPE_MEDIUM_BLOB,
        SqlType::Tinyblob => MYSQL_TYPE_TINY_BLOB,
        SqlType::Double => MYSQL_TYPE_DOUBLE,
        SqlType::Real => {
            // a generous reading of
            // https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html seems to
            // indicate that real is equivalent to float
            // TODO(grfn): Make sure that's the case
            MYSQL_TYPE_FLOAT
        }
        SqlType::Tinytext => {
            // TODO(grfn): How does the mysql binary protocol handle
            // tinytext? is it just an alias for tinyblob or is there a flag
            // we need?
            unsupported!()
        }
        SqlType::Date => MYSQL_TYPE_DATE,
        SqlType::Timestamp => MYSQL_TYPE_TIMESTAMP,
        SqlType::TimestampTz => {
            unsupported!("MySQL does not support the timestamp with time zone type")
        }
        SqlType::Binary(_) => {
            // TODO(grfn): I don't know if this is right
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            MYSQL_TYPE_STRING
        }
        SqlType::Varbinary(_) => {
            // TODO(grfn): I don't know if this is right
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            MYSQL_TYPE_VAR_STRING
        }
        SqlType::Enum(_) => {
            // TODO(grfn): I don't know if this is right
            colflags |= mysql_srv::ColumnFlags::ENUM_FLAG;
            MYSQL_TYPE_VAR_STRING
        }
        SqlType::Time => MYSQL_TYPE_TIME,
        SqlType::Json => MYSQL_TYPE_JSON,
        SqlType::ByteArray => MYSQL_TYPE_BLOB,
        SqlType::Numeric(_) => MYSQL_TYPE_DECIMAL,
        SqlType::MacAddr => unsupported!("MySQL does not support the MACADDR type"),
        SqlType::Uuid => unsupported!("MySQL does not support the UUID type"),
        SqlType::Jsonb => unsupported!("MySQL does not support the JSONB type"),
        SqlType::Bit(size_opt) => {
            let size = size_opt.unwrap_or(1);
            if size < 64 {
                MYSQL_TYPE_BIT
            } else {
                unsupported!("MySQL bit type cannot have a size bigger than 64")
            }
        }
        SqlType::Varbit(_) => unsupported!("MySQL does not support the bit varying type"),
        SqlType::Serial => MYSQL_TYPE_LONG,
        SqlType::BigSerial => MYSQL_TYPE_LONGLONG,
    };

    for c in &col.constraints {
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

    Ok(mysql_srv::Column {
        table: col.column.table.clone().unwrap_or_default().to_string(),
        column: col.column.name.to_string(),
        coltype,
        colflags,
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
