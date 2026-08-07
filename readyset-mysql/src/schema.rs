#![warn(clippy::panic)]

use readyset_client::schema::{ColumnBase, ColumnSchema};
use readyset_data::DfType;
use readyset_errors::{unsupported, ReadySetResult};
use readyset_sql::ast::{ColumnConstraint, Relation, SqlType};
use readyset_sql::DialectDisplay;

use crate::constants::DEFAULT_COLLATION_NUMERIC;

// Create a helper function to get default ColumnBase
fn default_column_base(col: &ColumnSchema, sql_type: SqlType) -> ColumnBase {
    ColumnBase {
        column: col.column.name.clone(),
        table: Relation {
            schema: None,
            name: "".into(),
        },
        constraints: vec![],
        table_oid: None,
        attnum: None,
        sql_type,
    }
}

fn get_sql_type(col: &ColumnSchema, default_type: SqlType) -> SqlType {
    col.base
        .as_ref()
        .map(|base| base.sql_type.clone())
        .unwrap_or_else(|| default_column_base(col, default_type).sql_type)
}

/// Whether a column of this type reports the session's results collation in its column
/// definition packet, the way MySQL reports text results.
pub(crate) fn uses_results_collation(ty: &DfType) -> bool {
    matches!(
        ty,
        DfType::Char(..)
            | DfType::VarChar(..)
            | DfType::Text(_)
            | DfType::Enum { .. }
            | DfType::Unknown
    )
}

pub(crate) fn convert_column(
    col: &ColumnSchema,
    results_collation: u16,
) -> ReadySetResult<mysql_srv::Column> {
    let mut colflags = mysql_srv::ColumnFlags::empty();
    use mysql_srv::ColumnType::*;

    // TODO(marcelo): We should read the proper collation to specify the length.
    // For now, we just use the default collation.
    let (coltype, character_set, decimals, column_length) = match col.column_type {
        DfType::TinyInt => {
            let sql_type = get_sql_type(col, SqlType::TinyInt(None));

            // Bool types are tinyint(1)
            let length = match sql_type {
                SqlType::TinyInt(l) => l.unwrap_or(4),
                _ => unreachable!("TinyInt should be a tinyint type"),
            };
            (MYSQL_TYPE_TINY, DEFAULT_COLLATION_NUMERIC, 0, length as u32)
        }

        // Combine unsigned types pattern
        ref t @ (DfType::UnsignedTinyInt
        | DfType::UnsignedSmallInt
        | DfType::UnsignedMediumInt
        | DfType::UnsignedInt
        | DfType::UnsignedBigInt) => {
            colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
            match t {
                DfType::UnsignedTinyInt => (MYSQL_TYPE_TINY, DEFAULT_COLLATION_NUMERIC, 0, 3),
                DfType::UnsignedSmallInt => (MYSQL_TYPE_SHORT, DEFAULT_COLLATION_NUMERIC, 0, 5),
                DfType::UnsignedMediumInt => (MYSQL_TYPE_INT24, DEFAULT_COLLATION_NUMERIC, 0, 8),
                DfType::UnsignedInt => (MYSQL_TYPE_LONG, DEFAULT_COLLATION_NUMERIC, 0, 10),
                DfType::UnsignedBigInt => (MYSQL_TYPE_LONGLONG, DEFAULT_COLLATION_NUMERIC, 0, 20),
                _ => unreachable!(),
            }
        }

        // SMALLINT
        DfType::SmallInt => (MYSQL_TYPE_SHORT, DEFAULT_COLLATION_NUMERIC, 0, 6),

        // MEDIUMINT
        DfType::MediumInt => (MYSQL_TYPE_INT24, DEFAULT_COLLATION_NUMERIC, 0, 9),

        // INT
        DfType::Int => (MYSQL_TYPE_LONG, DEFAULT_COLLATION_NUMERIC, 0, 11),

        // BIGINT
        DfType::BigInt => (MYSQL_TYPE_LONGLONG, DEFAULT_COLLATION_NUMERIC, 0, 20),

        // DECIMAL / NUMERIC
        DfType::Numeric { prec, scale } => (
            MYSQL_TYPE_NEWDECIMAL,
            DEFAULT_COLLATION_NUMERIC,
            scale,
            (prec + scale as u16) as u32,
        ),

        // FLOAT / DOUBLE / REAL
        DfType::Float => (
            MYSQL_TYPE_FLOAT,
            DEFAULT_COLLATION_NUMERIC,
            31, // MySQL default
            12,
        ),
        DfType::Double => (MYSQL_TYPE_DOUBLE, DEFAULT_COLLATION_NUMERIC, 31, 22),

        // BIT
        DfType::Bit(size) => {
            if size <= 64 {
                colflags |= mysql_srv::ColumnFlags::UNSIGNED_FLAG;
                (MYSQL_TYPE_BIT, DEFAULT_COLLATION_NUMERIC, 0, size as u32)
            } else {
                unsupported!("MySQL bit type cannot have a size bigger than 64")
            }
        }

        // DATE & TIME
        DfType::Date => {
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            (MYSQL_TYPE_DATE, DEFAULT_COLLATION_NUMERIC, 0, 10)
        }
        DfType::DateTime { subsecond_digits } => {
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            let length = if subsecond_digits > 0 {
                19 + 1 + subsecond_digits as usize
            } else {
                19
            };
            (
                MYSQL_TYPE_DATETIME,
                DEFAULT_COLLATION_NUMERIC,
                subsecond_digits as u8,
                length as u32,
            )
        }
        DfType::Timestamp { subsecond_digits } => {
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            let length = if subsecond_digits > 0 {
                19 + 1 + subsecond_digits as usize
            } else {
                19
            };
            (
                MYSQL_TYPE_TIMESTAMP,
                DEFAULT_COLLATION_NUMERIC,
                subsecond_digits as u8,
                length as u32,
            )
        }
        DfType::Time { subsecond_digits } => {
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            let length = if subsecond_digits > 0 {
                10 + 1 + subsecond_digits as usize
            } else {
                10
            };
            (
                MYSQL_TYPE_TIME,
                DEFAULT_COLLATION_NUMERIC,
                subsecond_digits as u8,
                length as u32,
            )
        }

        // CHAR / VARCHAR
        DfType::Char(l, ..) => {
            let charset = mysql_common::collations::Collation::from(
                mysql_common::collations::CollationId::UTF8MB4_GENERAL_CI,
            );
            (
                MYSQL_TYPE_STRING,
                results_collation,
                0,
                (l as u32 * charset.max_len as u32),
            )
        }
        DfType::VarChar(l, ..) => {
            let charset = mysql_common::collations::Collation::from(
                mysql_common::collations::CollationId::UTF8MB4_GENERAL_CI,
            );
            (
                MYSQL_TYPE_VAR_STRING,
                results_collation,
                0,
                (l as u32 * charset.max_len as u32),
            )
        }

        // Text types with shared logic
        DfType::Text(_) => {
            let sql_type = get_sql_type(col, SqlType::Text);
            let charset = mysql_common::collations::Collation::from(
                mysql_common::collations::CollationId::UTF8MB4_GENERAL_CI,
            );
            let charset_len = charset.max_len as u32;
            let length = match sql_type {
                SqlType::TinyText => 255 * charset_len,
                SqlType::Text => 65535 * charset_len,
                SqlType::MediumText => 16777215 * charset_len,
                SqlType::LongText => 4294967295,
                _ => unreachable!("Text should be a string type"),
            };
            colflags |= mysql_srv::ColumnFlags::BLOB_FLAG;
            (MYSQL_TYPE_BLOB, results_collation, 0, length)
        }

        // BLOB types
        DfType::Blob => {
            let sql_type = get_sql_type(col, SqlType::Blob);
            let length = match sql_type {
                SqlType::TinyBlob => 255,
                SqlType::Blob => 65535,
                SqlType::MediumBlob => 16777215,
                SqlType::LongBlob => 4294967295,
                _ => unreachable!("Blob should be a binary type"),
            };
            colflags |= mysql_srv::ColumnFlags::BLOB_FLAG;
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            (MYSQL_TYPE_BLOB, DEFAULT_COLLATION_NUMERIC, 0, length)
        }

        // ENUM
        DfType::Enum { ref variants, .. } => {
            // length is the max length of the variants
            let length = variants.iter().map(|v| v.len()).max().unwrap_or(0);
            let charset = mysql_common::collations::Collation::from(
                mysql_common::collations::CollationId::UTF8MB4_GENERAL_CI,
            );
            colflags |= mysql_srv::ColumnFlags::ENUM_FLAG;
            (
                MYSQL_TYPE_STRING,
                results_collation,
                0,
                length as u32 * charset.max_len as u32,
            )
        }

        // JSON
        DfType::Json => {
            colflags |= mysql_srv::ColumnFlags::BLOB_FLAG;
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            (MYSQL_TYPE_JSON, DEFAULT_COLLATION_NUMERIC, 0, u32::MAX)
        }

        // BOOL
        DfType::Bool => (MYSQL_TYPE_TINY, DEFAULT_COLLATION_NUMERIC, 0, 1),

        // Binary
        DfType::Binary(l) => {
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            (MYSQL_TYPE_STRING, DEFAULT_COLLATION_NUMERIC, 0, l as u32)
        }
        DfType::VarBinary(l) => {
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            (
                MYSQL_TYPE_VAR_STRING,
                DEFAULT_COLLATION_NUMERIC,
                0,
                l as u32,
            )
        }

        // Timestamp with timezone and unsupported types
        DfType::TimestampTz { .. } => {
            unsupported!("MySQL does not support the timestamp with time zone type")
        }
        DfType::MacAddr => unsupported!("MySQL does not support the MACADDR type"),
        DfType::Inet => unsupported!("MySQL does not support the INET type"),
        DfType::Uuid => unsupported!("MySQL does not support the UUID type"),
        DfType::Jsonb => unsupported!("MySQL does not support the JSONB type"),
        DfType::VarBit(_) => unsupported!("MySQL does not support the bit varying type"),
        DfType::Array(_) => unsupported!("MySQL does not support arrays"),
        // MySQL rejects a row constructor used where a scalar is expected (error 1241, "Operand
        // should contain 1 column(s)"); it has no projectable row type to map onto.
        DfType::Row(_) => unsupported!("MySQL does not support rows in the select list"),
        DfType::Tsvector => unsupported!("MySQL does not support the tsvector type"),

        // Geometric types - point
        DfType::Point => {
            // this matches what mysql itself returns for the length, wtf?!?
            let length = 4294967295;
            colflags |= mysql_srv::ColumnFlags::BLOB_FLAG;
            colflags |= mysql_srv::ColumnFlags::BINARY_FLAG;
            (MYSQL_TYPE_GEOMETRY, DEFAULT_COLLATION_NUMERIC, 0, length)
        }
        DfType::PostgisPoint => {
            unsupported!("MySQL does not support the postgres/postgis point type")
        }
        DfType::PostgisPolygon => {
            unsupported!("MySQL does not support the postgres/postgis polygon type")
        }

        // Fallback
        DfType::Unknown => (MYSQL_TYPE_UNKNOWN, results_collation, 0, 1024),
    };

    // Process constraints
    if let Some(base) = &col.base {
        for constraint in &base.constraints {
            colflags |= match constraint {
                ColumnConstraint::AutoIncrement => mysql_srv::ColumnFlags::AUTO_INCREMENT_FLAG,
                ColumnConstraint::NotNull => mysql_srv::ColumnFlags::NOT_NULL_FLAG,
                ColumnConstraint::PrimaryKey => mysql_srv::ColumnFlags::PRI_KEY_FLAG,
                ColumnConstraint::Unique => mysql_srv::ColumnFlags::UNIQUE_KEY_FLAG,
                _ => mysql_srv::ColumnFlags::empty(),
            };
        }
        if !base.has_default()
            && base.is_not_null()
            && !colflags.contains(mysql_srv::ColumnFlags::AUTO_INCREMENT_FLAG)
        {
            colflags |= mysql_srv::ColumnFlags::NO_DEFAULT_VALUE_FLAG;
        }
    }

    Ok(mysql_srv::Column {
        schema: String::new(),
        table: col
            .column
            .table
            .clone()
            .unwrap_or_else(|| Relation {
                schema: None,
                name: "".into(),
            })
            .display(readyset_sql::Dialect::MySQL)
            .to_string(),
        org_table: String::new(),
        column: col.column.name.to_string(),
        org_name: String::new(),
        coltype,
        column_length,
        colflags,
        character_set,
        decimals,
    })
}
