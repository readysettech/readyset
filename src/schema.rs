use msql_srv;
use nom_sql::{self, ColumnConstraint, ColumnSpecification, CreateTableStatement, SqlType};

#[derive(Debug)]
pub enum Schema {
    Table(CreateTableStatement),
    View(Vec<ColumnSpecification>),
}

pub(crate) fn convert_column(cs: &ColumnSpecification) -> msql_srv::Column {
    let mut flags = msql_srv::ColumnFlags::empty();
    msql_srv::Column {
        table: cs.column.table.clone().unwrap_or("".to_owned()),
        column: cs.column.name.clone(),
        coltype: match cs.sql_type {
            SqlType::Mediumtext => msql_srv::ColumnType::MYSQL_TYPE_VAR_STRING,
            SqlType::Longtext => msql_srv::ColumnType::MYSQL_TYPE_BLOB,
            SqlType::Text => msql_srv::ColumnType::MYSQL_TYPE_STRING,
            SqlType::Varchar(_) => msql_srv::ColumnType::MYSQL_TYPE_VAR_STRING,
            SqlType::Int(_) => msql_srv::ColumnType::MYSQL_TYPE_LONG,
            SqlType::UnsignedInt(_) => {
                flags |= msql_srv::ColumnFlags::UNSIGNED_FLAG;
                msql_srv::ColumnType::MYSQL_TYPE_LONG
            },
            SqlType::Bigint(_) => msql_srv::ColumnType::MYSQL_TYPE_LONGLONG,
            SqlType::UnsignedBigint(_) => {
                flags |= msql_srv::ColumnFlags::UNSIGNED_FLAG;
                msql_srv::ColumnType::MYSQL_TYPE_LONGLONG
            },
            SqlType::Tinyint(_) => msql_srv::ColumnType::MYSQL_TYPE_TINY,
            SqlType::UnsignedTinyint(_) => {
                flags |= msql_srv::ColumnFlags::UNSIGNED_FLAG;
                msql_srv::ColumnType::MYSQL_TYPE_TINY
            },
            SqlType::Bool => msql_srv::ColumnType::MYSQL_TYPE_BIT,
            SqlType::DateTime(_) => msql_srv::ColumnType::MYSQL_TYPE_DATETIME,
            SqlType::Float => msql_srv::ColumnType::MYSQL_TYPE_DOUBLE,
            SqlType::Decimal(_, _) => msql_srv::ColumnType::MYSQL_TYPE_DECIMAL,
            _ => unimplemented!(),
        },
        colflags: {
            for c in &cs.constraints {
                match *c {
                    ColumnConstraint::AutoIncrement => {
                        flags |= msql_srv::ColumnFlags::AUTO_INCREMENT_FLAG;
                    }
                    ColumnConstraint::NotNull => {
                        flags |= msql_srv::ColumnFlags::NOT_NULL_FLAG;
                    }
                    ColumnConstraint::PrimaryKey => {
                        flags |= msql_srv::ColumnFlags::PRI_KEY_FLAG;
                    }
                    ColumnConstraint::Unique => {
                        flags |= msql_srv::ColumnFlags::UNIQUE_KEY_FLAG;
                    }
                    _ => (),
                }
            }
            flags
        },
    }
}

pub(crate) fn convert_schema(schema: &Schema) -> Vec<msql_srv::Column> {
    match schema {
        Schema::Table(CreateTableStatement { ref fields, .. }) | Schema::View(ref fields) => {
            fields.iter().map(|c| convert_column(c)).collect()
        }
    }
}

pub(crate) fn schema_for_column(schema: &Schema, c: &nom_sql::Column) -> msql_srv::Column {
    if let Some(ref table) = c.table {
        match schema {
            Schema::Table(CreateTableStatement { ref fields, .. }) => {
                let colspec = fields
                    .iter()
                    .find(|cc| cc.column.name == c.name)
                    .expect(&format!("column {} not found", c.name));

                assert_eq!(colspec.column.name, c.name);

                convert_column(colspec)
            }
            Schema::View(ref fields) => convert_column(
                fields
                    .iter()
                    .find(|cs| cs.column == *c)
                    .expect(&format!("column {} not found in view {}", c.name, table)),
            ),
        }
    } else {
        // no table specified on column
        msql_srv::Column {
            table: "".into(),
            column: c.name.clone(),
            coltype: msql_srv::ColumnType::MYSQL_TYPE_LONG,
            colflags: msql_srv::ColumnFlags::empty(),
        }
    }
}
