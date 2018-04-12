use msql_srv;
use nom_sql::{self, ColumnConstraint, CreateTableStatement, InsertStatement, Literal,
              SelectStatement, SqlQuery, SqlType};

use std::collections::HashMap;

#[allow(dead_code)]
pub(crate) fn schema_for_query(
    table_schemas: &HashMap<String, CreateTableStatement>,
    q: &SqlQuery,
) -> Vec<msql_srv::Column> {
    match *q {
        SqlQuery::Select(ref q) => schema_for_select(table_schemas, q),
        SqlQuery::Insert(ref q) => schema_for_insert(table_schemas, q),

        _ => unimplemented!(),
    }
}

pub(crate) fn schema_for_insert(
    table_schemas: &HashMap<String, CreateTableStatement>,
    q: &InsertStatement,
) -> Vec<msql_srv::Column> {
    let mut schema = Vec::new();
    for c in &q.fields {
        // XXX(malte): ewww the hackery
        let mut cc = c.clone();
        cc.table = Some(q.table.name.clone());
        schema.push(schema_for_column(table_schemas, &cc));
    }
    schema
}

pub(crate) fn schema_for_select(
    table_schemas: &HashMap<String, CreateTableStatement>,
    q: &SelectStatement,
) -> Vec<msql_srv::Column> {
    let mut schema = Vec::new();
    for fe in &q.fields {
        match *fe {
            nom_sql::FieldExpression::Col(ref c) => {
                schema.push(schema_for_column(table_schemas, &c));
            }
            nom_sql::FieldExpression::Literal(ref le) => schema.push(msql_srv::Column {
                table: "".to_owned(),
                column: match le.alias {
                    Some(ref a) => a.to_owned(),
                    None => le.value.to_string(),
                },
                coltype: match le.value {
                    Literal::Integer(_) => msql_srv::ColumnType::MYSQL_TYPE_LONG,
                    Literal::String(_) => msql_srv::ColumnType::MYSQL_TYPE_VAR_STRING,
                    _ => unimplemented!(),
                },
                colflags: msql_srv::ColumnFlags::empty(),
            }),
            nom_sql::FieldExpression::Arithmetic(ref ae) => schema.push(msql_srv::Column {
                table: "".to_owned(),
                column: match ae.alias {
                    Some(ref a) => a.to_owned(),
                    None => format!("{}", ae),
                },
                coltype: msql_srv::ColumnType::MYSQL_TYPE_LONG,
                colflags: msql_srv::ColumnFlags::empty(),
            }),
            _ => unimplemented!(),
        }
    }
    schema
}

pub(crate) fn schema_for_column(
    table_schemas: &HashMap<String, CreateTableStatement>,
    c: &nom_sql::Column,
) -> msql_srv::Column {
    if let Some(ref table) = c.table {
        let col_schema = table_schemas[table]
            .fields
            .iter()
            .find(|cc| cc.column.name == c.name)
            .expect(&format!("column {} not found", c.name));
        assert_eq!(col_schema.column.name, c.name);

        msql_srv::Column {
            table: table.clone(),
            column: c.name.clone(),
            coltype: match col_schema.sql_type {
                SqlType::Mediumtext => msql_srv::ColumnType::MYSQL_TYPE_VAR_STRING,
                SqlType::Longtext => msql_srv::ColumnType::MYSQL_TYPE_BLOB,
                SqlType::Text => msql_srv::ColumnType::MYSQL_TYPE_STRING,
                SqlType::Varchar(_) => msql_srv::ColumnType::MYSQL_TYPE_VAR_STRING,
                SqlType::Int(_) => msql_srv::ColumnType::MYSQL_TYPE_LONG,
                SqlType::Bigint(_) => msql_srv::ColumnType::MYSQL_TYPE_LONGLONG,
                SqlType::Tinyint(_) => msql_srv::ColumnType::MYSQL_TYPE_TINY,
                SqlType::Bool => msql_srv::ColumnType::MYSQL_TYPE_BIT,
                SqlType::DateTime => msql_srv::ColumnType::MYSQL_TYPE_DATETIME,
                SqlType::Float => msql_srv::ColumnType::MYSQL_TYPE_DOUBLE,
                SqlType::Decimal(_, _) => msql_srv::ColumnType::MYSQL_TYPE_DECIMAL,
                _ => unimplemented!(),
            },
            colflags: {
                let mut flags = msql_srv::ColumnFlags::empty();
                for c in &col_schema.constraints {
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
    } else {
        msql_srv::Column {
            table: "".into(),
            column: c.name.clone(),
            coltype: msql_srv::ColumnType::MYSQL_TYPE_LONG,
            colflags: msql_srv::ColumnFlags::empty(),
        }
    }
}
