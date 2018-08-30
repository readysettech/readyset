use msql_srv;
use nom_sql::{
    self, ColumnConstraint, ColumnSpecification, CreateTableStatement, FieldValueExpression,
    InsertStatement, Literal, SelectStatement, SqlQuery, SqlType,
};

use std::collections::HashMap;

#[derive(Debug)]
pub enum Schema {
    Table(CreateTableStatement),
    View(Vec<ColumnSpecification>),
}

#[allow(dead_code)]
pub(crate) fn schema_for_query(
    schemas: &HashMap<String, Schema>,
    q: &SqlQuery,
) -> Vec<msql_srv::Column> {
    match *q {
        SqlQuery::Select(ref q) => schema_for_select(schemas, q),
        SqlQuery::Insert(ref q) => schema_for_insert(schemas, q),

        _ => unimplemented!(),
    }
}

pub(crate) fn convert_column(cs: &ColumnSpecification) -> msql_srv::Column {
    msql_srv::Column {
        table: cs.column.table.clone().unwrap_or("".to_owned()),
        column: cs.column.name.clone(),
        coltype: match cs.sql_type {
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

pub(crate) fn schema_for_insert(
    schemas: &HashMap<String, Schema>,
    q: &InsertStatement,
) -> Vec<msql_srv::Column> {
    let mut schema = Vec::new();
    for c in q.fields.as_ref().unwrap() {
        // XXX(malte): ewww the hackery
        let mut cc = c.clone();
        cc.table = Some(q.table.name.clone());
        schema.push(schema_for_column(schemas, &cc));
    }
    schema
}

pub(crate) fn schema_for_select(
    table_schemas: &HashMap<String, Schema>,
    q: &SelectStatement,
) -> Vec<msql_srv::Column> {
    let mut schema = Vec::new();
    for fe in &q.fields {
        match *fe {
            nom_sql::FieldDefinitionExpression::Col(ref c) => {
                schema.push(schema_for_column(table_schemas, &c));
            }
            nom_sql::FieldDefinitionExpression::Value(FieldValueExpression::Literal(ref le)) => {
                schema.push(msql_srv::Column {
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
                })
            }
            nom_sql::FieldDefinitionExpression::Value(FieldValueExpression::Arithmetic(ref ae)) => {
                schema.push(msql_srv::Column {
                    table: "".to_owned(),
                    column: match ae.alias {
                        Some(ref a) => a.to_owned(),
                        None => format!("{}", ae),
                    },
                    coltype: msql_srv::ColumnType::MYSQL_TYPE_LONG,
                    colflags: msql_srv::ColumnFlags::empty(),
                })
            }
            _ => unimplemented!(),
        }
    }
    schema
}

pub(crate) fn schema_for_column(
    schemas: &HashMap<String, Schema>,
    c: &nom_sql::Column,
) -> msql_srv::Column {
    if let Some(ref table) = c.table {
        match schemas
            .get(table)
            .expect(&format!("Table/view {} not found!", table))
        {
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
