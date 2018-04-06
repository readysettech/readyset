use nom_sql::{Column, CreateTableStatement, FieldExpression, SqlQuery};

use std::collections::HashMap;
use std::mem;

pub(crate) fn expand_stars(
    mut query: SqlQuery,
    table_schemas: &HashMap<String, CreateTableStatement>,
) -> SqlQuery {
    let expand_table = |table_name: String| {
        table_schemas
            .get(&table_name)
            .expect(&format!("table name `{}` does not exist", table_name))
            .clone()
            .fields
            .into_iter()
            .map(move |f| {
                FieldExpression::Col(Column {
                    table: Some(table_name.clone()),
                    name: f.column.name.clone(),
                    alias: None,
                    function: None,
                })
            })
    };

    if let SqlQuery::Select(ref mut sq) = query {
        let old_fields = mem::replace(&mut sq.fields, vec![]);
        sq.fields = old_fields
            .into_iter()
            .flat_map(|field| match field {
                FieldExpression::All => {
                    let v: Vec<_> = sq.tables
                        .iter()
                        .map(|t| t.name.clone())
                        .flat_map(&expand_table)
                        .collect();
                    v.into_iter()
                }
                FieldExpression::AllInTable(t) => {
                    let v: Vec<_> = expand_table(t).collect();
                    v.into_iter()
                }
                FieldExpression::Arithmetic(a) => vec![FieldExpression::Arithmetic(a)].into_iter(),
                FieldExpression::Literal(l) => vec![FieldExpression::Literal(l)].into_iter(),
                FieldExpression::Col(c) => vec![FieldExpression::Col(c)].into_iter(),
            })
            .collect();
    }
    query
}
