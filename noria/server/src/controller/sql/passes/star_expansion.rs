use nom_sql::{Column, Expression, FieldDefinitionExpression, SqlQuery};

use itertools::Either;
use std::collections::HashMap;
use std::iter;

pub trait StarExpansion {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery;
}

impl StarExpansion for SqlQuery {
    fn expand_stars(mut self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery {
        let expand_table = |table_name: String| {
            write_schemas
                .get(&table_name)
                // TODO(eta): check that this panic can never fire
                .unwrap_or_else(|| panic!("table name `{}` does not exist", table_name))
                .clone()
                .into_iter()
                .map(move |f| FieldDefinitionExpression::Expression {
                    expr: Expression::Column(Column::from(
                        format!("{}.{}", table_name, f).as_ref(),
                    )),
                    alias: None,
                })
        };

        if let SqlQuery::Select(ref mut sq) = self {
            let old_fields = std::mem::take(&mut sq.fields);
            sq.fields = old_fields
                .into_iter()
                .flat_map(|field| match field {
                    FieldDefinitionExpression::All => Either::Left(
                        sq.tables
                            .iter()
                            .map(|t| t.name.clone())
                            .flat_map(&expand_table),
                    ),
                    FieldDefinitionExpression::AllInTable(t) => {
                        Either::Right(Either::Left(expand_table(t)))
                    }
                    e @ FieldDefinitionExpression::Expression { .. } => {
                        Either::Right(Either::Right(iter::once(e)))
                    }
                })
                .collect();
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::StarExpansion;
    use nom_sql::SelectStatement;
    use nom_sql::{Column, FieldDefinitionExpression, SqlQuery, Table};
    use std::collections::HashMap;

    #[test]
    fn it_expands_stars() {
        // SELECT * FROM PaperTag
        // -->
        // SELECT paper_id, tag_id FROM PaperTag
        let q = SelectStatement {
            tables: vec![Table {
                name: String::from("PaperTag"),
                alias: None,
                schema: None,
            }],
            fields: vec![FieldDefinitionExpression::All],
            ..Default::default()
        };
        let mut schema = HashMap::new();
        schema.insert("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()]);

        let res = SqlQuery::Select(q).expand_stars(&schema);
        // * selector has been expanded to field list
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldDefinitionExpression::from(Column::from("PaperTag.paper_id")),
                        FieldDefinitionExpression::from(Column::from("PaperTag.tag_id")),
                    ]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }

    #[test]
    fn it_expands_stars_from_multiple_tables() {
        // SELECT * FROM PaperTag, Users [...]
        // -->
        // SELECT paper_id, tag_id, uid, name FROM PaperTag, Users [...]
        let q = SelectStatement {
            tables: vec![Table::from("PaperTag"), Table::from("Users")],
            fields: vec![FieldDefinitionExpression::All],
            ..Default::default()
        };
        let mut schema = HashMap::new();
        schema.insert("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()]);
        schema.insert("Users".into(), vec!["uid".into(), "name".into()]);

        let res = SqlQuery::Select(q).expand_stars(&schema);
        // * selector has been expanded to field list
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldDefinitionExpression::from(Column::from("PaperTag.paper_id")),
                        FieldDefinitionExpression::from(Column::from("PaperTag.tag_id")),
                        FieldDefinitionExpression::from(Column::from("Users.uid")),
                        FieldDefinitionExpression::from(Column::from("Users.name")),
                    ]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }

    #[test]
    fn it_expands_table_stars_from_multiple_tables() {
        // SELECT Users.*, * FROM PaperTag, Users [...]
        // -->
        // SELECT uid, name, paper_id, tag_id, uid, name FROM PaperTag, Users [...]
        let q = SelectStatement {
            tables: vec![Table::from("PaperTag"), Table::from("Users")],
            fields: vec![
                FieldDefinitionExpression::AllInTable("Users".into()),
                FieldDefinitionExpression::All,
            ],
            ..Default::default()
        };
        let mut schema = HashMap::new();
        schema.insert("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()]);
        schema.insert("Users".into(), vec!["uid".into(), "name".into()]);

        let res = SqlQuery::Select(q).expand_stars(&schema);
        // * selector has been expanded to field list
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldDefinitionExpression::from(Column::from("Users.uid")),
                        FieldDefinitionExpression::from(Column::from("Users.name")),
                        FieldDefinitionExpression::from(Column::from("PaperTag.paper_id")),
                        FieldDefinitionExpression::from(Column::from("PaperTag.tag_id")),
                        FieldDefinitionExpression::from(Column::from("Users.uid")),
                        FieldDefinitionExpression::from(Column::from("Users.name")),
                    ]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }
}
