use nom_sql::{Column, FieldExpression, SqlQuery};

use std::collections::HashMap;
use std::mem;

pub trait StarExpansion {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery;
}

impl StarExpansion for SqlQuery {
    fn expand_stars(mut self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery {
        let expand_table = |table_name: String| {
            write_schemas
                .get(&table_name)
                .unwrap()
                .clone()
                .into_iter()
                .map(move |f| {
                    FieldExpression::Col(Column::from(format!("{}.{}", table_name, f).as_ref()))
                })
        };

        if let SqlQuery::Select(ref mut sq) = self {
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
                    FieldExpression::Literal(l) => vec![FieldExpression::Literal(l)].into_iter(),
                    FieldExpression::Col(c) => vec![FieldExpression::Col(c)].into_iter(),
                })
                .collect();
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::SelectStatement;
    use nom_sql::{Column, FieldExpression, SqlQuery, Table};
    use std::collections::HashMap;
    use super::StarExpansion;

    #[test]
    fn it_expands_stars() {
        // SELECT * FROM PaperTag
        // -->
        // SELECT paper_id, tag_id FROM PaperTag
        let q = SelectStatement {
            tables: vec![
                Table {
                    name: String::from("PaperTag"),
                    alias: None,
                },
            ],
            fields: vec![FieldExpression::All],
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
                        FieldExpression::Col(Column::from("PaperTag.paper_id")),
                        FieldExpression::Col(Column::from("PaperTag.tag_id")),
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
            fields: vec![FieldExpression::All],
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
                        FieldExpression::Col(Column::from("PaperTag.paper_id")),
                        FieldExpression::Col(Column::from("PaperTag.tag_id")),
                        FieldExpression::Col(Column::from("Users.uid")),
                        FieldExpression::Col(Column::from("Users.name")),
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
                FieldExpression::AllInTable("Users".into()),
                FieldExpression::All,
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
                        FieldExpression::Col(Column::from("Users.uid")),
                        FieldExpression::Col(Column::from("Users.name")),
                        FieldExpression::Col(Column::from("PaperTag.paper_id")),
                        FieldExpression::Col(Column::from("PaperTag.tag_id")),
                        FieldExpression::Col(Column::from("Users.uid")),
                        FieldExpression::Col(Column::from("Users.name")),
                    ]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }
}
