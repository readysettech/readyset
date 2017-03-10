use nom_sql::{Column, FieldExpression, SqlQuery};

use std::collections::HashMap;

pub trait StarExpansion {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery;
}

impl StarExpansion for SqlQuery {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery {
        match self {
            SqlQuery::Select(mut sq) => {
                sq.fields = match sq.fields {
                    FieldExpression::All => {
                        // TODO(malte): not currently compatible with a "table.*" syntax, but only
                        // with "* FROM table" or "* FROM table1, table2".
                        let new_fs = sq.tables
                            .iter()
                            .fold(Vec::new(), |mut acc, ref t| {
                                let fs = write_schemas.get(&t.name)
                                    .unwrap()
                                    .clone()
                                    .iter()
                                    .map(|f| Column::from(format!("{}.{}", t.name, f).as_ref()))
                                    .collect::<Vec<_>>();
                                acc.extend(fs);
                                acc
                            });
                        FieldExpression::Seq(new_fs)
                    }
                    x => x,
                };

                SqlQuery::Select(sq)
            }
            // nothing to do for other query types, as they cannot have aliases
            x => x,
        }
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
            tables: vec![Table {
                             name: String::from("PaperTag"),
                             alias: None,
                         }],
            fields: FieldExpression::All,
            ..Default::default()
        };
        let mut schema = HashMap::new();
        schema.insert("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()]);

        let res = SqlQuery::Select(q).expand_stars(&schema);
        // * selector has been expanded to field list
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(tq.fields,
                           FieldExpression::Seq(vec![Column::from("PaperTag.paper_id"),
                                                     Column::from("PaperTag.tag_id")]));
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
            fields: FieldExpression::All,
            ..Default::default()
        };
        let mut schema = HashMap::new();
        schema.insert("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()]);
        schema.insert("Users".into(), vec!["uid".into(), "name".into()]);

        let res = SqlQuery::Select(q).expand_stars(&schema);
        // * selector has been expanded to field list
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(tq.fields,
                           FieldExpression::Seq(vec![Column::from("PaperTag.paper_id"),
                                                     Column::from("PaperTag.tag_id"),
                                                     Column::from("Users.uid"),
                                                     Column::from("Users.name")]));
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }

}
