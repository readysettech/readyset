use nom_sql::{Column, FieldExpression, SqlQuery};

use std::collections::HashMap;

pub trait StarExpansion {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery;
}

impl StarExpansion for SqlQuery {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery {
        match self {
            // nothing to do for INSERTs, as they cannot have stars
            SqlQuery::Insert(i) => SqlQuery::Insert(i),
            SqlQuery::Select(mut sq) => {
                sq.fields = match sq.fields {
                    FieldExpression::All => {
                        // XXX(malte): not currently compatible with selections from > 1 table.
                        // This will be fixed once we support "* FROM table" syntax.
                        let table = &sq.tables[0];
                        let new_fs = write_schemas.get(&table.name).unwrap().clone();
                        FieldExpression::Seq(new_fs.iter()
                            .map(|f| Column::from(format!("{}.{}", table.name, f).as_ref()))
                            .collect())
                    }
                    x => x,
                };

                SqlQuery::Select(sq)
            }
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
}
