use nom_sql::{ColumnConstraint, ColumnSpecification, CreateTableStatement, TableKey};

pub trait KeyDefinitionCoalescing {
    fn coalesce_key_definitions(self) -> Self;
}

impl KeyDefinitionCoalescing for CreateTableStatement {
    fn coalesce_key_definitions(mut self) -> CreateTableStatement {
        // TODO(malte): only handles primary keys so far!
        self.body = self.body.map(|mut body| {
            let pkeys: Vec<&ColumnSpecification> = body
                .fields
                .iter()
                .filter(|cs| cs.constraints.contains(&ColumnConstraint::PrimaryKey))
                .collect();
            let mut pk = vec![];
            for cs in pkeys {
                pk.push(cs.column.clone())
            }
            if !pk.is_empty() {
                body.keys = match body.keys {
                    None => Some(vec![TableKey::PrimaryKey {
                        index_name: None,
                        constraint_name: None,
                        columns: pk,
                    }]),
                    Some(mut ks) => {
                        let new_key = TableKey::PrimaryKey {
                            index_name: None,
                            constraint_name: None,
                            columns: pk,
                        };
                        if !ks.contains(&new_key) {
                            ks.push(new_key);
                        }
                        Some(ks)
                    }
                }
            }
            body
        });

        self
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{Column, CreateTableBody, Relation, SqlType};

    use super::*;

    #[test]
    fn it_coalesces_pkeys() {
        use nom_sql::CreateTableStatement;

        // CREATE TABLE t (id text PRIMARY KEY, val text)
        // -->
        // CREATE TABLE t (id text, val text, PRIMARY KEY (id))
        let q = CreateTableStatement {
            if_not_exists: false,
            table: Relation::from("t"),
            body: Ok(CreateTableBody {
                fields: vec![
                    ColumnSpecification::with_constraints(
                        Column::from("t.id"),
                        SqlType::Text,
                        vec![ColumnConstraint::PrimaryKey],
                    ),
                    ColumnSpecification::new(Column::from("t.val"), SqlType::Text),
                ],
                keys: None,
            }),
            options: Ok(vec![]),
        };

        let ctq = q.coalesce_key_definitions();
        assert_eq!(ctq.table, Relation::from("t"));
        assert_eq!(
            ctq.body.as_ref().unwrap().fields,
            vec![
                ColumnSpecification::with_constraints(
                    Column::from("t.id"),
                    SqlType::Text,
                    vec![ColumnConstraint::PrimaryKey],
                ),
                ColumnSpecification::new(Column::from("t.val"), SqlType::Text),
            ]
        );
        assert_eq!(
            ctq.body.as_ref().unwrap().keys,
            Some(vec![TableKey::PrimaryKey {
                index_name: None,
                constraint_name: None,
                columns: vec![Column::from("t.id")]
            }])
        );
    }
}
