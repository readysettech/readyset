use nom_sql::{ColumnConstraint, ColumnSpecification, CreateTableStatement, TableKey};

pub trait KeyDefinitionCoalescing {
    fn coalesce_key_definitions(self) -> Self;
}

impl KeyDefinitionCoalescing for CreateTableStatement {
    fn coalesce_key_definitions(mut self) -> CreateTableStatement {
        // TODO(malte): only handles primary keys so far!
        let pkeys: Vec<&ColumnSpecification> = self
            .fields
            .iter()
            .filter(|cs| cs.constraints.contains(&ColumnConstraint::PrimaryKey))
            .collect();
        let mut pk = vec![];
        for cs in pkeys {
            pk.push(cs.column.clone())
        }
        if !pk.is_empty() {
            self.keys = match self.keys {
                None => Some(vec![TableKey::PrimaryKey {
                    name: None,
                    columns: pk,
                }]),
                Some(mut ks) => {
                    let new_key = TableKey::PrimaryKey {
                        name: None,
                        columns: pk,
                    };
                    if !ks.contains(&new_key) {
                        ks.push(new_key);
                    }
                    Some(ks)
                }
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{Column, Relation, SqlType};

    use super::*;

    #[test]
    fn it_coalesces_pkeys() {
        use nom_sql::CreateTableStatement;

        // CREATE TABLE t (id text PRIMARY KEY, val text)
        // -->
        // CREATE TABLE t (id text, val text, PRIMARY KEY (id))
        let q = CreateTableStatement {
            table: Relation::from("t"),
            fields: vec![
                ColumnSpecification::with_constraints(
                    Column::from("t.id"),
                    SqlType::Text,
                    vec![ColumnConstraint::PrimaryKey],
                ),
                ColumnSpecification::new(Column::from("t.val"), SqlType::Text),
            ],
            keys: None,
            if_not_exists: false,
            options: vec![],
        };

        let ctq = q.coalesce_key_definitions();
        assert_eq!(ctq.table, Relation::from("t"));
        assert_eq!(
            ctq.fields,
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
            ctq.keys,
            Some(vec![TableKey::PrimaryKey {
                name: None,
                columns: vec![Column::from("t.id")]
            }])
        );
    }
}
