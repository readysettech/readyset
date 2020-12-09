use nom_sql::{ColumnConstraint, ColumnSpecification, SqlQuery, TableKey};

pub trait KeyDefinitionCoalescing {
    fn coalesce_key_definitions(self) -> SqlQuery;
}

impl KeyDefinitionCoalescing for SqlQuery {
    fn coalesce_key_definitions(self) -> SqlQuery {
        match self {
            SqlQuery::CreateTable(mut ctq) => {
                // TODO(malte): only handles primary keys so far!
                let pkeys: Vec<&ColumnSpecification> = ctq
                    .fields
                    .iter()
                    .filter(|cs| cs.constraints.contains(&ColumnConstraint::PrimaryKey))
                    .collect();
                let mut pk = vec![];
                for cs in pkeys {
                    pk.push(cs.column.clone())
                }
                if !pk.is_empty() {
                    ctq.keys = match ctq.keys {
                        None => Some(vec![TableKey::PrimaryKey(pk)]),
                        Some(mut ks) => {
                            let new_key = TableKey::PrimaryKey(pk);
                            if !ks.contains(&new_key) {
                                ks.push(new_key);
                            }
                            Some(ks)
                        }
                    }
                }
                SqlQuery::CreateTable(ctq)
            }
            x => x,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KeyDefinitionCoalescing;
    use nom_sql::{
        Column, ColumnConstraint, ColumnSpecification, SqlQuery, SqlType, Table, TableKey,
    };

    #[test]
    fn it_coalesces_pkeys() {
        use nom_sql::CreateTableStatement;

        // CREATE TABLE t (id text PRIMARY KEY, val text)
        // -->
        // CREATE TABLE t (id text, val text, PRIMARY KEY (id))
        let q = CreateTableStatement {
            table: Table::from("t"),
            fields: vec![
                ColumnSpecification::with_constraints(
                    Column::from("t.id"),
                    SqlType::Text,
                    vec![ColumnConstraint::PrimaryKey],
                ),
                ColumnSpecification::new(Column::from("t.val"), SqlType::Text),
            ],
            keys: None,
        };

        let res = SqlQuery::CreateTable(q).coalesce_key_definitions();
        match res {
            SqlQuery::CreateTable(ctq) => {
                assert_eq!(ctq.table, Table::from("t"));
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
                    Some(vec![TableKey::PrimaryKey(vec![Column::from("t.id")])])
                );
            }
            // if we get anything other than a CreateTable back, something really weird is up
            _ => panic!(),
        }
    }
}
