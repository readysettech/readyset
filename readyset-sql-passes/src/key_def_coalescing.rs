use readyset_sql::ast::{
    ColumnConstraint, ColumnSpecification, CreateTableStatement, IndexKeyPart, TableKey,
};

pub trait KeyDefinitionCoalescing {
    fn coalesce_key_definitions(&mut self) -> &mut Self;
}

impl KeyDefinitionCoalescing for CreateTableStatement {
    fn coalesce_key_definitions(&mut self) -> &mut Self {
        // TODO(malte): only handles primary keys so far!
        if let Ok(body) = &mut self.body {
            let pkeys: Vec<&ColumnSpecification> = body
                .fields
                .iter()
                .filter(|cs| cs.constraints.contains(&ColumnConstraint::PrimaryKey))
                .collect();
            let mut pk = vec![];
            for cs in pkeys {
                pk.push(IndexKeyPart::Column(cs.column.clone()))
            }
            if !pk.is_empty() {
                if let Some(ks) = &mut body.keys {
                    let new_key = TableKey::PrimaryKey {
                        index_name: None,
                        constraint_name: None,
                        constraint_timing: None,
                        columns: pk,
                    };
                    if !ks.contains(&new_key) {
                        ks.push(new_key);
                    }
                } else {
                    body.keys = Some(vec![TableKey::PrimaryKey {
                        index_name: None,
                        constraint_name: None,
                        constraint_timing: None,
                        columns: pk,
                    }])
                }
            }
        };

        self
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::ast::{Column, CreateTableBody, IndexKeyPart, Relation, SqlType};

    use super::*;

    #[test]
    fn it_coalesces_pkeys() {
        use readyset_sql::ast::CreateTableStatement;

        // CREATE TABLE t (id text PRIMARY KEY, val text)
        // -->
        // CREATE TABLE t (id text, val text, PRIMARY KEY (id))
        let mut q = CreateTableStatement {
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
            like: None,
            options: Ok(vec![]),
        };

        q.coalesce_key_definitions();
        assert_eq!(q.table, Relation::from("t"));
        assert_eq!(
            q.body.as_ref().unwrap().fields,
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
            q.body.as_ref().unwrap().keys,
            Some(vec![TableKey::PrimaryKey {
                index_name: None,
                constraint_name: None,
                constraint_timing: None,
                columns: vec![IndexKeyPart::Column(Column::from("t.id"))]
            }])
        );
    }
}
