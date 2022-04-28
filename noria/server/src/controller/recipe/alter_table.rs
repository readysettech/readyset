use nom_sql::{
    AlterColumnOperation, AlterTableDefinition, AlterTableStatement, ColumnConstraint,
    CreateTableStatement, TableKey,
};
use noria_errors::{ReadySetError, ReadySetResult};

/// Creates a new [`CreateTableStatement`] from the one given, and applies all the
/// alterations specified in the [`AlterTableStatement`].
pub(super) fn rewrite_table_definition(
    alter_table_definition: &AlterTableStatement,
    mut new_table: CreateTableStatement,
) -> ReadySetResult<CreateTableStatement> {
    for definition in alter_table_definition.definitions.iter() {
        match definition {
            AlterTableDefinition::AddColumn(c) => {
                let mut c = c.clone();
                c.column.table = Some(new_table.table.name.clone());
                new_table.fields.push(c);
            }
            AlterTableDefinition::AddKey(key) => {
                let mut new_key = key.clone();
                match &mut new_key {
                    TableKey::PrimaryKey {
                        ref mut columns, ..
                    }
                    | TableKey::UniqueKey {
                        ref mut columns, ..
                    }
                    | TableKey::FulltextKey {
                        ref mut columns, ..
                    }
                    | TableKey::Key {
                        ref mut columns, ..
                    }
                    | TableKey::ForeignKey {
                        ref mut columns, ..
                    } => {
                        columns.iter_mut().for_each(|c| {
                            c.table = Some(new_table.table.name.clone());
                        });
                    }
                    TableKey::CheckConstraint { .. } => {}
                }
                match new_table.keys {
                    None => new_table.keys = Some(vec![new_key]),
                    Some(ref mut keys) => keys.push(new_key),
                }
            }
            AlterTableDefinition::AlterColumn { name, operation } => {
                match new_table.fields.iter_mut().find(|f| f.column.name == name) {
                    None => {
                        return Err(ReadySetError::InvalidQuery(
                            alter_table_definition.to_string(),
                        ))
                    }
                    Some(col) => match operation {
                        AlterColumnOperation::SetColumnDefault(lit) => {
                            match col
                                .constraints
                                .iter_mut()
                                .find(|c| matches!(c, ColumnConstraint::DefaultValue(_)))
                            {
                                Some(ColumnConstraint::DefaultValue(ref mut def)) => {
                                    *def = lit.clone();
                                }
                                _ => col
                                    .constraints
                                    .push(ColumnConstraint::DefaultValue(lit.clone())),
                            }
                        }
                        AlterColumnOperation::DropColumnDefault => {
                            col.constraints
                                .retain(|c| !matches!(c, ColumnConstraint::DefaultValue(_)));
                        }
                    },
                };
            }
            AlterTableDefinition::DropColumn { name, .. } => {
                new_table.fields.retain(|f| f.column.name != name);
                if new_table.fields.is_empty() {
                    return Err(ReadySetError::InvalidQuery(
                        alter_table_definition.to_string(),
                    ));
                }
            }
            AlterTableDefinition::ChangeColumn { name, spec } => {
                let mut spec = spec.clone();
                spec.column.table = Some(new_table.table.name.clone());
                match new_table.fields.iter_mut().find(|f| f.column.name == name) {
                    None => {
                        return Err(ReadySetError::InvalidQuery(
                            alter_table_definition.to_string(),
                        ))
                    }
                    Some(col) => {
                        *col = spec;
                    }
                };
            }
            AlterTableDefinition::RenameColumn { name, new_name } => {
                match new_table.fields.iter_mut().find(|f| f.column.name == name) {
                    None => {
                        return Err(ReadySetError::InvalidQuery(
                            alter_table_definition.to_string(),
                        ))
                    }
                    Some(col) => {
                        col.column.name = new_name.clone();
                    }
                };
            }
        }
    }
    Ok(new_table)
}

#[cfg(test)]
mod tests {
    use nom_sql::{CreateTableStatement, Dialect, Literal, SqlIdentifier, SqlType};

    use super::*;

    fn create_table() -> CreateTableStatement {
        nom_sql::parse_create_table(
            Dialect::MySQL,
            "CREATE TABLE test (
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                PRIMARY KEY (id)
            );",
        )
        .unwrap()
    }

    #[test]
    fn add_column() {
        let original_table = create_table();
        let alteration = nom_sql::parse_alter_table(
            Dialect::MySQL,
            "ALTER TABLE test ADD COLUMN age DOUBLE NOT NULL;",
        )
        .unwrap();
        assert!(original_table
            .fields
            .iter()
            .find(|f| f.column.name == "age")
            .is_none());
        let new_table = rewrite_table_definition(&alteration, original_table.clone()).unwrap();
        assert_eq!(new_table.table, original_table.table);
        assert_eq!(new_table.keys, original_table.keys);
        assert_eq!(new_table.if_not_exists, original_table.if_not_exists);
        assert_eq!(new_table.options, original_table.options);
        let column_spec = new_table
            .fields
            .iter()
            .find(|f| f.column.name == "age")
            .unwrap();
        assert_eq!(column_spec.sql_type, SqlType::Double);
        assert_eq!(column_spec.constraints.len(), 1);
        let constraint = &column_spec.constraints[0];
        assert!(matches!(constraint, ColumnConstraint::NotNull));
        assert_eq!(column_spec.column.table, Some(new_table.table.name));
    }

    #[test]
    fn add_key() {
        let original_table = create_table();
        let alteration = nom_sql::parse_alter_table(
            Dialect::MySQL,
            "ALTER TABLE test ADD UNIQUE KEY new_key (id) USING hash;",
        )
        .unwrap();
        assert!(original_table
            .keys
            .as_ref()
            .unwrap()
            .iter()
            .find(|f| matches!(f, TableKey::UniqueKey { .. }))
            .is_none());
        let new_table = rewrite_table_definition(&alteration, original_table.clone()).unwrap();
        assert_eq!(new_table.table, original_table.table);
        assert_eq!(new_table.fields, original_table.fields);
        assert_eq!(new_table.if_not_exists, original_table.if_not_exists);
        assert_eq!(new_table.options, original_table.options);
        match new_table
            .keys
            .unwrap()
            .iter()
            .find(|f| matches!(f, TableKey::UniqueKey { .. }))
            .unwrap()
        {
            TableKey::UniqueKey {
                name,
                columns,
                index_type,
            } => {
                assert_eq!(name.clone().unwrap(), SqlIdentifier::from("new_key"));
                assert_eq!(columns.len(), 1);
                let column_spec = &columns[0];
                assert_eq!(column_spec.name, "id");
                assert_eq!(
                    column_spec.table.clone().unwrap(),
                    original_table.table.name
                );
                assert_eq!(index_type.unwrap(), nom_sql::IndexType::Hash);
            }
            _ => panic!("Expected unique key"),
        };
    }

    #[test]
    fn change_column() {
        let original_table = create_table();
        let alteration = nom_sql::parse_alter_table(
            Dialect::MySQL,
            "ALTER TABLE test CHANGE COLUMN id new_id TEXT;",
        )
        .unwrap();
        assert!(original_table
            .fields
            .iter()
            .find(|f| f.column.name == "new_id")
            .is_none());
        let original_col_spec = original_table
            .fields
            .iter()
            .find(|f| f.column.name == "id")
            .unwrap();
        assert_eq!(original_col_spec.sql_type, SqlType::Int(None));
        assert_eq!(original_col_spec.constraints.len(), 1);
        let constraint = &original_col_spec.constraints[0];
        assert!(matches!(constraint, ColumnConstraint::NotNull));
        assert_eq!(
            original_col_spec.column.table,
            Some(original_table.table.name.clone())
        );
        let new_table = rewrite_table_definition(&alteration, original_table.clone()).unwrap();
        assert_eq!(new_table.table, original_table.table);
        assert_eq!(new_table.keys, original_table.keys);
        assert_eq!(new_table.if_not_exists, original_table.if_not_exists);
        assert_eq!(new_table.options, original_table.options);
        assert!(new_table
            .fields
            .iter()
            .find(|f| f.column.name == "id")
            .is_none());
        let column_spec = new_table
            .fields
            .iter()
            .find(|f| f.column.name == "new_id")
            .unwrap();
        assert_eq!(column_spec.sql_type, SqlType::Text);
        assert!(column_spec.constraints.is_empty());
        assert_eq!(column_spec.column.table, Some(new_table.table.name));
    }

    #[test]
    fn drop_column() {
        let original_table = create_table();
        let alteration =
            nom_sql::parse_alter_table(Dialect::MySQL, "ALTER TABLE test DROP COLUMN name;")
                .unwrap();
        assert!(original_table
            .fields
            .iter()
            .find(|f| f.column.name == "name")
            .is_some());
        let new_table = rewrite_table_definition(&alteration, original_table.clone()).unwrap();
        assert_eq!(new_table.table, original_table.table);
        assert_eq!(new_table.keys, original_table.keys);
        assert_eq!(new_table.if_not_exists, original_table.if_not_exists);
        assert_eq!(new_table.options, original_table.options);
        assert!(new_table
            .fields
            .iter()
            .find(|f| f.column.name == "name")
            .is_none());
    }

    #[test]
    fn alter_column() {
        let original_table = create_table();
        let alteration = nom_sql::parse_alter_table(
            Dialect::MySQL,
            "ALTER TABLE test ALTER COLUMN name SET DEFAULT 'default_name';",
        )
        .unwrap();
        let original_column_spec = original_table
            .fields
            .iter()
            .find(|f| f.column.name == "name")
            .unwrap();
        assert_eq!(original_column_spec.constraints.len(), 1);
        assert!(matches!(
            original_column_spec.constraints[0],
            ColumnConstraint::NotNull
        ));
        let new_table = rewrite_table_definition(&alteration, original_table.clone()).unwrap();
        assert_eq!(new_table.table, original_table.table);
        assert_eq!(new_table.keys, original_table.keys);
        assert_eq!(new_table.if_not_exists, original_table.if_not_exists);
        assert_eq!(new_table.options, original_table.options);
        let column_spec = new_table
            .fields
            .iter()
            .find(|f| f.column.name == "name")
            .unwrap();
        assert_eq!(column_spec.sql_type, SqlType::Text);
        assert_eq!(column_spec.constraints.len(), 2);
        assert!(matches!(
            original_column_spec.constraints[0],
            ColumnConstraint::NotNull
        ));
        let constraint = &column_spec.constraints[1];
        match constraint {
            ColumnConstraint::DefaultValue(ref val) => {
                assert_eq!(val.clone(), Literal::String("default_name".to_string()));
            }
            _ => panic!("expected DefaultValue"),
        }
        assert_eq!(column_spec.column.table, Some(new_table.table.name));
    }

    #[test]
    fn rename_column() {
        let original_table = create_table();
        let alteration =
            nom_sql::parse_alter_table(Dialect::MySQL, "ALTER TABLE test RENAME COLUMN id new_id;")
                .unwrap();
        assert!(!original_table
            .fields
            .iter()
            .any(|f| f.column.name == "new_id"));
        assert!(original_table.fields.iter().any(|f| f.column.name == "id"));
        let new_table = rewrite_table_definition(&alteration, original_table).unwrap();
        assert!(new_table.fields.iter().any(|f| f.column.name == "new_id"));
        assert!(!new_table.fields.iter().any(|f| f.column.name == "id"));
    }
}
