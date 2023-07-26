use std::collections::HashMap;

use vitess_grpc::binlogdata::FieldEvent;

use crate::Table;

pub struct SchemaCache {
    pub keyspace: String,
    pub tables: HashMap<String, Table>,
}

impl SchemaCache {
    pub fn new(keyspace: &str) -> Self {
        Self {
            keyspace: keyspace.to_string(),
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    pub fn process_field_event(&mut self, field_event: &FieldEvent) {
        let table_name = &field_event.table_name;
        let table_name_parts: Vec<&str> = table_name.split('.').collect();
        if table_name_parts.len() != 2 {
            panic!("Invalid table name: {}", table_name);
        }

        let keyspace = table_name_parts[0];
        let table_name = table_name_parts[1];
        if keyspace != self.keyspace {
            panic!(
                "Invalid keyspace: {}, expected: {}",
                keyspace, self.keyspace
            );
        }

        let mut table = Table::new(keyspace, table_name);
        table.set_columns(&field_event.fields);

        self.add_table(table);
    }
}
