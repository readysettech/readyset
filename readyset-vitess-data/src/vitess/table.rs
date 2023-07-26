use vitess_grpc::query::Field;

use crate::vitess::Column;

pub struct Table {
    pub keyspace: String,
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(keyspace: &str, name: &str) -> Self {
        Self {
            keyspace: keyspace.to_string(),
            name: name.to_string(),
            columns: vec![],
        }
    }

    pub fn set_columns(&mut self, fields: &Vec<Field>) {
        self.columns = fields.iter().map(|field| field.into()).collect();
    }
}
