use crate::value::Value;
use noria::DataType;
use psql_srv as ps;
use std::sync::Arc;

pub struct Row {
    pub col_types: Arc<Vec<ps::ColType>>,
    pub values: Vec<DataType>,
    pub project_fields: Arc<Vec<usize>>,
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = RowIterator;

    fn into_iter(self) -> Self::IntoIter {
        RowIterator { row: self, pos: 0 }
    }
}

pub struct RowIterator {
    row: Row,
    pos: usize,
}

impl Iterator for RowIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        let i = *self.row.project_fields.get(self.pos)?;
        let col_type = self.row.col_types.get(i)?.clone();
        let value = self.row.values.get(i)?.clone();
        self.pos += 1;
        Some(Value { col_type, value })
    }
}
