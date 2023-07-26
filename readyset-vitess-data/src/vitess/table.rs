use anyhow::Result;
use readyset_data::DfValue;
use vitess_grpc::query::{Field, Row};

use crate::vitess::Column;
use crate::vstream_value_to_noria_value;

type NoriaRow = Vec<DfValue>;

pub struct Table {
    pub keyspace: String,
    pub name: String,
    columns: Vec<Column>,
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

    pub fn columns_count(&self) -> usize {
        self.columns.len()
    }

    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub fn vstream_row_to_noria_row(&self, row: &Row) -> Result<NoriaRow> {
        let field_count = self.columns.len();
        let mut noria_row = Vec::with_capacity(field_count);
        let mut field_start = 0;

        for field_idx in 0..field_count {
            let column = &self.columns[field_idx];
            let len = row.lengths[field_idx] as i32;
            if len < 1 {
                noria_row.push(DfValue::None);
                continue;
            }

            println!("field_start: {}, len: {}", field_start, len);
            let raw_value = &row.values[field_start..field_start + len as usize];
            // TODO: Pass a reference to the list of enum values pre-calculated upfront
            let value = vstream_value_to_noria_value(
                raw_value,
                column.grpc_type,
                Some(&column.column_type),
            )?;
            noria_row.push(value);
            field_start += len as usize;
        }

        return Ok(noria_row);
    }
}
