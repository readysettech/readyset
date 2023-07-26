use anyhow::Result;
use readyset_data::DfValue;
use vitess_grpc::query::Row;

use crate::vitess::Table;
use crate::vstream_value_to_noria_value;

type NoriaRow = Vec<DfValue>;

pub fn vstream_row_to_noria_row(row: &Row, table: &Table) -> Result<NoriaRow> {
    let field_count = table.columns.len();
    let mut noria_row = Vec::with_capacity(field_count);
    let mut field_start = 0;

    for field_idx in 0..field_count {
        let column = &table.columns[field_idx];
        let len = row.lengths[field_idx] as i32;
        if len < 1 {
            noria_row.push(DfValue::None);
            continue;
        }

        println!("field_start: {}, len: {}", field_start, len);
        let raw_value = &row.values[field_start..field_start + len as usize];
        // TODO: Pass a reference to the list of enum values pre-calculated upfront
        let value =
            vstream_value_to_noria_value(raw_value, column.grpc_type, Some(&column.column_type))?;
        noria_row.push(value);
        field_start += len as usize;
    }

    return Ok(noria_row);
}
