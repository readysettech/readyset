use anyhow::Result;
use vitess_grpc::binlogdata::FieldEvent;
use vitess_grpc::query::{Row, Type};

use crate::field_parsing::vstream_value_to_noria_value;
use crate::NoriaRow;

pub fn vstream_row_to_noria_row(row: &Row, field_event: &FieldEvent) -> Result<NoriaRow> {
    let field_count = field_event.fields.len();
    let mut noria_row = Vec::with_capacity(field_count);
    let mut field_start = 0;

    for field_idx in 0..field_count {
        let field = &field_event.fields[field_idx];
        if let Some(field_type) = Type::from_i32(field.r#type) {
            let len = row.lengths[field_idx] as usize;
            let raw_value = &row.values[field_start..field_start + len];
            // TODO: Pass a reference to the list of enum values pre-calculated upfront
            let value =
                vstream_value_to_noria_value(raw_value, field_type, Some(&field.column_type))?;
            noria_row.push(value);
            field_start += len;
        } else {
            return Err(anyhow::anyhow!(
                "Unknown field type value: {}",
                field_event.fields[field_idx].r#type
            ));
        }
    }

    return Ok(noria_row);
}
