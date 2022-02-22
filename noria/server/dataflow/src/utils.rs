use noria_data::noria_type::Type;

use crate::node::Column;

/// Helper to create a dataflow column
pub fn dataflow_column(name: &str) -> Column {
    Column::new(name.into(), Type::Unknown, None)
}

/// Helper to use in calls to add_base/ingredient
pub fn make_columns(names: &[&str]) -> Vec<Column> {
    let mut res = Vec::with_capacity(names.len());
    for name in names {
        res.push(dataflow_column(*name));
    }
    res
}
