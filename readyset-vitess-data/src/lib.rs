mod column;
mod schema_cache;
mod table;

pub use column::Column;
pub use schema_cache::SchemaCache;
pub use table::Table;
use vitess_grpc::binlogdata::RowChange;

pub enum RowOperation {
    Insert,
    Update,
    Delete,
}

// Returns a RowOperation based on the change information present in a given RowChange
pub fn row_operation(change: &RowChange) -> RowOperation {
    if change.before.is_none() {
        RowOperation::Insert
    } else if change.after.is_none() {
        RowOperation::Delete
    } else {
        RowOperation::Update
    }
}
