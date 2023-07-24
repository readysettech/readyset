pub mod field_parsing;
pub mod row_parsing;

use readyset_data::DfValue;
use vitess_grpc::binlogdata::RowChange;

pub enum ChangeType {
    Insert,
    Update,
    Delete,
}

// Alias noria row
type NoriaRow = Vec<DfValue>;

impl ChangeType {
    pub fn from(change: &RowChange) -> Self {
        if change.before.is_none() && change.after.is_some() {
            Self::Insert
        } else if change.before.is_some() && change.after.is_some() {
            Self::Update
        } else if change.before.is_some() && change.after.is_none() {
            Self::Delete
        } else {
            panic!("Invalid RowChange: {:?}", change);
        }
    }
}
