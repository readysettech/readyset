mod backend;
mod frontend;

pub use backend::*;
pub use frontend::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferFormat {
    Binary,
    Text,
}

impl From<TransferFormat> for i16 {
    fn from(value: TransferFormat) -> Self {
        match value {
            TransferFormat::Text => 0,
            TransferFormat::Binary => 1,
        }
    }
}
