mod backend;
mod frontend;

pub use backend::*;
pub use frontend::*;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TransferFormat {
    Binary,
    Text,
}
