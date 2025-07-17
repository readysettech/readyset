use std::fmt;

use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::ast::*;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Arbitrary)]
pub enum CreateTableOption {
    AutoIncrement(u64),
    Engine(Option<String>),
    Charset(CharsetName),
    Collate(CollationName),
    Comment(String),
    DataDirectory(String),
    Other { key: String, value: String },
}

impl fmt::Display for CreateTableOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateTableOption::AutoIncrement(v) => write!(f, "AUTO_INCREMENT={v}"),
            CreateTableOption::Engine(e) => {
                write!(f, "ENGINE={}", e.as_deref().unwrap_or(""))
            }
            CreateTableOption::Charset(c) => write!(f, "DEFAULT CHARSET={c}"),
            CreateTableOption::Collate(c) => write!(f, "COLLATE={c}"),
            CreateTableOption::Comment(c) => write!(f, "COMMENT='{c}'"),
            CreateTableOption::DataDirectory(d) => write!(f, "DATA DIRECTORY='{d}'"),
            CreateTableOption::Other { key, value } => write!(f, "{key}={value}"),
        }
    }
}
