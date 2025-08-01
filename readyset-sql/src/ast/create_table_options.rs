use std::fmt;

use proptest::{option, prelude::any_with};
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::ast::*;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Arbitrary)]
pub enum CreateTableOption {
    AutoIncrement(u64),
    Engine(
        #[strategy(option::of(any_with::<String>(r#"[a-zA-Z0-9_]{1,16}"#.into())))] Option<String>,
    ),
    Charset(CollationName),
    Collate(CollationName),
    Comment(String),
    DataDirectory(String),
    Other {
        #[any("[a-zA-Z0-9_]+".into())]
        key: String,
        #[any("[a-zA-Z0-9_]+".into())]
        value: String,
    },
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
