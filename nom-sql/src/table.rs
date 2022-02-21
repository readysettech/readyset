use std::{fmt, str};

use serde::{Deserialize, Serialize};

use crate::SqlIdentifier;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: SqlIdentifier,
    pub alias: Option<SqlIdentifier>,
    pub schema: Option<SqlIdentifier>,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref schema) = self.schema {
            write!(f, "`{}`.", schema)?;
        }
        write!(f, "`{}`", self.name)?;
        if let Some(ref alias) = self.alias {
            write!(f, " AS `{}`", alias)?;
        }
        Ok(())
    }
}

impl From<SqlIdentifier> for Table {
    fn from(name: SqlIdentifier) -> Self {
        Table {
            name,
            alias: None,
            schema: None,
        }
    }
}

impl From<&SqlIdentifier> for Table {
    fn from(name: &SqlIdentifier) -> Self {
        Table {
            name: name.clone(),
            alias: None,
            schema: None,
        }
    }
}

impl<'a> From<&'a str> for Table {
    fn from(t: &str) -> Table {
        Table {
            name: t.into(),
            alias: None,
            schema: None,
        }
    }
}
impl<'a> From<(&'a str, &'a str)> for Table {
    fn from(t: (&str, &str)) -> Table {
        Table {
            name: t.1.into(),
            alias: None,
            schema: Some(t.0.into()),
        }
    }
}
