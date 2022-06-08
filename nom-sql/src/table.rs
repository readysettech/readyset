use std::cmp::Ordering;
use std::hash::Hash;
use std::{fmt, str};

use serde::{Deserialize, Serialize};

use crate::SqlIdentifier;

#[derive(Clone, Debug, Default, Eq, Serialize, Deserialize)]
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

// `schema` is ignored for now as we do not support multiple DBs/namespaces
impl Hash for Table {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

// `schema` is ignored for now as we do not support multiple DBs/namespaces
impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

// Ordering ignores `alias`
// NOTE: this implementation violates the transitive property. The implementation is correct so long
// as all Tables either refer to the same schema or no schema at all. This should be treated
// carefully, and should be fixed if we want to handle multiple schemas.
impl Ord for Table {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.schema.as_ref(), other.schema.as_ref()) {
            (Some(s), Some(o)) => (s, &self.name).cmp(&(o, &other.name)),
            _ => self.name.cmp(&other.name),
        }
    }
}

impl PartialOrd for Table {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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
