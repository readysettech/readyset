/// This module contains types to be shared across adapter-related crates,
/// such as `readyset-adapter`, `readyset-{mysql|psql}`,
/// and `{mysql|psql}-srv`.
use std::fmt::{Display, Formatter, Result};

/// An identifier for a single prepared statement that is to be deallocated, or
/// all prepared statements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeallocateId {
    /// A simple numeric id for a single statement.
    Numeric(u32),
    /// An alphanumeric name for a statement.
    Named(String),
    /// Apply to all statements.
    All,
}

impl From<String> for DeallocateId {
    fn from(s: String) -> Self {
        if let Ok(i) = s.parse::<u32>() {
            DeallocateId::Numeric(i)
        } else {
            DeallocateId::Named(s)
        }
    }
}

impl From<Option<String>> for DeallocateId {
    fn from(t: Option<String>) -> Self {
        match t {
            Some(id) => {
                if let Ok(i) = id.parse::<u32>() {
                    DeallocateId::Numeric(i)
                } else {
                    DeallocateId::Named(id)
                }
            }
            None => DeallocateId::All,
        }
    }
}

impl Display for DeallocateId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            DeallocateId::Numeric(id) => write!(f, "{}", id),
            DeallocateId::Named(name) => write!(f, "{}", name),
            DeallocateId::All => write!(f, "all"),
        }
    }
}

/// Indicates that after parsing a query, a lower/outer level behavior needs to execute the command.
#[derive(Clone, Debug)]
pub enum ParsedCommand {
    /// A `DEALLOCATE (PREPARE)` SQL command was invoked.
    Deallocate(DeallocateId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedStatementType {
    Named,
    Unnamed,
}
