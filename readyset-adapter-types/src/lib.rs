/// This module contains types to be shared across adapter-related crates,
/// such as `readyset-adapter`, `readyset-{mysql|psql}`,
/// and `{mysql|psql}-srv`.
use std::fmt::{Display, Formatter, Result};

/// Unique identifier for a prepared statement, local to a single [`readyset_adapter::Backend`] instance.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum StatementId {
    /// A prepared statement with a numeric identifier.
    Named(u32),
    /// A special variant for identifying prepared statements that are unnamed.
    /// postgres allows unnamed prepared statements over it's extended query protocol.
    Unnamed,
}

impl From<usize> for StatementId {
    fn from(id: usize) -> Self {
        StatementId::Named(id as u32)
    }
}

impl From<u32> for StatementId {
    fn from(id: u32) -> Self {
        StatementId::Named(id)
    }
}

impl From<i32> for StatementId {
    fn from(id: i32) -> Self {
        StatementId::Named(id as u32)
    }
}

impl Display for StatementId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            StatementId::Named(id) => write!(f, "{}", id),
            StatementId::Unnamed => write!(f, "unnamed"),
        }
    }
}

/// An identifier for a single prepared statement that is to be deallocated, or
/// all prepared statements.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DeallocateId {
    /// A simple numeric id for a single statement.
    Numeric(StatementId),
    /// An alphanumeric name for a statement.
    Named(String),
    /// Apply to all statements.
    All,
}

impl From<u32> for DeallocateId {
    fn from(id: u32) -> Self {
        DeallocateId::Numeric(StatementId::Named(id))
    }
}

impl From<String> for DeallocateId {
    fn from(s: String) -> Self {
        if let Ok(i) = s.parse::<u32>() {
            DeallocateId::Numeric(StatementId::Named(i))
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
                    DeallocateId::Numeric(StatementId::Named(i))
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
    Named,   // 'reused' ??
    Unnamed, // 'parameterized query' ??
}
