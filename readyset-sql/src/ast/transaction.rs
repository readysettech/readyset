use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::ast::SqlIdentifier;
use crate::{Dialect, DialectDisplay};

// TODO(peter): Handle dialect differences.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum StartTransactionStatement {
    // TODO(ENG-2992): Implement optional fields for START TRANSACTION
    Start,
    Begin,
}

impl fmt::Display for StartTransactionStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StartTransactionStatement::Start => write!(f, "START TRANSACTION"),
            StartTransactionStatement::Begin => write!(f, "BEGIN"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CommitStatement;

impl fmt::Display for CommitStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "COMMIT")
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct RollbackStatement {
    /// If present, this is a ROLLBACK TO SAVEPOINT statement, which does NOT end the transaction.
    pub savepoint: Option<SqlIdentifier>,
}

impl RollbackStatement {
    /// Returns true if this rollback statement ends the transaction.
    /// A plain ROLLBACK ends the transaction, but ROLLBACK TO SAVEPOINT does not.
    pub fn ends_transaction(&self) -> bool {
        self.savepoint.is_none()
    }
}

impl DialectDisplay for RollbackStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "ROLLBACK")?;
            if let Some(savepoint) = &self.savepoint {
                write!(f, " TO SAVEPOINT {}", dialect.quote_identifier(savepoint))?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollback_display_quotes_identifiers() {
        // Simple savepoint name
        let stmt = RollbackStatement {
            savepoint: Some("sp1".into()),
        };
        assert_eq!(
            stmt.display(Dialect::MySQL).to_string(),
            "ROLLBACK TO SAVEPOINT `sp1`"
        );
        assert_eq!(
            stmt.display(Dialect::PostgreSQL).to_string(),
            r#"ROLLBACK TO SAVEPOINT "sp1""#
        );

        // Savepoint name with special characters
        let stmt = RollbackStatement {
            savepoint: Some("my-savepoint".into()),
        };
        assert_eq!(
            stmt.display(Dialect::MySQL).to_string(),
            "ROLLBACK TO SAVEPOINT `my-savepoint`"
        );
        assert_eq!(
            stmt.display(Dialect::PostgreSQL).to_string(),
            r#"ROLLBACK TO SAVEPOINT "my-savepoint""#
        );

        // Reserved keyword as savepoint name
        let stmt = RollbackStatement {
            savepoint: Some("select".into()),
        };
        assert_eq!(
            stmt.display(Dialect::MySQL).to_string(),
            "ROLLBACK TO SAVEPOINT `select`"
        );
        assert_eq!(
            stmt.display(Dialect::PostgreSQL).to_string(),
            r#"ROLLBACK TO SAVEPOINT "select""#
        );

        // No savepoint (plain ROLLBACK)
        let stmt = RollbackStatement { savepoint: None };
        assert_eq!(stmt.display(Dialect::MySQL).to_string(), "ROLLBACK");
        assert_eq!(stmt.display(Dialect::PostgreSQL).to_string(), "ROLLBACK");
    }
}
