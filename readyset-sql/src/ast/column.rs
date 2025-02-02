use std::{cmp::Ordering, fmt};

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct Column {
    pub name: SqlIdentifier,
    pub table: Option<Relation>,
}

impl From<SqlIdentifier> for Column {
    fn from(name: SqlIdentifier) -> Self {
        Column { name, table: None }
    }
}

impl From<&'_ str> for Column {
    fn from(c: &str) -> Column {
        match c.split_once('.') {
            None => Column {
                name: c.into(),
                table: None,
            },
            Some((table_name, col_name)) => Column {
                name: col_name.into(),
                table: Some(table_name.into()),
            },
        }
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Column) -> Ordering {
        match (self.table.as_ref(), other.table.as_ref()) {
            (Some(s), Some(o)) => (s, &self.name).cmp(&(o, &other.name)),
            _ => self.name.cmp(&other.name),
        }
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Column) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl DialectDisplay for Column {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            if let Some(ref table) = self.table {
                write!(f, "{}.", table.display(dialect))?;
            }
            write!(f, "{}", dialect.quote_identifier(&self.name))
        })
    }
}

impl Column {
    /// Like [`display()`](Self::display) except the schema, table, and column name will not be
    /// quoted.
    ///
    /// This should not be used to emit SQL code and instead should mostly be for error messages.
    pub fn display_unquoted(&self) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            if let Some(ref table) = self.table {
                write!(f, "{}.", table.display_unquoted())?;
            }
            write!(f, "{}", self.name)
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ColumnConstraint {
    Null,
    NotNull,
    CharacterSet(String),
    Collation(String),
    DefaultValue(Expr),
    AutoIncrement,
    PrimaryKey,
    Unique,
    /// NOTE(aspen): Yes, this really is its own special thing, not just an expression - see
    /// <https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html>
    OnUpdateCurrentTimestamp(Option<Literal>),
}

impl DialectDisplay for ColumnConstraint {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Null => write!(f, "NULL"),
            Self::NotNull => write!(f, "NOT NULL"),
            Self::CharacterSet(charset) => write!(f, "CHARACTER SET {}", charset),
            Self::Collation(collation) => write!(f, "COLLATE {}", collation),
            Self::DefaultValue(expr) => write!(f, "DEFAULT {}", expr.display(dialect)),
            Self::AutoIncrement => write!(f, "AUTO_INCREMENT"),
            Self::PrimaryKey => write!(f, "PRIMARY KEY"),
            Self::Unique => write!(f, "UNIQUE"),
            Self::OnUpdateCurrentTimestamp(opt) => {
                write!(f, "ON UPDATE CURRENT_TIMESTAMP")?;
                if let Some(lit) = opt {
                    write!(f, "({})", lit.display(dialect))?;
                }
                Ok(())
            }
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct ColumnSpecification {
    pub column: Column,
    pub sql_type: SqlType,
    pub generated: Option<GeneratedColumn>,
    pub constraints: Vec<ColumnConstraint>,
    pub comment: Option<String>,
}

impl ColumnSpecification {
    pub fn new(column: Column, sql_type: SqlType) -> ColumnSpecification {
        ColumnSpecification {
            column,
            sql_type,
            generated: None,
            constraints: vec![],
            comment: None,
        }
    }

    pub fn with_constraints(
        column: Column,
        sql_type: SqlType,
        constraints: Vec<ColumnConstraint>,
    ) -> ColumnSpecification {
        ColumnSpecification {
            column,
            sql_type,
            generated: None,
            constraints,
            comment: None,
        }
    }

    pub fn has_default(&self) -> Option<&Literal> {
        self.constraints.iter().find_map(|c| match c {
            ColumnConstraint::DefaultValue(Expr::Literal(ref l)) => Some(l),
            _ => None,
        })
    }

    /// Returns true if the column is not nullable
    pub fn is_not_null(&self) -> bool {
        self.constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::NotNull))
    }

    /// Returns the character set for the column, if one is set.
    pub fn get_charset(&self) -> Option<&str> {
        // Character set is a constraint in Text fields only
        if !self.sql_type.is_any_text() {
            return None;
        }
        self.constraints.iter().find_map(|c| match c {
            ColumnConstraint::CharacterSet(ref charset) => Some(charset.as_str()),
            _ => None,
        })
    }

    /// Returns the collation for the column, if one is set.
    pub fn get_collation(&self) -> Option<&str> {
        // Collation is a constraint in Text fields only
        if !self.sql_type.is_any_text() {
            return None;
        }
        self.constraints.iter().find_map(|c| match c {
            ColumnConstraint::Collation(ref collation) => Some(collation.as_str()),
            _ => None,
        })
    }
}

impl DialectDisplay for ColumnSpecification {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "{} {}",
                dialect.quote_identifier(&self.column.name),
                self.sql_type.display(dialect)
            )?;

            for constraint in &self.constraints {
                write!(f, " {}", constraint.display(dialect))?;
            }

            if let Some(ref comment) = self.comment {
                write!(f, " COMMENT '{}'", comment)?;
            }

            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct GeneratedColumn {
    pub expr: Expr,
    pub stored: bool,
}

impl fmt::Display for GeneratedColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GENERATED ALWAYS AS ({}) {} ",
            self.expr.display(Dialect::MySQL),
            if self.stored { "STORED" } else { "VIRTUAL" }
        )
    }
}
