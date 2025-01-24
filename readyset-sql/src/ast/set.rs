use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum SetStatement {
    Variable(SetVariables),
    Names(SetNames),
    PostgresParameter(SetPostgresParameter),
}

impl DialectDisplay for SetStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "SET ")?;
            match self {
                Self::Variable(set) => write!(f, "{}", set.display(dialect)),
                Self::Names(set) => write!(f, "{}", set),
                Self::PostgresParameter(set) => write!(f, "{}", set.display(dialect)),
            }
        })
    }
}

impl SetStatement {
    pub fn variables(&self) -> Option<&[(Variable, Expr)]> {
        match self {
            SetStatement::Names(_) | SetStatement::PostgresParameter { .. } => None,
            SetStatement::Variable(set) => Some(&set.variables),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum PostgresParameterScope {
    Session,
    Local,
}

impl fmt::Display for PostgresParameterScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresParameterScope::Session => write!(f, "SESSION"),
            PostgresParameterScope::Local => write!(f, "LOCAL"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum SetPostgresParameterValue {
    Default,
    Value(PostgresParameterValue),
}

impl DialectDisplay for SetPostgresParameterValue {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            SetPostgresParameterValue::Default => write!(f, "DEFAULT"),
            SetPostgresParameterValue::Value(val) => write!(f, "{}", val.display(dialect)),
        })
    }
}

/// A *single* value which can be used as the value for a postgres parameter
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum PostgresParameterValueInner {
    Identifier(SqlIdentifier),
    Literal(Literal),
}

impl DialectDisplay for PostgresParameterValueInner {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            PostgresParameterValueInner::Identifier(ident) => write!(f, "{}", ident),
            PostgresParameterValueInner::Literal(lit) => write!(f, "{}", lit.display(dialect)),
        })
    }
}

/// The value for a postgres parameter, which can either be an identifier, a literal, or a list of
/// those
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum PostgresParameterValue {
    Single(PostgresParameterValueInner),
    List(Vec<PostgresParameterValueInner>),
}

impl PostgresParameterValue {
    /// Construct a [`PostgresParameterValue`] for a single literal
    pub fn literal<L>(literal: L) -> Self
    where
        Literal: From<L>,
    {
        Self::Single(PostgresParameterValueInner::Literal(literal.into()))
    }

    /// Construct a [`PostgresParameterValue`] for a single identifier
    pub fn identifier<I>(ident: I) -> Self
    where
        SqlIdentifier: From<I>,
    {
        Self::Single(PostgresParameterValueInner::Identifier(ident.into()))
    }

    /// Construct a [`PostgresParameterValue`] from a list of literal values
    pub fn list<L, I>(vals: L) -> Self
    where
        L: IntoIterator<Item = I>,
        Literal: From<I>,
    {
        Self::List(
            vals.into_iter()
                .map(|v| PostgresParameterValueInner::Literal(v.into()))
                .collect(),
        )
    }
}

impl DialectDisplay for PostgresParameterValue {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            PostgresParameterValue::Single(v) => write!(f, "{}", v.display(dialect)),
            PostgresParameterValue::List(l) => {
                for (i, v) in l.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v.display(dialect))?;
                }
                Ok(())
            }
        })
    }
}

/// Scope for a [`Variable`]
#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub enum VariableScope {
    User,
    Local,
    Global,
    Session,
}

impl fmt::Display for VariableScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VariableScope::User => Ok(()),
            VariableScope::Local => write!(f, "LOCAL"),
            VariableScope::Global => write!(f, "GLOBAL"),
            VariableScope::Session => write!(f, "SESSION"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Variable {
    pub scope: VariableScope,
    pub name: SqlIdentifier,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct SetVariables {
    /// A list of variables and their assigned values
    pub variables: Vec<(Variable, Expr)>,
}

impl Variable {
    /// If the variable is one of Local, Global or Session, returns the variable name
    pub fn as_non_user_var(&self) -> Option<&str> {
        if self.scope == VariableScope::User {
            None
        } else {
            Some(&self.name)
        }
    }

    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            match dialect {
                Dialect::PostgreSQL => {
                    // Postgres doesn't have variable scope
                }
                Dialect::MySQL => {
                    if self.scope == VariableScope::User {
                        write!(f, "@")?;
                    } else {
                        write!(f, "@@{}.", self.scope)?;
                    }
                }
            }
            write!(f, "{}", self.name)
        })
    }
}

impl DialectDisplay for SetVariables {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "{}",
                self.variables
                    .iter()
                    .map(|(var, value)| format!(
                        "{} = {}",
                        var.display(dialect),
                        value.display(dialect)
                    ))
                    .join(", ")
            )
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct SetNames {
    pub charset: String,
    pub collation: Option<String>,
}

impl fmt::Display for SetNames {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NAMES '{}'", &self.charset)?;
        if let Some(collation) = self.collation.as_ref() {
            write!(f, " COLLATE '{}'", collation)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct SetPostgresParameter {
    pub scope: Option<PostgresParameterScope>,
    pub name: SqlIdentifier,
    pub value: SetPostgresParameterValue,
}

impl DialectDisplay for SetPostgresParameter {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            if let Some(scope) = self.scope {
                write!(f, "{} ", scope)?;
            }
            write!(f, "{} = {}", self.name, self.value.display(dialect))
        })
    }
}
