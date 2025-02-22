use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{
    ast::*, AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect,
    TryIntoDialect,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum SetStatement {
    Variable(SetVariables),
    Names(SetNames),
    PostgresParameter(SetPostgresParameter),
}

/// XXX(mvzink): We don't bother trying to produce `SetVariables` because we don't actually use it
/// anywhere. It will just get turned into a slightly botched `SetPostgresParameter` (see
/// [datafusion-sqlparser-rs#1697](https://github.com/apache/datafusion-sqlparser-rs/issues/1697)).
impl TryFromDialect<sqlparser::ast::Statement> for SetStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Statement,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        match value {
            sqlparser::ast::Statement::SetVariable {
                local,
                hivevar: _,
                variables,
                value,
            } => match dialect {
                Dialect::PostgreSQL => {
                    let name = variables
                        .into_iter()
                        .exactly_one()
                        .map_err(|_| failed_err!("Missing variable name"))?
                        .0
                        .pop()
                        .unwrap()
                        .into_dialect(dialect);
                    let value: SetPostgresParameterValue = value.try_into_dialect(dialect)?;
                    let scope = if local {
                        Some(PostgresParameterScope::Local)
                    } else {
                        None
                    };
                    Ok(Self::PostgresParameter(SetPostgresParameter {
                        scope,
                        name,
                        value,
                    }))
                }
                Dialect::MySQL => {
                    let name = variables.into_iter().exactly_one().map_err(|_| {
                        unsupported_err!("Only single variable assignment supported")
                    })?;
                    Ok(Self::Variable(SetVariables {
                        variables: vec![(
                            name.try_into()?,
                            value
                                .into_iter()
                                .exactly_one()
                                .map_err(|_| {
                                    unsupported_err!("Only single variable assignment supported")
                                })?
                                .try_into_dialect(dialect)?,
                        )],
                    }))
                }
            },
            sqlparser::ast::Statement::SetNames {
                charset_name,
                collation_name,
            } => Ok(Self::Names(SetNames {
                charset: charset_name.value,
                collation: collation_name,
            })),
            _ => todo!("unsupported set statement {value:?} (convert to TryFrom)"),
        }
    }
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

impl TryFromDialect<Vec<sqlparser::ast::Expr>> for SetPostgresParameterValue {
    fn try_from_dialect(
        value: Vec<sqlparser::ast::Expr>,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if value.len() == 1 {
            if let sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident { value, .. }) = &value[0]
            {
                if value.eq_ignore_ascii_case("DEFAULT") {
                    return Ok(Self::Default);
                }
            }
        }
        let values = value.into_iter().map(|expr| expr.try_into_dialect(dialect));
        if values.len() == 1 {
            Ok(Self::Value(PostgresParameterValue::Single(
                values.exactly_one().unwrap()?,
            )))
        } else {
            Ok(Self::Value(PostgresParameterValue::List(
                values.try_collect()?,
            )))
        }
    }
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

impl TryFromDialect<sqlparser::ast::Expr> for PostgresParameterValueInner {
    fn try_from_dialect(
        value: sqlparser::ast::Expr,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        match value {
            sqlparser::ast::Expr::Value(value) => Ok(Self::Literal(value.try_into()?)),
            sqlparser::ast::Expr::Identifier(ident) => {
                Ok(Self::Identifier(ident.into_dialect(dialect)))
            }
            _ => unsupported!("unsupported Postgres parameter value {value:?}"),
        }
    }
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

impl From<&str> for VariableScope {
    fn from(value: &str) -> Self {
        if value.eq_ignore_ascii_case("@@LOCAL") {
            Self::Local
        } else if value.eq_ignore_ascii_case("@@GLOBAL") {
            Self::Global
        } else if value.eq_ignore_ascii_case("@@SESSION") || value.eq_ignore_ascii_case("@@") {
            Self::Session
        } else if value.eq_ignore_ascii_case("@") {
            Self::User
        } else {
            panic!("unexpected variable scope {value}")
        }
    }
}

impl From<sqlparser::ast::ObjectNamePart> for VariableScope {
    fn from(value: sqlparser::ast::ObjectNamePart) -> Self {
        match value {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => ident.value.as_str().into(),
        }
    }
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

impl From<String> for Variable {
    fn from(value: String) -> Self {
        let lowered = value[..(10.min(value.len()))].to_lowercase();
        if lowered.starts_with("@@local.") {
            Self {
                scope: VariableScope::Local,
                name: value[8..].into(),
            }
        } else if lowered.starts_with("@@global.") {
            Self {
                scope: VariableScope::Global,
                name: value[9..].into(),
            }
        } else if lowered.starts_with("@@session.") {
            Self {
                scope: VariableScope::Session,
                name: value[10..].into(),
            }
        } else if lowered.starts_with("@@") {
            Self {
                scope: VariableScope::Session,
                name: value[2..].into(),
            }
        } else if lowered.starts_with("@") {
            Self {
                scope: VariableScope::User,
                name: value[1..].into(),
            }
        } else {
            Self {
                scope: VariableScope::Local,
                name: value.into(),
            }
        }
    }
}

impl From<sqlparser::ast::Ident> for Variable {
    fn from(value: sqlparser::ast::Ident) -> Self {
        value.value.into()
    }
}

impl TryFrom<Vec<sqlparser::ast::Ident>> for Variable {
    type Error = AstConversionError;

    fn try_from(mut value: Vec<sqlparser::ast::Ident>) -> Result<Self, Self::Error> {
        // XXX(mvzink): We lowercase across the board (even ignoring dialect) just to match nom-sql
        let name = value
            .pop()
            .ok_or_else(|| failed_err!("Empty variable name"))?
            .value
            .to_lowercase();
        if value.is_empty() {
            Ok(name.into())
        } else if value.len() == 1 {
            let scope = value.pop().unwrap().value.as_str().into();
            Ok(Self {
                scope,
                name: name.into(),
            })
        } else {
            failed!("Invalid variable name remainder {value:?} (name: {name:?})")
        }
    }
}

impl TryFrom<sqlparser::ast::ObjectName> for Variable {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::ObjectName) -> Result<Self, Self::Error> {
        value
            .0
            .into_iter()
            .map(|part| match part {
                sqlparser::ast::ObjectNamePart::Identifier(ident) => ident,
            })
            .collect::<Vec<_>>()
            .try_into()
    }
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
