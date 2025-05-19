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

impl TryFromDialect<sqlparser::ast::Set> for SetStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Set,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::Set;
        match value {
            Set::SingleAssignment {
                scope,
                hivevar: _,
                variable,
                values,
            } => match dialect {
                Dialect::PostgreSQL => {
                    let scope = scope.map(TryInto::try_into).transpose()?;
                    let variable = variable
                        .0
                        .into_iter()
                        .exactly_one()
                        .map_err(|_| {
                            not_yet_implemented_err!("SET with namespaced Postgres parameter names")
                        })?
                        .into_dialect(dialect);
                    let value = values.try_into_dialect(dialect)?;
                    Ok(Self::PostgresParameter(SetPostgresParameter {
                        scope,
                        name: variable,
                        value,
                    }))
                }
                Dialect::MySQL => {
                    let mut variable: Variable = variable.try_into_dialect(dialect)?;
                    if let Some(scope) = scope {
                        variable.scope = scope.into();
                    }
                    let value = values
                        .into_iter()
                        .exactly_one()
                        .map_err(|_| {
                            not_yet_implemented_err!("SET with single variable and multiple values")
                        })?
                        .try_into_dialect(dialect)?;
                    Ok(Self::Variable(SetVariables {
                        variables: vec![(variable, value)],
                    }))
                }
            },
            Set::MultipleAssignments { assignments } => {
                let variables = assignments
                    .into_iter()
                    .map(|sqlparser::ast::SetAssignment { scope, name, value }| {
                        let mut variable: Variable = name.try_into_dialect(dialect)?;
                        if let Some(scope) = scope {
                            variable.scope = scope.into();
                        }
                        let value = value.try_into_dialect(dialect)?;
                        Ok((variable, value))
                    })
                    .try_collect()?;
                Ok(Self::Variable(SetVariables { variables }))
            }
            Set::SetNames {
                charset_name,
                collation_name,
            } => Ok(Self::Names(SetNames {
                charset: charset_name.value.to_string(),
                collation: collation_name,
            })),
            Set::SetNamesDefault {} => Ok(Self::Names(SetNames {
                charset: "default".to_string(),
                collation: None,
            })),
            set => unsupported!("SET statement kind: {set}"),
        }
    }
}

impl DialectDisplay for SetStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "SET ")?;
            match self {
                Self::Variable(set) => write!(f, "{}", set.display(dialect)),
                Self::Names(set) => write!(f, "{set}"),
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

impl TryFrom<sqlparser::ast::ContextModifier> for PostgresParameterScope {
    type Error = AstConversionError;

    fn try_from(scope: sqlparser::ast::ContextModifier) -> Result<Self, Self::Error> {
        match scope {
            sqlparser::ast::ContextModifier::Session => Ok(PostgresParameterScope::Session),
            sqlparser::ast::ContextModifier::Local => Ok(PostgresParameterScope::Local),
            _ => unsupported!("Postgres only has LOCAL and SESSION scope, found {scope}"),
        }
    }
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
            PostgresParameterValueInner::Identifier(ident) => write!(f, "{ident}"),
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

impl From<sqlparser::ast::ContextModifier> for VariableScope {
    fn from(value: sqlparser::ast::ContextModifier) -> Self {
        match value {
            sqlparser::ast::ContextModifier::Session => Self::Session,
            sqlparser::ast::ContextModifier::Global => Self::Global,
            sqlparser::ast::ContextModifier::Local => Self::Local,
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

fn split_variable(value: String) -> (Option<VariableScope>, SqlIdentifier) {
    let lowered = value[..(10.min(value.len()))].to_lowercase();
    if lowered.starts_with("@@local.") {
        (Some(VariableScope::Local), value[8..].into())
    } else if lowered.starts_with("@@global.") {
        (Some(VariableScope::Global), value[9..].into())
    } else if lowered.starts_with("@@session.") {
        (Some(VariableScope::Session), value[10..].into())
    } else if lowered.starts_with("@@") {
        (Some(VariableScope::Session), value[2..].into())
    } else if lowered.starts_with("@") {
        (Some(VariableScope::User), value[1..].into())
    } else {
        (None, value.into())
    }
}

impl TryFromDialect<String> for Variable {
    fn try_from_dialect(value: String, dialect: Dialect) -> Result<Self, AstConversionError> {
        match dialect {
            Dialect::PostgreSQL => {
                failed!("Attempting to convert Postgres parameter to MySQL variable")
            }
            Dialect::MySQL => {
                let (scope, name) = split_variable(value);
                Ok(Variable {
                    scope: scope.unwrap_or(VariableScope::Local),
                    name,
                })
            }
        }
    }
}

impl TryFromDialect<sqlparser::ast::Ident> for Variable {
    fn try_from_dialect(
        value: sqlparser::ast::Ident,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        // XXX(mvzink): We lowercase across the board (even ignoring dialect) just to match nom-sql
        value.value.to_lowercase().try_into_dialect(dialect)
    }
}

impl TryFromDialect<Vec<sqlparser::ast::Ident>> for Variable {
    fn try_from_dialect(
        mut value: Vec<sqlparser::ast::Ident>,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if dialect == Dialect::PostgreSQL {
            return failed!("Attempt to convert Postgres parameter to MySQL variable");
        }
        // XXX(mvzink): We lowercase across the board (even ignoring dialect) just to match nom-sql
        let name = value
            .pop()
            .ok_or_else(|| failed_err!("Empty variable name"))?
            .value
            .to_lowercase();
        if value.is_empty() {
            Ok(name.try_into_dialect(dialect)?)
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

impl TryFromDialect<sqlparser::ast::ObjectName> for Variable {
    fn try_from_dialect(
        value: sqlparser::ast::ObjectName,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        value
            .0
            .into_iter()
            .map(|part| match part {
                sqlparser::ast::ObjectNamePart::Identifier(ident) => ident,
            })
            .collect::<Vec<_>>()
            .try_into_dialect(dialect)
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
            write!(f, " COLLATE '{collation}'")?;
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
                write!(f, "{scope} ")?;
            }
            write!(f, "{} = {}", self.name, self.value.display(dialect))
        })
    }
}
