use std::{cmp::Ordering, fmt};

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use sqlparser::ast::ObjectName;
use test_strategy::Arbitrary;

use crate::{
    ast::*, AstConversionError, Dialect, DialectDisplay, FromDialect, IntoDialect, TryFromDialect,
    TryIntoDialect,
};

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

impl FromDialect<sqlparser::ast::Ident> for Column {
    fn from_dialect(value: sqlparser::ast::Ident, dialect: Dialect) -> Self {
        Self {
            name: value.into_dialect(dialect),
            table: None,
        }
    }
}

impl FromDialect<Vec<sqlparser::ast::Ident>> for Column {
    fn from_dialect(mut value: Vec<sqlparser::ast::Ident>, dialect: Dialect) -> Self {
        let name: SqlIdentifier = value.pop().unwrap().into_dialect(dialect);
        let table = if let Some(table) = value.pop() {
            if let Some(schema) = value.pop() {
                Some(Relation {
                    schema: Some(schema.into_dialect(dialect)),
                    name: table.into_dialect(dialect),
                })
            } else {
                Some(Relation {
                    schema: None,
                    name: table.into_dialect(dialect),
                })
            }
        } else {
            None
        };
        Self { name, table }
    }
}

impl FromDialect<sqlparser::ast::ObjectName> for Column {
    fn from_dialect(value: sqlparser::ast::ObjectName, dialect: Dialect) -> Self {
        value
            .0
            .into_iter()
            .map(|sqlparser::ast::ObjectNamePart::Identifier(ident)| ident)
            .collect::<Vec<_>>()
            .into_dialect(dialect)
    }
}

impl FromDialect<sqlparser::ast::ViewColumnDef> for Column {
    fn from_dialect(value: sqlparser::ast::ViewColumnDef, dialect: Dialect) -> Self {
        Self {
            name: value.name.into_dialect(dialect),
            table: None,
        }
    }
}

impl TryFromDialect<Vec<sqlparser::ast::Assignment>> for Vec<(Column, Expr)> {
    fn try_from_dialect(
        assignments: Vec<sqlparser::ast::Assignment>,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let mut result = Vec::with_capacity(assignments.len());

        for assignment in assignments {
            match assignment.target {
                sqlparser::ast::AssignmentTarget::ColumnName(object_name) => {
                    let column = object_name.into_dialect(dialect);
                    let value = assignment.value.try_into_dialect(dialect)?;
                    result.push((column, value));
                }
                sqlparser::ast::AssignmentTarget::Tuple(v1) => match assignment.value {
                    sqlparser::ast::Expr::Tuple(v2) => {
                        if v1.len() != v2.len() {
                            unsupported!(
                                "Tuple assignment with mismatched element counts ({} elements with {} values)",
                                v1.len(),
                                v2.len()
                            )?;
                        }

                        for (target, value) in v1.into_iter().zip(v2) {
                            let column = target.into_dialect(dialect);
                            let expr = value.try_into_dialect(dialect)?;
                            result.push((column, expr));
                        }
                    }
                    _ => unsupported!("Tuple assignment with non-tuple value")?,
                },
            }
        }

        Ok(result)
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
            Self::CharacterSet(charset) => write!(f, "CHARACTER SET {charset}"),
            Self::Collation(collation) => write!(f, "COLLATE {collation}"),
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

impl TryFromDialect<sqlparser::ast::ColumnDef> for ColumnSpecification {
    fn try_from_dialect(
        value: sqlparser::ast::ColumnDef,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::{
            keywords::Keyword,
            tokenizer::{Token, Word},
        };

        let mut comment = None;
        let mut constraints = vec![];
        let mut generated = None;
        for option in value.options {
            match option.option {
                sqlparser::ast::ColumnOption::Null => constraints.push(ColumnConstraint::Null),
                sqlparser::ast::ColumnOption::NotNull => {
                    constraints.push(ColumnConstraint::NotNull)
                }
                sqlparser::ast::ColumnOption::Default(expr) => constraints.push(
                    ColumnConstraint::DefaultValue(expr.try_into_dialect(dialect)?),
                ),
                sqlparser::ast::ColumnOption::Unique {
                    is_primary,
                    characteristics,
                } => {
                    if characteristics.is_some() {
                        return not_yet_implemented!("constraint timing on column definitions");
                    } else if is_primary {
                        constraints.push(ColumnConstraint::PrimaryKey)
                    } else {
                        constraints.push(ColumnConstraint::Unique)
                    }
                }
                sqlparser::ast::ColumnOption::ForeignKey { .. } => {
                    return not_yet_implemented!("foreign key");
                }
                sqlparser::ast::ColumnOption::DialectSpecific(vec) => {
                    if vec.iter().any(|token| {
                        matches!(
                            token,
                            Token::Word(Word {
                                keyword: Keyword::AUTO_INCREMENT,
                                ..
                            })
                        )
                    }) {
                        constraints.push(ColumnConstraint::AutoIncrement)
                    }
                }
                sqlparser::ast::ColumnOption::CharacterSet(ObjectName(object_name)) => {
                    // strip the quoting style from the charset
                    let value = object_name
                        .iter()
                        .map(|s| s.as_ident().unwrap().value.clone())
                        .join(".");
                    constraints.push(ColumnConstraint::CharacterSet(value))
                }
                sqlparser::ast::ColumnOption::Collation(ObjectName(object_name)) => {
                    // strip the quoting style from the collation
                    let value = object_name
                        .iter()
                        .map(|s| s.as_ident().unwrap().value.clone())
                        .join(".");
                    constraints.push(ColumnConstraint::Collation(value))
                }
                sqlparser::ast::ColumnOption::Comment(s) => {
                    comment = Some(s);
                }
                sqlparser::ast::ColumnOption::OnUpdate(expr) => {
                    use sqlparser::ast::{
                        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
                        FunctionArguments,
                    };
                    if let sqlparser::ast::Expr::Function(Function { name, args, .. }) = expr {
                        if name.to_string().eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
                            let arg = match args {
                                FunctionArguments::None => None,
                                FunctionArguments::Subquery(_query) => {
                                    return failed!("subquery argument to CURRENT_TIMESTAMP")
                                }
                                FunctionArguments::List(FunctionArgumentList { args, .. }) => {
                                    match args.into_iter().exactly_one().map_err(|_| {
                                        failed_err!(
                                            "Expected exactly one argument to CURRENT_TIMESTAMP()"
                                        )
                                    })? {
                                        FunctionArg::Named { arg, .. }
                                        | FunctionArg::ExprNamed { arg, .. }
                                        | FunctionArg::Unnamed(arg) => Some(arg),
                                    }
                                }
                            };
                            let value: Option<Expr> = arg
                                .map(|arg| match arg {
                                    FunctionArgExpr::Expr(expr) => expr.try_into_dialect(dialect),
                                    FunctionArgExpr::QualifiedWildcard(_)
                                    | FunctionArgExpr::Wildcard => {
                                        failed!("Unexpected wildcard arg to CURRENT_TIMESTAMP()")
                                    }
                                })
                                .transpose()?;
                            let literal = value
                                .map(|expr| match expr {
                                    Expr::Literal(literal) => Ok(literal),
                                    _ => failed!("Unexpected expression in CURRENT_TIMESTAMP()"),
                                })
                                .transpose()?;
                            constraints.push(ColumnConstraint::OnUpdateCurrentTimestamp(literal));
                        }
                    }
                }
                sqlparser::ast::ColumnOption::Generated {
                    generated_as: _,
                    sequence_options: _,
                    generation_expr,
                    generation_expr_mode,
                    generated_keyword: _,
                } => {
                    generated = Some(GeneratedColumn {
                        expr: generation_expr
                            .expect("generated expr can't be None")
                            .try_into_dialect(dialect)?,
                        stored: generation_expr_mode
                            == Some(sqlparser::ast::GeneratedExpressionMode::Stored),
                    })
                }
                sqlparser::ast::ColumnOption::Materialized(_)
                | sqlparser::ast::ColumnOption::Ephemeral(_)
                | sqlparser::ast::ColumnOption::Alias(_)
                | sqlparser::ast::ColumnOption::Check(_)
                | sqlparser::ast::ColumnOption::Options(_)
                | sqlparser::ast::ColumnOption::Identity(_)
                | sqlparser::ast::ColumnOption::OnConflict(_)
                | sqlparser::ast::ColumnOption::Policy(_)
                | sqlparser::ast::ColumnOption::Srid(_)
                | sqlparser::ast::ColumnOption::Tags(_) => {
                    // Don't care about these options
                }
            }
        }
        Ok(Self {
            column: value.name.into_dialect(dialect),
            sql_type: value.data_type.try_into_dialect(dialect)?,
            constraints,
            comment,
            generated,
        })
    }
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

    pub fn get_default_value(&self) -> Option<&Literal> {
        self.constraints.iter().find_map(|c| match c {
            ColumnConstraint::DefaultValue(Expr::Literal(ref l)) => Some(l),
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
                write!(f, " COMMENT '{comment}'")?;
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
