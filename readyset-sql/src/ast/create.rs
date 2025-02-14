use std::fmt;

use derive_more::From;
use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{
    ast::*, AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect,
    TryIntoDialect,
};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Arbitrary)]
pub enum CharsetName {
    Quoted(SqlIdentifier),
    Unquoted(SqlIdentifier),
}

impl fmt::Display for CharsetName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CharsetName::Quoted(i) | CharsetName::Unquoted(i) => write!(f, "{i}"),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Hash, Clone, Serialize, Deserialize, Arbitrary)]
pub enum CollationName {
    Quoted(SqlIdentifier),
    Unquoted(SqlIdentifier),
}

impl fmt::Display for CollationName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CollationName::Quoted(i) | CollationName::Unquoted(i) => write!(f, "{i}"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum CreateDatabaseOption {
    CharsetName { default: bool, name: CharsetName },
    CollationName { default: bool, name: CollationName },
    Encryption { default: bool, encrypted: bool },
}

impl fmt::Display for CreateDatabaseOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateDatabaseOption::CharsetName { default, name } => write!(
                f,
                "{}CHARACTER SET = {}",
                if *default { "DEFAULT " } else { "" },
                name
            ),
            CreateDatabaseOption::CollationName { default, name } => write!(
                f,
                "{}COLLATE = {}",
                if *default { "DEFAULT " } else { "" },
                name
            ),
            CreateDatabaseOption::Encryption { default, encrypted } => write!(
                f,
                "{}ENCRYPTION = {}",
                if *default { "DEFAULT " } else { "" },
                if *encrypted { "Y" } else { "N" }
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CreateDatabaseStatement {
    pub is_schema: bool,
    pub if_not_exists: bool,
    pub name: SqlIdentifier,
    /// The result of parsing the `CREATE DATABASE` statement's options. If no options were
    /// present, the Vec will be empty.
    /// If parsing succeeded, then this will be an `Ok` result with the create options. If
    /// it failed to parse, this will be an `Err` with the remainder [`String`] that could not
    /// be parsed.
    pub options: Result<Vec<CreateDatabaseOption>, String>,
}

impl DialectDisplay for CreateDatabaseStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            assert_eq!(dialect, Dialect::MySQL);
            write!(
                f,
                "CREATE {} ",
                if self.is_schema { "SCHEMA" } else { "DATABASE" }
            )?;
            if self.if_not_exists {
                write!(f, "IF NOT EXISTS ")?;
            }
            write!(f, "{}", self.name.as_str())?;
            match &self.options {
                Ok(ref opts) => {
                    for opt in opts.iter() {
                        write!(f, " {opt}")?;
                    }
                }
                Err(unparsed) => write!(f, "{unparsed}")?,
            }
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CreateTableBody {
    pub fields: Vec<ColumnSpecification>,
    pub keys: Option<Vec<TableKey>>,
}

impl CreateTableBody {
    /// Returns the primary key of the table, if one exists.
    pub fn get_primary_key(&self) -> Option<&TableKey> {
        self.keys
            .as_ref()?
            .iter()
            .find(|key| matches!(key, TableKey::PrimaryKey { .. }))
    }

    /// Returns the first unique key of the table, if one exists.
    pub fn get_first_unique_key(&self) -> Option<&TableKey> {
        self.keys
            .as_ref()?
            .iter()
            .find(|key| matches!(key, TableKey::UniqueKey { .. }))
    }
}

impl DialectDisplay for CreateTableBody {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            for (i, field) in self.fields.iter().enumerate() {
                if i != 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", field.display(dialect))?;
            }

            if let Some(keys) = &self.keys {
                for key in keys {
                    write!(f, ", {}", key.display(dialect))?;
                }
            }

            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CreateTableStatement {
    pub if_not_exists: bool,
    pub table: Relation,
    /// The result of parsing the body of the `CREATE TABLE` statement.
    ///
    /// If parsing succeeded, then this will be an `Ok` result with the body of the statement. If
    /// it failed to parse, this will be an `Err` with the remainder [`String`] that could not
    /// be parsed.
    pub body: Result<CreateTableBody, String>,
    /// The result of parsing the options for the `CREATE TABLE` statement.
    ///
    /// If parsing succeeded, then this will be an `Ok` result with the options for the statement.
    /// If it failed to parse, this will be an `Err` with the remainder [`String`] that could not
    /// be parsed.
    pub options: Result<Vec<CreateTableOption>, String>,
}

impl TryFromDialect<sqlparser::ast::CreateTable> for CreateTableStatement {
    fn try_from_dialect(
        value: sqlparser::ast::CreateTable,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        // XXX: We don't actually care about most table options, and don't cover the DATA DIRECTORY
        // variant because sqlparser doesn't support it.
        let mut options = vec![];
        if let Some(auto_increment) = value.auto_increment_offset {
            options.push(CreateTableOption::AutoIncrement(auto_increment as u64));
        }
        if let Some(engine) = value.engine {
            options.push(CreateTableOption::Engine(Some(engine.name)));
        }
        if let Some(charset) = value.default_charset {
            options.push(CreateTableOption::Charset(CharsetName::Unquoted(
                charset.into(),
            )));
        }
        if let Some(collation) = value.collation {
            options.push(CreateTableOption::Collate(CollationName::Unquoted(
                collation.into(),
            )));
        }
        if let Some(
            sqlparser::ast::CommentDef::WithEq(comment)
            | sqlparser::ast::CommentDef::WithoutEq(comment)
            | sqlparser::ast::CommentDef::AfterColumnDefsWithoutEq(comment),
        ) = value.comment
        {
            options.push(CreateTableOption::Comment(comment));
        }
        Ok(Self {
            if_not_exists: value.if_not_exists,
            table: value.name.into_dialect(dialect),
            body: Ok(CreateTableBody {
                fields: value.columns.try_into_dialect(dialect)?,
                keys: if value.constraints.is_empty() {
                    None
                } else {
                    Some(value.constraints.try_into_dialect(dialect)?)
                },
            }),
            options: Ok(options),
        })
    }
}

impl DialectDisplay for CreateTableStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "CREATE TABLE ")?;
            if self.if_not_exists {
                write!(f, "IF NOT EXISTS ")?;
            }
            write!(f, "{} (", self.table.display(dialect))?;

            match &self.body {
                Ok(body) => write!(f, "{}", body.display(dialect))?,
                Err(unparsed) => write!(f, "{unparsed}")?,
            }

            write!(f, ")")?;

            match &self.options {
                Ok(options) => {
                    for (i, option) in options.iter().enumerate() {
                        if i == 0 {
                            write!(f, " ")?;
                        } else {
                            write!(f, ", ")?;
                        }
                        write!(f, "{option}")?;
                    }
                }

                Err(unparsed) => write!(f, "{unparsed}")?,
            }

            Ok(())
        })
    }
}

impl CreateTableStatement {
    /// If the create statement contained a comment, return it
    pub fn get_comment(&self) -> Option<&str> {
        self.options
            .as_ref()
            .ok()?
            .iter()
            .find_map(|opt| match opt {
                CreateTableOption::Comment(s) => Some(s.as_str()),
                _ => None,
            })
    }

    /// If the create statement contained AUTOINCREMENT, return it
    pub fn get_autoincrement(&self) -> Option<u64> {
        self.options
            .as_ref()
            .ok()?
            .iter()
            .find_map(|opt| match opt {
                CreateTableOption::AutoIncrement(i) => Some(*i),
                _ => None,
            })
    }

    /// If the create statement contained a charset, return it
    pub fn get_charset(&self) -> Option<&CharsetName> {
        self.options
            .as_ref()
            .ok()?
            .iter()
            .find_map(|opt| match opt {
                CreateTableOption::Charset(cn) => Some(cn),
                _ => None,
            })
    }

    /// If the create statement contained a collation, return it
    pub fn get_collation(&self) -> Option<&CollationName> {
        self.options
            .as_ref()
            .ok()?
            .iter()
            .find_map(|opt| match opt {
                CreateTableOption::Collate(cn) => Some(cn),
                _ => None,
            })
    }

    pub fn propagate_default_charset(&mut self, dialect: Dialect) {
        if let Dialect::MySQL = dialect {
            // MySQL omits column collation if they are default. Now that we have parsed it, propagate them back to the columns
            let default_charset = self.get_charset().cloned();
            let default_collation = self.get_collation().cloned();
            if let Ok(ref mut body) = self.body {
                for field in &mut body.fields {
                    if !field.sql_type.is_any_text() {
                        continue;
                    }
                    if field.get_charset().is_none() {
                        if let Some(charset) = &default_charset {
                            field
                                .constraints
                                .push(ColumnConstraint::CharacterSet(charset.to_string()));
                        }
                    }
                    if field.get_collation().is_none() {
                        if let Some(collation) = &default_collation {
                            field
                                .constraints
                                .push(ColumnConstraint::Collation(collation.to_string()));
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
#[allow(clippy::large_enum_variant)] // TODO: maybe this actually matters
pub enum SelectSpecification {
    Compound(CompoundSelectStatement),
    Simple(SelectStatement),
}

impl DialectDisplay for SelectSpecification {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Compound(csq) => write!(f, "{}", csq.display(dialect)),
            Self::Simple(sq) => write!(f, "{}", sq.display(dialect)),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CreateViewStatement {
    pub name: Relation,
    pub or_replace: bool,
    pub fields: Vec<Column>,
    /// The result of parsing the definition of the `CREATE VIEW` statement.
    ///
    /// If parsing succeeded, then this will be an `Ok` result with the definition of the
    /// statement. If it failed to parse, this will be an `Err` with the remainder [`String`]
    /// that could not be parsed.
    pub definition: Result<Box<SelectSpecification>, String>,
}

impl TryFromDialect<sqlparser::ast::Statement> for CreateViewStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Statement,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if let sqlparser::ast::Statement::CreateView {
            or_replace,
            name,
            columns,
            query,
            ..
        } = value
        {
            Ok(Self {
                name: name.into_dialect(dialect),
                or_replace,
                fields: columns.into_dialect(dialect),
                // TODO: handle compound selects, not sure how sqlparser represents them
                definition: Ok(Box::new(crate::ast::SelectSpecification::Simple(
                    query.try_into_dialect(dialect)?,
                ))),
            })
        } else {
            failed!("Should only be called with a CreateView statement")
        }
    }
}

impl DialectDisplay for CreateViewStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "CREATE VIEW {} ", self.name.display(dialect))?;

            if !self.fields.is_empty() {
                write!(f, "(")?;
                write!(
                    f,
                    "{}",
                    self.fields.iter().map(|f| f.display(dialect)).join(", ")
                )?;
                write!(f, ") ")?;
            }

            write!(f, "AS ")?;
            match &self.definition {
                Ok(def) => write!(f, "{}", def.display(dialect)),
                Err(unparsed) => write!(f, "{unparsed}"),
            }
        })
    }
}

/// The SelectStatement or query ID referenced in a [`CreateCacheStatement`]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, From, Arbitrary)]
pub enum CacheInner {
    Statement(Box<SelectStatement>),
    Id(SqlIdentifier),
}

impl DialectDisplay for CacheInner {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Statement(stmt) => write!(f, "{}", stmt.display(dialect)),
            Self::Id(id) => write!(f, "{id}"),
        })
    }
}

/// Optional `CREATE CACHE` arguments. This struct is only used for parsing.
#[derive(Default)]
pub struct CreateCacheOptions {
    pub always: bool,
    pub concurrently: bool,
}

/// `CREATE CACHE [CONCURRENTLY] [ALWAYS] [<name>] FROM ...`
///
/// This is a non-standard ReadySet specific extension to SQL
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CreateCacheStatement {
    /// The name of the cache. If not provided, a name will be generated based on the statement
    pub name: Option<Relation>,
    /// The result of parsing the inner statement or query ID for the `CREATE CACHE` statement.
    ///
    /// If parsing succeeded, then this will be an `Ok` result with the definition of the
    /// statement. If it failed to parse, this will be an `Err` with the remainder [`String`]
    /// that could not be parsed.
    pub inner: Result<CacheInner, String>,
    /// A full copy of the original 'create cache' statement that can be used to re-create the
    /// cache after an upgrade
    pub unparsed_create_cache_statement: Option<String>,
    /// If `always` is true, a cached query executed inside a transaction can be served from
    /// a readyset cache.
    /// if false, cached queries within a transaction are proxied to upstream
    pub always: bool,
    /// Whether the CREATE CACHE STATEMENT should block or run concurrently
    pub concurrently: bool,
}

impl DialectDisplay for CreateCacheStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "CREATE CACHE ")?;
            if self.concurrently {
                write!(f, "CONCURRENTLY ")?;
            }
            if self.always {
                write!(f, "ALWAYS ")?;
            }
            if let Some(name) = &self.name {
                write!(f, "{} ", name.display(dialect))?;
            }
            write!(f, "FROM ")?;
            match &self.inner {
                Ok(inner) => write!(f, "{}", inner.display(dialect)),
                Err(unparsed) => write!(f, "{unparsed}"),
            }
        })
    }
}
