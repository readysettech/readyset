use std::{
    fmt,
    hash::{Hash, Hasher},
};

use derive_more::From;
use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    CommentDef, CreateTableOptions, Ident, NamedParenthesizedList, SqlOption, Value, ValueWithSpan,
};
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
    /// Check if the index name is already in use for the table
    pub fn is_index_name_in_use(&self, index_name: &str) -> bool {
        self.keys.as_ref().unwrap_or(&Vec::new()).iter().any(|key| {
            key.index_name()
                .as_deref()
                .is_some_and(|name| name == index_name)
        })
    }

    /// Check if the constraint name is already in use for the table
    pub fn is_constraint_name_in_use(&self, constraint_name: &str) -> bool {
        self.keys.as_ref().unwrap_or(&Vec::new()).iter().any(|key| {
            key.constraint_name()
                .as_deref()
                .is_some_and(|name| name == constraint_name)
        })
    }

    /// Add a new key to the table
    pub fn add_key(&mut self, key: TableKey) {
        if let Some(keys) = &mut self.keys {
            keys.push(key);
            keys.sort();
        } else {
            self.keys = Some(vec![key]);
        }
    }

    /// Add a new key and generate a name for it if it doesn't have one
    /// The column name uses the first column as index name, if that name is already in use,
    /// we append a number to the name.
    pub fn add_key_with_name_mysql(&mut self, key: TableKey, table: Relation) {
        let mut new_key = key;

        // check index name for non-foreign keys
        if !new_key.is_foreign_key() && new_key.index_name().is_none() {
            let first_column = new_key.get_columns().first().unwrap();
            let base_index_name = first_column.name.as_str().to_string();
            let mut index_name = base_index_name.clone();
            // [make_unique_key_name](https://github.com/mysql/mysql-server/blob/mysql-8.0.42/sql/sql_table.cc#L10386)
            let mut i = 2;
            while self.is_index_name_in_use(&index_name) {
                index_name = format!("{base_index_name}_{i}");
                i += 1;
            }
            new_key.set_index_name(index_name.into());
        }

        // special case for foreign keys
        if new_key.is_foreign_key() {
            self.handle_add_foreign_key_mysql(&mut new_key, table);
        }
        self.add_key(new_key);
    }

    /// MySQL logic to add a new foreign key to the table
    fn handle_add_foreign_key_mysql(&mut self, new_key: &mut TableKey, table: Relation) {
        if new_key.constraint_name().is_none() {
            let table_name = table.name.clone();
            let base_constraint_name = format!("{table_name}_ibfk");
            let mut i = 1;
            let mut constraint_name = format!("{base_constraint_name}_{i}");
            while self.is_constraint_name_in_use(&constraint_name) {
                i += 1;
                constraint_name = format!("{base_constraint_name}_{i}");
            }
            new_key.set_constraint_name(constraint_name.into());
        }

        // target_table
        if let Some(target_table) = new_key.target_table_mut() {
            target_table.set_schema(table.schema.clone());
        }

        // target_columns
        if let Some(target_columns) = new_key.target_columns_mut() {
            for column in target_columns {
                column.table = Some(table.clone());
            }
        }
        let columns = new_key.get_columns();
        if !self.has_key_on_columns(columns) {
            // add a new key of type Key on target columns
            let target_col_new_key = TableKey::Key {
                index_name: new_key.constraint_name().clone(),
                columns: columns.to_vec(),
                index_type: None,
            };
            self.add_key(target_col_new_key);
        }
    }

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

    /// Check if there is an index of type Key covering a list of columns
    pub fn has_key_on_columns(&self, columns: &[Column]) -> bool {
        self.keys.as_ref().unwrap_or(&Vec::new()).iter().any(|key| {
            key.is_key()
                && key.get_columns().iter().all(|column| {
                    columns
                        .iter()
                        .any(|c| c.name.as_str() == column.name.as_str())
                })
        })
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

impl TryFromDialect<sqlparser::ast::Statement> for CreateTableStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Statement,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        match value {
            sqlparser::ast::Statement::CreateTable(stmt) => stmt.try_into_dialect(dialect),
            other => failed!("Should only be called on a CREATE TABLE statement, got: {other:?}"),
        }
    }
}

impl TryFromDialect<sqlparser::ast::CreateTable> for CreateTableStatement {
    fn try_from_dialect(
        value: sqlparser::ast::CreateTable,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let mut options = vec![];
        if let CreateTableOptions::Plain(opts) = value.table_options {
            for option in opts {
                match option {
                    SqlOption::KeyValue { key, value } => match key.value.as_str() {
                        "AUTO_INCREMENT" => {
                            if let sqlparser::ast::Expr::Value(ValueWithSpan {
                                value: Value::Number(v, _),
                                ..
                            }) = value
                            {
                                options.push(CreateTableOption::AutoIncrement(v.parse().map_err(
                                    |_| {
                                        AstConversionError::Failed(
                                            "Failed to parse AUTO_INCREMENT value as u64".into(),
                                        )
                                    },
                                )?))
                            }
                        }
                        "DEFAULT CHARSET"
                        | "CHARSET"
                        | "DEFAULT CHARACTER SET"
                        | "CHARACTER SET" => match value {
                            sqlparser::ast::Expr::Value(ValueWithSpan {
                                value: Value::SingleQuotedString(v),
                                ..
                            }) => options
                                .push(CreateTableOption::Charset(CharsetName::Quoted(v.into()))),
                            sqlparser::ast::Expr::Identifier(Ident { value: v, .. }) => options
                                .push(CreateTableOption::Charset(CharsetName::Unquoted(v.into()))),
                            v => {
                                return Err(AstConversionError::Failed(format!(
                                    "Unsupported charset option {v}"
                                )))
                            }
                        },
                        "COLLATE" | "DEFAULT COLLATE" => match value {
                            sqlparser::ast::Expr::Value(ValueWithSpan {
                                value: Value::SingleQuotedString(v),
                                ..
                            }) => options
                                .push(CreateTableOption::Collate(CollationName::Quoted(v.into()))),
                            sqlparser::ast::Expr::Identifier(Ident { value: v, .. }) => options
                                .push(CreateTableOption::Collate(CollationName::Unquoted(
                                    v.into(),
                                ))),
                            v => {
                                return Err(AstConversionError::Failed(format!(
                                    "Unsupported collate value {v}"
                                )))
                            }
                        },
                        "DATA DIRECTORY" => match value {
                            sqlparser::ast::Expr::Value(ValueWithSpan {
                                value: Value::SingleQuotedString(v),
                                ..
                            }) => options.push(CreateTableOption::DataDirectory(v)),
                            _ => {
                                return Err(AstConversionError::Failed(format!(
                                    "Unsupported table option {key}"
                                )))
                            }
                        },
                        _ => {
                            return Err(AstConversionError::Failed(format!(
                                "Unsupported table option {key}"
                            )))
                        }
                    },

                    SqlOption::NamedParenthesizedList(NamedParenthesizedList {
                        key, name, ..
                    }) => {
                        if key.value == "ENGINE" {
                            options
                                .push(CreateTableOption::Engine(name.map(|n| n.value.to_string())));
                        }
                    }
                    SqlOption::Comment(
                        CommentDef::WithEq(comment) | CommentDef::WithoutEq(comment),
                    ) => options.push(CreateTableOption::Comment(comment)),
                    e => {
                        return Err(AstConversionError::Failed(format!(
                            "Unsupported table option {e}"
                        )))
                    }
                }
            }
        }

        let mut create_table = Self {
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
        };
        create_table.propagate_default_charset(dialect);
        Ok(create_table)
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

    /// MySQL turns `CHAR(10) COLLATE binary` into `BINARY(10)`, `CHAR COLLATE binary` into
    /// `BINARY(1)`, etc. However, these are not done before DDL is sent over the binlog. So when
    /// snapshotting, we will see `BINARY(10)`, but if the table is created during replication, we
    /// need to do the translation ourselves.
    ///
    /// The below translations were determined empirically with MySQL 8.0. MySQL 5.7 does not have
    /// the `binary` collation, but I don't think it's worth finding a way to error gracefully if we
    /// encounter it instead of attempting this translation.
    pub fn rewrite_binary_collation_columns(&mut self) {
        let Ok(ref mut body) = self.body else {
            return;
        };
        for field in &mut body.fields {
            if field.sql_type.is_any_text()
                && field
                    .constraints
                    .extract_if(.., |constraint| {
                        matches!(
                            constraint,
                            ColumnConstraint::Collation(collation) if collation.eq_ignore_ascii_case("binary")
                        )
                    })
                    .next()
                    .is_some()
            {
                field.sql_type = match field.sql_type {
                    SqlType::Char(len) => SqlType::Binary(Some(len.unwrap_or(1))),
                    SqlType::VarChar(len) => SqlType::VarBinary(len.unwrap_or(1)),
                    SqlType::TinyText => SqlType::TinyBlob,
                    SqlType::MediumText => SqlType::MediumBlob,
                    SqlType::Text => SqlType::Blob,
                    SqlType::LongText => SqlType::LongBlob,
                    _ => unreachable!(),
                };
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

impl TryFromDialect<sqlparser::ast::Query> for SelectSpecification {
    fn try_from_dialect(
        value: sqlparser::ast::Query,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let sqlparser::ast::Query { ref body, .. } = value;
        match **body {
            sqlparser::ast::SetExpr::Select(_) => {
                Ok(Self::Simple(value.try_into_dialect(dialect)?))
            }
            sqlparser::ast::SetExpr::SetOperation { op, .. } => match op {
                sqlparser::ast::SetOperator::Union => {
                    Ok(Self::Compound(value.try_into_dialect(dialect)?))
                }
                op => unsupported!("Unsupported set operation {op}"),
            },
            _ => failed!("Should only be called on a SELECT or UNION statement"),
        }
    }
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
                definition: Ok(Box::new((*query).try_into_dialect(dialect)?)),
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
#[derive(Clone, Debug, Serialize, Deserialize, Arbitrary)]
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
    ///
    /// XXX(mvzink): Not included in hash or equality checks during nom-sql parity testing, since we
    /// don't divide up the input before starting parsing when using sqlparser-rs. See
    /// [`readyset_sql_parsing::parse_queries`].
    pub unparsed_create_cache_statement: Option<String>,
    /// If `always` is true, a cached query executed inside a transaction can be served from
    /// a readyset cache.
    /// if false, cached queries within a transaction are proxied to upstream
    pub always: bool,
    /// Whether the CREATE CACHE STATEMENT should block or run concurrently
    pub concurrently: bool,
}

impl PartialEq for CreateCacheStatement {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.inner == other.inner
            && self.always == other.always
            && self.concurrently == other.concurrently
    }
}

impl Eq for CreateCacheStatement {}

impl Hash for CreateCacheStatement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.inner.hash(state);
        self.always.hash(state);
        self.concurrently.hash(state);
    }
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
