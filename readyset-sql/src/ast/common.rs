use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{
    ast::*, AstConversionError, Dialect, DialectDisplay, FromDialect, IntoDialect, TryFromDialect,
    TryIntoDialect,
};

/// TODO(mvzink): Could be deleted in favor of directly using [`sqlparser::ast::IndexType`]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum IndexType {
    BTree,
    Hash,
    GIN,
    GiST,
    SPGiST,
    BRIN,
    Bloom,
    Custom(SqlIdentifier),
}

impl FromDialect<sqlparser::ast::IndexType> for IndexType {
    fn from_dialect(value: sqlparser::ast::IndexType, dialect: Dialect) -> Self {
        match value {
            sqlparser::ast::IndexType::BTree => Self::BTree,
            sqlparser::ast::IndexType::Hash => Self::Hash,
            sqlparser::ast::IndexType::GIN => Self::GIN,
            sqlparser::ast::IndexType::GiST => Self::GiST,
            sqlparser::ast::IndexType::SPGiST => Self::SPGiST,
            sqlparser::ast::IndexType::BRIN => Self::BRIN,
            sqlparser::ast::IndexType::Bloom => Self::Bloom,
            sqlparser::ast::IndexType::Custom(ident) => Self::Custom(ident.into_dialect(dialect)),
        }
    }
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexType::BTree => f.write_str("BTREE"),
            IndexType::Hash => f.write_str("HASH"),
            IndexType::GIN => f.write_str("GIN"),
            IndexType::GiST => f.write_str("GIST"),
            IndexType::SPGiST => f.write_str("SPGIST"),
            IndexType::BRIN => f.write_str("BRIN"),
            IndexType::Bloom => f.write_str("BLOOM"),
            IndexType::Custom(sql_identifier) => sql_identifier.fmt(f),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    Restrict,
    NoAction,
    SetDefault,
}

impl From<sqlparser::ast::ReferentialAction> for ReferentialAction {
    fn from(value: sqlparser::ast::ReferentialAction) -> Self {
        use sqlparser::ast::ReferentialAction::*;
        match value {
            Cascade => Self::Cascade,
            NoAction => Self::NoAction,
            Restrict => Self::Restrict,
            SetDefault => Self::SetDefault,
            SetNull => Self::SetNull,
        }
    }
}

impl fmt::Display for ReferentialAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Cascade => "CASCADE",
                Self::SetNull => "SET NULL",
                Self::Restrict => "RESTRICT",
                Self::NoAction => "NO ACTION",
                Self::SetDefault => "SET DEFAULT",
            }
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ConstraintTiming {
    Deferrable,
    DeferrableInitiallyDeferred,
    DeferrableInitiallyImmediate,
    NotDeferrable,
    NotDeferrableInitiallyImmediate,
}

// XXX: It's possible we are not completely correct in representing this matrix, i.e. we should
// check that deferrable = false + initially = deferred is correctly represented by
// `Self::NotDeferrable` and we don't need a separate variant for
// `Self::NotDeferrableInitiallyDeferred` (which doesn't sound like a valid combination). However,
// we don't actually do anything with these values and just need to parse them anyway.
impl From<sqlparser::ast::ConstraintCharacteristics> for ConstraintTiming {
    fn from(value: sqlparser::ast::ConstraintCharacteristics) -> Self {
        use sqlparser::ast::DeferrableInitial::*;
        match (value.deferrable, value.initially) {
            (None, None) => Self::NotDeferrable,
            (None, Some(Immediate)) => Self::NotDeferrableInitiallyImmediate,
            (None, Some(Deferred)) => Self::NotDeferrable,
            (Some(true), None) => Self::Deferrable,
            (Some(false), None) => Self::NotDeferrable,
            (Some(true), Some(Immediate)) => Self::DeferrableInitiallyImmediate,
            (Some(true), Some(Deferred)) => Self::DeferrableInitiallyDeferred,
            (Some(false), Some(Immediate)) => Self::NotDeferrableInitiallyImmediate,
            (Some(false), Some(Deferred)) => Self::NotDeferrable,
        }
    }
}

impl fmt::Display for ConstraintTiming {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ConstraintTiming::Deferrable => "DEFERRABLE",
                ConstraintTiming::DeferrableInitiallyDeferred => "DEFERRABLE INITIALLY DEFERRED",
                ConstraintTiming::DeferrableInitiallyImmediate => "DEFERRABLE INITIALLY IMMEDIATE",
                ConstraintTiming::NotDeferrable => "NOT DEFERRABLE",
                ConstraintTiming::NotDeferrableInitiallyImmediate =>
                    "NOT DEFERRABLE INITIALLY IMMEDIATE",
            }
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum NullsDistinct {
    Distinct,
    NotDistinct,
}

impl fmt::Display for NullsDistinct {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NullsDistinct::Distinct => "NULLS DISTINCT",
                NullsDistinct::NotDistinct => "NULLS NOT DISTINCT",
            }
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum TableKey {
    PrimaryKey {
        constraint_name: Option<SqlIdentifier>,
        constraint_timing: Option<ConstraintTiming>,
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
    },
    UniqueKey {
        constraint_name: Option<SqlIdentifier>,
        constraint_timing: Option<ConstraintTiming>,
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
        nulls_distinct: Option<NullsDistinct>,
    },
    FulltextKey {
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
    },
    Key {
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
    },
    ForeignKey {
        constraint_name: Option<SqlIdentifier>,
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        target_table: Relation,
        target_columns: Vec<Column>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    CheckConstraint {
        // NOTE: MySQL doesn't allow the `CONSTRAINT (name)` prefix for a CHECK, but Postgres does
        constraint_name: Option<SqlIdentifier>,
        expr: Expr,
        enforced: Option<bool>,
    },
}

impl TryFromDialect<sqlparser::ast::TableConstraint> for TableKey {
    fn try_from_dialect(
        value: sqlparser::ast::TableConstraint,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::TableConstraint::*;
        match value {
            Check { name, expr } => Ok(Self::CheckConstraint {
                constraint_name: name.into_dialect(dialect),
                expr: expr.try_into_dialect(dialect)?,
                enforced: None, // TODO(mvzink): Find out where this is supposed to come from
            }),
            ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                // XXX Not sure why, but we don't support characteristics
                characteristics: _characteristics,
            } => Ok(Self::ForeignKey {
                // TODO(mvzink): Where do these two different names come from for sqlparser?
                constraint_name: name.into_dialect(dialect),
                index_name: None,
                columns: columns.into_dialect(dialect),
                target_table: foreign_table.into_dialect(dialect),
                target_columns: referred_columns.into_dialect(dialect),
                on_delete: on_delete.map(Into::into),
                on_update: on_update.map(Into::into),
            }),
            FulltextOrSpatial {
                fulltext: true,
                opt_index_name,
                columns,
                index_type_display: _index_type_display,
            } => Ok(Self::FulltextKey {
                index_name: opt_index_name.into_dialect(dialect),
                columns: columns.into_dialect(dialect),
            }),
            FulltextOrSpatial {
                fulltext: false, ..
            } => unsupported!("spatial index not supported"),
            Index {
                name,
                index_type,
                columns,
                display_as_key: _display_as_key,
            } => Ok(Self::Key {
                index_name: name.into_dialect(dialect),
                columns: columns.into_dialect(dialect),
                index_type: index_type.into_dialect(dialect),
            }),
            PrimaryKey {
                name,
                index_name,
                columns,
                characteristics,
                // XXX: Not sure why, but we don't support any of these
                index_type: _index_type,
                index_options: _index_options,
            } => Ok(Self::PrimaryKey {
                constraint_name: name.into_dialect(dialect),
                index_name: index_name.into_dialect(dialect),
                columns: columns.into_dialect(dialect),
                constraint_timing: characteristics.map(Into::into),
            }),
            Unique {
                name,
                index_name,
                index_type,
                columns,
                nulls_distinct,
                characteristics,
                // XXX Not sure why, but we don't support any of these
                index_type_display: _index_type_display,
                index_options: _index_options,
            } => Ok(Self::UniqueKey {
                constraint_name: name.into_dialect(dialect),
                index_name: index_name.into_dialect(dialect),
                columns: columns.into_dialect(dialect),
                index_type: index_type.into_dialect(dialect),
                constraint_timing: characteristics.map(Into::into),
                nulls_distinct: match nulls_distinct {
                    sqlparser::ast::NullsDistinctOption::Distinct => Some(NullsDistinct::Distinct),
                    sqlparser::ast::NullsDistinctOption::NotDistinct => {
                        Some(NullsDistinct::NotDistinct)
                    }
                    sqlparser::ast::NullsDistinctOption::None => None,
                },
            }),
        }
    }
}

impl TableKey {
    pub fn constraint_name(&self) -> &Option<SqlIdentifier> {
        match self {
            TableKey::PrimaryKey {
                constraint_name, ..
            }
            | TableKey::UniqueKey {
                constraint_name, ..
            }
            | TableKey::ForeignKey {
                constraint_name, ..
            }
            | TableKey::CheckConstraint {
                constraint_name, ..
            } => constraint_name,
            TableKey::FulltextKey { .. } | TableKey::Key { .. } => &None,
        }
    }

    /// Returns the name of the index.
    ///
    /// # Returns
    /// - `Some` if the index has a name and a SqlIdentifier
    /// - `None` if the index does not have a name
    pub fn index_name(&self) -> &Option<SqlIdentifier> {
        match self {
            TableKey::PrimaryKey { index_name, .. }
            | TableKey::UniqueKey { index_name, .. }
            | TableKey::FulltextKey { index_name, .. }
            | TableKey::Key { index_name, .. }
            | TableKey::ForeignKey { index_name, .. } => index_name,
            TableKey::CheckConstraint { .. } => &None,
        }
    }

    /// Check if the key is a primary key
    pub fn is_primary_key(&self) -> bool {
        matches!(self, TableKey::PrimaryKey { .. })
    }

    /// Check if the key is a unique key
    pub fn is_unique_key(&self) -> bool {
        matches!(self, TableKey::UniqueKey { .. })
    }

    /// Get the columns that the key is defined on
    pub fn get_columns(&self) -> &[Column] {
        match self {
            TableKey::PrimaryKey { columns, .. }
            | TableKey::UniqueKey { columns, .. }
            | TableKey::FulltextKey { columns, .. }
            | TableKey::Key { columns, .. }
            | TableKey::ForeignKey { columns, .. } => columns,
            TableKey::CheckConstraint { .. } => &[],
        }
    }
}

impl DialectDisplay for TableKey {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            if let Some(constraint_name) = self.constraint_name() {
                write!(
                    f,
                    "CONSTRAINT {} ",
                    dialect.quote_identifier(constraint_name)
                )?;
            }

            match self {
                TableKey::PrimaryKey {
                    constraint_timing,
                    index_name,
                    columns,
                    ..
                } => {
                    write!(f, "PRIMARY KEY ")?;
                    if let Some(index_name) = index_name {
                        write!(f, "{} ", dialect.quote_identifier(index_name))?;
                    }
                    write!(
                        f,
                        "({})",
                        columns.iter().map(|c| c.display(dialect)).join(", ")
                    )?;
                    if let Some(constraint_timing) = constraint_timing {
                        write!(f, " {constraint_timing}")?;
                    }
                    Ok(())
                }
                TableKey::UniqueKey {
                    constraint_timing,
                    index_name,
                    columns,
                    index_type,
                    nulls_distinct,
                    ..
                } => {
                    if dialect == Dialect::MySQL {
                        write!(f, "UNIQUE KEY ")?;
                    } else {
                        write!(f, "UNIQUE ")?;
                    }
                    if let Some(index_name) = index_name {
                        write!(f, "{} ", dialect.quote_identifier(index_name))?;
                    }
                    if let Some(nulls_distinct) = nulls_distinct {
                        write!(f, "{nulls_distinct} ")?;
                    }
                    write!(
                        f,
                        "({})",
                        columns.iter().map(|c| c.display(dialect)).join(", ")
                    )?;
                    if let Some(constraint_timing) = constraint_timing {
                        write!(f, " {constraint_timing}")?;
                    }
                    if let Some(index_type) = index_type {
                        write!(f, " USING {index_type}")?;
                    }
                    Ok(())
                }
                TableKey::FulltextKey {
                    index_name,
                    columns,
                } => {
                    write!(f, "FULLTEXT KEY ")?;
                    if let Some(ref index_name) = *index_name {
                        write!(f, "{} ", dialect.quote_identifier(index_name))?;
                    }
                    write!(
                        f,
                        "({})",
                        columns.iter().map(|c| c.display(dialect)).join(", ")
                    )
                }
                TableKey::Key {
                    index_name,
                    columns,
                    index_type,
                    ..
                } => {
                    write!(f, "KEY ")?;
                    if let Some(index_name) = index_name {
                        write!(f, "{} ", dialect.quote_identifier(index_name))?;
                    }
                    write!(
                        f,
                        "({})",
                        columns.iter().map(|c| c.display(dialect)).join(", ")
                    )?;
                    if let Some(index_type) = index_type {
                        write!(f, " USING {index_type}")?;
                    }
                    Ok(())
                }
                TableKey::ForeignKey {
                    index_name,
                    columns,
                    target_table,
                    target_columns,
                    on_delete,
                    on_update,
                    ..
                } => {
                    let index_name = fmt_with(|f| {
                        if let Some(index_name) = index_name {
                            write!(f, "{}", dialect.quote_identifier(index_name))?;
                        }
                        Ok(())
                    });

                    write!(
                        f,
                        "FOREIGN KEY {} ({}) REFERENCES {} ({})",
                        index_name,
                        columns.iter().map(|c| c.display(dialect)).join(", "),
                        target_table.display(dialect),
                        target_columns.iter().map(|c| c.display(dialect)).join(", ")
                    )?;
                    if let Some(on_delete) = on_delete {
                        write!(f, " ON DELETE {on_delete}")?;
                    }
                    if let Some(on_update) = on_update {
                        write!(f, " ON UPDATE {on_update}")?;
                    }
                    Ok(())
                }
                TableKey::CheckConstraint { expr, enforced, .. } => {
                    // NOTE: MySQL does not allow an optional 'CONSTRAINT' here and expects only
                    // "ADD CHECK" https://dev.mysql.com/doc/refman/5.7/en/alter-table.html

                    // Postgres is fine with it, but since this is our own display, leave it out.
                    // https://www.postgresql.org/docs/current/sql-altertable.html
                    write!(f, " CHECK {}", expr.display(dialect))?;

                    if let Some(enforced) = enforced {
                        if !enforced {
                            write!(f, " NOT")?;
                        }
                        write!(f, " ENFORCED")?;
                    }

                    Ok(())
                }
            }
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
#[allow(clippy::large_enum_variant)] // NOTE(aspen): do we actually care about this?
#[derive(Default)]
pub enum FieldDefinitionExpr {
    #[default]
    All,
    AllInTable(Relation),
    // TODO: re-enable once Expr round trip is stable
    #[weight(0)]
    Expr {
        expr: Expr,
        alias: Option<SqlIdentifier>,
    },
}

/// Constructs a [`FieldDefinitionExpr::Expr`] without an alias
impl From<Expr> for FieldDefinitionExpr {
    fn from(expr: Expr) -> Self {
        FieldDefinitionExpr::Expr { expr, alias: None }
    }
}

/// Constructs a [`FieldDefinitionExpr::Expr`] based on an [`Expr::Column`] for
/// the column and without an alias
impl From<Column> for FieldDefinitionExpr {
    fn from(col: Column) -> Self {
        FieldDefinitionExpr::Expr {
            expr: Expr::Column(col),
            alias: None,
        }
    }
}

impl From<Literal> for FieldDefinitionExpr {
    fn from(lit: Literal) -> Self {
        FieldDefinitionExpr::from(Expr::Literal(lit))
    }
}

impl TryFromDialect<sqlparser::ast::SelectItem> for FieldDefinitionExpr {
    fn try_from_dialect(
        value: sqlparser::ast::SelectItem,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::SelectItem::*;
        match value {
            ExprWithAlias { expr, alias } => Ok(FieldDefinitionExpr::Expr {
                expr: expr.try_into_dialect(dialect)?,
                alias: Some(alias.into_dialect(dialect)),
            }),
            UnnamedExpr(expr) => Ok(FieldDefinitionExpr::Expr {
                expr: expr.try_into_dialect(dialect)?,
                alias: None,
            }),
            QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(relation),
                _options,
            ) => Ok(FieldDefinitionExpr::AllInTable(
                relation.into_dialect(dialect),
            )),
            QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(_expr),
                _options,
            ) => {
                unsupported!("struct wildcard expansion is not supported by MySQL or Postgres")
            }
            Wildcard(_options) => Ok(FieldDefinitionExpr::All),
        }
    }
}

impl DialectDisplay for FieldDefinitionExpr {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::All => write!(f, "*"),
            Self::AllInTable(table) => {
                write!(f, "{}.*", table.display(dialect))
            }
            Self::Expr { expr, alias } => {
                write!(f, "{}", expr.display(dialect))?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", dialect.quote_identifier(alias))?;
                }
                Ok(())
            }
        })
    }
}

pub enum Sign {
    Unsigned,
    Signed,
}

/// A reference to a field in a query, usable in either the `GROUP BY` or `ORDER BY` clauses of the
/// query
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum FieldReference {
    /// A reference to a field in the `SELECT` list by its (1-based) index.
    Numeric(u64),
    /// An expression
    Expr(Expr),
}

impl TryFromDialect<sqlparser::ast::Expr> for FieldReference {
    fn try_from_dialect(
        value: sqlparser::ast::Expr,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if let sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Number(ref n, _),
            ..
        }) = value
        {
            if let Ok(i) = n.parse() {
                return Ok(FieldReference::Numeric(i));
            }
        }
        Ok(FieldReference::Expr(value.try_into_dialect(dialect)?))
    }
}

impl DialectDisplay for FieldReference {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Numeric(n) => write!(f, "{n}"),
            Self::Expr(expr) => write!(f, "{}", expr.display(dialect)),
        })
    }
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary,
)]
pub enum TimestampField {
    Century,
    Day,
    Decade,
    Dow,
    Doy,
    Epoch,
    Hour,
    Isodow,
    Isoyear,
    Julian,
    Microseconds,
    Millennium,
    Milliseconds,
    Minute,
    Month,
    Quarter,
    Second,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
    Week,
    Year,
}

impl TimestampField {
    pub fn is_date_field(&self) -> bool {
        use TimestampField::*;

        matches!(
            self,
            Century
                | Day
                | Decade
                | Dow
                | Doy
                | Epoch
                | Isodow
                | Isoyear
                | Julian
                | Millennium
                | Month
                | Quarter
                | Week
                | Year
        )
    }
}

impl From<sqlparser::ast::DateTimeField> for TimestampField {
    fn from(value: sqlparser::ast::DateTimeField) -> Self {
        use sqlparser::ast::DateTimeField;
        match value {
            DateTimeField::Century => Self::Century,
            DateTimeField::Day | DateTimeField::Days => Self::Day,
            DateTimeField::DayOfWeek => Self::Dow,
            DateTimeField::DayOfYear => Self::Doy,
            DateTimeField::Decade => Self::Decade,
            DateTimeField::Dow => Self::Dow,
            DateTimeField::Doy => Self::Doy,
            DateTimeField::Epoch => Self::Epoch,
            DateTimeField::Hour | DateTimeField::Hours => Self::Hour,
            DateTimeField::Isodow => Self::Isodow,
            DateTimeField::Isoyear => Self::Isoyear,
            DateTimeField::Julian => Self::Julian,
            DateTimeField::Microsecond | DateTimeField::Microseconds => Self::Microseconds,
            DateTimeField::Millenium | DateTimeField::Millennium => Self::Millennium,
            DateTimeField::Millisecond | DateTimeField::Milliseconds => Self::Milliseconds,
            DateTimeField::Minute | DateTimeField::Minutes => Self::Minute,
            DateTimeField::Month | DateTimeField::Months => Self::Month,
            DateTimeField::Quarter => Self::Quarter,
            DateTimeField::Second | DateTimeField::Seconds => Self::Second,
            DateTimeField::Timezone => Self::Timezone,
            DateTimeField::TimezoneHour => Self::TimezoneHour,
            DateTimeField::TimezoneMinute => Self::TimezoneMinute,
            DateTimeField::Week(_) | DateTimeField::Weeks => Self::Week, // Optional weekday is only BigQuery
            DateTimeField::Year | DateTimeField::Years => Self::Year,
            DateTimeField::Custom(_)
            | DateTimeField::Date
            | DateTimeField::Datetime
            | DateTimeField::IsoWeek
            | DateTimeField::Nanosecond
            | DateTimeField::Nanoseconds
            | DateTimeField::NoDateTime
            | DateTimeField::Time
            | DateTimeField::TimezoneAbbr
            | DateTimeField::TimezoneRegion => {
                unimplemented!("not supported by MySQL or Postgres")
            }
        }
    }
}

impl fmt::Display for TimestampField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Century => write!(f, "CENTURY"),
            Self::Day => write!(f, "DAY"),
            Self::Decade => write!(f, "DECADE"),
            Self::Dow => write!(f, "DOW"),
            Self::Doy => write!(f, "DOY"),
            Self::Epoch => write!(f, "EPOCH"),
            Self::Hour => write!(f, "HOUR"),
            Self::Isodow => write!(f, "ISODOW"),
            Self::Isoyear => write!(f, "ISOYEAR"),
            Self::Julian => write!(f, "JULIAN"),
            Self::Microseconds => write!(f, "MICROSECONDS"),
            Self::Millennium => write!(f, "MILLENNIUM"),
            Self::Milliseconds => write!(f, "MILLISECONDS"),
            Self::Minute => write!(f, "MINUTE"),
            Self::Month => write!(f, "MONTH"),
            Self::Quarter => write!(f, "QUARTER"),
            Self::Second => write!(f, "SECOND"),
            Self::Timezone => write!(f, "TIMEZONE"),
            Self::TimezoneHour => write!(f, "TIMEZONE_HOUR"),
            Self::TimezoneMinute => write!(f, "TIMEZONE_MINUTE"),
            Self::Week => write!(f, "WEEK"),
            Self::Year => write!(f, "YEAR"),
        }
    }
}
