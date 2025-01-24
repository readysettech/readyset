use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum IndexType {
    BTree,
    Hash,
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexType::BTree => write!(f, "BTREE"),
            IndexType::Hash => write!(f, "HASH"),
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
        constraint_name: Option<SqlIdentifier>,
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
        // NOTE: MySQL dosn't allow the `CONSTRAINT (name)` prefix for a CHECK, but Postgres does
        constraint_name: Option<SqlIdentifier>,
        expr: Expr,
        enforced: Option<bool>,
    },
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
            | TableKey::Key {
                constraint_name, ..
            }
            | TableKey::ForeignKey {
                constraint_name, ..
            }
            | TableKey::CheckConstraint {
                constraint_name, ..
            } => constraint_name,
            TableKey::FulltextKey { .. } => &None,
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
                        write!(f, " {}", constraint_timing)?;
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
                        write!(f, "{} ", nulls_distinct)?;
                    }
                    write!(
                        f,
                        "({})",
                        columns.iter().map(|c| c.display(dialect)).join(", ")
                    )?;
                    if let Some(constraint_timing) = constraint_timing {
                        write!(f, " {}", constraint_timing)?;
                    }
                    if let Some(index_type) = index_type {
                        write!(f, " USING {}", index_type)?;
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
                        write!(f, " USING {}", index_type)?;
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
                        write!(f, " ON DELETE {}", on_delete)?;
                    }
                    if let Some(on_update) = on_update {
                        write!(f, " ON UPDATE {}", on_update)?;
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

impl DialectDisplay for FieldReference {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Numeric(n) => write!(f, "{}", n),
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
