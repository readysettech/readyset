use std::cmp::Ordering;
use std::{fmt, str};

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{AstConversionError, Dialect, DialectDisplay, TryFromDialect, TryIntoDialect, ast::*};

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub enum OrderType {
    OrderAscending,
    OrderDescending,
}

impl OrderType {
    /// Reverse the provided [`Ordering`] if this [`OrderType`] if of type
    /// [`OrderType::OrderDescending`], otherwise do nothing
    #[inline(always)]
    pub fn apply(&self, ord: Ordering) -> Ordering {
        match self {
            OrderType::OrderAscending => ord,
            OrderType::OrderDescending => ord.reverse(),
        }
    }
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OrderType::OrderAscending => write!(f, "ASC"),
            OrderType::OrderDescending => write!(f, "DESC"),
        }
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub enum NullOrder {
    NullsFirst,
    NullsLast,
}

impl NullOrder {
    /// Assign a default value based on the dialect and order type
    ///
    /// For MySQL, the default for `OrderType::OrderAscending` is `NullOrder::NullsFirst` and
    /// `OrderType::OrderDescending` is `NullOrder::NullsLast`.
    ///
    /// For PostgreSQL, the default for `OrderType::OrderAscending` is `NullOrder::NullsLast` and
    /// `OrderType::OrderDescending` is `NullOrder::NullsFirst`.
    ///
    /// [https://dev.mysql.com/doc/refman/8.0/en/problems-with-null.html]
    /// [https://www.postgresql.org/docs/current/queries-order.html]
    pub fn default_for(dialect: Dialect, order_type: &OrderType) -> Self {
        match (dialect, order_type) {
            (Dialect::PostgreSQL, OrderType::OrderDescending) => NullOrder::NullsFirst,
            (Dialect::PostgreSQL, OrderType::OrderAscending) => NullOrder::NullsLast,
            (Dialect::MySQL, OrderType::OrderDescending) => NullOrder::NullsLast,
            (Dialect::MySQL, OrderType::OrderAscending) => NullOrder::NullsFirst,
        }
    }

    pub fn apply(&self, a_null: bool, b_null: bool) -> Ordering {
        match (a_null, b_null) {
            (true, false) if self == &NullOrder::NullsFirst => Ordering::Less,
            (true, false) => Ordering::Greater,
            (false, true) if self == &NullOrder::NullsLast => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) | (true, true) => Ordering::Equal,
        }
    }
}

impl fmt::Display for NullOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NullOrder::NullsFirst => write!(f, "NULLS FIRST"),
            NullOrder::NullsLast => write!(f, "NULLS LAST"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct OrderBy {
    pub field: FieldReference,
    pub order_type: Option<OrderType>,
    /// Not an Option because it is assigned a default value
    /// during parsing as the default value differs between dialects
    pub null_order: NullOrder,
}

impl TryFromDialect<sqlparser::ast::OrderByExpr> for OrderBy {
    fn try_from_dialect(
        value: sqlparser::ast::OrderByExpr,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let sqlparser::ast::OrderByExpr {
            expr,
            options,
            with_fill: _,
        } = value;

        let order_type = options.asc.map(|asc| {
            if asc {
                OrderType::OrderAscending
            } else {
                OrderType::OrderDescending
            }
        });

        let null_order = options.nulls_first.map(|first| {
            if first {
                NullOrder::NullsFirst
            } else {
                NullOrder::NullsLast
            }
        });

        Ok(Self {
            field: expr.try_into_dialect(dialect)?,
            order_type,
            // TODO: We assign a default here because the default value differs between dialects
            // Maybe it's better to assign the default in MIR lowering; however,
            // that requires plumbing the dialect through all the way to MIR
            // which doesn't seem trivial at the moment
            null_order: null_order.unwrap_or(NullOrder::default_for(
                dialect,
                &order_type.unwrap_or(OrderType::OrderAscending),
            )),
        })
    }
}

impl OrderBy {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            write!(f, "{}", self.field.display(dialect))?;
            if let Some(ot) = self.order_type {
                write!(f, " {ot}")?;
            }

            // MySQL doesn't support explicit NULLS FIRST|LAST
            if !matches!(dialect, Dialect::MySQL) {
                write!(f, " {}", self.null_order)?;
            }

            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct OrderClause {
    pub order_by: Vec<OrderBy>,
}

impl TryFromDialect<sqlparser::ast::OrderBy> for OrderClause {
    fn try_from_dialect(
        value: sqlparser::ast::OrderBy,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        match value.kind {
            sqlparser::ast::OrderByKind::All(_order_by_options) => {
                unsupported!("ORDER BY ALL")
            }
            sqlparser::ast::OrderByKind::Expressions(exprs) => Ok(OrderClause {
                order_by: exprs.try_into_dialect(dialect)?,
            }),
        }
    }
}

impl DialectDisplay for OrderClause {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "ORDER BY {}",
                self.order_by
                    .iter()
                    .map(|ob| ob.display(dialect))
                    .join(", ")
            )
        })
    }
}
