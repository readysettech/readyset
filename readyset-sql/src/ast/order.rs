use std::cmp::Ordering;
use std::{fmt, str};

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, AstConversionError, Dialect, DialectDisplay, TryFromDialect, TryIntoDialect};

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
    /// Returns `true` if this is the default null order for the given order type.
    ///
    /// From [the postgres docs][pg-docs]:
    ///
    /// > By default, null values sort as if larger than any non-null value; that is, `NULLS FIRST`
    /// > is the default for `DESC` order, and `NULLS LAST` otherwise.
    ///
    /// [pg-docs]: https://www.postgresql.org/docs/current/queries-order.html
    pub fn is_default_for(self, ot: OrderType) -> bool {
        self == match ot {
            OrderType::OrderDescending => Self::NullsFirst,
            OrderType::OrderAscending => Self::NullsLast,
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
    pub null_order: Option<NullOrder>,
}

impl TryFromDialect<sqlparser::ast::OrderByExpr> for OrderBy {
    fn try_from_dialect(
        value: sqlparser::ast::OrderByExpr,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let sqlparser::ast::OrderByExpr {
            expr,
            asc,
            nulls_first,
            ..
        } = value;
        Ok(Self {
            field: expr.try_into_dialect(dialect)?,
            order_type: asc.map(|asc| {
                if asc {
                    OrderType::OrderAscending
                } else {
                    OrderType::OrderDescending
                }
            }),
            null_order: nulls_first.map(|nulls_first| {
                if nulls_first {
                    NullOrder::NullsFirst
                } else {
                    NullOrder::NullsLast
                }
            }),
        })
    }
}

impl OrderBy {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            write!(f, "{}", self.field.display(dialect))?;
            if let Some(ot) = self.order_type {
                write!(f, " {}", ot)?;
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
        Ok(OrderClause {
            order_by: value.exprs.try_into_dialect(dialect)?,
        })
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
