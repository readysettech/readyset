use std::fmt::{self, Display as _};

use itertools::Itertools;
use proptest::option;
use proptest::prelude::{Arbitrary, BoxedStrategy};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::dialect_display::CommaSeparatedList;
use crate::{ast::*, AstConversionError, Dialect, DialectDisplay};

#[derive(
    Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Default, Serialize, Deserialize, Arbitrary,
)]
pub struct GroupByClause {
    pub fields: Vec<FieldReference>,
}

impl TryFrom<sqlparser::ast::GroupByExpr> for GroupByClause {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::GroupByExpr) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::GroupByExpr::Expressions(exprs, _modifiers) => Ok(GroupByClause {
                fields: exprs.into_iter().map(TryInto::try_into).try_collect()?,
            }),
            sqlparser::ast::GroupByExpr::All(_) => {
                unsupported!("Snowflake/DuckDB/ClickHouse group by syntax {value:?}")
            }
        }
    }
}

impl DialectDisplay for GroupByClause {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "GROUP BY {}",
                self.fields
                    .iter()
                    .map(|field| field.display(dialect))
                    .join(", ")
            )
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct JoinClause {
    pub operator: JoinOperator,
    pub right: JoinRightSide,
    pub constraint: JoinConstraint,
}

impl TryFrom<sqlparser::ast::Join> for JoinClause {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::Join) -> Result<Self, Self::Error> {
        Ok(Self {
            operator: (&value.join_operator).into(),
            constraint: value.join_operator.try_into()?,
            right: JoinRightSide::Table(value.relation.try_into()?),
        })
    }
}

impl DialectDisplay for JoinClause {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "{} {} {}",
                self.operator,
                self.right.display(dialect),
                self.constraint.display(dialect)
            )
        })
    }
}

#[derive(
    Clone, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct CommonTableExpr {
    pub name: SqlIdentifier,
    pub statement: SelectStatement,
}

impl TryFrom<sqlparser::ast::Cte> for CommonTableExpr {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::Cte) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.alias.name.into(),
            statement: (*value.query).try_into()?,
        })
    }
}

impl DialectDisplay for CommonTableExpr {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "{} AS ({})",
                dialect.quote_identifier(&self.name),
                self.statement.display(dialect)
            )
        })
    }
}

/// AST representation of the values that can be in LIMIT clause.
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LimitValue {
    /// A explicitly provided non-negative integer
    // TODO: Make this enforce non-negative
    Literal(Literal),
    /// Explicitly provided ALL value (Postgres only)
    All,
}

impl DialectDisplay for LimitValue {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Literal(literal) => literal.display(dialect).fmt(f),
            Self::All => write!(f, "ALL"),
        })
    }
}

impl From<i64> for LimitValue {
    fn from(other: i64) -> LimitValue {
        LimitValue::Literal(other.into())
    }
}

impl From<Literal> for LimitValue {
    fn from(literal: Literal) -> LimitValue {
        LimitValue::Literal(literal)
    }
}

/// AST representation of the SQL limit and offset clauses.
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LimitClause {
    /// The standard limit and offset syntax: `LIMIT <limit> OFFSET <offset>`.
    LimitOffset {
        limit: Option<LimitValue>,
        offset: Option<Literal>,
    },
    /// MySQL's alternative limit and offset syntax: `LIMIT <offset>, <limit>`.
    OffsetCommaLimit { offset: Literal, limit: LimitValue },
}

/// Options for generating arbitrary [`LimitClause`]s
#[derive(Default, Debug, Clone, Copy)]
pub struct LimitClauseArbitraryOptions {
    /// [`LimitClause`] has differences between mysql and postgres, so this dialect controls which
    /// one we use to generate arbitrary [`LimitClause`]s
    pub dialect: Option<Dialect>,
}

impl Arbitrary for LimitClause {
    type Parameters = LimitClauseArbitraryOptions;

    type Strategy = BoxedStrategy<LimitClause>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        use LimitClause::*;

        match args.dialect {
            Some(Dialect::PostgreSQL) => {
                let offset_value = prop_oneof![
                    (0i64..=i64::MAX).prop_map(Literal::Integer),
                    Just(Literal::Null),
                ];
                let limit_value = prop_oneof![
                    (0i64..=i64::MAX).prop_map(|v| LimitValue::Literal(Literal::Integer(v))),
                    Just(LimitValue::Literal(Literal::Null)),
                    Just(LimitValue::All),
                ];
                (option::of(limit_value), option::of(offset_value))
                    .prop_map(|(limit, offset)| LimitOffset { limit, offset })
                    .boxed()
            }
            Some(Dialect::MySQL) => {
                let limit_literal = (0i64..=i64::MAX).prop_map(Literal::Integer);

                prop_oneof![
                    (limit_literal.clone(), option::of(limit_literal.clone())).prop_map(
                        |(limit, offset)| {
                            LimitOffset {
                                limit: Some(LimitValue::Literal(limit)),
                                offset,
                            }
                        }
                    ),
                    (limit_literal.clone(), limit_literal).prop_map(|(limit, offset)| {
                        OffsetCommaLimit {
                            limit: LimitValue::Literal(limit),
                            offset,
                        }
                    })
                ]
                .boxed()
            }
            None => {
                let limit_literal = (0i64..=i64::MAX).prop_map(Literal::Integer);
                (limit_literal.clone(), option::of(limit_literal))
                    .prop_map(|(limit, offset)| LimitOffset {
                        limit: Some(LimitValue::Literal(limit)),
                        offset,
                    })
                    .boxed()
            }
        }
    }
}

impl LimitClause {
    /// Returns an [`Option`] with the [`Literal`] value corresponding to the `limit` clause.
    /// Returns [`None`] if there's no `limit`.
    pub fn limit(&self) -> Option<&Literal> {
        let limit = match self {
            LimitClause::LimitOffset { limit, .. } => limit.as_ref(),
            LimitClause::OffsetCommaLimit { limit, .. } => Some(limit),
        };
        match limit {
            Some(LimitValue::Literal(limit)) => Some(limit),
            Some(LimitValue::All) => None,
            None => None,
        }
    }

    /// Returns an [`Option`] with the [`Literal`] value corresponding to the `offset` clause.
    /// Returns [`None`] if there's no `offset`.
    pub fn offset(&self) -> Option<&Literal> {
        match self {
            LimitClause::LimitOffset { offset, .. } => offset.as_ref(),
            LimitClause::OffsetCommaLimit { offset, .. } => Some(offset),
        }
    }

    /// Returns two [`Option`]s, both with mutable [`Literal`] values corresponding to the `limit`
    /// and `offset` clauses respectively.
    pub fn limit_and_offset_mut(&mut self) -> (Option<&mut LimitValue>, Option<&mut Literal>) {
        match self {
            LimitClause::LimitOffset { limit, offset } => (limit.as_mut(), offset.as_mut()),
            LimitClause::OffsetCommaLimit { offset, limit } => (Some(limit), Some(offset)),
        }
    }

    /// Whether this [`LimitClause`] is empty (has no `limit` and no `offset`) or not.
    pub fn is_empty(&self) -> bool {
        match self {
            LimitClause::LimitOffset { limit, offset } => limit.is_some() || offset.is_some(),
            LimitClause::OffsetCommaLimit { .. } => false,
        }
    }
}

impl Default for LimitClause {
    fn default() -> Self {
        Self::LimitOffset {
            limit: None,
            offset: None,
        }
    }
}

impl DialectDisplay for LimitClause {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            match self {
                LimitClause::LimitOffset { limit, offset } => {
                    if let Some(limit) = limit {
                        write!(f, "LIMIT {}", limit.display(dialect))?;
                    }
                    if let Some(offset) = offset {
                        if limit.is_some() {
                            write!(f, " ")?;
                        }
                        write!(f, "OFFSET {}", offset.display(dialect))?;
                    }
                }
                LimitClause::OffsetCommaLimit { offset, limit } => {
                    write!(
                        f,
                        "LIMIT {}, {}",
                        offset.display(dialect),
                        limit.display(dialect)
                    )?;
                }
            }

            Ok(())
        })
    }
}

#[derive(
    Clone, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SelectStatement {
    pub ctes: Vec<CommonTableExpr>,
    pub distinct: bool,
    pub fields: Vec<FieldDefinitionExpr>,
    pub tables: Vec<TableExpr>,
    pub join: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<GroupByClause>,
    pub having: Option<Expr>,
    pub order: Option<OrderClause>,
    pub limit_clause: LimitClause,
}

impl SelectStatement {
    pub fn contains_aggregate_select(&self) -> bool {
        self.fields.iter().any(|e| match e {
            FieldDefinitionExpr::Expr { expr, .. } => match expr {
                Expr::Call(func) => matches!(
                    func,
                    FunctionExpr::Avg { .. }
                        | FunctionExpr::Count { .. }
                        | FunctionExpr::CountStar
                        | FunctionExpr::Sum { .. }
                        | FunctionExpr::Max(_)
                        | FunctionExpr::Min(_)
                        | FunctionExpr::GroupConcat { .. }
                        | FunctionExpr::JsonObjectAgg { .. }
                ),
                Expr::NestedSelect(select) => select.contains_aggregate_select(),
                _ => false,
            },
            _ => false,
        })
    }
}

impl TryFrom<sqlparser::ast::Query> for SelectStatement {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::Query) -> Result<Self, Self::Error> {
        let sqlparser::ast::Query {
            body,
            order_by,
            limit,
            offset,
            with,
            ..
        } = value;
        match *body {
            sqlparser::ast::SetExpr::Select(select) => {
                let (tables, join_clauses): (
                    Vec<crate::ast::TableExpr>,
                    Vec<Vec<crate::ast::JoinClause>>,
                ) = select
                    .from
                    .into_iter()
                    .map(|table_with_joins| {
                        Ok((
                            table_with_joins.relation.try_into()?,
                            table_with_joins
                                .joins
                                .into_iter()
                                .map(TryInto::try_into)
                                .try_collect()?,
                        ))
                    })
                    .collect::<Result<Vec<(TableExpr, Vec<JoinClause>)>, _>>()?
                    .into_iter()
                    .unzip();
                let join = join_clauses.into_iter().flatten().collect();
                Ok(SelectStatement {
                    ctes: if let Some(sqlparser::ast::With { cte_tables, .. }) = with {
                        cte_tables
                            .into_iter()
                            .map(TryInto::try_into)
                            .try_collect()?
                    } else {
                        Vec::new()
                    },
                    distinct: matches!(select.distinct, Some(sqlparser::ast::Distinct::Distinct)),
                    fields: select
                        .projection
                        .into_iter()
                        .map(TryInto::try_into)
                        .try_collect()?,
                    tables,
                    join,
                    where_clause: select.selection.map(TryInto::try_into).transpose()?,
                    group_by: {
                        let group_by: GroupByClause = select.group_by.try_into()?;
                        if group_by.fields.is_empty() {
                            None
                        } else {
                            Some(group_by)
                        }
                    },
                    having: select.having.map(TryInto::try_into).transpose()?,
                    order: order_by.map(TryInto::try_into).transpose()?,
                    limit_clause: crate::ast::LimitClause::LimitOffset {
                        limit: limit
                            .map(|expr| {
                                if let crate::ast::Expr::Literal(literal) = expr.try_into()? {
                                    Ok(crate::ast::LimitValue::Literal(literal))
                                } else {
                                    not_yet_implemented!("non-literal limit expression")
                                }
                            })
                            .transpose()?,
                        offset: offset
                            .map(|sqlparser::ast::Offset { value: expr, .. }| {
                                if let crate::ast::Expr::Literal(literal) = expr.try_into()? {
                                    Ok(literal)
                                } else {
                                    not_yet_implemented!("non-literal offset expression")
                                }
                            })
                            .transpose()?,
                    },
                })
            }
            _ => failed!("Should only be called on a SELECT query"),
        }
    }
}

impl TryFrom<Box<sqlparser::ast::Query>> for SelectStatement {
    type Error = AstConversionError;

    fn try_from(value: Box<sqlparser::ast::Query>) -> Result<Self, Self::Error> {
        (*value).try_into()
    }
}

impl TryFrom<Box<sqlparser::ast::Query>> for Box<SelectStatement> {
    type Error = AstConversionError;

    fn try_from(value: Box<sqlparser::ast::Query>) -> Result<Self, Self::Error> {
        Ok(Box::new(value.try_into()?))
    }
}

impl DialectDisplay for SelectStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            if !self.ctes.is_empty() {
                write!(
                    f,
                    "WITH {} ",
                    CommaSeparatedList::from(&self.ctes).display(dialect)
                )?;
            }

            write!(f, "SELECT ")?;
            if self.distinct {
                write!(f, "DISTINCT ")?;
            }

            write!(
                f,
                "{}",
                CommaSeparatedList::from(&self.fields).display(dialect)
            )?;

            if !self.tables.is_empty() {
                write!(f, " FROM ")?;
                write!(
                    f,
                    "{}",
                    CommaSeparatedList::from(&self.tables).display(dialect)
                )?;
            }

            for jc in &self.join {
                write!(f, " {}", jc.display(dialect))?;
            }

            if let Some(where_clause) = &self.where_clause {
                write!(f, " WHERE {}", where_clause.display(dialect))?;
            }
            if let Some(group_by) = &self.group_by {
                write!(f, " {}", group_by.display(dialect))?;
            }
            if let Some(having) = &self.having {
                write!(f, " HAVING {}", having.display(dialect))?;
            }
            if let Some(order) = &self.order {
                write!(f, " {}", order.display(dialect))?;
            }
            if self.limit_clause.limit().is_some() || self.limit_clause.offset().is_some() {
                write!(f, " {}", self.limit_clause.display(dialect))?;
            }

            Ok(())
        })
    }
}
