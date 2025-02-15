use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, AstConversionError, Dialect, DialectDisplay, TryFromDialect, TryIntoDialect};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize, Arbitrary)]
pub enum CompoundSelectOperator {
    Union,
    DistinctUnion,
    Intersect,
    Except,
}

impl TryFrom<sqlparser::ast::SetOperator> for CompoundSelectOperator {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::SetOperator) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::SetOperator::Union => Ok(CompoundSelectOperator::Union),
            sqlparser::ast::SetOperator::Intersect => Ok(CompoundSelectOperator::Intersect),
            sqlparser::ast::SetOperator::Except => Ok(CompoundSelectOperator::Except),
            sqlparser::ast::SetOperator::Minus => {
                unsupported!("Neither Postgres nor MySQL support MINUS set operator")
            }
        }
    }
}

impl fmt::Display for CompoundSelectOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CompoundSelectOperator::Union => write!(f, "UNION"),
            CompoundSelectOperator::DistinctUnion => write!(f, "UNION DISTINCT"),
            CompoundSelectOperator::Intersect => write!(f, "INTERSECT"),
            CompoundSelectOperator::Except => write!(f, "EXCEPT"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize, Arbitrary)]
pub struct CompoundSelectStatement {
    pub selects: Vec<(Option<CompoundSelectOperator>, SelectStatement)>,
    pub order: Option<OrderClause>,
    pub limit_clause: LimitClause,
}

impl TryFromDialect<sqlparser::ast::Query> for CompoundSelectStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Query,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let sqlparser::ast::Query {
            body,
            order_by,
            limit,
            offset,
            with,
            ..
        } = value;
        if !matches!(*body, sqlparser::ast::SetExpr::SetOperation { .. }) {
            return failed!("Expected a set operation");
        }
        if with.is_some() {
            // TODO(mvzink): Figure out whether the sqlparser (probably) or nom-sql (probably not)
            // AST is putting CTEs in the right place
            return unsupported!("WITH clause is not supported in compound SELECT statements");
        }
        let selects = flatten_set_expr(body, dialect)?;
        let order = order_by.try_into_dialect(dialect)?;
        let limit_clause = (limit, offset).try_into_dialect(dialect)?;
        Ok(Self {
            selects,
            order,
            limit_clause,
        })
    }
}

fn flatten_set_expr(
    expr: Box<sqlparser::ast::SetExpr>,
    dialect: Dialect,
) -> Result<Vec<(Option<CompoundSelectOperator>, SelectStatement)>, AstConversionError> {
    match *expr {
        sqlparser::ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            // Recursively flatten left and right branches
            let mut left_vec = flatten_set_expr(left, dialect)?;
            let mut right_vec = flatten_set_expr(right, dialect)?;

            // The right part gets the operator
            if let Some((ref mut right_op @ None, _stmt)) = right_vec.first_mut() {
                // XXX(mvzink): nom-sql implicitly parses `UNION` as `UNION DISTINCT`, and has a
                // separate operator variant for it instead of separately representing the
                // quantifier. So we mimic that here by checking for an explicit `ALL`.
                //
                // However, if you look at `SqlIncorporator::add_compound_query`, it looks like we
                // throw the operator away entirely and assume they are all `UNION` (which,
                // according to the parser, would be `UNION DISTINCT`). Worse yet, from
                // `SqlToMirConverter::compound_query_to_mir`, it appears that we actually interpret
                // plain `UNION` as `UNION ALL`! So unless I am missing these operators being
                // handled sanely elsewhere, maybe in a rewrite pass, we may be doing things wildly
                // wrong any time we encounter any compound select statement.
                let new_op = match op.try_into()? {
                    CompoundSelectOperator::Union
                        if set_quantifier != sqlparser::ast::SetQuantifier::All =>
                    {
                        CompoundSelectOperator::DistinctUnion
                    }
                    new_op => new_op,
                };
                right_op.replace(new_op);
            }

            left_vec.extend(right_vec);
            Ok(left_vec)
        }
        sqlparser::ast::SetExpr::Select(select) => {
            Ok(vec![(None, (*select).try_into_dialect(dialect)?)])
        }
        sqlparser::ast::SetExpr::Query(query) => {
            // TODO: This nested (parenthesized) query could also be compounded, but readyset-sql
            // AST doesn't support that. We might be able to unnest it somehow, or update the
            // readyset-sql AST to support it.
            Ok(vec![(None, (*query).try_into_dialect(dialect)?)])
        }
        _ => failed!("Unexpected SetExpr variant: {:?}", expr),
    }
}

impl DialectDisplay for CompoundSelectStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            for (op, sel) in &self.selects {
                if let Some(o) = op {
                    write!(f, " {}", o)?;
                }
                write!(f, " {}", sel.display(dialect))?;
            }

            if let Some(ord) = &self.order {
                write!(f, " {}", ord.display(dialect))?;
            }

            if self.limit_clause.is_empty() {
                write!(f, " {}", self.limit_clause.display(dialect))?;
            }

            Ok(())
        })
    }
}
