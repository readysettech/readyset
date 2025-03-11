use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, AstConversionError, Dialect, DialectDisplay, TryFromDialect, TryIntoDialect};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize, Arbitrary)]
pub enum CompoundSelectOperator {
    UnionAll,
    UnionDistinct,
    Intersect,
    Except,
}

impl TryFrom<(sqlparser::ast::SetOperator, sqlparser::ast::SetQuantifier)>
    for CompoundSelectOperator
{
    type Error = AstConversionError;

    fn try_from(
        (op, quantifier): (sqlparser::ast::SetOperator, sqlparser::ast::SetQuantifier),
    ) -> Result<Self, Self::Error> {
        match op {
            sqlparser::ast::SetOperator::Union => match quantifier {
                sqlparser::ast::SetQuantifier::All => Ok(CompoundSelectOperator::UnionAll),
                sqlparser::ast::SetQuantifier::Distinct | sqlparser::ast::SetQuantifier::None => {
                    Ok(CompoundSelectOperator::UnionDistinct)
                }
                _ => unsupported!("set quantifier {quantifier}"),
            },
            sqlparser::ast::SetOperator::Intersect => Ok(CompoundSelectOperator::Intersect),
            sqlparser::ast::SetOperator::Except => Ok(CompoundSelectOperator::Except),
            sqlparser::ast::SetOperator::Minus => {
                unsupported!("MINUS set operator")
            }
        }
    }
}

impl fmt::Display for CompoundSelectOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CompoundSelectOperator::UnionAll => write!(f, "UNION ALL"),
            CompoundSelectOperator::UnionDistinct => write!(f, "UNION DISTINCT"),
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
            limit_clause,
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
        let limit_clause: Option<LimitClause> = limit_clause.try_into_dialect(dialect)?;
        Ok(Self {
            selects,
            order,
            limit_clause: limit_clause.unwrap_or(LimitClause::LimitOffset {
                limit: None,
                offset: None,
            }),
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

            // The right part gets the operator in nom-sql
            if let Some((ref mut right_op @ None, _stmt)) = right_vec.first_mut() {
                right_op.replace((op, set_quantifier).try_into()?);
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
