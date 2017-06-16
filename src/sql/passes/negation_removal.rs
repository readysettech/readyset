use nom_sql::{ConditionExpression, ConditionTree, SqlQuery, Operator, JoinConstraint,
              ConditionBase};

use std::mem;

pub trait NegationRemoval {
    fn remove_negation(self) -> SqlQuery;
}

fn normalize_condition_expr(ce: &mut ConditionExpression, negate: bool) {
    match *ce {
        ConditionExpression::LogicalOp(ConditionTree {
            ref mut operator,
            box ref mut left,
            box ref mut right,
        }) => {
            if negate {
                *operator = match *operator {
                    Operator::And => Operator::Or,
                    Operator::Or => Operator::And,
                    _ => unreachable!(),
                };
            }

            normalize_condition_expr(left, negate);
            normalize_condition_expr(right, negate);
        }
        ConditionExpression::ComparisonOp(ConditionTree {
            ref mut operator,
            box ref mut left,
            box ref mut right,
        }) => {
            if negate {
                *operator = match *operator {
                    Operator::Equal => Operator::NotEqual,
                    Operator::NotEqual => Operator::Equal,
                    Operator::Greater => Operator::LessOrEqual,
                    Operator::GreaterOrEqual => Operator::Less,
                    Operator::Less => Operator::GreaterOrEqual,
                    Operator::LessOrEqual => Operator::Greater,
                    _ => unreachable!(),
                };
            }

            normalize_condition_expr(left, false);
            normalize_condition_expr(right, false);
        }
        ConditionExpression::NegationOp(_) => {
            let inner = if let &mut ConditionExpression::NegationOp(box ref mut inner) = ce {
                mem::replace(inner, ConditionExpression::Base(ConditionBase::Placeholder))
            } else {
                unreachable!()
            };
            mem::replace(ce, inner);
            normalize_condition_expr(ce, !negate);
        }
        ConditionExpression::Base(_) => {}
    }
}

impl NegationRemoval for SqlQuery {
    fn remove_negation(mut self) -> SqlQuery {
        if let SqlQuery::Select(ref mut s) = self {
            if let Some(ref mut w) = s.where_clause {
                normalize_condition_expr(w, false);
            }

            for j in s.join.iter_mut() {
                if let JoinConstraint::On(ref mut ce) = j.constraint {
                    normalize_condition_expr(ce, false);
                }
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_normalizes() {
        let mut expr = ConditionExpression::NegationOp(
            Box::new(ConditionExpression::LogicalOp(ConditionTree {
                operator: Operator::And,
                left: Box::new(ConditionExpression::ComparisonOp(ConditionTree {
                    operator: Operator::Less,
                    left: Box::new(ConditionExpression::Base(ConditionBase::Field("a".into()))),
                    right: Box::new(ConditionExpression::Base(ConditionBase::Field("b".into()))),
                })),
                right: Box::new(ConditionExpression::ComparisonOp(ConditionTree {
                    operator: Operator::Equal,
                    left: Box::new(ConditionExpression::Base(ConditionBase::Field("c".into()))),
                    right: Box::new(ConditionExpression::Base(ConditionBase::Field("b".into()))),
                })),
            })),
        );

        let target = ConditionExpression::LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Box::new(ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::GreaterOrEqual,
                left: Box::new(ConditionExpression::Base(ConditionBase::Field("a".into()))),
                right: Box::new(ConditionExpression::Base(ConditionBase::Field("b".into()))),
            })),
            right: Box::new(ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::NotEqual,
                left: Box::new(ConditionExpression::Base(ConditionBase::Field("c".into()))),
                right: Box::new(ConditionExpression::Base(ConditionBase::Field("b".into()))),
            })),
        });

        normalize_condition_expr(&mut expr, false);
        assert_eq!(expr, target);
    }
}
