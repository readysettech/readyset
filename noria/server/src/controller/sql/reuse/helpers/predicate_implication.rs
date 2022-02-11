use std::convert::TryFrom;

use nom_sql::Expression::*;
use nom_sql::{BinaryOperator, Expression, Literal};
use noria_errors::{internal, unsupported, ReadySetError, ReadySetResult};
use noria_sql_passes::LogicalOp;

use crate::controller::sql::query_graph::JoinPredicate;

fn direct_elimination(
    op1: BinaryOperator,
    op2: BinaryOperator,
) -> Result<Option<BinaryOperator>, ReadySetError> {
    let res = match op1 {
        BinaryOperator::Equal => match op2 {
            BinaryOperator::Equal => Some(BinaryOperator::Equal),
            BinaryOperator::Less => Some(BinaryOperator::Less),
            BinaryOperator::Greater => Some(BinaryOperator::Greater),
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::NotEqual => match op2 {
            BinaryOperator::Equal => Some(BinaryOperator::NotEqual),
            BinaryOperator::Less => None,
            BinaryOperator::Greater => None,
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::Less => match op2 {
            BinaryOperator::Equal => Some(BinaryOperator::Less),
            BinaryOperator::Less => Some(BinaryOperator::Less),
            BinaryOperator::Greater => None,
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::LessOrEqual => match op2 {
            BinaryOperator::Equal => Some(BinaryOperator::LessOrEqual),
            BinaryOperator::Less => Some(BinaryOperator::LessOrEqual),
            BinaryOperator::Greater => None,
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::Greater => match op2 {
            BinaryOperator::Equal => Some(BinaryOperator::Greater),
            BinaryOperator::Less => None,
            BinaryOperator::Greater => Some(BinaryOperator::Greater),
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::GreaterOrEqual => match op2 {
            BinaryOperator::Equal => Some(BinaryOperator::GreaterOrEqual),
            BinaryOperator::Less => None,
            BinaryOperator::Greater => Some(BinaryOperator::Greater),
            _ => {
                unsupported!();
            }
        },
        _ => None,
    };

    Ok(res)
}

fn check_op_elimination<T>(
    nv: T,
    ev: T,
    nop: BinaryOperator,
    eop: BinaryOperator,
) -> Result<bool, ReadySetError>
where
    T: PartialOrd,
{
    let ep_op_needed = if nv == ev {
        direct_elimination(nop, BinaryOperator::Equal)?
    } else if nv < ev {
        direct_elimination(nop, BinaryOperator::Less)?
    } else if nv > ev {
        direct_elimination(nop, BinaryOperator::Greater)?
    } else {
        None
    };

    match ep_op_needed {
        None => Ok(false),
        Some(op) => {
            // TODO(malte): the condition is actually weaker than
            // this inequality suggests -- it's sufficient for the
            // needed operator to be *weaker* than ep.operator to
            // reject the EQG.
            Ok(eop == op)
        }
    }
}

/// Returns true if two sets of join predicates are equivalent, regardless of the order of the
/// predicates or the order of fields given to the predicates
///
/// # Invariants
///
/// All of the passed conditions must be direct comparisons on fields.
pub fn join_predicates_are_equivalent(
    nps: &[JoinPredicate],
    eps: &[JoinPredicate],
) -> ReadySetResult<bool> {
    fn cols(pred: &JoinPredicate) -> ReadySetResult<(&nom_sql::Column, &nom_sql::Column)> {
        let l_col = match &pred.left {
            Expression::Column(f) => f,
            expr => unsupported!("Unsupported join predicate on non-column {}", expr),
        };
        let r_col = match &pred.right {
            Expression::Column(f) => f,
            expr => unsupported!("Unsupported join predicate on non-column {}", expr),
        };
        Ok(if l_col < r_col {
            (l_col, r_col)
        } else {
            (r_col, l_col)
        })
    }

    let mut np_fields = nps.iter().map(cols).collect::<Result<Vec<_>, _>>()?;
    np_fields.sort_unstable();

    let mut ep_fields = eps.iter().map(cols).collect::<Result<Vec<_>, _>>()?;
    ep_fields.sort_unstable();

    Ok(np_fields == ep_fields)
}

/// Direct elimination for complex predicates with nested `and` and `or` expressions
pub fn complex_predicate_implies(np: &Expression, ep: &Expression) -> Result<bool, ReadySetError> {
    match ep {
        BinaryOp {
            lhs: e_lhs,
            rhs: e_rhs,
            op: e_op,
        } => {
            if let Ok(logical_op) = LogicalOp::try_from(*e_op) {
                {
                    if let BinaryOp {
                        lhs: n_lhs,
                        rhs: n_rhs,
                        op: n_op,
                    } = np
                    {
                        if n_op == e_op {
                            return Ok(complex_predicate_implies(&*n_lhs, &*e_lhs)?
                                && complex_predicate_implies(&*n_rhs, &*e_rhs)?
                                || (complex_predicate_implies(&*n_lhs, &*e_rhs)?
                                    && complex_predicate_implies(&*n_rhs, &*e_lhs)?));
                        }
                    }

                    match logical_op {
                        LogicalOp::And => Ok(complex_predicate_implies(np, &*e_lhs)?
                            && complex_predicate_implies(np, &*e_rhs)?),
                        LogicalOp::Or => Ok(complex_predicate_implies(np, &*e_lhs)?
                            || complex_predicate_implies(np, &*e_rhs)?),
                    }
                }
            } else if let BinaryOp {
                lhs: n_lhs,
                rhs: n_rhs,
                op: n_op,
            } = np
            {
                match n_op {
                    BinaryOperator::And => Ok(complex_predicate_implies(n_lhs, ep)?
                        || complex_predicate_implies(n_rhs, ep)?),
                    BinaryOperator::Or => Ok(complex_predicate_implies(n_lhs, ep)?
                        && complex_predicate_implies(n_rhs, ep)?),
                    _ => Ok(n_lhs == e_lhs && predicate_implies((*n_op, n_rhs), (*e_op, e_rhs))?),
                }
            } else {
                internal!()
            }
        }
        _ => internal!(),
    }
}

fn predicate_implies(
    (n_op, n_rhs): (BinaryOperator, &Expression),
    (e_op, e_rhs): (BinaryOperator, &Expression),
) -> Result<bool, ReadySetError> {
    // use Finkelstein-style direct elimination to check if this new query graph predicate
    // implies the corresponding predicates in the existing query graph
    match n_rhs {
        Expression::Literal(Literal::String(nv)) => match e_rhs {
            Expression::Literal(Literal::String(ref ev)) => {
                check_op_elimination(nv, ev, n_op, e_op)
            }
            Expression::Literal(_) => Ok(false),
            _ => unsupported!(),
        },
        Expression::Literal(Literal::Integer(ref nv)) => match e_rhs {
            Expression::Literal(Literal::Integer(ref ev)) => {
                check_op_elimination(nv, ev, n_op, e_op)
            }
            Expression::Literal(_) => Ok(false),
            _ => unsupported!(),
        },
        Expression::Literal(Literal::Null) => match e_rhs {
            Expression::Literal(Literal::Null)
                if n_op != BinaryOperator::Is
                    && e_op != BinaryOperator::Is
                    && n_op != BinaryOperator::IsNot
                    && e_op != BinaryOperator::IsNot =>
            {
                Ok(true)
            }
            Expression::Literal(_) => Ok(false),
            _ => unsupported!(),
        },
        _ => unsupported!(),
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{Column, Literal};

    use super::*;

    #[test]
    fn predicate_implication() {
        let pa = (
            BinaryOperator::Less,
            &Expression::Literal(Literal::Integer(10.into())),
        );
        let pb = (
            BinaryOperator::Less,
            &Expression::Literal(Literal::Integer(20.into())),
        );
        let pc = (
            BinaryOperator::Equal,
            &Expression::Literal(Literal::Integer(5.into())),
        );

        assert!(predicate_implies(pa, pb).unwrap());
        assert!(!predicate_implies(pb, pa).unwrap());
        assert!(!predicate_implies(pa, pc).unwrap());
        assert!(predicate_implies(pc, pa).unwrap());
    }

    #[test]
    fn complex_predicate_implication_or() {
        let pa = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("a"))),
            op: BinaryOperator::Less,
            rhs: Box::new(Expression::Literal(Literal::Integer(20.into()))),
        };
        let pb = Expression::BinaryOp {
            op: BinaryOperator::Greater,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Expression::Literal(Literal::Integer(60.into()))),
        };
        let pc = Expression::BinaryOp {
            op: BinaryOperator::Less,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Literal(Literal::Integer(10.into()))),
        };
        let pd = Expression::BinaryOp {
            op: BinaryOperator::Greater,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Literal(Literal::Integer(80.into()))),
        };

        // a < 20 or a > 60
        let cp1 = Expression::BinaryOp {
            lhs: Box::new(pa.clone()),
            rhs: Box::new(pb.clone()),
            op: BinaryOperator::Or,
        };

        // a < 10 or a > 80
        let cp2 = Expression::BinaryOp {
            lhs: Box::new(pc),
            rhs: Box::new(pd),
            op: BinaryOperator::Or,
        };

        // a > 60 or a < 20
        let cp3 = Expression::BinaryOp {
            lhs: Box::new(pb),
            rhs: Box::new(pa),
            op: BinaryOperator::Or,
        };

        assert!(complex_predicate_implies(&cp2, &cp1).unwrap());
        assert!(!complex_predicate_implies(&cp1, &cp2).unwrap());
        assert!(complex_predicate_implies(&cp2, &cp3).unwrap());
        assert!(!complex_predicate_implies(&cp3, &cp2).unwrap());
    }

    #[test]
    fn complex_predicate_implication_and() {
        let pa = Expression::BinaryOp {
            op: BinaryOperator::Greater,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Expression::Literal(Literal::Integer(20.into()))),
        };
        let pb = Expression::BinaryOp {
            op: BinaryOperator::Less,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Expression::Literal(Literal::Integer(60.into()))),
        };
        let pc = Expression::BinaryOp {
            op: BinaryOperator::Greater,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Expression::Literal(Literal::Integer(10.into()))),
        };
        let pd = Expression::BinaryOp {
            op: BinaryOperator::Less,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Expression::Literal(Literal::Integer(80.into()))),
        };

        // a > 20 and a < 60
        let cp1 = Expression::BinaryOp {
            lhs: Box::new(pa.clone()),
            rhs: Box::new(pb.clone()),
            op: BinaryOperator::And,
        };

        // a > 10 and a < 80
        let cp2 = Expression::BinaryOp {
            lhs: Box::new(pc),
            rhs: Box::new(pd),
            op: BinaryOperator::And,
        };

        // a < 60 and a > 20
        let cp3 = Expression::BinaryOp {
            lhs: Box::new(pb),
            rhs: Box::new(pa),
            op: BinaryOperator::And,
        };

        assert!(complex_predicate_implies(&cp1, &cp2).unwrap());
        assert!(!complex_predicate_implies(&cp2, &cp1).unwrap());
        assert!(complex_predicate_implies(&cp3, &cp2).unwrap());
        assert!(!complex_predicate_implies(&cp2, &cp3).unwrap());
    }

    #[test]
    fn complex_predicate_implication_superset_or() {
        let pa = Expression::BinaryOp {
            op: BinaryOperator::Less,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Expression::Literal(Literal::Integer(20.into()))),
        };
        let pb = Expression::BinaryOp {
            op: BinaryOperator::Greater,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Expression::Literal(Literal::Integer(60.into()))),
        };

        // a < 20 or a > 60
        let cp1 = Expression::BinaryOp {
            lhs: Box::new(pa.clone()),
            rhs: Box::new(pb.clone()),
            op: BinaryOperator::Or,
        };

        assert!(complex_predicate_implies(&pa, &cp1).unwrap());
        assert!(complex_predicate_implies(&pb, &cp1).unwrap());
        assert!(!complex_predicate_implies(&cp1, &pa).unwrap());
        assert!(!complex_predicate_implies(&cp1, &pb).unwrap());
    }

    #[test]
    fn complex_predicate_implication_subset_and() {
        let pa = Expression::BinaryOp {
            op: BinaryOperator::Greater,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Expression::Literal(Literal::Integer(20.into()))),
        };
        let pb = Expression::BinaryOp {
            op: BinaryOperator::Less,
            lhs: Box::new(Expression::Column(Column::from("a"))),
            rhs: Box::new(Expression::Literal(Literal::Integer(60.into()))),
        };

        // a > 20 and a < 60
        let cp1 = Expression::BinaryOp {
            lhs: Box::new(pa.clone()),
            rhs: Box::new(pb.clone()),
            op: BinaryOperator::And,
        };

        assert!(!complex_predicate_implies(&pa, &cp1).unwrap());
        assert!(!complex_predicate_implies(&pb, &cp1).unwrap());
        assert!(complex_predicate_implies(&cp1, &pa).unwrap());
        assert!(complex_predicate_implies(&cp1, &pb).unwrap());
    }

    #[test]
    fn is_null_does_not_imply_is_not_null() {
        assert!(!complex_predicate_implies(
            &Expression::BinaryOp {
                lhs: Box::new(Expression::Column("t.a".into())),
                op: BinaryOperator::Is,
                rhs: Box::new(Expression::Literal(Literal::Null))
            },
            &Expression::BinaryOp {
                lhs: Box::new(Expression::Column("t.a".into())),
                op: BinaryOperator::IsNot,
                rhs: Box::new(Expression::Literal(Literal::Null))
            }
        )
        .unwrap())
    }
}
