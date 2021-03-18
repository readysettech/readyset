use crate::ReadySetResult;
use nom_sql::ConditionExpression::*;
use nom_sql::{BinaryOperator, ConditionBase, ConditionExpression, ConditionTree, Literal};
use noria::{internal, unsupported, ReadySetError};

fn direct_elimination(
    op1: &BinaryOperator,
    op2: &BinaryOperator,
) -> Result<Option<BinaryOperator>, ReadySetError> {
    let res = match *op1 {
        BinaryOperator::Equal => match *op2 {
            BinaryOperator::Equal => Some(BinaryOperator::Equal),
            BinaryOperator::Less => Some(BinaryOperator::Less),
            BinaryOperator::Greater => Some(BinaryOperator::Greater),
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::NotEqual => match *op2 {
            BinaryOperator::Equal => Some(BinaryOperator::NotEqual),
            BinaryOperator::Less => None,
            BinaryOperator::Greater => None,
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::Less => match *op2 {
            BinaryOperator::Equal => Some(BinaryOperator::Less),
            BinaryOperator::Less => Some(BinaryOperator::Less),
            BinaryOperator::Greater => None,
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::LessOrEqual => match *op2 {
            BinaryOperator::Equal => Some(BinaryOperator::LessOrEqual),
            BinaryOperator::Less => Some(BinaryOperator::LessOrEqual),
            BinaryOperator::Greater => None,
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::Greater => match *op2 {
            BinaryOperator::Equal => Some(BinaryOperator::Greater),
            BinaryOperator::Less => None,
            BinaryOperator::Greater => Some(BinaryOperator::Greater),
            _ => {
                unsupported!();
            }
        },
        BinaryOperator::GreaterOrEqual => match *op2 {
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
    nop: &BinaryOperator,
    eop: &BinaryOperator,
) -> Result<bool, ReadySetError>
where
    T: PartialOrd,
{
    let ep_op_needed = if nv == ev {
        direct_elimination(nop, &BinaryOperator::Equal)?
    } else if nv < ev {
        direct_elimination(nop, &BinaryOperator::Less)?
    } else if nv > ev {
        direct_elimination(nop, &BinaryOperator::Greater)?
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
            Ok(*eop == op)
        }
    }
}

pub fn predicate_is_equivalent(np: &ConditionTree, ep: &ConditionTree) -> ReadySetResult<bool> {
    let nl_col = match *np.left {
        ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
        _ => internal!(),
    };
    let nr_col = match *np.right {
        ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
        _ => internal!(),
    };

    let el_col = match *ep.left {
        ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
        _ => internal!(),
    };
    let er_col = match *ep.right {
        ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
        _ => internal!(),
    };

    Ok((nl_col == el_col && nr_col == er_col) || (nl_col == er_col && nr_col == el_col))
}

/// Direct elimination for complex predicates with nested `and` and `or` expressions
pub fn complex_predicate_implies(
    np: &ConditionExpression,
    ep: &ConditionExpression,
) -> Result<bool, ReadySetError> {
    match *ep {
        LogicalOp(ref ect) => {
            if let LogicalOp(ref nct) = *np {
                if nct.operator == ect.operator {
                    return Ok(complex_predicate_implies(&*nct.left, &*ect.left)?
                        && complex_predicate_implies(&*nct.right, &*ect.right)?
                        || (complex_predicate_implies(&*nct.left, &*ect.right)?
                            && complex_predicate_implies(&*nct.right, &*ect.left)?));
                }
            }

            match ect.operator {
                BinaryOperator::And => Ok(complex_predicate_implies(np, &*ect.left)?
                    && complex_predicate_implies(np, &*ect.right)?),
                BinaryOperator::Or => Ok(complex_predicate_implies(np, &*ect.left)?
                    || complex_predicate_implies(np, &*ect.right)?),
                _ => internal!(),
            }
        }
        ComparisonOp(ref ect) => match *np {
            LogicalOp(ref nct) => match nct.operator {
                BinaryOperator::And => Ok(complex_predicate_implies(&*nct.left, ep)?
                    || complex_predicate_implies(&*nct.right, ep)?),
                BinaryOperator::Or => Ok(complex_predicate_implies(&*nct.left, ep)?
                    && complex_predicate_implies(&*nct.right, ep)?),
                _ => internal!(),
            },
            ComparisonOp(ref nct) => Ok(nct.left == ect.left && predicate_implies(nct, ect)?),
            _ => internal!(),
        },
        _ => internal!(),
    }
}

fn predicate_implies(np: &ConditionTree, ep: &ConditionTree) -> Result<bool, ReadySetError> {
    // use Finkelstein-style direct elimination to check if this NQG predicate
    // implies the corresponding predicates in the EQG
    match *np.right {
        ConditionExpression::Base(ConditionBase::Literal(Literal::String(ref nv))) => {
            match *ep.right {
                ConditionExpression::Base(ConditionBase::Literal(Literal::String(ref ev))) => {
                    check_op_elimination(nv, ev, &np.operator, &ep.operator)
                }
                ConditionExpression::Base(ConditionBase::Literal(_)) => Ok(false),
                _ => unsupported!(),
            }
        }
        ConditionExpression::Base(ConditionBase::Literal(Literal::Integer(ref nv))) => {
            match *ep.right {
                ConditionExpression::Base(ConditionBase::Literal(Literal::Integer(ref ev))) => {
                    check_op_elimination(nv, ev, &np.operator, &ep.operator)
                }
                ConditionExpression::Base(ConditionBase::Literal(_)) => Ok(false),
                _ => unsupported!(),
            }
        }
        ConditionExpression::Base(ConditionBase::Literal(Literal::Null)) => match *ep.right {
            ConditionExpression::Base(ConditionBase::Literal(Literal::Null)) => Ok(true),
            ConditionExpression::Base(ConditionBase::Literal(_)) => Ok(false),
            _ => unsupported!(),
        },
        _ => unsupported!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::Column;

    #[test]
    fn predicate_implication() {
        use nom_sql::ConditionBase::*;
        use nom_sql::ConditionExpression::*;
        use nom_sql::Literal;

        let pa = ConditionTree {
            operator: BinaryOperator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(10.into())))),
        };
        let pb = ConditionTree {
            operator: BinaryOperator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        };
        let pc = ConditionTree {
            operator: BinaryOperator::Equal,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(5.into())))),
        };

        assert!(predicate_implies(&pa, &pb).unwrap());
        assert!(!predicate_implies(&pb, &pa).unwrap());
        assert!(!predicate_implies(&pa, &pc).unwrap());
        assert!(predicate_implies(&pc, &pa).unwrap());
    }

    #[test]
    fn complex_predicate_implication_or() {
        use nom_sql::ConditionBase::*;
        use nom_sql::ConditionExpression::*;
        use nom_sql::Literal;

        let pa = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        });
        let pb = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(60.into())))),
        });
        let pc = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(10.into())))),
        });
        let pd = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(80.into())))),
        });

        // a < 20 or a > 60
        let cp1 = LogicalOp(ConditionTree {
            left: Box::new(pa.clone()),
            right: Box::new(pb.clone()),
            operator: BinaryOperator::Or,
        });

        // a < 10 or a > 80
        let cp2 = LogicalOp(ConditionTree {
            left: Box::new(pc),
            right: Box::new(pd),
            operator: BinaryOperator::Or,
        });

        // a > 60 or a < 20
        let cp3 = LogicalOp(ConditionTree {
            left: Box::new(pb),
            right: Box::new(pa),
            operator: BinaryOperator::Or,
        });

        assert!(complex_predicate_implies(&cp2, &cp1).unwrap());
        assert!(!complex_predicate_implies(&cp1, &cp2).unwrap());
        assert!(complex_predicate_implies(&cp2, &cp3).unwrap());
        assert!(!complex_predicate_implies(&cp3, &cp2).unwrap());
    }

    #[test]
    fn complex_predicate_implication_and() {
        use nom_sql::ConditionBase::*;
        use nom_sql::ConditionExpression::*;
        use nom_sql::Literal;
        let pa = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        });
        let pb = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(60.into())))),
        });
        let pc = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(10.into())))),
        });
        let pd = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(80.into())))),
        });

        // a > 20 and a < 60
        let cp1 = LogicalOp(ConditionTree {
            left: Box::new(pa.clone()),
            right: Box::new(pb.clone()),
            operator: BinaryOperator::And,
        });

        // a > 10 and a < 80
        let cp2 = LogicalOp(ConditionTree {
            left: Box::new(pc),
            right: Box::new(pd),
            operator: BinaryOperator::And,
        });

        // a < 60 and a > 20
        let cp3 = LogicalOp(ConditionTree {
            left: Box::new(pb),
            right: Box::new(pa),
            operator: BinaryOperator::And,
        });

        assert!(complex_predicate_implies(&cp1, &cp2).unwrap());
        assert!(!complex_predicate_implies(&cp2, &cp1).unwrap());
        assert!(complex_predicate_implies(&cp3, &cp2).unwrap());
        assert!(!complex_predicate_implies(&cp2, &cp3).unwrap());
    }

    #[test]
    fn complex_predicate_implication_superset_or() {
        use nom_sql::ConditionBase::*;
        use nom_sql::ConditionExpression::*;
        use nom_sql::Literal;
        let pa = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        });
        let pb = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(60.into())))),
        });

        // a < 20 or a > 60
        let cp1 = LogicalOp(ConditionTree {
            left: Box::new(pa.clone()),
            right: Box::new(pb.clone()),
            operator: BinaryOperator::Or,
        });

        assert!(complex_predicate_implies(&pa, &cp1).unwrap());
        assert!(complex_predicate_implies(&pb, &cp1).unwrap());
        assert!(!complex_predicate_implies(&cp1, &pa).unwrap());
        assert!(!complex_predicate_implies(&cp1, &pb).unwrap());
    }

    #[test]
    fn complex_predicate_implication_subset_and() {
        use nom_sql::ConditionBase::*;
        use nom_sql::ConditionExpression::*;
        use nom_sql::Literal;
        let pa = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        });
        let pb = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(60.into())))),
        });

        // a > 20 and a < 60
        let cp1 = LogicalOp(ConditionTree {
            left: Box::new(pa.clone()),
            right: Box::new(pb.clone()),
            operator: BinaryOperator::And,
        });

        assert!(!complex_predicate_implies(&pa, &cp1).unwrap());
        assert!(!complex_predicate_implies(&pb, &cp1).unwrap());
        assert!(complex_predicate_implies(&cp1, &pa).unwrap());
        assert!(complex_predicate_implies(&cp1, &pb).unwrap());
    }
}
