use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Literal, Operator};
use nom_sql::ConditionExpression::*;

fn direct_elimination(op1: &Operator, op2: &Operator) -> Option<Operator> {
    match *op1 {
        Operator::Equal => {
            match *op2 {
                Operator::Equal => Some(Operator::Equal),
                Operator::Less => Some(Operator::Less),
                Operator::Greater => Some(Operator::Greater),
                _ => unimplemented!(),
            }
        }
        Operator::NotEqual => {
            match *op2 {
                Operator::Equal => Some(Operator::NotEqual),
                Operator::Less => None,
                Operator::Greater => None,
                _ => unimplemented!(),
            }
        }
        Operator::Less => {
            match *op2 {
                Operator::Equal => Some(Operator::Less),
                Operator::Less => Some(Operator::Less),
                Operator::Greater => None,
                _ => unimplemented!(),
            }
        }
        Operator::LessOrEqual => {
            match *op2 {
                Operator::Equal => Some(Operator::LessOrEqual),
                Operator::Less => Some(Operator::LessOrEqual),
                Operator::Greater => None,
                _ => unimplemented!(),
            }
        }
        Operator::Greater => {
            match *op2 {
                Operator::Equal => Some(Operator::Greater),
                Operator::Less => None,
                Operator::Greater => Some(Operator::Greater),
                _ => unimplemented!(),
            }
        }
        Operator::GreaterOrEqual => {
            match *op2 {
                Operator::Equal => Some(Operator::GreaterOrEqual),
                Operator::Less => None,
                Operator::Greater => Some(Operator::Greater),
                _ => unimplemented!(),
            }
        }
        _ => None,
    }
}

fn check_op_elimination<T>(nv: T, ev: T, nop: &Operator, eop: &Operator) -> bool
where
    T: PartialOrd,
{
    let ep_op_needed = if nv == ev {
        direct_elimination(nop, &Operator::Equal)
    } else if nv < ev {
        direct_elimination(nop, &Operator::Less)
    } else if nv > ev {
        direct_elimination(nop, &Operator::Greater)
    } else {
        None
    };
    match ep_op_needed {
        None => return false,
        Some(op) => {
            // TODO(malte): the condition is actually weaker than
            // this inequality suggests -- it's sufficient for the
            // needed operator to be *weaker* than ep.operator to
            // reject the EQG.
            if *eop != op {
                return false;
            } else {
                return true;
            }
        }
    }
}

pub fn predicate_is_equivalent(np: &ConditionTree, ep: &ConditionTree) -> bool {
    let nl_col = match *np.left {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
            _ => unimplemented!(),
        };
    let nr_col = match *np.right {
        ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
        _ => unimplemented!(),
    };

    let el_col = match *ep.left {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
            _ => unimplemented!(),
        };
    let er_col = match *ep.right {
        ConditionExpression::Base(ConditionBase::Field(ref f)) => f.clone(),
        _ => unimplemented!(),
    };

    (nl_col == el_col && nr_col == er_col) || (nl_col == er_col && nr_col == el_col)
}

/// Direct elimination for complex predicates with nested `and` and `or` expressions
pub fn complex_predicate_implies(np: &ConditionExpression, ep: &ConditionExpression) -> bool {
    match *ep {
        LogicalOp(ref ect) => {
            match *np {
                LogicalOp(ref nct) => {
                    if nct.operator == ect.operator {
                        return (complex_predicate_implies(&*nct.left, &*ect.left) && complex_predicate_implies(&*nct.right, &*ect.right)) ||
                                (complex_predicate_implies(&*nct.left, &*ect.right) && complex_predicate_implies(&*nct.right, &*ect.left));
                    }
                }
                _ => (),
            }

            match ect.operator {
                Operator::And => {
                    complex_predicate_implies(np, &*ect.left) && complex_predicate_implies(np, &*ect.right)
                }
                Operator::Or => {
                    complex_predicate_implies(np, &*ect.left) || complex_predicate_implies(np, &*ect.right)
                }
                _ => unreachable!()
            }

        },
        ComparisonOp(ref ect) => {
            match *np {
                LogicalOp(ref nct) => {
                    match nct.operator {
                        Operator::And => {
                            complex_predicate_implies(&*nct.left, ep) || complex_predicate_implies(&*nct.right, ep)
                        }
                        Operator::Or => {
                            complex_predicate_implies(&*nct.left, ep) && complex_predicate_implies(&*nct.right, ep)
                        }
                        _ => unreachable!()
                    }
                }
                ComparisonOp(ref nct) => {
                    nct.left == ect.left && predicate_implies(nct, ect)
                }
                _ => unreachable!(),
            }
        },
        _ => unreachable!(),
    }
}

fn predicate_implies(np: &ConditionTree, ep: &ConditionTree) -> bool {
    // use Finkelstein-style direct elimination to check if this NQG predicate
    // implies the corresponding predicates in the EQG
    match *np.right {
        ConditionExpression::Base(ConditionBase::Literal(Literal::String(ref nv))) => {
            match *ep.right {
                ConditionExpression::Base(ConditionBase::Literal(Literal::String(ref ev))) => {
                    check_op_elimination(nv, ev, &np.operator, &ep.operator)
                }
                ConditionExpression::Base(ConditionBase::Literal(_)) => false,
                _ => panic!("right-hand side of predicate must currently be literal"),
            }
        }
        ConditionExpression::Base(ConditionBase::Literal(Literal::Integer(ref nv))) => {
            match *ep.right {
                ConditionExpression::Base(ConditionBase::Literal(Literal::Integer(ref ev))) => {
                    check_op_elimination(nv, ev, &np.operator, &ep.operator)
                }
                ConditionExpression::Base(ConditionBase::Literal(_)) => false,
                _ => panic!("right-hand side of predicate must currently be literal"),
            }
        }
        _ => panic!("right-hand side of predicate must currently be literal"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::Column;

    #[test]
    fn predicate_implication() {
        use nom_sql::ConditionExpression::*;
        use nom_sql::ConditionBase::*;
        use nom_sql::Literal;

        let pa = ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(10.into())))),
        };
        let pb = ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        };
        let pc = ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(5.into())))),
        };

        assert!(predicate_implies(&pa, &pb));
        assert!(!predicate_implies(&pb, &pa));
        assert!(!predicate_implies(&pa, &pc));
        assert!(predicate_implies(&pc, &pa));
    }

    #[test]
    fn complex_predicate_implication_or() {
        use nom_sql::ConditionExpression::*;
        use nom_sql::ConditionBase::*;
        use nom_sql::Literal;

        let pa = ComparisonOp(ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        });
        let pb = ComparisonOp(ConditionTree {
            operator: Operator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(60.into())))),
        });
        let pc = ComparisonOp(ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(10.into())))),
        });
        let pd = ComparisonOp(ConditionTree {
            operator: Operator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(80.into())))),
        });

        // a < 20 or a > 60
        let cp1 = LogicalOp(ConditionTree {
            left: Box::new(pa.clone()),
            right: Box::new(pb.clone()),
            operator: Operator::Or
        });

        // a < 10 or a > 80
        let cp2 = LogicalOp(ConditionTree {
            left: Box::new(pc),
            right: Box::new(pd),
            operator: Operator::Or
        });

        // a > 60 or a < 20
        let cp3 = LogicalOp(ConditionTree {
            left: Box::new(pb),
            right: Box::new(pa),
            operator: Operator::Or
        });


        assert!(complex_predicate_implies(&cp2, &cp1));
        assert!(!complex_predicate_implies(&cp1, &cp2));
        assert!(complex_predicate_implies(&cp2, &cp3));
        assert!(!complex_predicate_implies(&cp3, &cp2));
    }

    #[test]
    fn complex_predicate_implication_and() {
        use nom_sql::ConditionExpression::*;
        use nom_sql::ConditionBase::*;
        use nom_sql::Literal;
        let pa = ComparisonOp(ConditionTree {
            operator: Operator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        });
        let pb = ComparisonOp(ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(60.into())))),
        });
        let pc = ComparisonOp(ConditionTree {
            operator: Operator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(10.into())))),
        });
        let pd = ComparisonOp(ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(80.into())))),
        });

        // a > 20 and a < 60
        let cp1 = LogicalOp(ConditionTree {
            left: Box::new(pa.clone()),
            right: Box::new(pb.clone()),
            operator: Operator::And
        });

        // a > 10 and a < 80
        let cp2 = LogicalOp(ConditionTree {
            left: Box::new(pc),
            right: Box::new(pd),
            operator: Operator::And
        });

        // a < 60 and a > 20
        let cp3 = LogicalOp(ConditionTree {
            left: Box::new(pb),
            right: Box::new(pa),
            operator: Operator::And
        });


        assert!(complex_predicate_implies(&cp1, &cp2));
        assert!(!complex_predicate_implies(&cp2, &cp1));
        assert!(complex_predicate_implies(&cp3, &cp2));
        assert!(!complex_predicate_implies(&cp2, &cp3));
    }

    #[test]
    fn complex_predicate_implication_superset_or() {
        use nom_sql::ConditionExpression::*;
        use nom_sql::ConditionBase::*;
        use nom_sql::Literal;
        let pa = ComparisonOp(ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        });
        let pb = ComparisonOp(ConditionTree {
            operator: Operator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(60.into())))),
        });

        // a < 20 or a > 60
        let cp1 = LogicalOp(ConditionTree {
            left: Box::new(pa.clone()),
            right: Box::new(pb.clone()),
            operator: Operator::Or
        });


        assert!(complex_predicate_implies(&pa, &cp1));
        assert!(complex_predicate_implies(&pb, &cp1));
        assert!(!complex_predicate_implies(&cp1, &pa));
        assert!(!complex_predicate_implies(&cp1, &pb));
    }

    #[test]
    fn complex_predicate_implication_subset_and() {
        use nom_sql::ConditionExpression::*;
        use nom_sql::ConditionBase::*;
        use nom_sql::Literal;
        let pa = ComparisonOp(ConditionTree {
            operator: Operator::Greater,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(20.into())))),
        });
        let pb = ComparisonOp(ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal(Literal::Integer(60.into())))),
        });

        // a > 20 and a < 60
        let cp1 = LogicalOp(ConditionTree {
            left: Box::new(pa.clone()),
            right: Box::new(pb.clone()),
            operator: Operator::And
        });


        assert!(!complex_predicate_implies(&pa, &cp1));
        assert!(!complex_predicate_implies(&pb, &cp1));
        assert!(complex_predicate_implies(&cp1, &pa));
        assert!(complex_predicate_implies(&cp1, &pb));
    }
}

