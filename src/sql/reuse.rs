use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Operator, Table};
use sql::query_graph::{QueryGraph, QueryGraphEdge};

use std::str;
use std::vec::Vec;

pub enum ReuseType {
    DirectExtension,
    BackjoinRequired,
}

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
        _ => unimplemented!(),
    }
}

fn predicate_implies(np: &ConditionTree, ep: &ConditionTree) -> bool {
    if np.left == ep.left {
        // use Finkelstein-style direct elimination to check if this NQG predicate
        // implies the corresponding predicates in the EQG
        match *np.right {
            ConditionExpression::Base(ConditionBase::Literal(ref nv)) => {
                match *ep.right {
                    ConditionExpression::Base(ConditionBase::Literal(ref ev)) => {
                        let ep_op_needed = if nv == ev {
                            direct_elimination(&np.operator, &Operator::Equal)
                        } else if nv < ev {
                            direct_elimination(&np.operator, &Operator::Less)
                        } else if nv > ev {
                            direct_elimination(&np.operator, &Operator::Greater)
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
                                if ep.operator != op {
                                    return false;
                                } else {
                                    return true;
                                }
                            }
                        }
                    }
                    _ => panic!("right-hand side of predicate must currently be literal"),
                }
            }
            _ => panic!("right-hand side of predicate must currently be literal"),
        }
    } else {
        // difference columns on LHS of predicate => cannot imply each other
        return false;
    }
}

pub fn check_compatibility(new_qg: &QueryGraph, existing_qg: &QueryGraph) -> Option<ReuseType> {
    // 1. NQG's nodes is subset of EQG's nodes
    // -- already established via signature check
    // 2. NQG's attributes is subset of NQG's edges
    // -- already established via signature check
    assert!(existing_qg
                .signature()
                .is_generalization_of(&new_qg.signature()));

    // 3. NQC's edges are superset of EQG's
    //    (N.B.: this does not yet consider the relationships of the edge predicates; we do that
    //    below in the next step.)
    for e in &existing_qg.edges {
        if !new_qg.edges.contains_key(e.0) {
            return None;
        }
    }

    // 4. NQG's predicates imply EQG's
    //   4a. on nodes
    for (name, ex_qgn) in &existing_qg.relations {
        let new_qgn = &new_qg.relations[name];

        // iterate over predicates and ensure that each matching one on the existing QG is implied
        // by the new one
        for np in &new_qgn.predicates {
            for ep in &ex_qgn.predicates {
                println!("matching predicates --\nexisting: {:#?},\nnew: {:#?}",
                         ep,
                         np);
                if !predicate_implies(np, ep) {
                    return None;
                }
            }
        }
    }
    //   4b. on edges
    for (srcdst, ex_qge) in &existing_qg.edges {
        let new_qge = &new_qg.edges[srcdst];

        match *ex_qge {
            QueryGraphEdge::GroupBy(ref ex_columns) => {
                match *new_qge {
                    QueryGraphEdge::GroupBy(ref new_columns) => {
                        // GroupBy implication holds if the new QG groups by the same columns as the
                        // original one, or by a *superset* (as we can always apply more grouped operatinos
                        // on top of earlier ones)
                        if new_columns.len() < ex_columns.len() {
                            // more columns in existing QG's GroupBy, so we're done
                            return None;
                        }
                        for ex_col in ex_columns {
                            // EQG groups by a column that we don't group by, so we can't reuse
                            if !new_columns.contains(ex_col) {
                                return None;
                            }
                        }
                    }
                    // If there is no matching GroupBy edge, we cannot reuse
                    _ => return None,
                }
            }
            QueryGraphEdge::Join(_) => {
                match *new_qge {
                    QueryGraphEdge::Join(_) => {}
                    // If there is no matching Join edge, we cannot reuse
                    _ => return None,
                }
            }
            QueryGraphEdge::LeftJoin(_) => {
                match *new_qge {
                    QueryGraphEdge::LeftJoin(_) => {}
                    // If there is no matching LeftJoin edge, we cannot reuse
                    _ => return None,
                }
            }
        }
    }

    // 5. Consider projected columns
    //   5a. NQG projects a subset of EQG's edges --> can use directly
    for (name, ex_qgn) in &existing_qg.relations {
        let new_qgn = &new_qg.relations[name];

        // iterate over predicates and ensure that each matching one on the existing QG is implied
        // by the new one
        let all_projected = new_qgn
            .columns
            .iter()
            .all(|nc| ex_qgn.columns.contains(nc));
        if all_projected {
            return Some(ReuseType::DirectExtension);
        } else {
            return None;
        }
    }
    // XXX(malte):  5b. NQG projects a superset of EQG's edges --> need backjoin

    // XXX(malte): should this be a positive? If so, what?
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::Column;

    #[test]
    fn predicate_implication() {
        use nom_sql::ConditionExpression::*;
        use nom_sql::ConditionBase::*;

        let pa = ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal("10".into()))),
        };
        let pb = ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal("10".into()))),
        };
        let pc = ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(Literal("5".into()))),
        };

        assert!(predicate_implies(&pa, &pb));
        assert!(predicate_implies(&pb, &pa));
        assert!(!predicate_implies(&pa, &pc));
        assert!(!predicate_implies(&pc, &pa));
    }
}
