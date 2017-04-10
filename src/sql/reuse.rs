use mir::MirQuery;
use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Operator, Table};
use sql::query_graph::{QueryGraph, QueryGraphEdge};

use std::str;
use std::vec::Vec;

#[derive(Clone, Debug)]
pub enum ReuseType {
    DirectExtension,
    BackjoinRequired(Vec<Table>),
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

fn check_op_elimination<T>(nv: T, ev: T, nop: &Operator, eop: &Operator) -> bool
    where T: PartialOrd
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

fn predicate_implies(np: &ConditionTree, ep: &ConditionTree) -> bool {
    // use Finkelstein-style direct elimination to check if this NQG predicate
    // implies the corresponding predicates in the EQG
    match *np.right {
        ConditionExpression::Base(ConditionBase::StringLiteral(ref nv)) => {
            match *ep.right {
                ConditionExpression::Base(ConditionBase::StringLiteral(ref ev)) => {
                    check_op_elimination(nv, ev, &np.operator, &ep.operator)
                }
                ConditionExpression::Base(ConditionBase::IntegerLiteral(_)) => false,
                _ => panic!("right-hand side of predicate must currently be literal"),
            }
        }
        ConditionExpression::Base(ConditionBase::IntegerLiteral(ref nv)) => {
            match *ep.right {
                ConditionExpression::Base(ConditionBase::IntegerLiteral(ref ev)) => {
                    check_op_elimination(nv, ev, &np.operator, &ep.operator)
                }
                ConditionExpression::Base(ConditionBase::StringLiteral(_)) => false,
                _ => panic!("right-hand side of predicate must currently be literal"),
            }
        }
        _ => panic!("right-hand side of predicate must currently be literal"),
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
        for ep in &ex_qgn.predicates {
            let mut matched = false;
            for np in &new_qgn.predicates {
                if np.left == ep.left {
                    println!("matching predicates --\nexisting: {:#?},\nnew: {:#?}",
                             ep,
                             np);
                    if !predicate_implies(np, ep) {
                        println!("Failed: {:?} does not imply {:?}", np, ep);
                        return None;
                    } else {
                        matched = true;
                    }
                }
            }
            if !matched {
                // We found no matching predicate for np, so we give up now.
                println!("Failed: no matching predicate for {:#?}", ep);
                return None;
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

        // does EQG already have *all* the columns we project?
        let all_projected = new_qgn
            .columns
            .iter()
            .all(|nc| ex_qgn.columns.contains(nc));
        if all_projected {
            // if so, super -- we can extend directly
            return Some(ReuseType::DirectExtension);
        } else {
            if name == "computed_columns" {
                // NQG has some extra columns, and they're computed ones (i.e., grouped/function
                // columns). We can recompute those, but not via a backjoin.
                // TODO(malte): be cleverer about this situation
                return None;
            }

            // find the extra columns in the EQG to identify backjoins required
            let backjoin_tables: Vec<_> = new_qgn
                .columns
                .iter()
                .filter(|nc| !ex_qgn.columns.contains(nc) && nc.table.is_some())
                .map(|c| Table::from(c.table.as_ref().unwrap().as_str()))
                .collect();

            if backjoin_tables.len() > 0 {
                return Some(ReuseType::BackjoinRequired(backjoin_tables));
            } else {
                panic!("expected to find some backjoin tables!");
            }
        }
    }
    // XXX(malte):  5b. NQG projects a superset of EQG's edges --> need backjoin

    // XXX(malte): should this be a positive? If so, what?
    None
}

pub fn choose_best_option(options: Vec<(ReuseType, &QueryGraph)>) -> (ReuseType, &QueryGraph) {
    let mut best_choice = None;
    let mut best_score = 0;

    for (o, qg) in options {
        let mut score = 0;

        // crude scoring: direct extension always preferrable over backjoins; reusing larger
        // queries is also preferrable as they are likely to cover a larger fraction of the new
        // query's nodes. Edges (group by, join) count for more than extra relations.
        match o {
            ReuseType::DirectExtension => {
                score += 2 * qg.relations.len() + 4 * qg.edges.len() + 10;
            }
            ReuseType::BackjoinRequired(ref tables) => {
                score += qg.relations.len() + 3 * qg.edges.len();
            }
        }

        if score > best_score {
            best_score = score;
            best_choice = Some((o, qg));
        }
    }

    assert!(best_score > 0);

    best_choice.unwrap()
}

pub fn merge_mir_for_queries(new_query: &MirQuery, old_query: &MirQuery) -> MirQuery {
    // XXX(malte): topologically traverse both MIR graphs and convert new nodes into Reuse nodes
    // for those that match
    new_query.clone()
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
            right: Box::new(Base(IntegerLiteral(10.into()))),
        };
        let pb = ConditionTree {
            operator: Operator::Less,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(IntegerLiteral(20.into()))),
        };
        let pc = ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(Column::from("a")))),
            right: Box::new(Base(IntegerLiteral(5.into()))),
        };

        assert!(predicate_implies(&pa, &pb));
        assert!(!predicate_implies(&pb, &pa));
        assert!(!predicate_implies(&pa, &pc));
        assert!(predicate_implies(&pc, &pa));
    }
}
