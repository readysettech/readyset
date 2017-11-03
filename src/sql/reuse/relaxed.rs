use sql::reuse::helpers::predicate_implication::complex_predicate_implies;
use sql::reuse::{ReuseConfiguration, ReuseType};
use sql::query_graph::{QueryGraph, QueryGraphEdge};
use sql::query_signature::Signature;

use std::vec::Vec;
use std::collections::HashMap;


/// Implementation of reuse algorithm with relaxed constraints.
/// While Finkelstein checks if queries are compatible for direct extension,
/// this algorithm considers the possibility of reuse of internal views.
/// For example, given the queries:
/// 1) select * from Paper, PaperReview
///         where Paper.paperId = PaperReview.paperId
///               and PaperReview.reviewType = 1;
/// 2) select * from Paper, PaperReview where Paper.paperId = PaperReview.paperId;
///
/// Finkelstein reuse would be conditional on the order the queries are added,
/// since 1) is a direct extension of 2), but not the other way around.
///
/// This relaxed version of the reuse algorithm considers cases where a query might
/// reuse just a prefix of another. First, it checks if the queries perform the
/// same joins, then it checks predicate implication and at last, checks group
/// by compatibility.
/// If all checks pass, them the algorithm works like Finkelstein and the query
/// is a direct extension of the other. However, if not all, but at least one
/// check passes, then the algorithm returns that the queries have a prefix in
/// common.
pub struct Relaxed;

impl ReuseConfiguration for Relaxed {
    fn reuse_candidates<'a>(
        qg: &QueryGraph,
        query_graphs: &'a HashMap<u64, QueryGraph>,
    ) -> Vec<(ReuseType, (u64, &'a QueryGraph))> {
        let mut reuse_candidates = Vec::new();
        for (sig, existing_qg) in query_graphs {
            if existing_qg
                .signature()
                .is_weak_generalization_of(&qg.signature())
            {
                match Self::check_compatibility(&qg, &existing_qg) {
                    Some(reuse) => {
                        // QGs are compatible, we can reuse `existing_qg` as part of `qg`!
                        reuse_candidates.push((reuse, (sig.clone(), existing_qg)));
                    }
                    None => (),
                }
            }
        }

        reuse_candidates
    }
}

impl Relaxed {
    fn check_compatibility(new_qg: &QueryGraph, existing_qg: &QueryGraph) -> Option<ReuseType> {
        // 1. NQG's nodes is subset of EQG's nodes
        // -- already established via signature check
        assert!(
            existing_qg
                .signature()
                .is_weak_generalization_of(&new_qg.signature())
        );

        // Check if the queries are join compatible -- if the new query
        // performs a superset of the joins in the existing query.

        // TODO 1: this currently only checks that the joins use the same
        // tables. some possible improvements are:
        // 1) relaxing to fail only on non-disjoint join sets
        // 2) constraining to also check implication of join predicates

        // TODO 2: malte's suggestion of possibly reuse LeftJoin as a
        // plain Join by simply adding a filter that discards rows with
        // NULLs in the right side columns
        for (srcdst, ex_qge) in &existing_qg.edges {
            match *ex_qge {
                QueryGraphEdge::Join(_) => {
                    if !new_qg.edges.contains_key(srcdst) {
                        return None;
                    }
                    let new_qge = &new_qg.edges[srcdst];
                    match *new_qge {
                        QueryGraphEdge::Join(_) => {}
                        // If there is no matching Join edge, we cannot reuse
                        _ => return None,
                    }
                }
                QueryGraphEdge::LeftJoin(_) => {
                    if !new_qg.edges.contains_key(srcdst) {
                        return None;
                    }
                    let new_qge = &new_qg.edges[srcdst];
                    match *new_qge {
                        QueryGraphEdge::LeftJoin(_) => {}
                        // If there is no matching LeftJoin edge, we cannot reuse
                        _ => return None,
                    }
                }
                _ => continue,
            }
        }

        // Checks group by compatibility between queries.
        for (srcdst, ex_qge) in &existing_qg.edges {
            match *ex_qge {
                QueryGraphEdge::GroupBy(ref ex_columns) => {
                    if !new_qg.edges.contains_key(srcdst) {
                        return Some(ReuseType::PrefixReuse);
                    }
                    let new_qge = &new_qg.edges[srcdst];
                    match *new_qge {
                        QueryGraphEdge::GroupBy(ref new_columns) => {
                            // GroupBy implication holds if the new QG groups by the same columns as
                            // the original one, or by a *superset* (as we can always apply more
                            // grouped operatinos on top of earlier ones)
                            if new_columns.len() < ex_columns.len() {
                                // more columns in existing QG's GroupBy, so we're done
                                // however, we can still reuse joins and predicates.
                                return Some(ReuseType::PrefixReuse);
                            }
                            for ex_col in ex_columns {
                                // EQG groups by a column that we don't group by, so we can't reuse
                                // the group by nodes, but we can still reuse joins and predicates.
                                if !new_columns.contains(ex_col) {
                                    return Some(ReuseType::PrefixReuse);
                                }
                            }
                        }
                        // If there is no matching GroupBy edge, we cannot reuse the group by clause
                        _ => return Some(ReuseType::PrefixReuse),
                    }
                }
                _ => continue,
            }
        }

        // Check that the new query's predicates imply the existing query's predicate.
        for (name, ex_qgn) in &existing_qg.relations {
            if !new_qg.relations.contains_key(name) {
                return Some(ReuseType::PrefixReuse);
            }
            let new_qgn = &new_qg.relations[name];

            // iterate over predicates and ensure that each
            // matching one on the existing QG is implied by the new one
            for ep in &ex_qgn.predicates {
                let mut matched = false;

                for np in &new_qgn.predicates {
                    if complex_predicate_implies(np, ep) {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    // We found no matching predicate for np, so we give up now.
                    // However, we can still reuse the join nodes from the existing query.
                    return Some(ReuseType::PrefixReuse);
                }
            }
        }

        // projected columns don't influence the reuse opportunities in this case, since
        // we are only trying to reuse the query partially, not completely extending it.

        return Some(ReuseType::DirectExtension);
    }
}
