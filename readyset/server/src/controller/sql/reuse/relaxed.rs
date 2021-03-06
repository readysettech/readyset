use std::collections::HashMap;
use std::vec::Vec;

use readyset::ReadySetError;

use super::super::query_graph::{QueryGraph, QueryGraphEdge};
use super::super::query_signature::Signature;
use super::helpers::predicate_implication::complex_predicate_implies;
use super::{ReuseConfiguration, ReuseType};

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
    ) -> Result<Vec<(ReuseType, (u64, &'a QueryGraph))>, ReadySetError> {
        let mut reuse_candidates = Vec::new();
        for (sig, existing_qg) in query_graphs {
            if existing_qg
                .signature()
                .is_weak_generalization_of(&qg.signature())
            {
                if let Some(reuse) = Self::check_compatibility(qg, existing_qg)? {
                    // QGs are compatible, we can reuse `existing_qg` as part of `qg`!
                    reuse_candidates.push((reuse, (*sig, existing_qg)));
                }
            }
        }

        Ok(reuse_candidates)
    }
}

impl Relaxed {
    fn check_compatibility(
        new_qg: &QueryGraph,
        existing_qg: &QueryGraph,
    ) -> Result<Option<ReuseType>, ReadySetError> {
        // 1. NQG's nodes is subset of EQG's nodes
        // -- already established via signature check
        debug_assert!(existing_qg
            .signature()
            .is_weak_generalization_of(&new_qg.signature()));

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
                QueryGraphEdge::Join { .. } => {
                    if !new_qg.edges.contains_key(srcdst) {
                        return Ok(None);
                    }
                    let new_qge = &new_qg.edges[srcdst];
                    match *new_qge {
                        QueryGraphEdge::Join { .. } => {}
                        // If there is no matching Join edge, we cannot reuse
                        _ => return Ok(None),
                    }
                }
                QueryGraphEdge::LeftJoin { .. } => {
                    if !new_qg.edges.contains_key(srcdst) {
                        return Ok(None);
                    }
                    let new_qge = &new_qg.edges[srcdst];
                    match *new_qge {
                        QueryGraphEdge::LeftJoin { .. } => {}
                        // If there is no matching LeftJoin edge, we cannot reuse
                        _ => return Ok(None),
                    }
                }
            }
        }

        // Checks group by compatibility between queries.
        // GroupBy implication holds if the new QG groups by the same columns as
        // the original one, or by a *superset* (as we can always apply more
        if !new_qg.group_by.is_superset(&existing_qg.group_by) {
            // EQG groups by a column that we don't group by, so we can't reuse
            // the group by nodes, but we can still reuse joins and predicates.
            return Ok(Some(ReuseType::PrefixReuse));
        }

        // Check that the new query's predicates imply the existing query's predicate.
        for (name, ex_qgn) in &existing_qg.relations {
            if !new_qg.relations.contains_key(name) {
                return Ok(Some(ReuseType::PrefixReuse));
            }
            let new_qgn = &new_qg.relations[name];

            // iterate over predicates and ensure that each
            // matching one on the existing QG is implied by the new one
            for ep in &ex_qgn.predicates {
                let mut matched = false;

                for np in &new_qgn.predicates {
                    if complex_predicate_implies(np, ep)? {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    // We found no matching predicate for np, so we give up now.
                    // However, we can still reuse the join nodes from the existing query.
                    return Ok(Some(ReuseType::PrefixReuse));
                }
            }
        }

        // projected columns don't influence the reuse opportunities in this case, since
        // we are only trying to reuse the query partially, not completely extending it.

        Ok(Some(ReuseType::DirectExtension))
    }
}
