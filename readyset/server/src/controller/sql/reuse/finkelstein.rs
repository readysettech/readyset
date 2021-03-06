use std::collections::HashMap;
use std::vec::Vec;

use readyset::ReadySetError;

use super::super::query_graph::{QueryGraph, QueryGraphEdge};
use super::super::query_signature::Signature;
use super::helpers::predicate_implication::complex_predicate_implies;
use super::{ReuseConfiguration, ReuseType};

pub struct Finkelstein;

/// Finkelstein reuse algorithm.
/// This algorithm checks if any existing query graphs are generalizations
/// of the query graph of the new query being added.
/// For Soup's purpose this algorithm is sometimes too strict as it only
/// considers reuse of final materializations, not intermediate ones.
impl ReuseConfiguration for Finkelstein {
    fn reuse_candidates<'a>(
        qg: &QueryGraph,
        query_graphs: &'a HashMap<u64, QueryGraph>,
    ) -> Result<Vec<(ReuseType, (u64, &'a QueryGraph))>, ReadySetError> {
        let mut reuse_candidates = Vec::new();
        for (sig, existing_qg) in query_graphs {
            if existing_qg
                .signature()
                .is_generalization_of(&qg.signature())
            {
                if let Some(reuse) = Self::check_compatibility(qg, existing_qg)? {
                    // QGs are compatible, we can reuse `existing_qg` as part of `qg`!
                    reuse_candidates.push((reuse, (*sig, existing_qg)));
                }
            }
        }

        if !reuse_candidates.is_empty() {
            Ok(vec![Self::choose_best_option(reuse_candidates)])
        } else {
            Ok(reuse_candidates)
        }
    }
}

impl Finkelstein {
    fn choose_best_option(
        options: Vec<(ReuseType, (u64, &QueryGraph))>,
    ) -> (ReuseType, (u64, &QueryGraph)) {
        let mut best_choice = None;
        let mut best_score = 0;

        for (o, (sig, qg)) in options {
            let mut score = 0;

            // crude scoring: direct extension always preferrable over backjoins; reusing larger
            // queries is also preferrable as they are likely to cover a larger fraction of the new
            // query's nodes. Edges (group by, join) count for more than extra relations.
            match o {
                ReuseType::DirectExtension => {
                    score += 2 * qg.relations.len() + 4 * qg.edges.len() + 10;
                }
                ReuseType::BackjoinRequired(_) => {
                    score += qg.relations.len() + 3 * qg.edges.len();
                }
                ReuseType::PrefixReuse =>
                #[allow(clippy::unreachable)] // Finkelstein reuse can never produce PrefixReuse
                {
                    unreachable!()
                }
            }

            if score > best_score {
                best_score = score;
                best_choice = Some((o, (sig, qg)));
            }
        }

        assert!(best_score > 0);

        best_choice.unwrap()
    }

    fn check_compatibility(
        new_qg: &QueryGraph,
        existing_qg: &QueryGraph,
    ) -> Result<Option<ReuseType>, ReadySetError> {
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
                return Ok(None);
            }
        }

        // 4. NQG's predicates imply EQG's
        //   4a. on nodes
        for (name, ex_qgn) in &existing_qg.relations {
            let new_qgn = &new_qg.relations[name];

            // iterate over predicates and ensure that each matching
            // one on the existing QG is implied by the new one
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
                    // trace!(log, "Failed: no matching predicate for {:#?}", ep);
                    return Ok(None);
                }
            }
        }
        //   4b. on edges
        for (srcdst, ex_qge) in &existing_qg.edges {
            let new_qge = &new_qg.edges[srcdst];

            match *ex_qge {
                QueryGraphEdge::Join { .. } => {
                    match *new_qge {
                        QueryGraphEdge::Join { .. } => {}
                        // If there is no matching Join edge, we cannot reuse
                        _ => return Ok(None),
                    }
                }
                QueryGraphEdge::LeftJoin { .. } => {
                    match *new_qge {
                        QueryGraphEdge::LeftJoin { .. } => {}
                        // If there is no matching LeftJoin edge, we cannot reuse
                        _ => return Ok(None),
                    }
                }
            }
        }

        // 5. consider group_by
        // GroupBy implication holds if the new QG groups by the same columns as
        // the original one, or by a *superset* (as we can always apply more
        // grouped operatinos on top of earlier ones)
        if !new_qg.group_by.is_superset(&existing_qg.group_by) {
            return Ok(None);
        }

        // we don't need to check projected columns to reuse a prefix of the query
        Ok(Some(ReuseType::DirectExtension))

        // 5. Consider projected columns
        //   5a. NQG projects a subset of EQG's edges --> can use directly
        // for (name, ex_qgn) in &existing_qg.relations {
        //     let new_qgn = &new_qg.relations[name];

        //     // does EQG already have *all* the columns we project?
        //     let all_projected = new_qgn
        //         .columns
        //         .iter()
        //         .all(|nc| ex_qgn.columns.contains(nc));
        //     if all_projected {
        //         // if so, super -- we can extend directly
        //         return Some(ReuseType::DirectExtension);
        //     } else {

        //         // find the extra columns in the EQG to identify backjoins required
        //         let backjoin_tables: Vec<_> = new_qgn
        //             .columns
        //             .iter()
        //             .filter(|nc| !ex_qgn.columns.contains(nc) && nc.table.is_some())
        //             .map(|c| Table::from(c.table.as_ref().unwrap().as_str()))
        //             .collect();

        //         if backjoin_tables.len() > 0 {
        //             return Some(ReuseType::BackjoinRequired(backjoin_tables));
        //         } else {
        //             panic!("expected to find some backjoin tables!");
        //         }
        //     }
        // }
        // XXX(malte):  5b. NQG projects a superset of EQG's edges --> need backjoin

        // XXX(malte): should this be a positive? If so, what?
        // None
    }
}
