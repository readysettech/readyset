use mir::MirNodeRef;
use controller::sql::UniverseId;
use controller::sql::mir::SqlToMirConverter;
use std::collections::HashMap;
use controller::sql::query_signature::Signature;

pub trait SecurityBoundary {
    fn make_security_boundary(
        &mut self,
        universe: UniverseId,
        node_for_rel: &mut HashMap<&str, MirNodeRef>,
        prev_node: Option<MirNodeRef>,
        is_leaf: bool,
    ) -> Vec<MirNodeRef>;

    fn make_security_nodes(
        &mut self,
        rel: &str,
        base_node: &MirNodeRef,
        universe_id: UniverseId,
        node_for_rel: HashMap<&str, MirNodeRef>,
        is_leaf: bool,
    ) -> Vec<MirNodeRef>;
}

impl SecurityBoundary for SqlToMirConverter {
    // TODO(larat): this is basically make_selection_nodes
    fn make_security_nodes(
        &mut self,
        rel: &str,
        prev_node: &MirNodeRef,
        universe_id: UniverseId,
        node_for_rel: HashMap<&str, MirNodeRef>,
        is_leaf: bool,
    ) -> Vec<MirNodeRef> {
        let policies = match self.universe.policies.get(&String::from(rel)) {
            Some(p) => p.clone(),
            // no policies associated with this base node
            None => return Vec::new(),
        };

        let mut node_count = 0;
        let mut local_node_for_rel = node_for_rel.clone();

        debug!(
            self.log,
            "Found {} policies for table {}",
            policies.len(),
            rel
        );

        let output_cols = prev_node.borrow().columns().iter().cloned().collect();
        let mut security_nodes = Vec::new();
        let mut last_policy_nodes = Vec::new();

        for qg in policies.iter() {
            let mut prev_node = Some(prev_node.clone());
            let mut base_nodes: Vec<MirNodeRef> = Vec::new();
            let mut filter_nodes: Vec<MirNodeRef> = Vec::new();

            let mut sorted_rels: Vec<&str> = qg.relations.keys().map(String::as_str).collect();

            sorted_rels.sort();

            // all base nodes should be present in local_node_for_rel, except for UserContext
            // if policy uses UserContext, add it to local_node_for_rel
            for rel in &sorted_rels {
                if *rel == "computed_columns" {
                    continue;
                }
                if local_node_for_rel.contains_key(*rel) {
                    local_node_for_rel.insert(*rel, prev_node.clone().unwrap());
                    continue;
                }

                let view_for_rel = self.get_view(rel);

                local_node_for_rel.insert(*rel, view_for_rel.clone());
                base_nodes.push(view_for_rel.clone());
            }

            use controller::sql::mir::join::make_joins;
            let join_nodes = make_joins(
                self,
                &format!("sp_{:x}", qg.signature().hash),
                qg,
                &local_node_for_rel,
                node_count
            );

            node_count += join_nodes.len();

            prev_node = match join_nodes.last() {
                Some(n) => Some(n.clone()),
                None => prev_node,
            };

            // handles predicate nodes
            for rel in &sorted_rels {
                let qgn = qg.relations
                    .get(*rel)
                    .expect("relation should have a query graph node.");
                assert!(*rel != "computed_collumns");

                // Skip empty predicates
                if qgn.predicates.is_empty() {
                    continue;
                }

                for pred in &qgn.predicates {
                    let new_nodes = self.make_predicate_nodes(
                        &format!("sp_{:x}_n{:x}", qg.signature().hash, node_count),
                        prev_node.expect("empty previous node"),
                        pred,
                        0,
                    );

                    prev_node = Some(
                        new_nodes
                            .iter()
                            .last()
                            .expect("no new nodes were created")
                            .clone(),
                    );
                    filter_nodes.extend(new_nodes);
                }
            }

            let policy_nodes: Vec<_> = base_nodes
                .into_iter()
                .chain(join_nodes.into_iter())
                .chain(filter_nodes.into_iter())
                .collect();

            assert!(policy_nodes.len() > 0, "no nodes where created for policy");

            security_nodes.extend(policy_nodes.clone());
            last_policy_nodes.push(policy_nodes.last().unwrap().clone())
        }

        if last_policy_nodes.len() > 1 {
            let final_node =
                self.make_union_from_same_base(&format!("sp_union_u{}", universe_id.0), last_policy_nodes, output_cols, is_leaf);

            security_nodes.push(final_node);
        }

        security_nodes
    }

    fn make_security_boundary(
        &mut self,
        universe: UniverseId,
        node_for_rel: &mut HashMap<&str, MirNodeRef>,
        prev_node: Option<MirNodeRef>,
        is_leaf: bool,
    ) -> Vec<MirNodeRef> {
        let mut security_nodes: Vec<MirNodeRef> = Vec::new();
        if universe.0 == "global".into() {
            return security_nodes;
        }

        let mut prev_node = prev_node.unwrap().clone();

        for (rel, _) in &node_for_rel.clone() {
            let nodes =
                self.make_security_nodes(*rel, &prev_node, universe.clone(), node_for_rel.clone(), is_leaf);
            debug!(
                self.log,
                "Created {} security nodes for table {}",
                nodes.len(),
                *rel
            );

            security_nodes.extend(nodes.clone());

            // Further nodes added should refer to the last node in base node's policy chain
            // instead of the base node itself.
            if !security_nodes.is_empty() {
                let last_pol = security_nodes.last().unwrap();
                node_for_rel.insert(*rel, last_pol.clone());
                prev_node = last_pol.clone();
            }
        }


        security_nodes
    }
}
