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
    ) -> (Vec<MirNodeRef>, Vec<MirNodeRef>);
}

impl SecurityBoundary for SqlToMirConverter {
    // TODO(larat): this is basically make_selection_nodes
    fn make_security_boundary(
        &mut self,
        universe: UniverseId,
        node_for_rel: &mut HashMap<&str, MirNodeRef>,
        prev_node: Option<MirNodeRef>,
    ) -> (Vec<MirNodeRef>, Vec<MirNodeRef>) {
        let mut security_nodes: Vec<MirNodeRef> = Vec::new();
        let mut last_security_nodes: Vec<MirNodeRef> = Vec::new();
        let mut prev_node = prev_node.unwrap().clone();

        if universe.0 == "global".into() {
            return (vec![prev_node], security_nodes);
        }


        for (rel, _) in &node_for_rel.clone() {
            let (last_nodes, nodes) =
                make_security_nodes(self, *rel, &prev_node, node_for_rel.clone());
            debug!(
                self.log,
                "Created {} security nodes for table {}",
                nodes.len(),
                *rel
            );

            security_nodes.extend(nodes.clone());
            last_security_nodes.extend(last_nodes.clone());

            // Further nodes added should refer to the last node in base node's policy chain
            // instead of the base node itself.
            if !security_nodes.is_empty() {
                let last_pol = security_nodes.last().unwrap();
                node_for_rel.insert(*rel, last_pol.clone());
                prev_node = last_pol.clone();
            }
        }


        (last_security_nodes, security_nodes)
    }
}

fn make_security_nodes(
    mir_converter: &mut SqlToMirConverter,
    rel: &str,
    prev_node: &MirNodeRef,
    node_for_rel: HashMap<&str, MirNodeRef>,
) -> (Vec<MirNodeRef>, Vec<MirNodeRef>) {
    let policies = match mir_converter.universe.policies.get(&String::from(rel)) {
        Some(p) => p.clone(),
        // no policies associated with this base node
        None => return (vec![], vec![]),
    };

    let mut node_count = 0;
    let mut local_node_for_rel = node_for_rel.clone();

    debug!(
        mir_converter.log,
        "Found {} policies for table {}",
        policies.len(),
        rel
    );

    let mut security_nodes = Vec::new();
    let mut last_policy_nodes = Vec::new();

    // Policies are created in parallel and later union'ed
    // Differently from normal queries, the policies order filters
    // before joins, since we can always reuse filter, but if a policy
    // joins against a context view, we are unable to reuse ir (context
    // views are universe-specific).
    for qg in policies.iter() {
        let mut prev_node = Some(prev_node.clone());
        let mut base_nodes: Vec<MirNodeRef> = Vec::new();
        let mut filter_nodes: Vec<MirNodeRef> = Vec::new();

        let mut sorted_rels: Vec<&str> = qg.relations.keys().map(String::as_str).collect();

        sorted_rels.sort();

        // all base nodes should be present in local_node_for_rel, except for context views
        // if policy uses a context view, add it to local_node_for_rel
        for rel in &sorted_rels {
            if *rel == "computed_columns" {
                continue;
            }
            if local_node_for_rel.contains_key(*rel) {
                local_node_for_rel.insert(*rel, prev_node.clone().unwrap());
                continue;
            }

            let view_for_rel = mir_converter.get_view(rel);

            local_node_for_rel.insert(*rel, view_for_rel.clone());
            base_nodes.push(view_for_rel.clone());
        }

        use controller::sql::mir::join::make_joins;

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
                let new_nodes = mir_converter.make_predicate_nodes(
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

            // update local node relations so joins know which views to join
            local_node_for_rel.insert(*rel, prev_node.clone().unwrap());
        }

        let join_nodes = make_joins(
            mir_converter,
            &format!("sp_{:x}", qg.signature().hash),
            qg,
            &local_node_for_rel,
            node_count
        );

        node_count += join_nodes.len();

        let policy_nodes: Vec<_> = base_nodes
            .into_iter()
            .chain(filter_nodes.into_iter())
            .chain(join_nodes.into_iter())
            .collect();

        assert!(policy_nodes.len() > 0, "no nodes where created for policy");

        security_nodes.extend(policy_nodes.clone());
        last_policy_nodes.push(policy_nodes.last().unwrap().clone())
    }

    (last_policy_nodes, security_nodes)
}
