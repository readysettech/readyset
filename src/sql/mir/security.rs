use mir::MirNodeRef;
use mir::node::MirNode;
use ops::join::JoinType;
use sql::mir::SqlToMirConverter;
use sql::query_graph::QueryGraphEdge;
use std::collections::{HashMap, HashSet};
use flow::core::DataType;

pub trait SecurityBoundary {
    fn make_security_boundary(
        &self,
        universe: DataType,
        node_for_rel: &mut HashMap<&str, MirNodeRef>,
        prev_node: Option<MirNodeRef>,
    ) -> Vec<MirNodeRef>;

    fn make_security_nodes(
        &self,
        rel: &str,
        base_node: &MirNodeRef,
        universe_id: DataType,
        node_for_rel: HashMap<&str, MirNodeRef>,
    ) -> Vec<MirNodeRef>;
}

impl SecurityBoundary for SqlToMirConverter {
    // TODO(larat): this is basically make_selection_nodes
    fn make_security_nodes(
        &self,
        rel: &str,
        prev_node: &MirNodeRef,
        universe_id: DataType,
        node_for_rel: HashMap<&str, MirNodeRef>,
    ) -> Vec<MirNodeRef> {
        use std::cmp::Ordering;
        let policies = match self.policies.get(&(universe_id.clone(), String::from(rel))) {
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
            let mut join_nodes: Vec<MirNodeRef> = Vec::new();
            let mut filter_nodes: Vec<MirNodeRef> = Vec::new();
            let mut joined_tables = HashSet::new();

            let mut sorted_rels: Vec<&str> = qg.relations.keys().map(String::as_str).collect();

            sorted_rels.sort();

            let mut sorted_edges: Vec<(&(String, String), &QueryGraphEdge)> =
                qg.edges.iter().collect();

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

                let latest_existing = self.current.get(*rel);
                let view_for_rel = match latest_existing {
                    None => panic!("Policy refers to unknown view \"{}\"", rel),
                    Some(v) => {
                        let existing = self.nodes.get(&(String::from(*rel), *v));
                        match existing {
                            None => {
                                panic!(
                                    "Inconsistency: base node \"{}\" does not exist at v{}",
                                    *rel,
                                    v
                                );
                            }
                            Some(bmn) => MirNode::reuse(bmn.clone(), self.schema_version),
                        }
                    }
                };

                local_node_for_rel.insert(*rel, view_for_rel.clone());
                base_nodes.push(view_for_rel.clone());
            }

            // Sort the edges to ensure deterministic join order.
            sorted_edges.sort_by(|&(a, _), &(b, _)| {
                let src_ord = a.0.cmp(&b.0);
                if src_ord == Ordering::Equal {
                    a.1.cmp(&b.1)
                } else {
                    src_ord
                }
            });

            // handles joins against UserContext table
            for &(&(ref src, ref dst), edge) in &sorted_edges {
                let (join_type, jps) = match *edge {
                    QueryGraphEdge::LeftJoin(ref jps) => (JoinType::Left, jps),
                    QueryGraphEdge::Join(ref jps) => (JoinType::Inner, jps),
                    _ => continue,
                };

                for jp in jps.iter() {
                    let (left_node, right_node) = self.pick_join_columns(
                        src,
                        dst,
                        prev_node,
                        &joined_tables,
                        &local_node_for_rel,
                    );
                    let jn = self.make_join_node(
                        &format!("sp_{:x}_n{:x}", qg.signature().hash, node_count),
                        jp,
                        left_node,
                        right_node,
                        join_type.clone(),
                    );
                    join_nodes.push(jn.clone());
                    prev_node = Some(jn);

                    joined_tables.insert(src);
                    joined_tables.insert(dst);
                    node_count += 1;
                }
            }

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
                self.make_union_node(&format!("sp_union_u{}", universe_id), last_policy_nodes, output_cols);

            security_nodes.push(final_node);
        }

        security_nodes
    }

    fn make_security_boundary(
        &self,
        universe: DataType,
        node_for_rel: &mut HashMap<&str, MirNodeRef>,
        prev_node: Option<MirNodeRef>,
    ) -> Vec<MirNodeRef> {
        let mut security_nodes: Vec<MirNodeRef> = Vec::new();
        if universe == "global".into() {
            return security_nodes;
        }

        let mut prev_node = prev_node.unwrap().clone();

        for (rel, _) in &node_for_rel.clone() {
            let nodes =
                self.make_security_nodes(*rel, &prev_node, universe.clone(), node_for_rel.clone());
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
