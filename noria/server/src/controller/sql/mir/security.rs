use crate::controller::sql::mir::rewrite::make_rewrite_nodes;
use crate::controller::sql::mir::SqlToMirConverter;
use crate::controller::sql::query_graph::QueryGraph;
use crate::controller::sql::query_signature::Signature;
use crate::controller::sql::UniverseId;
use crate::errors::internal_err;
use crate::ReadySetResult;
use dataflow::ops::union;
use mir::MirNodeRef;
use noria::{internal, invariant, ReadySetError};
use std::collections::HashMap;

pub trait SecurityBoundary {
    fn reconcile(
        &mut self,
        name: &str,
        qg: &QueryGraph,
        ancestors: &[MirNodeRef],
        node_count: usize,
        sec: bool,
    ) -> ReadySetResult<(
        Vec<MirNodeRef>,
        Option<HashMap<(String, Option<String>), String>>,
        String,
    )>;

    fn make_security_boundary(
        &self,
        universe: UniverseId,
        node_for_rel: &mut HashMap<&str, MirNodeRef>,
        prev_node: Option<MirNodeRef>,
    ) -> Result<(Vec<MirNodeRef>, Vec<MirNodeRef>), ReadySetError>;
}

impl SecurityBoundary for SqlToMirConverter {
    fn reconcile(
        &mut self,
        name: &str,
        qg: &QueryGraph,
        ancestors: &[MirNodeRef],
        node_count: usize,
        sec: bool,
    ) -> ReadySetResult<(
        Vec<MirNodeRef>,
        Option<HashMap<(String, Option<String>), String>>,
        String,
    )> {
        use crate::controller::sql::mir::grouped::make_grouped;

        let mut nodes_added = Vec::new();
        let mut node_count = node_count;

        // If query DOESN'T have any computed columns, we are done.
        // if qg.relations.get("computed_columns").is_none() {
        //     return (nodes_added, None, "".to_string());
        // }

        // First, union the results from all ancestors
        let (union, mapping) = if !sec {
            (
                Some(self.make_union_node(
                    &format!("{}_n{}", name, node_count),
                    &ancestors,
                    union::DuplicateMode::UnionAll,
                )?),
                None,
            )
        } else {
            let (u, m) =
                self.make_union_node_sec(&format!("{}_n{}", name, node_count), &ancestors)?;
            (Some(u), m)
        };

        Ok(match union {
            Some(node) => {
                let n = node.borrow().name.clone();
                nodes_added.push(node.clone());
                node_count += 1;

                // If query has computed columns, we need to reconcile grouped
                // results. This means grouping and aggregation the results one
                // more time.
                let grouped = make_grouped(
                    self,
                    name,
                    &qg,
                    &HashMap::new(), // we only care about this, if no parent node is specified.
                    node_count,
                    &mut Some(node),
                    true,
                    &HashMap::new(),
                )?;

                nodes_added.extend(grouped);
                (nodes_added, mapping, n)
            }
            None => {
                internal!("union not computed correctly");
            }
        })
    }

    // TODO(larat): this is basically make_selection_nodes
    fn make_security_boundary(
        &self,
        universe: UniverseId,
        node_for_rel: &mut HashMap<&str, MirNodeRef>,
        prev_node: Option<MirNodeRef>,
    ) -> Result<(Vec<MirNodeRef>, Vec<MirNodeRef>), ReadySetError> {
        let mut security_nodes: Vec<MirNodeRef> = Vec::new();
        let mut last_security_nodes: Vec<MirNodeRef> = Vec::new();
        let mut prev_node = prev_node.unwrap();

        if universe.0 == "global".into() {
            return Ok((vec![prev_node], security_nodes));
        }

        // TODO(jfrg): why is this okay? we're iterating over keys of a collection we're modifying
        for rel in node_for_rel.clone().keys() {
            let (last_nodes, nodes) =
                make_security_nodes(self, *rel, &prev_node, node_for_rel.clone())?;
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

        if last_security_nodes.is_empty() {
            last_security_nodes.push(prev_node);
        }

        Ok((last_security_nodes, security_nodes))
    }
}

fn make_security_nodes(
    mir_converter: &SqlToMirConverter,
    table: &str,
    prev_node: &MirNodeRef,
    node_for_rel: HashMap<&str, MirNodeRef>,
) -> Result<(Vec<MirNodeRef>, Vec<MirNodeRef>), ReadySetError> {
    let policies = match mir_converter
        .universe
        .row_policies
        .get(&String::from(table))
    {
        Some(p) => p.clone(),
        // no policies associated with this base node
        None => return Ok((vec![], vec![])),
    };

    let mut node_count = 0;
    let mut local_node_for_rel = node_for_rel.clone();

    debug!(
        mir_converter.log,
        "Found {} row policies for table {}",
        policies.len(),
        table
    );

    let mut security_nodes = Vec::new();
    let mut last_policy_nodes = Vec::new();

    // Policies are created in parallel and later union'ed
    // Differently from normal queries, the policies order filters
    // before joins, since we can always reuse filter, but if a policy
    // joins against a context view, we are unable to reuse it (context
    // views are universe-specific).
    for qg in policies.iter() {
        let mut prev_node = Some(prev_node.clone());
        let mut base_nodes: Vec<MirNodeRef> = Vec::new();
        let mut filter_nodes: Vec<MirNodeRef> = Vec::new();

        let mut sorted_rels: Vec<&str> = qg.relations.keys().map(String::as_str).collect();

        sorted_rels.sort_unstable();

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

            let view_for_rel = mir_converter.get_view(rel)?;

            local_node_for_rel.insert(*rel, view_for_rel.clone());
            base_nodes.push(view_for_rel.clone());
        }

        use crate::controller::sql::mir::join::make_joins;

        // handles predicate nodes
        for rel in &sorted_rels {
            let qgn = qg
                .relations
                .get(*rel)
                .ok_or_else(|| internal_err("relation should have a query graph node."))?;
            invariant!(*rel != "computed_collumns");

            // Skip empty predicates
            if qgn.predicates.is_empty() {
                continue;
            }

            for pred in &qgn.predicates {
                let new_nodes = mir_converter.make_predicate_nodes(
                    &format!("sp_{:x}_n{:x}", qg.signature().hash, node_count),
                    prev_node.ok_or_else(|| internal_err("empty previous node"))?,
                    pred,
                    0,
                )?;

                prev_node = Some(
                    new_nodes
                        .iter()
                        .last()
                        .ok_or_else(|| internal_err("no new nodes were created"))?
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
            node_count,
        )?;

        node_count += join_nodes.len();

        let prev_node = match join_nodes.last() {
            Some(n) => n.clone(),
            None => local_node_for_rel[table].clone(),
        };

        let rewrite_nodes = make_rewrite_nodes(
            mir_converter,
            &format!("sp_{:x}", qg.signature().hash),
            prev_node,
            table,
            node_count,
        )?;

        node_count += rewrite_nodes.len();

        let policy_nodes: Vec<_> = base_nodes
            .into_iter()
            .chain(filter_nodes.into_iter())
            .chain(join_nodes.into_iter())
            .chain(rewrite_nodes.into_iter())
            .collect();

        assert!(
            !policy_nodes.is_empty(),
            "no nodes where created for policy"
        );

        security_nodes.extend(policy_nodes.clone());
        last_policy_nodes.push(policy_nodes.last().unwrap().clone())
    }

    Ok((last_policy_nodes, security_nodes))
}
