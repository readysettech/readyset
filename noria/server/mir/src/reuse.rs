use tracing::trace;

use crate::column::Column;
use crate::node::node_inner::MirNodeInner;
use crate::node::MirNode;
use crate::query::MirQuery;
use crate::MirNodeRef;

pub fn rewind_until_columns_found(leaf: MirNodeRef, columns: &[Column]) -> Option<MirNodeRef> {
    let mut cur = leaf;
    loop {
        if cur.borrow().ancestors().len() != 1 {
            return None;
        }
        // silly, but the borrow checker doesn't let us do this in a single line
        let next = cur.borrow().ancestors().first()?.clone().upgrade().unwrap();

        cur = next;

        let mut missing_any = false;
        for c in columns {
            if !cur.borrow().columns().contains(c) {
                missing_any = true;
            }
        }
        if !missing_any {
            return Some(cur);
        }
    }
}

#[allow(clippy::cognitive_complexity)]
pub fn merge_mir_for_queries(new_query: &MirQuery, old_query: &MirQuery) -> (MirQuery, usize) {
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::rc::Rc;

    let mut trace_nodes = VecDeque::new();
    for old_base in &old_query.roots {
        let mut found = false;
        for new_base in &new_query.roots {
            if old_base.borrow().can_reuse_as(&*new_base.borrow()) {
                found = true;
                trace!("tracing from reusable base {:?}", old_base);
                trace_nodes.push_back((old_base.clone(), new_base.clone()));
            }
        }
        if !found {
            trace!("no reuseable base found for {:?}", old_base);
        }
    }

    // trace forwards from all matching bases in `old_query`, until no reuseable children are
    // found.
    let mut visited = HashSet::new();
    let mut reuse = HashMap::new();
    let mut reused = HashSet::new();
    while let Some((old, new)) = trace_nodes.pop_front() {
        let new_id = new.borrow().versioned_name();
        // reuseable node found, keep going
        trace!("found reuseable node {:?} for {:?}, continuing", old, new);
        assert!(!reuse.contains_key(&new_id));

        let reuse_node;
        {
            let o_ref = old.clone();
            let o = old.borrow();
            // Note that we manually build the `MirNode` here, rather than calling `MirNode::new()`
            // because `new()` automatically registers the node as a child with its ancestors. We
            // don't want to do this here because we later re-write the ancestors' child that this
            // node replaces to point to this node.
            reuse_node = Rc::new(RefCell::new(MirNode {
                name: o.name.clone(),
                from_version: o.from_version,
                columns: o.columns.clone(),
                inner: MirNodeInner::Reuse { node: o_ref },
                ancestors: o.ancestors.clone(),
                children: o.children.clone(),
                flow_node: None,
            }));
        }
        reuse.insert(new_id.clone(), reuse_node);

        // look for matching old node children for each of the new node's children.
        // If any are found, we can continue exploring that path, as the new query contains one
        // ore more child nodes from the old query.
        for new_child in new.borrow().children() {
            let new_child_id = new_child.borrow().versioned_name();
            if visited.contains(&new_child_id) {
                trace!("hit previously visited node {:?}, ignoring", new_child_id);
                continue;
            }

            trace!("visiting node {:?}", new_child_id);
            visited.insert(new_child_id.clone());

            let mut found = false;
            for old_child in old.borrow().children() {
                if old_child.borrow().can_reuse_as(&*new_child.borrow()) {
                    if reused.contains(&old_child.borrow().versioned_name()) {
                        continue;
                    }

                    trace!("add child {:?} to queue as it has a match", new_child_id);
                    trace_nodes.push_back((old_child.clone(), new_child.clone()));
                    found = true;
                    reused.insert(old_child.borrow().versioned_name());
                    break;
                }
            }
            if !found {
                // if no child of this node is reusable, we give up on this path
                trace!(
                    "no reuseable node found for {:?} in old query, giving up",
                    new_child
                );
            }
        }
    }

    // wire in the new `Reuse` nodes
    let mut rewritten_roots = Vec::new();
    let mut rewritten_leaf = new_query.leaf.clone();

    let mut q: VecDeque<MirNodeRef> = new_query.roots.iter().cloned().collect();
    let mut in_edge_counts = HashMap::new();
    for n in &q {
        in_edge_counts.insert(n.borrow().versioned_name(), 0);
    }

    let mut found_leaf = false;
    // topological order traversal of new query, replacing each node with its reuse node if one
    // exists (i.e., was created above)
    while let Some(n) = q.pop_front() {
        assert_eq!(in_edge_counts[&n.borrow().versioned_name()], 0);

        let ancestors: Vec<_> = n
            .borrow()
            .ancestors()
            .iter()
            .map(|n| n.upgrade().unwrap())
            .map(|a| match reuse.get(&a.clone().borrow().versioned_name()) {
                None => a,
                Some(reused) => reused.clone(),
            })
            .collect();
        let original_children = n.borrow().children().to_vec();
        let children: Vec<_> = n
            .borrow()
            .children()
            .iter()
            .map(|c| match reuse.get(&c.borrow().versioned_name()) {
                None => c,
                Some(reused) => reused,
            })
            .cloned()
            .collect();

        let real_n = match reuse.get(&n.borrow().versioned_name()) {
            None => n.clone(),
            Some(reused) => reused.clone(),
        };

        if ancestors.is_empty() {
            rewritten_roots.push(real_n.clone());
        }
        if children.is_empty() {
            assert!(!found_leaf); // should only find one leaf!
            found_leaf = true;
            rewritten_leaf = real_n.clone();
        }

        real_n.borrow_mut().ancestors = ancestors.iter().map(MirNodeRef::downgrade).collect();
        real_n.borrow_mut().children = children;

        for c in original_children {
            let cid = c.borrow().versioned_name();
            let in_edges = if in_edge_counts.contains_key(&cid) {
                in_edge_counts[&cid]
            } else {
                c.borrow().ancestors.len()
            };
            assert!(in_edges >= 1, "{} has no incoming edges!", cid);
            if in_edges == 1 {
                // last edge removed
                q.push_back(c.clone());
            }
            in_edge_counts.insert(cid, in_edges - 1);
        }
    }

    let rewritten_query = MirQuery {
        name: new_query.name.clone(),
        roots: rewritten_roots,
        leaf: rewritten_leaf,
    };

    (rewritten_query, reuse.len())
}

#[cfg(test)]
mod tests {
    use nom_sql::{self, ColumnSpecification, SqlType};
    use noria::internal::IndexType;
    use noria::ViewPlaceholder;

    use crate::column::Column;
    use crate::node::node_inner::MirNodeInner;
    use crate::node::MirNode;
    use crate::reuse::merge_mir_for_queries;
    use crate::MirNodeRef;

    fn make_nodes() -> (MirNodeRef, MirNodeRef, MirNodeRef, MirNodeRef) {
        let cspec = |n: &str| -> (ColumnSpecification, Option<usize>) {
            (
                ColumnSpecification::new(nom_sql::Column::from(n), SqlType::Text),
                None,
            )
        };
        let a = MirNode::new(
            "a".into(),
            0,
            vec![Column::from("aa"), Column::from("ab")],
            MirNodeInner::Base {
                column_specs: vec![cspec("aa"), cspec("ab")],
                primary_key: Some([Column::from("aa")].into()),
                unique_keys: Default::default(),
                adapted_over: None,
            },
            vec![],
            vec![],
        );
        let b = MirNode::new(
            "b".into(),
            0,
            vec![Column::from("ba"), Column::from("bb")],
            MirNodeInner::Base {
                column_specs: vec![cspec("ba"), cspec("bb")],
                primary_key: Some([Column::from("ba")].into()),
                unique_keys: Default::default(),
                adapted_over: None,
            },
            vec![],
            vec![],
        );
        let c = MirNode::new(
            "c".into(),
            0,
            vec![Column::from("aa"), Column::from("ba")],
            MirNodeInner::Join {
                on_left: vec![Column::from("ab")],
                on_right: vec![Column::from("bb")],
                project: vec![Column::from("aa"), Column::from("ba")],
            },
            vec![],
            vec![],
        );
        let d = MirNode::new(
            "d".into(),
            0,
            vec![Column::from("aa"), Column::from("ba")],
            MirNodeInner::leaf(
                vec![(Column::from("ba"), ViewPlaceholder::Generated)],
                IndexType::HashMap,
            ),
            vec![],
            vec![],
        );
        (a, b, c, d)
    }

    #[test]
    fn merge_mir() {
        use crate::node::MirNode;
        use crate::query::MirQuery;

        let (a, b, c, d) = make_nodes();

        let reuse_a = MirNode::reuse(a, 0);
        reuse_a.borrow_mut().add_child(c.clone());
        b.borrow_mut().add_child(c.clone());
        c.borrow_mut().add_ancestor(reuse_a.clone());
        c.borrow_mut().add_ancestor(b.clone());
        c.borrow_mut().add_child(d.clone());
        d.borrow_mut().add_ancestor(c);

        let mq1 = MirQuery {
            name: "q1".into(),
            roots: vec![reuse_a, b],
            leaf: d,
        };

        // when merging with ourselves, the result should consist entirely of reuse nodes
        let (merged_reflexive, _) = merge_mir_for_queries(&mq1, &mq1);
        assert!(merged_reflexive
            .topo_nodes()
            .iter()
            .all(|n| match n.borrow().inner {
                MirNodeInner::Reuse { .. } => true,
                _ => false,
            },));

        let (a, b, c, d) = make_nodes();
        let e = MirNode::new(
            "e".into(),
            0,
            vec![Column::from("aa")],
            MirNodeInner::Project {
                emit: vec![Column::from("aa")],
                expressions: vec![],
                literals: vec![],
            },
            vec![MirNodeRef::downgrade(&c)],
            vec![d.clone()],
        );
        a.borrow_mut().add_child(c.clone());
        b.borrow_mut().add_child(c.clone());
        c.borrow_mut().add_ancestor(a.clone());
        c.borrow_mut().add_ancestor(b.clone());
        d.borrow_mut().add_ancestor(e);

        // let's merge with a test query that is a simple extension of q1
        let mq2 = MirQuery {
            name: "q2".into(),
            roots: vec![a, b],
            leaf: d,
        };
        let (merged_extension, _) = merge_mir_for_queries(&mq2, &mq1);
        for n in merged_extension.topo_nodes() {
            match n.borrow().name().as_str() {
                // first three nodes (2x base, 1x join) should have been reused
                "a" | "b" | "c" => assert!(n.borrow().is_reused()),
                // new projection (e) and leaf node (d) should NOT have been reused
                "e" | "d" => assert!(!n.borrow().is_reused()),
                _ => unreachable!(),
            }
        }
    }
}
