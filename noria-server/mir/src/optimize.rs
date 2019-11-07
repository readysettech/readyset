use dataflow::ops::filter::FilterCondition;
use dataflow::ops::grouped::filteraggregate::FilterAggregation;
use dataflow::ops::grouped::aggregate::Aggregation;
use node::{MirNode, MirNodeType};
use query::MirQuery;
use MirNodeRef;

use std::collections::HashMap;

pub fn optimize(mut q: &mut MirQuery) -> Vec<MirNodeRef> {
    //remove_extraneous_projections(&mut q);
    find_and_merge_filter_aggregates(&mut q)
}

pub fn optimize_post_reuse(_q: &mut MirQuery) {
    // find_and_merge_filter_chains(q);
}

fn find_and_merge_filter_aggregates(q: &mut MirQuery) -> Vec<MirNodeRef> {

    // 1. depth first search to find all the nodes, so we can process them later

    let mut node_stack = Vec::new();
    node_stack.extend(q.roots.iter().cloned());

    let mut visited_nodes = HashMap::new();
    let mut found_nodes = Vec::new();

    while !node_stack.is_empty() {
        let n = node_stack.pop().unwrap();
        let node_name = n.borrow().versioned_name();

        if visited_nodes.contains_key(&node_name) {
            continue;
        }

        for child in n.borrow().children.iter() {
            node_stack.push(child.clone());
        }

        visited_nodes.insert(node_name, true);
        found_nodes.push(n);
    }

    // 2. iterate over the nodes to find candidates, i.e.
    // nodes that are a filter followed by an aggregate with no other children/parents.
    // Initially we also considered nodes that are an aggregate followed by a filter,
    // but this case is rarer and may have the problem that it is filtering on the result
    // of the aggregate, in which case they cannot be merged.

    let mut candidate_nodes = Vec::new();

    for n in found_nodes {
        // if we've already set this to false, we shouldn't look at it more;
        // e.g. it might be merging with its parent node.
        let node_name = n.borrow().versioned_name();
        if !visited_nodes.get(&node_name).unwrap() {
            continue;
        }
        // now scan for candidacy
        let mut candidate = false;
        match n.borrow().inner {
            MirNodeType::Filter { .. }  => {
                // if there's exactly one child and it's an aggregation and it has exactly one parent,
                // then this is a candidate
                if n.borrow().children.len() == 1 {
                    let temp = n.borrow();
                    let child = temp.children.first().unwrap().borrow();
                    match child.inner {
                        MirNodeType::Aggregation { .. } => {
                            if child.ancestors.len() == 1 {
                                candidate = true;
                            }
                        },
                        _ => {},
                    };
                }
            },
            _ => {},
        };
        if candidate {
            candidate_nodes.push(n);
        }
    }
    println!("candidate_nodes={:?}", candidate_nodes);

    // 3. For each candidate, merge it, and update all parents/children
    // of the newly merged node.

    let mut new_nodes = Vec::new();

    for n in candidate_nodes {
        let temp = n.borrow();
        let child = temp.children.first().unwrap().borrow();
        println!("columns={:?}", n.borrow().columns);
        println!("child={:?}", child);
        println!("columns={:?}", child.columns);
        // determine which is which
        // (this was relevant when we supported either order of agg/filter,
        // but I'm leaving it in place in case we resume that)
        let (agg, filter) = match n.borrow().inner {
            MirNodeType::Aggregation { .. }  => (n.borrow().inner.clone(), child.inner.clone()),
            MirNodeType::Filter { .. } => (child.inner.clone(), n.borrow().inner.clone()),
            _ => unreachable!(),
        };

        let mut new_name = child.name.clone();
        new_name.push_str("_filteragg");

        let new_node = MirNode::new(
            &new_name,
            child.from_version,
            child.columns.clone(),
            MirNodeType::FilterAggregation {
                on: match &agg {
                    MirNodeType::Aggregation { on, group_by: _, kind: _ } => on.clone(),
                    _ => unreachable!(),
                },
                else_on: None,
                group_by: match &agg {
                    MirNodeType::Aggregation { on: _, group_by, kind: _ } => group_by.to_vec(),
                    _ => unreachable!(),
                },
                //group_by.into_iter().cloned().collect(),
                kind: match &agg {
                    MirNodeType::Aggregation { on: _, group_by: _, kind } => {
                        match kind {
                            Aggregation::COUNT => FilterAggregation::COUNT,
                            Aggregation::SUM => FilterAggregation::SUM,
                        }
                    },
                    _ => unreachable!(),
                },
                conditions: match &filter {
                    MirNodeType::Filter { ref conditions } => conditions.to_vec(),
                    _ => unreachable!(),
                },
            },
            n.borrow().ancestors.clone(),
            child.children.clone(),
        );
        println!("new_node={:?}", new_node);
        println!("columns={:?}", new_node.borrow().columns);

        // now update parents/children to reference the new node
        for c in child.children.iter() {
            println!("child={:?}", c);
            let mut new_ancestors = Vec::new();
            for a in c.borrow().ancestors.iter() {
                // TODO is versioned_name sufficiently unique for here (and below)?
                if a.borrow().versioned_name() == child.versioned_name() {
                    new_ancestors.push(new_node.clone());
                }
                else {
                    new_ancestors.push(a.clone());
                }
            }
            c.borrow_mut().ancestors = new_ancestors;
            println!("child_after={:?}", c);
        }
        for a in n.borrow().ancestors.iter() {
            println!("ancestor={:?}", a);
            let mut new_children = Vec::new();
            for c in a.borrow().children.iter() {
                if c.borrow().versioned_name() == n.borrow().versioned_name() {
                    // don't add anything here because MirNode::new()
                    // automatically adds things to their ancestors, so
                    // new_node is already in the list and we just need to
                    // ignore the old node here
                }
                else {
                    new_children.push(c.clone());
                }
            }
            a.borrow_mut().children = new_children;
            println!("ancestor_after={:?}", a);
        }
        println!("children={:?}", new_node.borrow().children);
        println!("ancestors={:?}", new_node.borrow().ancestors);

        new_nodes.push(new_node);
    }

    new_nodes
}

#[allow(dead_code)]
fn find_and_merge_filter_chains(q: &MirQuery) {
    let mut chained_filters = Vec::new();
    // depth first search
    let mut node_stack = Vec::new();
    node_stack.extend(q.roots.iter().cloned());

    let mut visited_nodes = HashMap::new();

    while !node_stack.is_empty() {
        let n = node_stack.pop().unwrap();
        let node_name = n.borrow().versioned_name();
        let mut end_chain = false;
        if visited_nodes.contains_key(&node_name) {
            continue;
        }

        visited_nodes.insert(node_name, true);

        match n.borrow().inner {
            MirNodeType::Filter { .. } => {
                try_add_node_to_chain(&n, &mut chained_filters);
            }
            _ => {
                // we need this because n most likely will be a children of
                // last_node. if that's the case, mutably borrowing the
                // child in end_filter_chain will cause a BorrowMutError
                // because it was already borrowed in the match.
                end_chain = true;
            }
        }

        if end_chain {
            end_filter_chain(&mut chained_filters);
        }

        for child in n.borrow().children.iter() {
            node_stack.push(child.clone());
        }
    }
}

fn try_add_node_to_chain(node: &MirNodeRef, chained_filters: &mut Vec<MirNodeRef>) {
    // any filter node can start a new chain
    if chained_filters.is_empty() || node.borrow().ancestors.len() == 1 {
        chained_filters.push(node.clone());
    } else {
        end_filter_chain(chained_filters);
        return;
    }

    if node.borrow().children.len() != 1 {
        end_filter_chain(chained_filters);
    }
}

fn end_filter_chain(chained_filters: &mut Vec<MirNodeRef>) {
    use std::cmp::max;

    if chained_filters.len() < 2 {
        chained_filters.clear();
        return;
    }

    {
        let first_node = chained_filters.first().unwrap();
        let last_node = chained_filters.last().unwrap();
        let schema_version = first_node.borrow().from_version;

        let name =
            chained_filters
                .iter()
                .fold("merged_filter_".to_string(), |mut acc, ref node| {
                    acc.push_str(node.borrow().name());
                    acc
                });

        let prev_node = first_node.borrow().ancestors.first().unwrap().clone();
        let fields = prev_node.borrow().columns().to_vec();
        let width = chained_filters.iter().fold(0, |mut acc, ref node| {
            let w = match node.borrow().inner {
                MirNodeType::Filter { ref conditions } => conditions.len(),
                _ => 0,
            };
            acc = max(acc, w);
            acc
        });
        let merged_conditions = to_conditions(chained_filters, width);

        let merged_filter = MirNode::new(
            name.as_str(),
            schema_version,
            fields,
            MirNodeType::Filter {
                conditions: merged_conditions.clone(),
            },
            vec![prev_node],
            vec![],
        );

        for ancestor in &first_node.borrow().ancestors {
            ancestor.borrow_mut().remove_child(first_node.clone());
        }

        first_node.borrow_mut().ancestors.clear();

        for child in &last_node.borrow().children {
            merged_filter.borrow_mut().add_child(child.clone());
            child.borrow_mut().add_ancestor(merged_filter.clone());
            child.borrow_mut().remove_ancestor(last_node.clone());
        }
    }

    chained_filters.clear();
}

fn to_conditions(
    chained_filters: &[MirNodeRef],
    num_columns: usize,
) -> Vec<Option<FilterCondition>> {
    let mut merged_conditions = vec![None; num_columns];
    for filter in chained_filters {
        match filter.borrow().inner {
            MirNodeType::Filter { ref conditions } => {
                // Note that this assumes that there is only ever one column being filtered on for
                // each filter that is being merged.
                let i = conditions.iter().position(Option::is_some).unwrap();
                merged_conditions[i] = conditions[i].clone();
            }
            _ => unreachable!(),
        }
    }

    merged_conditions
}

// currently unused
#[allow(dead_code)]
fn remove_extraneous_projections(_q: &mut MirQuery) {
    unimplemented!()
}
