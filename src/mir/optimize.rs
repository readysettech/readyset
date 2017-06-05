use mir::{MirQuery, MirNodeRef, MirNodeType, MirNode};
use std::collections::HashMap;
use nom_sql::Operator;
use flow::core::DataType;

pub fn optimize(q: MirQuery) -> MirQuery {
    //remove_extraneous_projections(&mut q);
    q
}

pub fn optimize_post_reuse(q: &mut MirQuery) {
    find_and_merge_filter_chains(q);
}

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
    if chained_filters.is_empty() {
        chained_filters.push(node.clone());
    } else if node.borrow().ancestors.len() == 1 {
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
        let schema_version = first_node.borrow().from_version.clone();

        let name = chained_filters
            .iter()
            .fold("merged_filter_".to_string(), |mut acc, ref node| {
                acc.push_str(node.borrow().name());
                acc
            });

        let prev_node = first_node.borrow().ancestors.first().unwrap().clone();
        let fields: Vec<_> = prev_node.borrow().columns().iter().cloned().collect();
        let width = chained_filters.iter().fold(0, |mut acc, ref node| {
            let w = match node.borrow().inner {
                MirNodeType::Filter { ref conditions } => conditions.len(),
                _ => 0,
            };
            acc = max(acc, w);
            acc
        });
        let merged_conditions = to_conditions(chained_filters, width);

        let merged_filter =
            MirNode::new(name.as_str(),
                         schema_version,
                         fields,
                         MirNodeType::Filter { conditions: merged_conditions.clone() },
                         vec![prev_node],
                         vec![]);

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

fn to_conditions(chained_filters: &Vec<MirNodeRef>,
                 num_columns: usize)
                 -> Vec<Option<(Operator, DataType)>> {

    let mut merged_conditions = vec![None; num_columns];
    for filter in chained_filters {
        match filter.borrow().inner {
            MirNodeType::Filter { ref conditions } => {
                // Note that this assumes that there is only ever one column being filtered on for
                // each filter that is being merged.
                let i = conditions.iter().position(|c| c.is_some()).unwrap();
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
