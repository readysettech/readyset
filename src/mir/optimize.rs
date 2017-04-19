use mir::{MirQuery, MirNodeRef, MirNodeType};
use std::collections::HashMap;

pub fn optimize(q: MirQuery) -> MirQuery {
    //remove_extraneous_projections(&mut q);
    q
}

pub fn optimize_post_reuse(q: &mut MirQuery) {
  let chains = find_filter_chains(q);
  println!("filter chains are {:?}", chains );
  for chain in chains {
    merge_filter_chain(q, chain);
  }
}

fn merge_filter_chain(q: &mut MirQuery, chain: Vec<MirNodeRef>) {

}

fn find_filter_chains(q: &MirQuery) -> Vec<Vec<MirNodeRef>> {
  let mut chains = Vec::new();

  let mut current_chain = Vec::new();
  // depth first search
  let mut node_stack = Vec::new();
  node_stack.extend(q.roots.iter().cloned());

  let mut visited_nodes = HashMap::new();

  while !node_stack.is_empty() {
    let n = node_stack.pop().unwrap();
    let node_name = n.borrow().versioned_name();
    if visited_nodes.contains_key(&node_name) {
      continue
    }

    visited_nodes.insert(node_name, true);

    match n.borrow().inner {
      MirNodeType::Filter { .. } => try_add_node_to_chain(&n, &mut current_chain, &mut chains),
      _ => end_filter_chain(&mut current_chain, &mut chains),
    }

    for child in n.borrow().children.iter() {
      node_stack.push(child.clone());
    }
  }

  chains
}

fn try_add_node_to_chain(node: &MirNodeRef,
                        current_chain: &mut Vec<MirNodeRef>,
                        chains: &mut Vec<Vec<MirNodeRef>>) {
  // any filter node can start a new chain
  if current_chain.is_empty() {
    current_chain.push(node.clone());
  } else if node.borrow().ancestors.len() == 1 { // only nodes with a single ancestor can be added to the chain
    current_chain.push(node.clone());
  } else {
    end_filter_chain(current_chain, chains);
  }

  if node.borrow().children.len() > 1 {
    end_filter_chain(current_chain, chains);
  }


}

fn end_filter_chain(current_chain: &mut Vec<MirNodeRef>, chains: &mut Vec<Vec<MirNodeRef>>) {
  if !current_chain.is_empty() {
    chains.push(current_chain.to_vec());
    current_chain.clear();
  }
}

// currently unused
#[allow(dead_code)]
fn remove_extraneous_projections(_q: &mut MirQuery) {
    unimplemented!()
}
