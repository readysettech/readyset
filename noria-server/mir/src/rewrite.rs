use column::Column;
use query::MirQuery;
use std::collections::HashMap;
use MirNodeRef;

fn has_column(n: &MirNodeRef, column: &Column) -> bool {
    if n.borrow().columns().contains(column) {
        return true;
    } else {
        for a in n.borrow().ancestors() {
            if has_column(a, column) {
                return true;
            }
        }
    }
    false
}

pub(super) fn make_universe_naming_consistent(
    q: &mut MirQuery,
    table_mapping: &HashMap<(String, Option<String>), String>,
    base_name: String,
) {
    let mut queue = Vec::new();
    let new_q = q.clone();
    queue.push(q.leaf.clone());

    let leaf_node: MirNodeRef = new_q.leaf;
    let mut nodes_to_check: Vec<MirNodeRef> = Vec::new();
    nodes_to_check.push(leaf_node.clone());

    // get the node that is the base table of the universe
    let mut base_node: MirNodeRef = leaf_node.clone();
    while !nodes_to_check.is_empty() {
        let node_to_check = nodes_to_check.pop().unwrap();
        if node_to_check.borrow().name == base_name {
            base_node = node_to_check;
            break;
        }
        for parent in node_to_check.borrow().ancestors() {
            nodes_to_check.push(parent.clone());
        }
    }

    let mut nodes_to_rewrite: Vec<MirNodeRef> = Vec::new();
    nodes_to_rewrite.push(base_node.clone());

    while !nodes_to_rewrite.is_empty() {
        let node_to_rewrite = nodes_to_rewrite.pop().unwrap();
        for mut col in &mut node_to_rewrite.borrow_mut().columns {
            let mut _res = {
                match col.table {
                    Some(ref table) => {
                        let key = (col.name.to_owned(), Some(table.to_owned()));
                        table_mapping.get(&key).cloned()
                    }
                    None => None,
                }
            };
        }

        for child in node_to_rewrite.borrow().children() {
            nodes_to_rewrite.push(child.clone());
        }
    }
}

pub(super) fn pull_required_base_columns(
    q: &mut MirQuery,
    table_mapping: Option<&HashMap<(String, Option<String>), String>>,
    sec: bool,
) {
    let mut queue = Vec::new();
    queue.push(q.leaf.clone());

    if sec {
        match table_mapping {
            Some(_) => (),
            None => panic!("no table mapping computed, but in secure universe."),
        }
    }

    while !queue.is_empty() {
        let mn = queue.pop().unwrap();
        // a node needs all of the columns it projects into its output
        // however, it may also need *additional* columns to perform its functionality; consider,
        // e.g., a filter that filters on a column that it doesn't project
        let needed_columns: Vec<Column> = mn
            .borrow()
            .referenced_columns()
            .into_iter()
            .filter(|c| {
                !mn.borrow()
                    .ancestors()
                    .iter()
                    .any(|a| a.borrow().columns().iter().any(|ac| ac == c))
            })
            .collect();

        let mut found: Vec<&Column> = Vec::new();
        match table_mapping {
            Some(ref map) => {
                for ancestor in mn.borrow().ancestors() {
                    if ancestor.borrow().ancestors().is_empty() {
                        // base, do nothing
                        continue;
                    }
                    for c in &needed_columns {
                        match c.table {
                            Some(ref table) => {
                                let key = (c.name.to_owned(), Some(table.to_owned()));
                                if !map.contains_key(&key)
                                    && !found.contains(&c)
                                    && has_column(ancestor, c)
                                {
                                    ancestor.borrow_mut().add_column(c.clone());
                                    found.push(c);
                                }
                            }
                            None => {
                                if !map.contains_key(&(c.name.to_owned(), None))
                                    && !found.contains(&c)
                                    && has_column(ancestor, c)
                                {
                                    ancestor.borrow_mut().add_column(c.clone());
                                    found.push(c);
                                }
                            }
                        }
                    }
                    queue.push(ancestor.clone());
                }
            }
            None => {
                for ancestor in mn.borrow().ancestors() {
                    if ancestor.borrow().ancestors().is_empty() {
                        // base, do nothing
                        continue;
                    }
                    for c in &needed_columns {
                        if !found.contains(&c) && has_column(ancestor, c) {
                            ancestor.borrow_mut().add_column(c.clone());
                            found.push(c);
                        }
                    }
                    queue.push(ancestor.clone());
                }
            }
        }
    }
}

// currently unused
#[allow(dead_code)]
pub(super) fn push_all_base_columns(q: &mut MirQuery) {
    let mut queue = Vec::new();
    queue.extend(q.roots.clone());

    while !queue.is_empty() {
        let mn = queue.pop().unwrap();
        let columns = mn.borrow().columns().to_vec();
        for child in mn.borrow().children() {
            // N.B. this terminates before reaching the actual leaf, since the last node of the
            // query (before the MIR `Leaf` node) already carries the query name. (`Leaf` nodes are
            // virtual nodes that will be removed and converted into materializations.)
            if child.borrow().versioned_name() == q.leaf.borrow().versioned_name() {
                continue;
            }
            for c in &columns {
                // push through if the child doesn't already have this column
                if !child.borrow().columns().contains(c) {
                    child.borrow_mut().add_column(c.clone());
                }
            }
            queue.push(child.clone());
        }
    }
}
