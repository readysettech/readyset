use mir::MirNodeRef;
use mir::query::MirQuery;
use nom_sql::Column;

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

pub fn pull_required_base_columns(q: &mut MirQuery) {
    let mut queue = Vec::new();
    queue.push(q.leaf.clone());

    while !queue.is_empty() {
        let mn = queue.pop().unwrap();

        // a node needs all of the columns it projects into its output
        // however, it may also need *additional* columns to perform its functionality; consider,
        // e.g., a filter that filters on a column that it doesn't project
        let needed_columns: Vec<Column> = mn.borrow()
            .referenced_columns()
            .into_iter()
            .filter(|c| {
                !mn.borrow()
                    .ancestors()
                    .iter()
                    .any(|a| a.borrow().columns().contains(c))
            })
            .collect();

        let mut found: Vec<&Column> = Vec::new();
        for ancestor in mn.borrow().ancestors() {
            if ancestor.borrow().ancestors().len() == 0 {
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

// currently unused
#[allow(dead_code)]
pub fn push_all_base_columns(q: &mut MirQuery) {
    let mut queue = Vec::new();
    queue.extend(q.roots.clone());

    while !queue.is_empty() {
        let mn = queue.pop().unwrap();
        let columns: Vec<Column> = mn.borrow().columns().into_iter().cloned().collect();
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
