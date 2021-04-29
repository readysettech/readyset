use std::collections::HashMap;

use crate::column::Column;
use crate::query::MirQuery;
use crate::MirNodeRef;

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
    let mut base_node: MirNodeRef = leaf_node;
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
    nodes_to_rewrite.push(base_node);

    while !nodes_to_rewrite.is_empty() {
        let node_to_rewrite = nodes_to_rewrite.pop().unwrap();
        for col in &mut node_to_rewrite.borrow_mut().columns {
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

    while let Some(mn) = queue.pop() {
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

#[cfg(test)]
mod tests {
    use super::*;

    mod pull_required_base_columns {
        use dataflow::ops::filter::{FilterCondition, Value};
        use dataflow::ops::grouped::aggregate::Aggregation;
        use nom_sql::{
            BinaryOperator, ColumnSpecification, Expression, FunctionExpression, SqlType,
        };

        use crate::node::MirNode;
        use crate::node::node_inner::MirNodeInner;

        use super::*;

        #[test]
        fn changing_index() {
            let base = MirNode::new(
                "base",
                0,
                vec!["a".into(), "b".into(), "c".into()],
                MirNodeInner::Base {
                    column_specs: vec![
                        (
                            ColumnSpecification {
                                column: nom_sql::Column::from("a"),
                                sql_type: SqlType::Int(0),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                        (
                            ColumnSpecification {
                                column: nom_sql::Column::from("b"),
                                sql_type: SqlType::Int(0),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                        (
                            ColumnSpecification {
                                column: nom_sql::Column::from("c"),
                                sql_type: SqlType::Int(0),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                    ],
                    keys: vec!["a".into()],
                    adapted_over: None,
                },
                vec![],
                vec![],
            );

            // SUM(b)
            let grp = MirNode::new(
                "grp",
                0,
                vec!["agg".into()],
                MirNodeInner::Aggregation {
                    on: "b".into(),
                    group_by: vec![],
                    kind: Aggregation::SUM,
                },
                vec![base.clone()],
                vec![],
            );

            // σ[a = 1]
            let fil = MirNode::new(
                "fil",
                0,
                vec!["a".into(), "agg".into()],
                MirNodeInner::Filter {
                    conditions: vec![(
                        0,
                        FilterCondition::Comparison(
                            BinaryOperator::Equal,
                            Value::Constant(1.into()),
                        ),
                    )],
                },
                vec![grp.clone()],
                vec![],
            );

            // a, agg, IFNULL(c, 0) as c0
            let prj = MirNode::new(
                "prj",
                0,
                vec!["a".into(), "agg".into(), "c0".into()],
                MirNodeInner::Project {
                    emit: vec!["a".into(), "agg".into()],
                    expressions: vec![(
                        "c0".to_owned(),
                        Expression::Call(FunctionExpression::Call {
                            name: "ifnull".to_owned(),
                            arguments: vec![
                                Expression::Column("c".into()),
                                Expression::Literal(0.into()),
                            ],
                        }),
                    )],
                    literals: vec![],
                },
                vec![fil.clone()],
                vec![],
            );

            let mut query = MirQuery {
                name: "changing_index".to_owned(),
                roots: vec![base],
                leaf: prj,
            };

            pull_required_base_columns(&mut query, None, false);

            assert_eq!(
                grp.borrow().columns(),
                &[Column::from("c"), Column::from("a"), Column::from("agg")]
            );

            // The filter has to add the column in the same place as the aggregate
            assert_eq!(
                fil.borrow().columns(),
                &[Column::from("c"), Column::from("a"), Column::from("agg")]
            );

            // The filter has to filter on the correct field
            match &fil.borrow().inner {
                MirNodeInner::Filter { conditions } => assert_eq!(conditions.first().unwrap().0, 1),
                _ => unreachable!(),
            };
        }
    }
}
