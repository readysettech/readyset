use nom_sql::{ArithmeticBase, ArithmeticExpression, Column, ColumnConstraint, ColumnSpecification,
              Operator, OrderType};
use std::collections::HashMap;

use core::{DataType, NodeIndex};
use mir::{FlowNode, MirNodeRef};
use mir::node::{GroupedNodeType, MirNode, MirNodeType};
use mir::query::{MirQuery, QueryFlowParts};
use dataflow::ops::join::{Join, JoinType};
use dataflow::ops::latest::Latest;
use dataflow::ops::project::{Project, ProjectExpression, ProjectExpressionBase};
use dataflow::ops;
use flow::Migration;

pub fn mir_query_to_flow_parts(mir_query: &mut MirQuery, mig: &mut Migration) -> QueryFlowParts {
    use std::collections::VecDeque;

    let mut new_nodes = Vec::new();
    let mut reused_nodes = Vec::new();

    // starting at the roots, add nodes in topological order
    let mut node_queue = VecDeque::new();
    node_queue.extend(mir_query.roots.iter().cloned());
    let mut in_edge_counts = HashMap::new();
    for n in &node_queue {
        in_edge_counts.insert(n.borrow().versioned_name(), 0);
    }
    while !node_queue.is_empty() {
        let n = node_queue.pop_front().unwrap();
        assert_eq!(in_edge_counts[&n.borrow().versioned_name()], 0);

        let flow_node = mir_node_to_flow_parts(&mut n.borrow_mut(), mig);
        match flow_node {
            FlowNode::New(na) => new_nodes.push(na),
            FlowNode::Existing(na) => reused_nodes.push(na),
        }

        for child in n.borrow().children.iter() {
            let nd = child.borrow().versioned_name();
            let in_edges = if in_edge_counts.contains_key(&nd) {
                in_edge_counts[&nd]
            } else {
                child.borrow().ancestors.len()
            };
            assert!(in_edges >= 1);
            if in_edges == 1 {
                // last edge removed
                node_queue.push_back(child.clone());
            }
            in_edge_counts.insert(nd, in_edges - 1);
        }
    }

    let leaf_na = mir_query
        .leaf
        .borrow()
        .flow_node
        .as_ref()
        .expect("Leaf must have FlowNode by now")
        .address();

    QueryFlowParts {
        name: mir_query.name.clone(),
        new_nodes: new_nodes,
        reused_nodes: reused_nodes,
        query_leaf: leaf_na,
    }
}

pub fn mir_node_to_flow_parts(mir_node: &mut MirNode, mig: &mut Migration) -> FlowNode {
    let name = mir_node.name.clone();
    match mir_node.flow_node {
        None => {
            let flow_node = match mir_node.inner {
                MirNodeType::Aggregation {
                    ref on,
                    ref group_by,
                    ref kind,
                } => {
                    assert_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_grouped_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        on,
                        group_by,
                        GroupedNodeType::Aggregation(kind.clone()),
                        mig,
                    )
                }
                MirNodeType::Base {
                    ref mut column_specs,
                    ref keys,
                    transactional,
                    ref adapted_over,
                } => match *adapted_over {
                    None => {
                        make_base_node(&name, column_specs.as_mut_slice(), keys, mig, transactional)
                    }
                    Some(ref bna) => adapt_base_node(
                        bna.over.clone(),
                        mig,
                        column_specs.as_mut_slice(),
                        &bna.columns_added,
                        &bna.columns_removed,
                    ),
                },
                MirNodeType::Extremum {
                    ref on,
                    ref group_by,
                    ref kind,
                } => {
                    assert_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_grouped_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        on,
                        group_by,
                        GroupedNodeType::Extremum(kind.clone()),
                        mig,
                    )
                }
                MirNodeType::Filter { ref conditions } => {
                    assert_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_filter_node(&name, parent, mir_node.columns.as_slice(), conditions, mig)
                }
                MirNodeType::GroupConcat {
                    ref on,
                    ref separator,
                } => {
                    assert_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    let group_cols = parent.borrow().columns().iter().cloned().collect();
                    make_grouped_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        on,
                        &group_cols,
                        GroupedNodeType::GroupConcat(separator.to_string()),
                        mig,
                    )
                }
                MirNodeType::Identity => {
                    assert_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_identity_node(&name, parent, mir_node.columns.as_slice(), mig)
                }
                MirNodeType::Join {
                    ref on_left,
                    ref on_right,
                    ref project,
                } => {
                    assert_eq!(mir_node.ancestors.len(), 2);
                    let left = mir_node.ancestors[0].clone();
                    let right = mir_node.ancestors[1].clone();
                    make_join_node(
                        &name,
                        left,
                        right,
                        mir_node.columns.as_slice(),
                        on_left,
                        on_right,
                        project,
                        JoinType::Inner,
                        mig,
                    )
                }
                MirNodeType::Latest { ref group_by } => {
                    assert_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_latest_node(&name, parent, mir_node.columns.as_slice(), group_by, mig)
                }
                MirNodeType::Leaf { ref keys, .. } => {
                    assert_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    materialize_leaf_node(&parent, name, keys, mig);
                    // TODO(malte): below is yucky, but required to satisfy the type system:
                    // each match arm must return a `FlowNode`, so we use the parent's one
                    // here.
                    let node = match *parent.borrow().flow_node.as_ref().unwrap() {
                        FlowNode::New(na) => FlowNode::Existing(na),
                        ref n @ FlowNode::Existing(..) => n.clone(),
                    };
                    node
                }
                MirNodeType::LeftJoin {
                    ref on_left,
                    ref on_right,
                    ref project,
                } => {
                    assert_eq!(mir_node.ancestors.len(), 2);
                    let left = mir_node.ancestors[0].clone();
                    let right = mir_node.ancestors[1].clone();
                    make_join_node(
                        &name,
                        left,
                        right,
                        mir_node.columns.as_slice(),
                        on_left,
                        on_right,
                        project,
                        JoinType::Left,
                        mig,
                    )
                }
                MirNodeType::Project {
                    ref emit,
                    ref literals,
                    ref arithmetic,
                } => {
                    assert_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_project_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        emit,
                        arithmetic,
                        literals,
                        mig,
                    )
                }
                MirNodeType::Reuse { ref node } => {
                    match *node.borrow()
                           .flow_node
                           .as_ref()
                           .expect("Reused MirNode must have FlowNode") {
                               // "New" => flow node was originally created for the node that we
                               // are reusing
                               FlowNode::New(na) |
                               // "Existing" => flow node was already reused from some other
                               // MIR node
                               FlowNode::Existing(na) => FlowNode::Existing(na),
                        }
                }
                MirNodeType::Union { ref emit } => {
                    assert_eq!(mir_node.ancestors.len(), emit.len());
                    make_union_node(
                        &name,
                        mir_node.columns.as_slice(),
                        emit,
                        mir_node.ancestors(),
                        mig,
                    )
                }
                MirNodeType::TopK {
                    ref order,
                    ref group_by,
                    ref k,
                    ref offset,
                } => {
                    assert_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_topk_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        order,
                        group_by,
                        *k,
                        *offset,
                        mig,
                    )
                }
            };

            // any new flow nodes have been instantiated by now, so we replace them with
            // existing ones, but still return `FlowNode::New` below in order to notify higher
            // layers of the new nodes.
            mir_node.flow_node = match flow_node {
                FlowNode::New(na) => Some(FlowNode::Existing(na)),
                ref n @ FlowNode::Existing(..) => Some(n.clone()),
            };
            flow_node
        }
        Some(ref flow_node) => flow_node.clone(),
    }
}


pub(crate) fn adapt_base_node(
    over_node: MirNodeRef,
    mig: &mut Migration,
    column_specs: &mut [(ColumnSpecification, Option<usize>)],
    add: &Vec<ColumnSpecification>,
    remove: &Vec<ColumnSpecification>,
) -> FlowNode {
    let na = match over_node.borrow().flow_node {
        None => panic!("adapted base node must have a flow node already!"),
        Some(ref flow_node) => flow_node.address(),
    };

    for a in add.iter() {
        let default_value = match a.constraints
            .iter()
            .filter_map(|c| match *c {
                ColumnConstraint::DefaultValue(ref dv) => Some(dv.into()),
                _ => None,
            })
            .next()
        {
            None => DataType::None,
            Some(dv) => dv,
        };
        let column_id = mig.add_column(na, &a.column.name, default_value);

        // store the new column ID in the column specs for this node
        for &mut (ref cs, ref mut cid) in column_specs.iter_mut() {
            if cs == a {
                assert_eq!(*cid, None);
                *cid = Some(column_id);
            }
        }
    }
    for r in remove.iter() {
        let over_node = over_node.borrow();
        let pos = over_node
            .column_specifications()
            .iter()
            .position(|&(ref ecs, _)| ecs == r)
            .unwrap();
        let cid = over_node.column_specifications()[pos]
            .1
            .expect("base column ID must be set to remove column");
        mig.drop_column(na, cid);
    }

    FlowNode::Existing(na)
}

pub(crate) fn make_base_node(
    name: &str,
    column_specs: &mut [(ColumnSpecification, Option<usize>)],
    pkey_columns: &Vec<Column>,
    mig: &mut Migration,
    transactional: bool,
) -> FlowNode {
    // remember the absolute base column ID for potential later removal
    for (i, cs) in column_specs.iter_mut().enumerate() {
        cs.1 = Some(i);
    }

    let column_names = column_specs
        .iter()
        .map(|&(ref cs, _)| &cs.column.name)
        .collect::<Vec<_>>();

    // note that this defaults to a "None" (= NULL) default value for columns that do not have one
    // specified; we don't currently handle a "NOT NULL" SQL constraint for defaults
    let default_values = column_specs
        .iter()
        .map(|&(ref cs, _)| {
            for c in &cs.constraints {
                match *c {
                    ColumnConstraint::DefaultValue(ref dv) => return dv.into(),
                    _ => (),
                }
            }
            return DataType::None;
        })
        .collect::<Vec<DataType>>();

    let base = if pkey_columns.len() > 0 {
        let pkey_column_ids = pkey_columns
            .iter()
            .map(|pkc| {
                //assert_eq!(pkc.table.as_ref().unwrap(), name);
                column_specs
                    .iter()
                    .position(|&(ref cs, _)| cs.column == *pkc)
                    .unwrap()
            })
            .collect();
        ops::base::Base::new(default_values).with_key(pkey_column_ids)
    } else {
        ops::base::Base::new(default_values)
    };

    if transactional {
        FlowNode::New(mig.add_transactional_base(
            name,
            column_names.as_slice(),
            base,
        ))
    } else {
        FlowNode::New(mig.add_ingredient(name, column_names.as_slice(), base))
    }
}

pub(crate) fn make_union_node(
    name: &str,
    columns: &[Column],
    emit: &Vec<Vec<Column>>,
    ancestors: &[MirNodeRef],
    mig: &mut Migration,
) -> FlowNode {
    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();
    let mut emit_column_id: HashMap<NodeIndex, Vec<usize>> = HashMap::new();

    // column_id_for_column doesn't take into consideration table aliases
    // which might cause improper ordering of columns in a union node
    // eg. Q6 in finkelstein.txt
    for (i, n) in ancestors.clone().iter().enumerate() {
        let emit_cols = emit[i]
            .iter()
            .map(|c| n.borrow().column_id_for_column(c))
            .collect::<Vec<_>>();

        let ni = n.borrow().flow_node_addr().unwrap();
        emit_column_id.insert(ni, emit_cols);
    }
    let node = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        ops::union::Union::new(emit_column_id),
    );

    FlowNode::New(node)
}

pub(crate) fn make_filter_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    conditions: &Vec<Option<(Operator, DataType)>>,
    mig: &mut Migration,
) -> FlowNode {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();

    let node = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        ops::filter::Filter::new(parent_na, conditions.as_slice()),
    );
    FlowNode::New(node)
}

pub(crate) fn make_grouped_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    on: &Column,
    group_by: &Vec<Column>,
    kind: GroupedNodeType,
    mig: &mut Migration,
) -> FlowNode {
    assert!(group_by.len() > 0);
    assert!(
        group_by.len() <= 6,
        format!(
            "can't have >6 group columns due to compound key restrictions, {} needs {}",
            name,
            group_by.len()
        )
    );

    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();

    let over_col_indx = parent.borrow().column_id_for_column(on);
    let group_col_indx = group_by
        .iter()
        .map(|c| parent.borrow().column_id_for_column(c))
        .collect::<Vec<_>>();

    assert!(group_col_indx.len() > 0);

    let na = match kind {
        GroupedNodeType::Aggregation(agg) => mig.add_ingredient(
            String::from(name),
            column_names.as_slice(),
            agg.over(parent_na, over_col_indx, group_col_indx.as_slice()),
        ),
        GroupedNodeType::Extremum(extr) => mig.add_ingredient(
            String::from(name),
            column_names.as_slice(),
            extr.over(parent_na, over_col_indx, group_col_indx.as_slice()),
        ),
        GroupedNodeType::GroupConcat(sep) => {
            use dataflow::ops::grouped::concat::{GroupConcat, TextComponent};

            let gc = GroupConcat::new(parent_na, vec![TextComponent::Column(over_col_indx)], sep);
            mig.add_ingredient(String::from(name), column_names.as_slice(), gc)
        }
    };
    FlowNode::New(na)
}


pub(crate) fn make_identity_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    mig: &mut Migration,
) -> FlowNode {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();

    let node = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        ops::identity::Identity::new(parent_na),
    );
    FlowNode::New(node)
}

pub(crate) fn make_join_node(
    name: &str,
    left: MirNodeRef,
    right: MirNodeRef,
    columns: &[Column],
    on_left: &Vec<Column>,
    on_right: &Vec<Column>,
    proj_cols: &Vec<Column>,
    kind: JoinType,
    mig: &mut Migration,
) -> FlowNode {
    use dataflow::ops::join::JoinSource;

    assert_eq!(on_left.len(), on_right.len());

    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();

    let projected_cols_left: Vec<Column> = left.borrow()
        .columns
        .iter()
        .filter(|c| proj_cols.contains(c))
        .cloned()
        .collect();
    let projected_cols_right: Vec<Column> = right
        .borrow()
        .columns
        .iter()
        .filter(|c| proj_cols.contains(c))
        .cloned()
        .collect();

    assert_eq!(
        projected_cols_left.len() + projected_cols_right.len(),
        proj_cols.len()
    );

    assert_eq!(on_left.len(), 1, "no support for multiple column joins");
    assert_eq!(on_right.len(), 1, "no support for multiple column joins");

    // this assumes the columns we want to join on appear first in the list
    // of projected columns. this is fine for joins against different tables
    // since we assume unique column names in each table. however, this is
    // not correct for joins against the same table, for example:
    // SELECT r1.a as a1, r2.a as a2 from r as r1, r as r2 where r1.a = r2.b and r2.a = r1.b;
    //
    // the `r1.a = r2.b` join predicate will create a join node with columns: r1.a, r1.b, r2.a, r2,b
    // however, because the way we deal with aliases, we can't distinguish between `r1.a` and `r2.a`
    // at this point in the codebase, so the `r2.a = r1.b` will join on the wrong `a` column.
    let left_join_col_id = projected_cols_left
        .iter()
        .position(|lc| lc == on_left.first().unwrap())
        .unwrap();
    let right_join_col_id = projected_cols_right
        .iter()
        .position(|rc| rc == on_right.first().unwrap())
        .unwrap();

    let join_config = projected_cols_left
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if i == left_join_col_id {
                JoinSource::B(i, right_join_col_id)
            } else {
                JoinSource::L(i)
            }
        })
        .chain(
            projected_cols_right
                .iter()
                .enumerate()
                .map(|(i, _)| JoinSource::R(i)),
        )
        .collect();

    let left_na = left.borrow().flow_node_addr().unwrap();
    let right_na = right.borrow().flow_node_addr().unwrap();

    let j = match kind {
        JoinType::Inner => Join::new(left_na, right_na, JoinType::Inner, join_config),
        JoinType::Left => Join::new(left_na, right_na, JoinType::Left, join_config),
    };
    let n = mig.add_ingredient(String::from(name), column_names.as_slice(), j);

    FlowNode::New(n)
}

pub(crate) fn make_latest_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    group_by: &Vec<Column>,
    mig: &mut Migration,
) -> FlowNode {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();

    let group_col_indx = group_by
        .iter()
        .map(|c| parent.borrow().column_id_for_column(c))
        .collect::<Vec<_>>();

    let na = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        Latest::new(parent_na, group_col_indx),
    );
    FlowNode::New(na)
}

// Converts a nom_sql::ArithmeticBase into a project::ProjectExpressionBase:
fn generate_projection_base(parent: &MirNodeRef, base: &ArithmeticBase) -> ProjectExpressionBase {
    match *base {
        ArithmeticBase::Column(ref column) => {
            let column_id = parent.borrow().column_id_for_column(column);
            ProjectExpressionBase::Column(column_id)
        }
        ArithmeticBase::Scalar(ref literal) => {
            let data: DataType = literal.into();
            ProjectExpressionBase::Literal(data)
        }
    }
}

pub(crate) fn make_project_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    emit: &Vec<Column>,
    arithmetic: &Vec<(String, ArithmeticExpression)>,
    literals: &Vec<(String, DataType)>,
    mig: &mut Migration,
) -> FlowNode {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();

    let projected_column_ids = emit.iter()
        .map(|c| parent.borrow().column_id_for_column(c))
        .collect::<Vec<_>>();

    let (_, literal_values): (Vec<_>, Vec<_>) = literals.iter().cloned().unzip();

    let projected_arithmetic: Vec<ProjectExpression> = arithmetic
        .iter()
        .map(|&(_, ref e)| {
            ProjectExpression::new(
                e.op.clone(),
                generate_projection_base(&parent, &e.left),
                generate_projection_base(&parent, &e.right),
            )
        })
        .collect();

    let n = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        Project::new(
            parent_na,
            projected_column_ids.as_slice(),
            Some(literal_values),
            Some(projected_arithmetic),
        ),
    );
    FlowNode::New(n)
}

pub(crate) fn make_topk_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    order: &Option<Vec<(Column, OrderType)>>,
    group_by: &Vec<Column>,
    k: usize,
    offset: usize,
    mig: &mut Migration,
) -> FlowNode {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();

    let group_by_indx = if group_by.is_empty() {
        // no query parameters, so we index on the first column
        vec![0 as usize]
    } else {
        group_by
            .iter()
            .map(|c| parent.borrow().column_id_for_column(c))
            .collect::<Vec<_>>()
    };

    let cmp_rows = match *order {
        Some(ref o) => {
            assert_eq!(offset, 0); // Non-zero offset not supported

            let columns: Vec<_> = o.iter()
                .map(|&(ref c, ref order_type)| {
                    // SQL and Soup disagree on what ascending and descending order means, so do the
                    // conversion here.
                    let reversed_order_type = match *order_type {
                        OrderType::OrderAscending => OrderType::OrderDescending,
                        OrderType::OrderDescending => OrderType::OrderAscending,
                    };
                    (parent.borrow().column_id_for_column(c), reversed_order_type)
                })
                .collect();

            columns
        }
        None => Vec::new(),
    };

    // make the new operator and record its metadata
    let na = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        ops::topk::TopK::new(parent_na, cmp_rows, group_by_indx, k),
    );
    FlowNode::New(na)
}

pub(crate) fn materialize_leaf_node(
    parent: &MirNodeRef,
    name: String,
    key_cols: &Vec<Column>,
    mig: &mut Migration,
) {
    let na = parent.borrow().flow_node_addr().unwrap();

    // we must add a new reader for this query. This also requires adding an identity node (at
    // least currently), since a node can only have a single associated reader. However, the
    // identity node exists at the MIR level, so we don't need to consider it here, as it has
    // already been added.

    // TODO(malte): consider the case when the projected columns need reordering

    if !key_cols.is_empty() {
        // TODO(malte): this does not yet cover the case when there are multiple query
        // parameters, which requires compound key support on Reader nodes.
        //assert_eq!(key_cols.len(), 1);
        let first_key_col_id = parent
            .borrow()
            .column_id_for_column(key_cols.iter().next().unwrap());
        mig.maintain(name, na, first_key_col_id);
    } else {
        // if no key specified, default to the first column
        mig.maintain(name, na, 0);
    }
}
