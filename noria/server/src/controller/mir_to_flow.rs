use nom_sql::{
    BinaryOperator, ColumnConstraint, ColumnSpecification, Expression, FunctionExpression, Literal,
    OrderType, UnaryOperator,
};
use std::collections::HashMap;

use crate::controller::Migration;
use crate::errors::internal_err;
use crate::manual::ops::grouped::aggregate::Aggregation;
use crate::{internal, invariant, invariant_eq, unsupported, ReadySetError, ReadySetResult};
use common::DataType;
use dataflow::ops::filter::FilterCondition;
use dataflow::ops::join::{Join, JoinType};
use dataflow::ops::latest::Latest;
use dataflow::ops::param_filter::ParamFilter;
use dataflow::ops::project::Project;
use dataflow::{node, ops, BuiltinFunction, Expression as DataflowExpression};
use mir::node::node_inner::MirNodeInner;
use mir::node::{GroupedNodeType, MirNode};
use mir::query::{MirQuery, QueryFlowParts};
use mir::{Column, FlowNode, MirNodeRef};

use crate::errors::ReadySetError::MirUnsupportedCondition;
use petgraph::graph::NodeIndex;

pub(super) fn mir_query_to_flow_parts(
    mir_query: &mut MirQuery,
    mig: &mut Migration,
    table_mapping: Option<&HashMap<(String, Option<String>), String>>,
) -> ReadySetResult<QueryFlowParts> {
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
        invariant_eq!(in_edge_counts[&n.borrow().versioned_name()], 0);
        let (name, from_version) = {
            let n = n.borrow_mut();
            (n.name.clone(), n.from_version)
        };
        let flow_node =
            mir_node_to_flow_parts(&mut n.borrow_mut(), mig, table_mapping).map_err(|e| {
                ReadySetError::MirNodeCreationFailed {
                    name,
                    from_version,
                    source: Box::new(e),
                }
            })?;
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
            invariant!(in_edges >= 1);
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
        .ok_or_else(|| internal_err("Leaf must have FlowNode by now"))?
        .address();

    Ok(QueryFlowParts {
        name: mir_query.name.clone(),
        new_nodes,
        reused_nodes,
        query_leaf: leaf_na,
    })
}

fn mir_node_to_flow_parts(
    mir_node: &mut MirNode,
    mig: &mut Migration,
    table_mapping: Option<&HashMap<(String, Option<String>), String>>,
) -> ReadySetResult<FlowNode> {
    let name = mir_node.name.clone();
    Ok(match mir_node.flow_node {
        None => {
            #[allow(clippy::let_and_return)]
            let flow_node = match mir_node.inner {
                MirNodeInner::Aggregation {
                    ref on,
                    ref group_by,
                    ref kind,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_grouped_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        on,
                        group_by,
                        GroupedNodeType::Aggregation(kind.clone()),
                        mig,
                        table_mapping,
                    )?
                }
                MirNodeInner::Base {
                    ref mut column_specs,
                    ref keys,
                    ref adapted_over,
                } => match *adapted_over {
                    None => make_base_node(&name, column_specs.as_mut_slice(), keys, mig),
                    Some(ref bna) => adapt_base_node(
                        bna.over.clone(),
                        mig,
                        column_specs.as_mut_slice(),
                        &bna.columns_added,
                        &bna.columns_removed,
                    )?,
                },
                MirNodeInner::Extremum {
                    ref on,
                    ref group_by,
                    ref kind,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_grouped_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        on,
                        group_by,
                        GroupedNodeType::Extremum(kind.clone()),
                        mig,
                        table_mapping,
                    )?
                }
                MirNodeInner::Filter {
                    ref conditions,
                    ref remapped_exprs_to_parent_names,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();

                    make_filter_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        conditions.clone(),
                        mig,
                        remapped_exprs_to_parent_names.as_ref(),
                    )?
                }
                MirNodeInner::Identity => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_identity_node(&name, parent, mir_node.columns.as_slice(), mig)
                }
                MirNodeInner::Join {
                    ref on_left,
                    ref on_right,
                    ref project,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 2);
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
                    )?
                }
                MirNodeInner::JoinAggregates => {
                    invariant_eq!(mir_node.ancestors.len(), 2);
                    let left = mir_node.ancestors[0].clone();
                    let right = mir_node.ancestors[1].clone();
                    make_join_aggregates_node(&name, left, right, mir_node.columns.as_slice(), mig)?
                }
                MirNodeInner::ParamFilter {
                    ref col,
                    ref emit_key,
                    ref operator,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_param_filter_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        col,
                        emit_key,
                        operator,
                        mig,
                        table_mapping,
                    )?
                }
                MirNodeInner::Latest { ref group_by } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_latest_node(&name, parent, mir_node.columns.as_slice(), group_by, mig)?
                }
                MirNodeInner::Leaf {
                    ref keys,
                    ref operator,
                    ..
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    materialize_leaf_node(&parent, name, keys, mig, *operator);
                    // TODO(malte): below is yucky, but required to satisfy the type system:
                    // each match arm must return a `FlowNode`, so we use the parent's one
                    // here.
                    // FIXME(eta): get rid of unwraps
                    let node = match *parent.borrow().flow_node.as_ref().unwrap() {
                        FlowNode::New(na) => FlowNode::Existing(na),
                        ref n @ FlowNode::Existing(..) => n.clone(),
                    };
                    node
                }
                MirNodeInner::LeftJoin {
                    ref on_left,
                    ref on_right,
                    ref project,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 2);
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
                    )?
                }
                MirNodeInner::Project {
                    ref emit,
                    ref literals,
                    ref expressions,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_project_node(
                        &name,
                        parent,
                        mir_node.columns.as_slice(),
                        emit,
                        expressions,
                        literals,
                        mig,
                        table_mapping,
                    )?
                }
                MirNodeInner::Reuse { ref node } => {
                    match *node.borrow()
                        .flow_node
                        .as_ref()
                        .ok_or_else(|| internal_err("Reused MirNode must have FlowNode"))? {
                        // "New" => flow node was originally created for the node that we
                        // are reusing
                        FlowNode::New(na) |
                        // "Existing" => flow node was already reused from some other
                        // MIR node
                        FlowNode::Existing(na) => FlowNode::Existing(na),
                    }
                }
                MirNodeInner::Union { ref emit } => {
                    invariant_eq!(mir_node.ancestors.len(), emit.len());
                    make_union_node(
                        &name,
                        mir_node.columns.as_slice(),
                        emit,
                        mir_node.ancestors(),
                        mig,
                        table_mapping,
                    )
                }
                MirNodeInner::Distinct { ref group_by } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    let parent = mir_node.ancestors[0].clone();
                    make_distinct_node(&name, parent, mir_node.columns.as_slice(), group_by, mig)
                }
                MirNodeInner::TopK {
                    ref order,
                    ref group_by,
                    ref k,
                    ref offset,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
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
                    )?
                }
                MirNodeInner::Rewrite {
                    ref value,
                    ref column,
                    ref key,
                } => {
                    let src = mir_node.ancestors[0].clone();
                    let should_rewrite = mir_node.ancestors[1].clone();

                    make_rewrite_node(
                        &name,
                        src,
                        should_rewrite,
                        mir_node.columns.as_slice(),
                        value,
                        column,
                        key,
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
    })
}

fn adapt_base_node(
    over_node: MirNodeRef,
    mig: &mut Migration,
    column_specs: &mut [(ColumnSpecification, Option<usize>)],
    add: &[ColumnSpecification],
    remove: &[ColumnSpecification],
) -> ReadySetResult<FlowNode> {
    let na = match over_node.borrow().flow_node {
        None => internal!("adapted base node must have a flow node already!"),
        Some(ref flow_node) => flow_node.address(),
    };

    for a in add.iter() {
        let default_value = a
            .constraints
            .iter()
            .find_map(|c| match *c {
                ColumnConstraint::DefaultValue(ref dv) => Some(dv.into()),
                _ => None,
            })
            .unwrap_or(DataType::None);
        let column_id = mig.add_column(na, &a.column.name, default_value)?;

        // store the new column ID in the column specs for this node
        for &mut (ref cs, ref mut cid) in column_specs.iter_mut() {
            if cs == a {
                invariant!(cid.is_none()); // FIXME(eta): used to be assert_eq
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
            .ok_or_else(|| internal_err("base column ID must be set to remove column"))?;
        mig.drop_column(na, cid)?;
    }

    Ok(FlowNode::Existing(na))
}

fn column_names(cs: &[Column]) -> Vec<&str> {
    cs.iter().map(|c| c.name.as_str()).collect()
}

fn make_base_node(
    name: &str,
    column_specs: &mut [(ColumnSpecification, Option<usize>)],
    pkey_columns: &[Column],
    mig: &mut Migration,
) -> FlowNode {
    // remember the absolute base column ID for potential later removal
    for (i, cs) in column_specs.iter_mut().enumerate() {
        cs.1 = Some(i);
    }

    let columns: Vec<_> = column_specs
        .iter()
        .map(|&(ref cs, _)| Column::from(&cs.column))
        .collect();
    let column_names = column_names(columns.as_slice());

    // note that this defaults to a "None" (= NULL) default value for columns that do not have one
    // specified; we don't currently handle a "NOT NULL" SQL constraint for defaults
    let default_values = column_specs
        .iter()
        .map(|&(ref cs, _)| {
            for c in &cs.constraints {
                if let ColumnConstraint::DefaultValue(ref dv) = *c {
                    return dv.into();
                }
            }
            DataType::None
        })
        .collect::<Vec<DataType>>();

    let base = if !pkey_columns.is_empty() {
        let pkey_column_ids = pkey_columns
            .iter()
            .map(|pkc| {
                //assert_eq!(pkc.table.as_ref().unwrap(), name);
                column_specs
                    .iter()
                    .position(|&(ref cs, _)| Column::from(&cs.column) == *pkc)
                    .unwrap()
            })
            .collect();
        node::special::Base::new(default_values).with_key(pkey_column_ids)
    } else {
        node::special::Base::new(default_values)
    };

    FlowNode::New(mig.add_base(name, column_names.as_slice(), base))
}

fn make_union_node(
    name: &str,
    columns: &[Column],
    emit: &[Vec<Column>],
    ancestors: &[MirNodeRef],
    mig: &mut Migration,
    table_mapping: Option<&HashMap<(String, Option<String>), String>>,
) -> FlowNode {
    let column_names = column_names(columns);
    let mut emit_column_id: HashMap<NodeIndex, Vec<usize>> = HashMap::new();

    // column_id_for_column doesn't take into consideration table aliases
    // which might cause improper ordering of columns in a union node
    // eg. Q6 in finkelstein.txt
    for (i, n) in ancestors.iter().enumerate() {
        let emit_cols = emit[i]
            .iter()
            .map(|c| n.borrow().column_id_for_column(c, table_mapping))
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

fn make_rewrite_node(
    name: &str,
    src: MirNodeRef,
    should_rewrite: MirNodeRef,
    columns: &[Column],
    value: &str,
    rewrite_col: &str,
    key: &str,
    mig: &mut Migration,
) -> FlowNode {
    let src_na = src.borrow().flow_node_addr().unwrap();
    let should_rewrite_na = should_rewrite.borrow().flow_node_addr().unwrap();
    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();
    let rewrite_col = column_names
        .iter()
        .rposition(|c| *c == rewrite_col)
        .unwrap();
    let key = column_names.iter().rposition(|c| *c == key).unwrap();

    let node = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        ops::rewrite::Rewrite::new(src_na, should_rewrite_na, rewrite_col, value.into(), key),
    );
    FlowNode::New(node)
}

fn make_filter_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    conditions: Expression,
    mig: &mut Migration,
    remapped_exprs_to_parent_names: Option<&HashMap<FunctionExpression, String>>,
) -> ReadySetResult<FlowNode> {
    let mut fields = parent.borrow().columns().to_vec();
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = column_names(columns);

    let filter_conditions = extract_conditions(
        &conditions,
        &mut fields,
        &parent,
        remapped_exprs_to_parent_names,
    )?;

    let node = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        ops::filter::Filter::new(parent_na, &filter_conditions),
    );
    Ok(FlowNode::New(node))
}

fn make_grouped_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    on: &Column,
    group_by: &[Column],
    kind: GroupedNodeType,
    mig: &mut Migration,
    table_mapping: Option<&HashMap<(String, Option<String>), String>>,
) -> ReadySetResult<FlowNode> {
    invariant!(!group_by.is_empty());
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let parent_node = parent.borrow();
    let column_names = column_names(columns);
    let over_col_indx = parent_node.column_id_for_column(on, table_mapping);
    let group_col_indx = group_by
        .iter()
        .map(|c| parent_node.column_id_for_column(c, table_mapping))
        .collect::<Vec<_>>();
    invariant!(!group_col_indx.is_empty());

    let na = match kind {
        // This is the product of an incomplete refactor. It simplifies MIR to consider Group_Concat to
        // be an aggregation, however once we are in dataflow land the logic has not been merged yet.
        // For this reason, we need to pattern match for a groupconcat aggregation before we pattern
        // match for a generic aggregation.
        GroupedNodeType::Aggregation(Aggregation::GroupConcat { separator: sep }) => {
            use dataflow::ops::grouped::concat::{GroupConcat, TextComponent};
            let gc = GroupConcat::new(parent_na, vec![TextComponent::Column(over_col_indx)], sep)?;
            mig.add_ingredient(String::from(name), column_names.as_slice(), gc)
        }
        GroupedNodeType::Aggregation(agg) => mig.add_ingredient(
            String::from(name),
            column_names.as_slice(),
            agg.over(parent_na, over_col_indx, group_col_indx.as_slice())?,
        ),
        GroupedNodeType::Extremum(extr) => mig.add_ingredient(
            String::from(name),
            column_names.as_slice(),
            extr.over(parent_na, over_col_indx, group_col_indx.as_slice()),
        ),
    };
    Ok(FlowNode::New(na))
}

fn make_identity_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    mig: &mut Migration,
) -> FlowNode {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = column_names(columns);

    let node = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        ops::identity::Identity::new(parent_na),
    );
    FlowNode::New(node)
}

fn make_join_node(
    name: &str,
    left: MirNodeRef,
    right: MirNodeRef,
    columns: &[Column],
    on_left: &[Column],
    on_right: &[Column],
    proj_cols: &[Column],
    kind: JoinType,
    mig: &mut Migration,
) -> ReadySetResult<FlowNode> {
    use dataflow::ops::join::JoinSource;

    invariant_eq!(on_left.len(), on_right.len());

    let column_names = column_names(columns);

    let (projected_cols_left, rest): (Vec<Column>, Vec<Column>) = proj_cols
        .iter()
        .cloned()
        .partition(|c| left.borrow().columns.contains(c));
    let (projected_cols_right, rest): (Vec<Column>, Vec<Column>) = rest
        .into_iter()
        .partition(|c| right.borrow().columns.contains(c));
    invariant!(
        rest.is_empty(),
        "could not resolve output columns projected from join: {:?}",
        rest
    );

    invariant_eq!(
        projected_cols_left.len() + projected_cols_right.len(),
        proj_cols.len()
    );

    // this assumes the columns we want to join on appear first in the list
    // of projected columns. this is fine for joins against different tables
    // since we assume unique column names in each table. however, this is
    // not correct for joins against the same table, for example:
    // SELECT r1.a as a1, r2.a as a2 from r as r1, r as r2 where r1.a = r2.b and r2.a = r1.b;
    //
    // the `r1.a = r2.b` join predicate will create a join node with columns: r1.a, r1.b, r2.a, r2,b
    // however, because the way we deal with aliases, we can't distinguish between `r1.a` and `r2.a`
    // at this point in the codebase, so the `r2.a = r1.b` will join on the wrong `a` column.
    let join_col_mappings = on_left
        .iter()
        .zip(on_right)
        .map(|(l, r)| -> ReadySetResult<_> {
            let left_join_col_id = left
                .borrow()
                .columns
                .iter()
                .position(|lc| lc == l)
                .ok_or_else(|| {
                    internal_err(format!(
                        "missing left-side join column {:#?} in {:#?}",
                        on_left.first().unwrap(),
                        left.borrow().columns
                    ))
                })?;

            let right_join_col_id = right
                .borrow()
                .columns
                .iter()
                .position(|rc| rc == r)
                .ok_or_else(|| {
                    internal_err(format!(
                        "missing right-side join column {:#?} in {:#?}",
                        on_right.first().unwrap(),
                        right.borrow().columns
                    ))
                })?;

            Ok((left_join_col_id, right_join_col_id))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;

    let mut from_left = 0;
    let mut from_right = 0;
    let join_config = left
        .borrow()
        .columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            if let Some(r) = join_col_mappings.get(&i) {
                from_left += 1;
                Some(JoinSource::B(i, *r))
            } else if projected_cols_left.contains(c) {
                from_left += 1;
                Some(JoinSource::L(i))
            } else {
                None
            }
        })
        .chain(
            right
                .borrow()
                .columns
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    if projected_cols_right.contains(c) {
                        from_right += 1;
                        Some(JoinSource::R(i))
                    } else {
                        None
                    }
                }),
        )
        .collect();
    invariant_eq!(from_left, projected_cols_left.len());
    invariant_eq!(from_right, projected_cols_right.len());

    let left_na = left.borrow().flow_node_addr().unwrap();
    let right_na = right.borrow().flow_node_addr().unwrap();

    let j = match kind {
        JoinType::Inner => Join::new(left_na, right_na, JoinType::Inner, join_config),
        JoinType::Left => Join::new(left_na, right_na, JoinType::Left, join_config),
    };
    let n = mig.add_ingredient(String::from(name), column_names.as_slice(), j);

    Ok(FlowNode::New(n))
}

/// Joins two parent aggregate nodes together. Columns that are shared between both parents are
/// assumed to be group_by columns and all unique columns are considered to be the aggregate
/// columns themselves.
fn make_join_aggregates_node(
    name: &str,
    left: MirNodeRef,
    right: MirNodeRef,
    columns: &[Column],
    mig: &mut Migration,
) -> ReadySetResult<FlowNode> {
    use dataflow::ops::join::JoinSource;

    let column_names = column_names(columns);

    // Build up maps from each parents columns to those columns indices. Necessary for building up
    // Vec<JoinSource> below.
    let left_map = node_columns_to_idx_map(left.clone());
    let right_map = node_columns_to_idx_map(right.clone());

    // We gather up all of the columns from each respective parent. If a column is in both parents,
    // then we know it was a group_by column and create a JoinSource::B type with the indices from
    // both parents. Otherwise if the column is exclusively in the left parent (such as the
    // aggregate column itself), we make a JoinSource::L with the left parent index. We finally
    // iterate through the right parent and add the columns that were exclusively in the right
    // parent as JoinSource::R with the right parent index for each given unique column.
    let join_config = left
        .borrow()
        .columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            if let Some(j) = right_map.get(&c) {
                // If the column was found in both, it's a group_by column and gets added as
                // JoinSource::B.
                Some(JoinSource::B(i, *j))
            } else {
                // Column exclusively in left parent, so gets added as JoinSource::L.
                Some(JoinSource::L(i))
            }
        })
        .chain(
            right
                .borrow()
                .columns
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    // If column is in left, don't do anything it's already been added.
                    // If it's in right, add it with right index.
                    if left_map.contains_key(&c) {
                        None
                    } else {
                        // Column exclusively in right parent, so gets added as JoinSource::R.
                        Some(JoinSource::R(i))
                    }
                }),
        )
        .collect();

    let left_na = left.borrow().flow_node_addr().unwrap();
    let right_na = right.borrow().flow_node_addr().unwrap();

    // Always treated as a JoinType::Inner based on joining on group_by cols, which always match
    // between parents.
    let j = Join::new(left_na, right_na, JoinType::Inner, join_config);
    let n = mig.add_ingredient(String::from(name), column_names.as_slice(), j);

    Ok(FlowNode::New(n))
}

// Builds up a map from the nodes columns, to the indices those columns exist at in the nodes
// columns list.
fn node_columns_to_idx_map(node: MirNodeRef) -> HashMap<Column, usize> {
    node.borrow()
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.clone(), i))
        .collect()
}

fn make_param_filter_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    col: &Column,
    emit_key: &Column,
    operator: &BinaryOperator,
    mig: &mut Migration,
    table_mapping: Option<&HashMap<(String, Option<String>), String>>,
) -> ReadySetResult<FlowNode> {
    use nom_sql::BinaryOperator as nom_op;
    use ops::param_filter::Operator as pf_op;

    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = column_names(columns);
    let col = parent.borrow().column_id_for_column(col, table_mapping);
    let emit_key = column_names
        .iter()
        .rposition(|c| *c == emit_key.name)
        .unwrap();
    let operator = match operator {
        nom_op::ILike => pf_op::ILike,
        nom_op::Like => pf_op::Like,
        _ => internal!("Only supported operators are expected in a mir ParamFilter."),
    };
    let node = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        ParamFilter::new(parent_na, col, emit_key, operator),
    );
    Ok(FlowNode::New(node))
}

fn make_latest_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    group_by: &[Column],
    mig: &mut Migration,
) -> ReadySetResult<FlowNode> {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = column_names(columns);

    let group_col_indx = group_by
        .iter()
        .map(|c| parent.borrow().column_id_for_column(c, None))
        .collect::<Vec<_>>();

    // latest doesn't support compound group by
    if group_col_indx.len() != 1 {
        unsupported!("latest node doesn't support compound GROUP BY")
    }
    let na = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        Latest::new(parent_na, group_col_indx[0]),
    );
    Ok(FlowNode::New(na))
}

fn generate_project_expression(
    parent: &MirNodeRef,
    expr: Expression,
) -> ReadySetResult<DataflowExpression> {
    match expr {
        Expression::Call(FunctionExpression::Cast(arg, ty)) => Ok(DataflowExpression::Cast(
            Box::new(generate_project_expression(parent, (*arg).into())?),
            ty,
        )),
        Expression::Call(FunctionExpression::Call {
            name: fname,
            arguments,
        }) => Ok(DataflowExpression::Call(
            BuiltinFunction::from_name_and_args(
                &fname,
                arguments
                    .into_iter()
                    .map(|arg| generate_project_expression(parent, arg.into()))
                    .collect::<Result<Vec<_>, _>>()?,
            )?,
        )),
        Expression::Call(call) => internal!(
            "Unexpected (aggregate?) call node in project expression: {:?}",
            call
        ),
        Expression::Literal(lit) => Ok(DataflowExpression::Literal(lit.into())),
        Expression::Column(nom_sql::Column { name, table, .. }) => Ok(DataflowExpression::Column(
            parent
                .borrow()
                .column_id_for_column(&Column::new(table.as_deref(), &name), None),
        )),
        Expression::BinaryOp { lhs, op, rhs } => Ok(DataflowExpression::Op {
            op,
            left: Box::new(generate_project_expression(parent, *lhs)?),
            right: Box::new(generate_project_expression(parent, *rhs)?),
        }),
        Expression::CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => Ok(DataflowExpression::CaseWhen {
            condition: Box::new(generate_project_expression(parent, *condition)?),
            then_expr: Box::new(generate_project_expression(parent, *then_expr)?),
            else_expr: match else_expr {
                Some(else_expr) => Box::new(generate_project_expression(parent, *else_expr)?),
                None => Box::new(DataflowExpression::Literal(DataType::None)),
            },
        }),
        Expression::UnaryOp { .. }
        | Expression::Between { .. }
        | Expression::Exists(_)
        | Expression::In { .. } => {
            unsupported!("Unsupported projected expression: {}", expr)
        }
        Expression::NestedSelect(_) => {
            internal!("Nested selection should have been rewritten earlier")
        }
    }
}

fn make_project_node(
    name: &str,
    parent: MirNodeRef,
    source_columns: &[Column],
    emit: &[Column],
    expressions: &[(String, Expression)],
    literals: &[(String, DataType)],
    mig: &mut Migration,
    table_mapping: Option<&HashMap<(String, Option<String>), String>>,
) -> ReadySetResult<FlowNode> {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = column_names(source_columns);

    let projected_column_ids = emit
        .iter()
        .map(|c| {
            parent
                .borrow()
                .find_source_for_child_column(c, table_mapping)
                .ok_or_else(|| {
                    internal_err(format!("could not find source for child column: {:?}", c))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let (_, literal_values): (Vec<_>, Vec<_>) = literals.iter().cloned().unzip();

    let projected_expressions: Vec<DataflowExpression> = expressions
        .iter()
        .map(|(_, e)| generate_project_expression(&parent, e.clone()))
        .collect::<Result<Vec<_>, _>>()?;

    let n = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        Project::new(
            parent_na,
            projected_column_ids.as_slice(),
            Some(literal_values),
            Some(projected_expressions),
        ),
    );
    Ok(FlowNode::New(n))
}

fn make_distinct_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    group_by: &[Column],
    mig: &mut Migration,
) -> FlowNode {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = column_names(columns);

    let group_by_indx = if group_by.is_empty() {
        // no query parameters, so we index on the first column
        columns
            .iter()
            .map(|c| parent.borrow().column_id_for_column(c, None))
            .collect::<Vec<_>>()
    } else {
        group_by
            .iter()
            .map(|c| parent.borrow().column_id_for_column(c, None))
            .collect::<Vec<_>>()
    };

    // make the new operator and record its metadata
    let na = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        ops::distinct::Distinct::new(parent_na, group_by_indx),
    );
    FlowNode::New(na)
}

fn make_topk_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    order: &Option<Vec<(Column, OrderType)>>,
    group_by: &[Column],
    k: usize,
    offset: usize,
    mig: &mut Migration,
) -> ReadySetResult<FlowNode> {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = column_names(columns);

    invariant!(
        !group_by.is_empty(),
        "need bogokey for TopK without group columns"
    );

    let group_by_indx = group_by
        .iter()
        .map(|c| parent.borrow().column_id_for_column(c, None))
        .collect::<Vec<_>>();

    let cmp_rows = match *order {
        Some(ref o) => {
            invariant_eq!(offset, 0); // Non-zero offset not supported

            let columns: Vec<_> = o
                .iter()
                .map(|&(ref c, ref order_type)| {
                    // SQL and Soup disagree on what ascending and descending order means, so do the
                    // conversion here.
                    let reversed_order_type = match *order_type {
                        OrderType::OrderAscending => OrderType::OrderDescending,
                        OrderType::OrderDescending => OrderType::OrderAscending,
                    };
                    (
                        parent.borrow().column_id_for_column(c, None),
                        reversed_order_type,
                    )
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
    Ok(FlowNode::New(na))
}

fn materialize_leaf_node(
    parent: &MirNodeRef,
    name: String,
    key_cols: &[Column],
    mig: &mut Migration,
    operator: nom_sql::BinaryOperator,
) {
    let na = parent.borrow().flow_node_addr().unwrap();

    // we must add a new reader for this query. This also requires adding an identity node (at
    // least currently), since a node can only have a single associated reader. However, the
    // identity node exists at the MIR level, so we don't need to consider it here, as it has
    // already been added.

    // TODO(malte): consider the case when the projected columns need reordering

    if !key_cols.is_empty() {
        let key_cols: Vec<_> = key_cols
            .iter()
            .map(|c| parent.borrow().column_id_for_column(c, None))
            .collect();
        mig.maintain(name, na, &key_cols[..], operator);
    } else {
        // if no key specified, default to the first column
        mig.maintain(name, na, &[0], operator);
    }
}

/// Converts a condition tree stored in the `Expression` returned by the SQL parser and adds it to a
/// vector of conditions.
fn extract_conditions(
    conditions: &Expression,
    fields: &mut Vec<Column>,
    parent: &MirNodeRef,
    remapped_exprs_to_parent_names: Option<&HashMap<FunctionExpression, String>>,
) -> ReadySetResult<Vec<(usize, FilterCondition)>> {
    use dataflow::ops::filter;

    let mut column_from_parent = |col: &nom_sql::Column| -> usize {
        let mut name = &col.name as &str;
        if let Some(ref epm) = remapped_exprs_to_parent_names {
            if let Some(func) = &col.function {
                if let Some(remapped_name) = epm.get(func.as_ref()) {
                    // haha remap go brr!
                    name = remapped_name;
                }
            }
        }

        let absolute_column_ids: Vec<usize> = fields
            .iter()
            .map(|c| {
                // grr NLL
                let source: Option<usize> =
                    { parent.borrow().find_source_for_child_column(c, None) };
                source.unwrap_or_else(|| parent.borrow_mut().add_column(c.clone()))
            })
            .collect();

        if let Some(pos) = fields
            .iter()
            .rposition(|c| *c.name == *name && c.table == col.table)
        {
            absolute_column_ids[pos]
        } else {
            // Might occur if the column doesn't exist in the parent; e.g., for
            // aggregations.  We assume that the column is appended at the end, unless we
            // have an aggregation, in which case it needs to go before the computed column,
            // which is last.
            let col = mir::Column::from(col.clone());
            let pos = parent.borrow_mut().add_column(col.clone());
            fields.insert(pos, col);
            pos
        }
    };

    match conditions {
        Expression::BinaryOp {
            lhs,
            op: BinaryOperator::And,
            rhs,
        } => {
            let mut conds =
                extract_conditions(lhs.as_ref(), fields, parent, remapped_exprs_to_parent_names)?;
            conds.append(&mut extract_conditions(
                rhs.as_ref(),
                fields,
                parent,
                remapped_exprs_to_parent_names,
            )?);
            Ok(conds)
        }
        Expression::BinaryOp { lhs, op, rhs } => {
            let l = match **lhs {
                Expression::Column(ref f) => column_from_parent(f),
                Expression::Call(ref f) =>
                // FIXME(ENG-215): Replace this with some sort of projected expression to column
                // name mapping once we get rid of nom_sql::Column::function
                {
                    column_from_parent(&nom_sql::Column {
                        function: Some(Box::new(f.clone())),
                        name: f.to_string(),
                        table: None,
                    })
                }
                ref lhs => {
                    return Err(MirUnsupportedCondition {
                        expression: lhs.to_string(),
                    })
                }
            };

            let f = match **rhs {
                Expression::Literal(Literal::Integer(ref i)) => {
                    FilterCondition::Comparison(*op, filter::Value::Constant(DataType::from(*i)))
                }
                Expression::Literal(Literal::String(ref s)) => FilterCondition::Comparison(
                    *op,
                    filter::Value::Constant(DataType::from(s.clone())),
                ),
                Expression::Literal(Literal::Null) => {
                    FilterCondition::Comparison(*op, filter::Value::Constant(DataType::None))
                }
                Expression::Column(ref f) => {
                    FilterCondition::Comparison(*op, filter::Value::Column(column_from_parent(f)))
                }
                ref rhs => {
                    return Err(MirUnsupportedCondition {
                        expression: rhs.to_string(),
                    })
                }
            };
            Ok(vec![(l, f)])
        }
        Expression::In {
            lhs,
            rhs: nom_sql::InValue::List(exprs),
            negated: false,
        } => {
            let l = match **lhs {
                Expression::Column(ref f) => column_from_parent(f),
                ref lhs => {
                    return Err(MirUnsupportedCondition {
                        expression: lhs.to_string(),
                    })
                }
            };

            let f = FilterCondition::In(
                exprs
                    .iter()
                    .map(|expr| match expr {
                        Expression::Literal(lit) => Ok(DataType::from(lit.clone())),
                        expr => Err(MirUnsupportedCondition {
                            expression: expr.to_string(),
                        }),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );

            Ok(vec![(l, f)])
        }
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            ..
        } => internal!("negation should have been removed earlier"),
        Expression::Between { .. } => internal!("BETWEEN should have been removed earlier"),
        Expression::Call(_)
        | Expression::Literal(_)
        | Expression::CaseWhen { .. }
        | Expression::Column(_)
        | Expression::Exists(_)
        | Expression::NestedSelect(_)
        | Expression::In { .. } => Err(MirUnsupportedCondition {
            expression: conditions.to_string(),
        }),
    }
}
