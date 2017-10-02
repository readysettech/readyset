use nom_sql::{Column, ColumnConstraint, ColumnSpecification, Operator, OrderType};
use std::collections::HashMap;

use flow::Migration;
use flow::core::DataType;
use flow::prelude::NodeIndex;
use mir::MirNodeRef;
use mir::node::GroupedNodeType;
use ops;
use ops::join::{Join, JoinType};
use ops::latest::Latest;
use ops::project::Project;

#[derive(Clone, Debug)]
pub enum FlowNode {
    New(NodeIndex),
    Existing(NodeIndex),
}

impl FlowNode {
    pub fn address(&self) -> NodeIndex {
        match *self {
            FlowNode::New(na) | FlowNode::Existing(na) => na,
        }
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
            use ops::grouped::concat::{GroupConcat, TextComponent};

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
    use ops::join::JoinSource;

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
        .map(|(i, _)| if i == left_join_col_id {
            JoinSource::B(i, right_join_col_id)
        } else {
            JoinSource::L(i)
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

pub(crate) fn make_project_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    emit: &Vec<Column>,
    literals: &Vec<(String, DataType)>,
    mig: &mut Migration,
) -> FlowNode {
    let parent_na = parent.borrow().flow_node_addr().unwrap();
    let column_names = columns.iter().map(|c| &c.name).collect::<Vec<_>>();

    let projected_column_ids = emit.iter()
        .map(|c| parent.borrow().column_id_for_column(c))
        .collect::<Vec<_>>();

    let (_, literal_values): (Vec<_>, Vec<_>) = literals.iter().cloned().unzip();

    let n = mig.add_ingredient(
        String::from(name),
        column_names.as_slice(),
        Project::new(
            parent_na,
            projected_column_ids.as_slice(),
            Some(literal_values),
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

pub(crate) fn materialize_leaf_node(node: &MirNodeRef, key_cols: &Vec<Column>, mig: &mut Migration) {
    let na = node.borrow().flow_node_addr().unwrap();

    // we must add a new reader for this query. This also requires adding an identity node (at
    // least currently), since a node can only have a single associated reader. However, the
    // identity node exists at the MIR level, so we don't need to consider it here, as it has
    // already been added.

    // TODO(malte): consider the case when the projected columns need reordering

    if !key_cols.is_empty() {
        // TODO(malte): this does not yet cover the case when there are multiple query
        // parameters, which requires compound key support on Reader nodes.
        //assert_eq!(key_cols.len(), 1);
        let first_key_col_id = node.borrow()
            .column_id_for_column(key_cols.iter().next().unwrap());
        mig.maintain(na, first_key_col_id);
    } else {
        // if no key specified, default to the first column
        mig.maintain(na, 0);
    }
}
