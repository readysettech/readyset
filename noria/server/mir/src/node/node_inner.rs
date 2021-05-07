use crate::node::BaseNodeAdaptation;
use crate::{Column, MirNodeRef};
use common::DataType;
use dataflow::ops::grouped::aggregate::Aggregation;
use dataflow::ops::grouped::extremum::Extremum;
use nom_sql::{
    BinaryOperator, ColumnSpecification, ConditionExpression, Expression, FunctionExpression,
    Literal, OrderType,
};
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};

pub enum MirNodeInner {
    /// over column, group_by columns
    Aggregation {
        on: Column,
        group_by: Vec<Column>,
        kind: Aggregation,
    },
    /// column specifications, keys (non-compound), tx flag, adapted base
    Base {
        column_specs: Vec<(ColumnSpecification, Option<usize>)>,
        keys: Vec<Column>,
        adapted_over: Option<BaseNodeAdaptation>,
    },
    /// over column, group_by columns
    Extremum {
        on: Column,
        group_by: Vec<Column>,
        kind: Extremum,
    },
    /// filter conditions (one for each parent column)
    Filter {
        conditions: ConditionExpression,
        // Maps the Columns that contained function calls into the
        // names of the projected columns that contain the evaluated results.
        // This is the 2nd return value of `project_expressions`.
        remapped_exprs_to_parent_names: Option<HashMap<FunctionExpression, String>>,
    },
    /// filter condition and grouping
    // FilterAggregation Mir Node type still exists, due to optimization and rewrite logic
    FilterAggregation {
        on: Column,
        else_on: Option<Literal>,
        group_by: Vec<Column>,
        // kind is same as a normal aggregation (sum, count, avg)
        kind: Aggregation,
        conditions: ConditionExpression,
        // Maps the Columns that contained function calls into the
        // names of the projected columns that contain the evaluated results.
        // This is the 2nd return value of `project_expressions`.
        remapped_exprs_to_parent_names: Option<HashMap<FunctionExpression, String>>,
    },
    /// no extra info required
    Identity,
    /// left node, right node, on left columns, on right columns, emit columns
    Join {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// on left column, on right column, emit columns
    LeftJoin {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// group columns
    // currently unused
    #[allow(dead_code)]
    Latest {
        group_by: Vec<Column>,
    },
    /// emit columns
    Project {
        emit: Vec<Column>,
        expressions: Vec<(String, Expression)>,
        literals: Vec<(String, DataType)>,
    },
    /// emit columns
    Union {
        emit: Vec<Vec<Column>>,
    },
    /// order function, group columns, limit k
    TopK {
        order: Option<Vec<(Column, OrderType)>>,
        group_by: Vec<Column>,
        k: usize,
        offset: usize,
    },
    // Get the distinct element sorted by a specific column
    Distinct {
        group_by: Vec<Column>,
    },
    /// reuse another node
    Reuse {
        node: MirNodeRef,
    },
    /// leaf (reader) node, keys
    Leaf {
        node: MirNodeRef,
        keys: Vec<Column>,
        operator: nom_sql::BinaryOperator,
    },
    /// Rewrite node
    Rewrite {
        value: String,
        column: String,
        key: String,
    },
    /// Param Filter node
    ParamFilter {
        col: Column,
        emit_key: Column,
        operator: BinaryOperator,
    },
}

impl MirNodeInner {
    pub(crate) fn description(&self) -> String {
        format!("{:?}", self)
    }

    pub(crate) fn insert_column(&mut self, c: Column) {
        match *self {
            MirNodeInner::Aggregation {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
            MirNodeInner::Base { .. } => panic!("can't add columns to base nodes!"),
            MirNodeInner::Extremum {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
            MirNodeInner::FilterAggregation {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
            MirNodeInner::Join {
                ref mut project, ..
            }
            | MirNodeInner::LeftJoin {
                ref mut project, ..
            } => {
                project.push(c);
            }
            MirNodeInner::Project { ref mut emit, .. } => {
                emit.push(c);
            }
            MirNodeInner::Union { ref mut emit } => {
                for e in emit.iter_mut() {
                    e.push(c.clone());
                }
            }
            MirNodeInner::Distinct {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
            MirNodeInner::TopK {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
            _ => (),
        }
    }

    pub(crate) fn can_reuse_as(&self, other: &MirNodeInner) -> bool {
        match *self {
            MirNodeInner::Reuse { .. } => (), // handled below
            _ => {
                // we're not a `Reuse` ourselves, but the other side might be
                if let MirNodeInner::Reuse { ref node } = *other {
                    // it is, so dig deeper
                    // this does not check the projected columns of the inner node for two
                    // reasons:
                    // 1) our own projected columns aren't accessible on `MirNodeType`, but
                    //    only on the outer `MirNode`, which isn't accessible here; but more
                    //    importantly
                    // 2) since this is already a node reuse, the inner, reused node must have
                    //    *at least* a superset of our own (inaccessible) projected columns.
                    // Hence, it is sufficient to check the projected columns on the parent
                    // `MirNode`, and if that check passes, it also holds for the nodes reused
                    // here.
                    return self.can_reuse_as(&node.borrow().inner);
                } else {
                    // handled below
                }
            }
        }

        match *self {
            MirNodeInner::Aggregation {
                on: ref our_on,
                group_by: ref our_group_by,
                kind: ref our_kind,
            } => {
                match *other {
                    MirNodeInner::Aggregation {
                        ref on,
                        ref group_by,
                        ref kind,
                    } => {
                        // TODO(malte): this is stricter than it needs to be, as it could cover
                        // COUNT-as-SUM-style relationships.
                        our_on == on && our_group_by == group_by && our_kind == kind
                    }
                    _ => false,
                }
            }
            MirNodeInner::Base {
                column_specs: ref our_column_specs,
                keys: ref our_keys,
                adapted_over: ref our_adapted_over,
            } => {
                match *other {
                    MirNodeInner::Base {
                        ref column_specs,
                        ref keys,
                        ..
                    } => {
                        // if we are instructed to adapt an earlier base node, we cannot reuse
                        // anything directly; we'll have to keep a new MIR node here.
                        if our_adapted_over.is_some() {
                            // TODO(malte): this is a bit more conservative than it needs to be,
                            // since base node adaptation actually *changes* the underlying base
                            // node, so we will actually reuse. However, returning false here
                            // terminates the reuse search unnecessarily. We should handle this
                            // special case.
                            return false;
                        }
                        // note that as long as we are not adapting a previous base node,
                        // we do *not* need `adapted_over` to *match*, since current reuse
                        // does not depend on how base node was created from an earlier one
                        our_column_specs == column_specs && our_keys == keys
                    }
                    _ => false,
                }
            }
            MirNodeInner::Extremum {
                on: ref our_on,
                group_by: ref our_group_by,
                kind: ref our_kind,
            } => match *other {
                MirNodeInner::Extremum {
                    ref on,
                    ref group_by,
                    ref kind,
                } => our_on == on && our_group_by == group_by && our_kind == kind,
                _ => false,
            },
            MirNodeInner::Filter {
                conditions: ref our_conditions,
                remapped_exprs_to_parent_names: ref our_remapped_exprs_to_parent_names,
            } => match *other {
                MirNodeInner::Filter {
                    ref conditions,
                    ref remapped_exprs_to_parent_names,
                } => {
                    our_conditions == conditions
                        && our_remapped_exprs_to_parent_names == remapped_exprs_to_parent_names
                }
                _ => false,
            },
            MirNodeInner::FilterAggregation {
                on: ref our_on,
                else_on: ref our_else_on,
                group_by: ref our_group_by,
                kind: ref our_kind,
                conditions: ref our_conditions,
                remapped_exprs_to_parent_names: ref our_remapped_exprs_to_parent_names,
            } => match *other {
                MirNodeInner::FilterAggregation {
                    ref on,
                    ref else_on,
                    ref group_by,
                    ref kind,
                    ref conditions,
                    ref remapped_exprs_to_parent_names,
                } => {
                    our_on == on
                        && our_else_on == else_on
                        && our_group_by == group_by
                        && our_kind == kind
                        && our_conditions == conditions
                        && our_remapped_exprs_to_parent_names == remapped_exprs_to_parent_names
                }
                _ => false,
            },
            MirNodeInner::Join {
                on_left: ref our_on_left,
                on_right: ref our_on_right,
                project: ref our_project,
            } => {
                match *other {
                    MirNodeInner::Join {
                        ref on_left,
                        ref on_right,
                        ref project,
                    } => {
                        // TODO(malte): column order does not actually need to match, but this only
                        // succeeds if it does.
                        our_on_left == on_left && our_on_right == on_right && our_project == project
                    }
                    _ => false,
                }
            }
            MirNodeInner::LeftJoin {
                on_left: ref our_on_left,
                on_right: ref our_on_right,
                project: ref our_project,
            } => {
                match *other {
                    MirNodeInner::LeftJoin {
                        ref on_left,
                        ref on_right,
                        ref project,
                    } => {
                        // TODO(malte): column order does not actually need to match, but this only
                        // succeeds if it does.
                        our_on_left == on_left && our_on_right == on_right && our_project == project
                    }
                    _ => false,
                }
            }
            MirNodeInner::Project {
                emit: ref our_emit,
                literals: ref our_literals,
                expressions: ref our_expressions,
            } => match *other {
                MirNodeInner::Project {
                    ref emit,
                    ref literals,
                    ref expressions,
                } => our_emit == emit && our_literals == literals && our_expressions == expressions,
                _ => false,
            },
            MirNodeInner::Distinct {
                group_by: ref our_group_by,
            } => match *other {
                MirNodeInner::Distinct { ref group_by } => group_by == our_group_by,
                _ => false,
            },
            MirNodeInner::Reuse { node: ref us } => {
                match *other {
                    // both nodes are `Reuse` nodes, so we simply compare the both sides' reuse
                    // target
                    MirNodeInner::Reuse { ref node } => us.borrow().can_reuse_as(&*node.borrow()),
                    // we're a `Reuse`, the other side isn't, so see if our reuse target's `inner`
                    // can be reused for the other side. It's sufficient to check the target's
                    // `inner` because reuse implies that our target has at least a superset of our
                    // projected columns (see earlier comment).
                    _ => us.borrow().inner.can_reuse_as(other),
                }
            }
            MirNodeInner::TopK {
                order: ref our_order,
                group_by: ref our_group_by,
                k: our_k,
                offset: our_offset,
            } => match *other {
                MirNodeInner::TopK {
                    ref order,
                    ref group_by,
                    k,
                    offset,
                } => {
                    order == our_order
                        && group_by == our_group_by
                        && k == our_k
                        && offset == our_offset
                }
                _ => false,
            },
            MirNodeInner::Leaf {
                keys: ref our_keys, ..
            } => match *other {
                MirNodeInner::Leaf { ref keys, .. } => keys == our_keys,
                _ => false,
            },
            MirNodeInner::Union { emit: ref our_emit } => match *other {
                MirNodeInner::Union { ref emit } => emit == our_emit,
                _ => false,
            },
            MirNodeInner::Rewrite {
                value: ref our_value,
                key: ref our_key,
                column: ref our_col,
            } => match *other {
                MirNodeInner::Rewrite {
                    ref value,
                    ref key,
                    ref column,
                } => (value == our_value && our_key == key && our_col == column),
                _ => false,
            },
            MirNodeInner::ParamFilter {
                col: ref our_col,
                emit_key: ref our_emit_key,
                operator: ref our_operator,
            } => match *other {
                MirNodeInner::ParamFilter {
                    ref col,
                    ref emit_key,
                    ref operator,
                } => (col == our_col && emit_key == our_emit_key && operator == our_operator),
                _ => false,
            },
            _ => unimplemented!(),
        }
    }
}

impl Debug for MirNodeInner {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            MirNodeInner::Aggregation {
                ref on,
                ref group_by,
                ref kind,
            } => {
                let op_string = match *kind {
                    Aggregation::Count => format!("|*|({})", on.name.as_str()),
                    Aggregation::Sum => format!("𝛴({})", on.name.as_str()),
                    Aggregation::Avg => format!("AVG({})", on.name.as_str()),
                    Aggregation::GroupConcat { separator: ref s } => {
                        format!("||([{}], \"{}\")", on.name.as_str(), s.as_str())
                    }
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} γ[{}]", op_string, group_cols)
            }
            MirNodeInner::Base {
                ref column_specs,
                ref keys,
                ..
            } => write!(
                f,
                "B [{}; ⚷: {}]",
                column_specs
                    .iter()
                    .map(|&(ref cs, _)| cs.column.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                keys.iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            MirNodeInner::Extremum {
                ref on,
                ref group_by,
                ref kind,
            } => {
                let op_string = match *kind {
                    Extremum::Min => format!("min({})", on.name.as_str()),
                    Extremum::Max => format!("max({})", on.name.as_str()),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} γ[{}]", op_string, group_cols)
            }
            MirNodeInner::Filter { ref conditions, .. } => {
                write!(f, "σ[{}]", conditions)
            }
            MirNodeInner::FilterAggregation {
                ref on,
                else_on: _,
                ref group_by,
                ref kind,
                conditions: _,
                remapped_exprs_to_parent_names: _,
            } => {
                let op_string = match *kind {
                    Aggregation::Count => format!("|*|(filter {})", on.name.as_str()),
                    Aggregation::Sum => format!("𝛴(filter {})", on.name.as_str()),
                    Aggregation::Avg => format!("Avg(filter {})", on.name.as_str()),
                    Aggregation::GroupConcat { separator: ref s } => {
                        format!("||([{}], \"{}\")", on.name, s)
                    }
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} γ[{}]", op_string, group_cols)
            }
            MirNodeInner::Identity => write!(f, "≡"),
            MirNodeInner::Join {
                ref on_left,
                ref on_right,
                ref project,
            } => {
                let jc = on_left
                    .iter()
                    .zip(on_right)
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "⋈ [{} on {}]",
                    project
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    jc
                )
            }
            MirNodeInner::Leaf { ref keys, .. } => {
                let key_cols = keys
                    .iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Leaf [⚷: {}]", key_cols)
            }
            MirNodeInner::LeftJoin {
                ref on_left,
                ref on_right,
                ref project,
            } => {
                let jc = on_left
                    .iter()
                    .zip(on_right)
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "⋉ [{} on {}]",
                    project
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    jc
                )
            }
            MirNodeInner::Latest { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "⧖ γ[{}]", key_cols)
            }
            MirNodeInner::Project {
                ref emit,
                ref literals,
                ref expressions,
            } => write!(
                f,
                "π [{}]",
                emit.iter()
                    .map(|c| c.name.clone())
                    .chain(
                        expressions
                            .iter()
                            .map(|&(ref n, ref e)| format!("{}: {}", n, e))
                    )
                    .chain(
                        literals
                            .iter()
                            .map(|&(ref n, ref v)| format!("{}: {}", n, v))
                    )
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            MirNodeInner::Reuse { ref node } => write!(
                f,
                "Reuse [{}: {}]",
                node.borrow().versioned_name(),
                node.borrow()
            ),
            MirNodeInner::Distinct { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Distinct [γ: {}]", key_cols)
            }
            MirNodeInner::TopK {
                ref order, ref k, ..
            } => write!(f, "TopK [k: {}, {:?}]", k, order),
            MirNodeInner::Union { ref emit } => {
                let cols = emit
                    .iter()
                    .map(|c| {
                        c.iter()
                            .map(|e| e.name.clone())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .collect::<Vec<_>>()
                    .join(" ⋃ ");

                write!(f, "{}", cols)
            }
            MirNodeInner::Rewrite { ref column, .. } => write!(f, "Rw [{}]", column),
            MirNodeInner::ParamFilter {
                ref col,
                ref emit_key,
                ref operator,
            } => write!(f, "σφ [{:?}, {:?}, {:?}]", col, emit_key, operator),
        }
    }
}
