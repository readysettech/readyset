use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Display};

use dataflow::ops::grouped::aggregate::Aggregation as AggregationKind;
use dataflow::ops::grouped::extremum::Extremum as ExtremumKind;
use dataflow::ops::union;
use itertools::Itertools;

use crate::column::Column;
use crate::node::node_inner::MirNodeInner;
use crate::node::MirNode;
use crate::query::MirQuery;

pub struct GraphVizzed<'a, T: ?Sized>(&'a T);

impl<'a, T> Display for GraphVizzed<'a, T>
where
    T: GraphViz,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.graphviz_fmt(f)
    }
}

pub trait GraphViz {
    fn graphviz_fmt(&self, f: &mut fmt::Formatter) -> fmt::Result;
    fn to_graphviz(&self) -> GraphVizzed<Self> {
        GraphVizzed(self)
    }
}

impl GraphViz for MirQuery {
    fn graphviz_fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // starting at the roots, print nodes in topological order
        let mut node_queue = VecDeque::new();
        node_queue.extend(self.roots.iter().cloned());
        let mut in_edge_counts = HashMap::new();
        for n in &node_queue {
            in_edge_counts.insert(n.borrow().versioned_name(), 0);
        }

        f.write_str("digraph {\n")?;
        f.write_str("node [shape=record, fontsize=10]\n")?;

        while !node_queue.is_empty() {
            let n = node_queue.pop_front().unwrap();
            assert_eq!(in_edge_counts[&n.borrow().versioned_name()], 0);

            let vn = n.borrow().versioned_name();
            writeln!(
                f,
                "\"{}\" [label=\"{{ {} | {} }}\"]",
                vn,
                vn,
                n.borrow().to_graphviz(),
            )?;

            for child in n.borrow().children.iter() {
                let nd = child.borrow().versioned_name();
                writeln!(f, "\"{}\" -> \"{}\"", n.borrow().versioned_name(), nd)?;
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
        f.write_str("}\n")
    }
}

impl GraphViz for MirNode {
    fn graphviz_fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} | {}",
            self.inner.to_graphviz(),
            self.columns
                .iter()
                .map(|c| match c.table {
                    None => c.name.to_string(),
                    Some(ref t) => format!("{}.{}", t, c.name),
                })
                .collect::<Vec<_>>()
                .join(",\\n"),
        )
    }
}

impl GraphViz for MirNodeInner {
    fn graphviz_fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let print_col = |c: &Column| -> String {
            match c.table {
                None => c.name.to_string(),
                Some(ref t) => format!("{}.{}", t, c.name),
            }
        };

        match self {
            MirNodeInner::Aggregation {
                ref on,
                ref group_by,
                ref kind,
            } => {
                let op_string = match &*kind {
                    AggregationKind::Count { .. } => format!("\\|*\\|({})", print_col(on)),
                    AggregationKind::Sum => format!("𝛴({})", print_col(on)),
                    AggregationKind::Avg => format!("AVG({})", print_col(on)),
                    AggregationKind::GroupConcat { separator: s } => {
                        format!("||({}, \"{}\")", print_col(on), s)
                    }
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| print_col(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} | γ: {}", op_string, group_cols)
            }
            MirNodeInner::Base {
                column_specs,
                unique_keys,
                ..
            } => {
                write!(
                    f,
                    "B | {} | ⚷: {}",
                    column_specs
                        .iter()
                        .map(|&(ref cs, _)| cs.column.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    unique_keys
                        .iter()
                        .map(|k| k
                            .iter()
                            .map(|c| c.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", "))
                        .join("; ")
                )
            }
            MirNodeInner::Extremum {
                ref on,
                ref group_by,
                ref kind,
            } => {
                let op_string = match *kind {
                    ExtremumKind::Min => format!("min({})", print_col(on)),
                    ExtremumKind::Max => format!("max({})", print_col(on)),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| print_col(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} | γ: {}", op_string, group_cols)
            }
            MirNodeInner::Filter { ref conditions, .. } => write!(f, "σ: {}", conditions),

            MirNodeInner::Identity => write!(f, "≡"),
            MirNodeInner::Join {
                ref on_left,
                ref on_right,
                ..
            } => {
                let jc = on_left
                    .iter()
                    .zip(on_right)
                    .map(|(l, r)| format!("{}:{}", print_col(l), print_col(r)))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "⋈  | on: {}", jc)
            }
            MirNodeInner::JoinAggregates => write!(f, "AGG ⋈"),
            MirNodeInner::Leaf { ref keys, .. } => {
                let key_cols = keys
                    .iter()
                    .map(|k| print_col(&k.0))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Leaf | ⚷: {}", key_cols)
            }
            MirNodeInner::LeftJoin {
                ref on_left,
                ref on_right,
                ..
            } => {
                let jc = on_left
                    .iter()
                    .zip(on_right)
                    .map(|(l, r)| format!("{}:{}", print_col(l), print_col(r)))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "⋉  | on: {}", jc)
            }
            MirNodeInner::Latest { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| print_col(k))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "⧖ | γ: {}", key_cols)
            }
            MirNodeInner::Project {
                ref emit,
                ref literals,
                ref expressions,
            } => {
                write!(
                    f,
                    "π: {}",
                    emit.iter()
                        .map(|c| print_col(c))
                        .chain(
                            literals
                                .iter()
                                .map(|&(ref n, ref v)| format!("{}: {}", n, v))
                        )
                        .chain(
                            expressions
                                .iter()
                                .map(|&(ref n, ref e)| format!("{}: {}", n, e))
                        )
                        .join(", ")
                )
            }
            MirNodeInner::Reuse { ref node } => {
                write!(f, "Reuse | using: {}", node.borrow().versioned_name(),)
            }
            MirNodeInner::Distinct { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| print_col(k))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Distinct | γ: {}", key_cols)
            }
            MirNodeInner::Paginate {
                ref order,
                ref limit,
                ..
            } => {
                let order = order
                    .as_ref()
                    .map(|v| {
                        v.iter()
                            .map(|(c, o)| format!("{}: {}", c.name.as_str(), o))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_else(|| "".into());
                write!(f, "Paginate [limit: {}; {}]", limit, order)
            }
            MirNodeInner::TopK {
                ref order,
                ref limit,
                ..
            } => {
                let order = order
                    .as_ref()
                    .map(|v| {
                        v.iter()
                            .map(|(c, o)| format!("{}: {}", c.name.as_str(), o))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_else(|| "".into());
                write!(f, "TopK [k: {}; {}]", limit, order)
            }
            MirNodeInner::Union {
                ref emit,
                ref duplicate_mode,
            } => {
                let symbol = match duplicate_mode {
                    union::DuplicateMode::BagUnion => '⊎',
                    union::DuplicateMode::UnionAll => '⋃',
                };
                let cols = emit
                    .iter()
                    .map(|c| {
                        c.iter()
                            .map(|e| print_col(e))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .join(&format!(" {} ", symbol));

                write!(f, "{}", cols)
            }
        }
    }
}
