use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Display};

use dataflow::ops::grouped::aggregate::Aggregation as AggregationKind;
use dataflow::ops::grouped::extremum::Extremum as ExtremumKind;
use dataflow::ops::union;
use dataflow::PostLookupAggregateFunction;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;

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

pub struct Sanitized<T>(T);

impl<T> Display for Sanitized<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        lazy_static! {
            static ref SANITIZE_RE: Regex = Regex::new("([<>])").unwrap();
        };
        write!(
            f,
            "{}",
            SANITIZE_RE.replace_all(&self.0.to_string(), "\\$1")
        )
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
                Sanitized(n.borrow().to_graphviz()),
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
        write!(f, "{} | ", self.inner.to_graphviz(),)?;
        for (i, col) in self.columns().iter().enumerate() {
            if i != 0 {
                write!(f, ",\\n")?;
            }
            write!(f, "{col:#}")?;
        }
        Ok(())
    }
}

impl GraphViz for MirNodeInner {
    fn graphviz_fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MirNodeInner::Aggregation {
                ref on,
                ref group_by,
                ref kind,
                ..
            } => {
                let op_string = match kind {
                    AggregationKind::Count { .. } => format!("\\|*\\|({})", on),
                    AggregationKind::Sum => format!("𝛴({})", on),
                    AggregationKind::Avg => format!("AVG({})", on),
                    AggregationKind::GroupConcat { separator: s } => {
                        format!("||({}, \"{}\")", on, s)
                    }
                };
                let group_cols = group_by.iter().join(", ");
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
                ..
            } => {
                let op_string = match *kind {
                    ExtremumKind::Min => format!("min({})", on),
                    ExtremumKind::Max => format!("max({})", on),
                };
                let group_cols = group_by.iter().join(", ");
                write!(f, "{} | γ: {}", op_string, group_cols)
            }
            MirNodeInner::Filter { ref conditions, .. } => write!(f, "σ: {}", conditions),

            MirNodeInner::Identity => write!(f, "≡"),
            MirNodeInner::Join { ref on, .. } => {
                let jc = on.iter().map(|(l, r)| format!("{}:{}", l, r)).join(", ");
                write!(f, "⋈  | on: {}", jc)
            }
            MirNodeInner::JoinAggregates => write!(f, "AGG ⋈"),
            MirNodeInner::Leaf {
                ref keys,
                index_type,
                order_by,
                limit,
                returned_cols,
                aggregates,
                ..
            } => {
                let key_cols = keys.iter().map(|k| &k.0).join(", ");
                write!(f, "Leaf | ⚷: {index_type:?}[{key_cols}]")?;

                if let Some(order_by) = order_by {
                    write!(
                        f,
                        "\\norder_by: {}",
                        order_by
                            .iter()
                            .map(|(col, ot)| format!("{} {}", col, ot))
                            .join(", ")
                    )?;
                }

                if let Some(limit) = limit {
                    write!(f, "\\nlimit: {limit}")?;
                }

                if let Some(returned_cols) = returned_cols {
                    write!(f, "\\nreturn: {}", returned_cols.iter().join(", "))?;
                }

                if let Some(aggregates) = aggregates {
                    write!(
                        f,
                        "\\naggregates: {} γ: {}",
                        aggregates
                            .aggregates
                            .iter()
                            .map(|aggregate| format!(
                                "{}({})",
                                match aggregate.function {
                                    PostLookupAggregateFunction::Sum => "Σ",
                                    PostLookupAggregateFunction::Product => "Π",
                                    PostLookupAggregateFunction::GroupConcat { .. } => "GC",
                                    PostLookupAggregateFunction::Max => "Max",
                                    PostLookupAggregateFunction::Min => "Min",
                                },
                                &aggregate.column
                            ))
                            .join(", "),
                        aggregates.group_by.iter().join(", ")
                    )?;
                }

                Ok(())
            }
            MirNodeInner::LeftJoin { ref on, .. } => {
                let jc = on.iter().map(|(l, r)| format!("{}:{}", l, r)).join(", ");
                write!(f, "⋉  | on: {}", jc)
            }
            MirNodeInner::DependentJoin { ref on, .. } => {
                write!(
                    f,
                    "⧑ | on: {}",
                    on.iter().map(|(l, r)| format!("{}:{}", l, r)).join(", ")
                )
            }
            MirNodeInner::Latest { ref group_by } => {
                let key_cols = group_by.iter().join(", ");
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
                        .map(|c| c.to_string())
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
                write!(f, "Reuse | using: {}", node.borrow().versioned_name(),)?;
                if let Some(flow_node) = &node.borrow().flow_node {
                    write!(f, " | flow node: {}", flow_node.address().index())?;
                }
                Ok(())
            }
            MirNodeInner::Distinct { ref group_by } => {
                let key_cols = group_by.iter().join(", ");
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
                    .map(|c| c.iter().join(", "))
                    .join(&format!(" {} ", symbol));

                write!(f, "{}", cols)
            }
            MirNodeInner::AliasTable { ref table } => {
                write!(f, "AliasTable [{}]", table)
            }
        }
    }
}
