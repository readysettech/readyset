use itertools::Itertools;
use std::collections::HashMap;
use std::fmt::{self, Write};

use crate::node::{MirNode, MirNodeType};
use crate::query::MirQuery;
use dataflow::ops::filter::FilterCondition;
use dataflow::ops::grouped::aggregate::Aggregation as AggregationKind;
use dataflow::ops::grouped::extremum::Extremum as ExtremumKind;

pub trait GraphViz {
    fn to_graphviz(&self) -> Result<String, fmt::Error>;
}

impl GraphViz for MirQuery {
    fn to_graphviz(&self) -> Result<String, fmt::Error> {
        use std::collections::VecDeque;

        let mut out = String::new();

        // starting at the roots, print nodes in topological order
        let mut node_queue = VecDeque::new();
        node_queue.extend(self.roots.iter().cloned());
        let mut in_edge_counts = HashMap::new();
        for n in &node_queue {
            in_edge_counts.insert(n.borrow().versioned_name(), 0);
        }

        out.write_str("digraph {\n")?;
        out.write_str("node [shape=record, fontsize=10]\n")?;

        while !node_queue.is_empty() {
            let n = node_queue.pop_front().unwrap();
            assert_eq!(in_edge_counts[&n.borrow().versioned_name()], 0);

            let vn = n.borrow().versioned_name();
            writeln!(
                out,
                "\"{}\" [label=\"{{ {} | {} }}\"]",
                vn,
                vn,
                n.borrow().to_graphviz()?,
            )?;

            for child in n.borrow().children.iter() {
                let nd = child.borrow().versioned_name();
                writeln!(out, "\"{}\" -> \"{}\"", n.borrow().versioned_name(), nd)?;
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
        out.write_str("}\n")?;

        Ok(out)
    }
}

impl GraphViz for MirNode {
    fn to_graphviz(&self) -> Result<String, fmt::Error> {
        let mut out = String::new();

        write!(
            out,
            "{} | {}",
            self.inner.to_graphviz()?,
            self.columns
                .iter()
                .map(|c| match c.table {
                    None => c.name.clone(),
                    Some(ref t) => format!("{}.{}", t, c.name),
                })
                .collect::<Vec<_>>()
                .join(",\\n"),
        )?;
        Ok(out)
    }
}

impl GraphViz for MirNodeType {
    fn to_graphviz(&self) -> Result<String, fmt::Error> {
        use crate::column::Column;

        let mut out = String::new();

        let print_col = |c: &Column| -> String {
            match c.table {
                None => c.name.clone(),
                Some(ref t) => format!("{}.{}", t, c.name),
            }
        };

        match *self {
            MirNodeType::Aggregation {
                ref on,
                ref group_by,
                ref kind,
            } => {
                let op_string = match *kind {
                    AggregationKind::COUNT => format!("\\|*\\|({})", print_col(on)),
                    AggregationKind::SUM => format!("𝛴({})", print_col(on)),
                    AggregationKind::AVG => format!("AVG({})", print_col(on)),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| print_col(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "{} | γ: {}", op_string, group_cols)?;
            }
            MirNodeType::Base {
                ref column_specs,
                ref keys,
                ..
            } => {
                write!(
                    out,
                    "B | {} | ⚷: {}",
                    column_specs
                        .iter()
                        .map(|&(ref cs, _)| cs.column.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    keys.iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
            }
            MirNodeType::Extremum {
                ref on,
                ref group_by,
                ref kind,
            } => {
                let op_string = match *kind {
                    ExtremumKind::MIN => format!("min({})", print_col(on)),
                    ExtremumKind::MAX => format!("max({})", print_col(on)),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| print_col(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "{} | γ: {}", op_string, group_cols)?;
            }
            MirNodeType::FilterAggregation {
                ref on,
                ref group_by,
                ref kind,
                ..
            } => {
                let op_string = match *kind {
                    AggregationKind::COUNT => format!("\\|*\\|(filter {})", print_col(on)),
                    AggregationKind::SUM => format!("𝛴(filter {})", print_col(on)),
                    AggregationKind::AVG => format!("Avg(filter {})", print_col(on)),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| print_col(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "{} | γ: {}", op_string, group_cols)?;
            }
            MirNodeType::Filter { ref conditions } => {
                use regex::Regex;

                let escape = |s: &str| {
                    Regex::new("([<>])")
                        .unwrap()
                        .replace_all(s, "\\$1")
                        .to_string()
                };
                write!(
                    out,
                    "σ: {}",
                    conditions
                        .iter()
                        .filter_map(|(i, ref cond)| match *cond {
                            FilterCondition::Comparison(ref op, ref x) => {
                                Some(format!("f{} {} {}", i, escape(&format!("{}", op)), x))
                            }
                            FilterCondition::In(ref xs) => Some(format!(
                                "f{} IN ({})",
                                i,
                                xs.iter()
                                    .map(|d| format!("{}", d))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )),
                        })
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(", ")
                )?;
            }
            MirNodeType::GroupConcat {
                ref on,
                ref separator,
            } => {
                write!(out, "||({}, \"{}\")", print_col(on), separator)?;
            }
            MirNodeType::Identity => {
                write!(out, "≡")?;
            }
            MirNodeType::Join {
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
                write!(out, "⋈  | on: {}", jc)?;
            }
            MirNodeType::Leaf { ref keys, .. } => {
                let key_cols = keys
                    .iter()
                    .map(|k| print_col(k))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "Leaf | ⚷: {}", key_cols)?;
            }
            MirNodeType::LeftJoin {
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
                write!(out, "⋉  | on: {}", jc)?;
            }
            MirNodeType::Latest { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| print_col(k))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "⧖ | γ: {}", key_cols)?;
            }
            MirNodeType::Project {
                ref emit,
                ref literals,
                ref expressions,
            } => {
                write!(
                    out,
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
                )?;
            }
            MirNodeType::Reuse { ref node } => {
                write!(out, "Reuse | using: {}", node.borrow().versioned_name(),)?;
            }
            MirNodeType::Distinct { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| print_col(k))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "Distinct | γ: {}", key_cols)?;
            }
            MirNodeType::TopK {
                ref order, ref k, ..
            } => {
                write!(
                    out,
                    "TopK [k: {}; {}]",
                    k,
                    order
                        .as_ref()
                        .map(|v| v
                            .iter()
                            .map(|(c, o)| format!("{}: {}", c.name.as_str(), o))
                            .collect::<Vec<_>>()
                            .join(", "))
                        .unwrap_or_else(|| "".into())
                )?;
            }
            MirNodeType::Union { ref emit } => {
                let cols = emit
                    .iter()
                    .map(|c| {
                        c.iter()
                            .map(|e| print_col(e))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .collect::<Vec<_>>()
                    .join(" ⋃ ");

                write!(out, "{}", cols)?;
            }
            MirNodeType::Rewrite { ref column, .. } => {
                write!(out, "Rw | column: {}", column)?;
            }
            MirNodeType::ParamFilter {
                ref col,
                ref emit_key,
                ref operator,
            } => {
                write!(
                    out,
                    "σφ | col: {}, emit_key: {}, operator: {}",
                    print_col(col),
                    print_col(emit_key),
                    operator
                )?;
            }
        }
        Ok(out)
    }
}
