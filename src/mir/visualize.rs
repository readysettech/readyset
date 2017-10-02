use std::collections::HashMap;
use std::fmt::{self, Write};

use ops::grouped::aggregate::Aggregation as AggregationKind;
use ops::grouped::extremum::Extremum as ExtremumKind;
use mir::node::{MirNode, MirNodeType};
use mir::query::MirQuery;

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
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(",\\n"),
        )?;
        Ok(out)
    }
}

impl GraphViz for MirNodeType {
    fn to_graphviz(&self) -> Result<String, fmt::Error> {
        let mut out = String::new();
        match *self {
            MirNodeType::Aggregation {
                ref on,
                ref group_by,
                ref kind,
            } => {
                let op_string = match *kind {
                    AggregationKind::COUNT => format!("|*|({})", on.name.as_str()),
                    AggregationKind::SUM => format!("𝛴({})", on.name.as_str()),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "{} | γ: {}", op_string, group_cols)?;
            }
            MirNodeType::Base {
                ref column_specs,
                ref keys,
                transactional,
                ..
            } => {
                write!(
                    out,
                    "B{} | {} | ⚷: {}",
                    if transactional { "*" } else { "" },
                    column_specs
                        .into_iter()
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
                    ExtremumKind::MIN => format!("min({})", on.name.as_str()),
                    ExtremumKind::MAX => format!("max({})", on.name.as_str()),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
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
                        .enumerate()
                        .filter_map(|(i, ref e)| match e.as_ref() {
                            Some(&(ref op, ref x)) => {
                                Some(format!("f{} {} {}", i, escape(&format!("{}", op)), x))
                            }
                            None => None,
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
                write!(out, "||({}, \"{}\")", on.name, separator)?;
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
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "⋈  | on: {}", jc)?;
            }
            MirNodeType::Leaf { ref keys, .. } => {
                let key_cols = keys.iter()
                    .map(|k| k.name.clone())
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
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "⋉  | on: {}", jc)?;
            }
            MirNodeType::Latest { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(out, "⧖ | γ: {}", key_cols)?;
            }
            MirNodeType::Project {
                ref emit,
                ref literals,
            } => {
                write!(
                    out,
                    "π: {}{}",
                    emit.iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    if literals.len() > 0 {
                        format!(
                            ", lit: {}",
                            literals
                                .iter()
                                .map(|&(ref n, ref v)| format!("{}: {}", n, v))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    } else {
                        format!("")
                    }
                )?;
            }
            MirNodeType::Reuse { ref node } => {
                write!(out, "Reuse | using: {}", node.borrow().versioned_name(),)?;
            }
            MirNodeType::TopK {
                ref order, ref k, ..
            } => {
                write!(out, "TopK | k: {}, {:?}", k, order)?;
            }
            MirNodeType::Union { ref emit } => {
                let cols = emit.iter()
                    .map(|c| {
                        c.iter()
                            .map(|e| e.name.clone())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .collect::<Vec<_>>()
                    .join(" ⋃ ");

                write!(out, "{}", cols)?;
            }
        }
        Ok(out)
    }
}
