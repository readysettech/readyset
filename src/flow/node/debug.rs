use std::fmt;
use petgraph::graph::NodeIndex;
use flow::node::{Node, NodeType};
use flow::core::processing::Ingredient;
use flow::prelude::*;

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.inner {
            NodeType::Dropped => write!(f, "dropped node"),
            NodeType::Source => write!(f, "source node"),
            NodeType::Ingress => write!(f, "ingress node"),
            NodeType::Egress { .. } => write!(f, "egress node"),
            NodeType::Sharder { .. } => write!(f, "sharder"),
            NodeType::Reader(..) => write!(f, "reader node"),
            NodeType::Internal(ref i) => write!(f, "internal {} node", i.description()),
            NodeType::Hook(..) => write!(f, "hook node"),
        }
    }
}

impl Node {
    pub fn describe(&self, f: &mut fmt::Write, idx: NodeIndex) -> fmt::Result {
        let border = match self.sharded_by {
            Sharding::ByColumn(_) |
            Sharding::Random => "filled,dashed",
            _ => "filled",
        };
        write!(f,
               " [style=\"{}\", fillcolor={}, label=\"",
               border,
               self.domain
                   .map(|d| -> usize { d.into() })
                   .map(|d| format!("\"/set312/{}\"", (d % 12) + 1))
                   .unwrap_or("white".into()))?;

        let addr = match self.index {
            Some(ref idx) => {
                if idx.has_local() {
                    format!("{} / {}", idx.as_global().index(), **idx)
                } else {
                    format!("{} / -", idx.as_global().index())
                }
            }
            None => format!("{} / -", idx.index()),
        };
        match self.inner {
            NodeType::Source => write!(f, "(source)"),
            NodeType::Dropped => write!(f, "✗"),
            NodeType::Ingress => write!(f, "{{ {} | (ingress) }}", addr),
            NodeType::Egress { .. } => write!(f, "{{ {} | (egress) }}", addr),
            NodeType::Sharder { .. } => write!(f, "{{ {} | (sharder) }}", addr),
            NodeType::Hook(..) => write!(f, "{{ {} | (hook) }}", addr),
            NodeType::Reader(ref r) => {
                let key = match r.key() {
                    None => String::from("none"),
                    Some(k) => format!("{}", k),
                };
                use flow::VIEW_READERS;
                let size = match VIEW_READERS
                          .lock()
                          .unwrap()
                          .get(&idx)
                          .map(|state| state.len()) {
                    None => String::from("empty"),
                    Some(s) => format!("{} distinct keys", s),
                };
                write!(f, "{{ {} | (reader / key: {}) | {} }}", addr, key, size)
            }
            NodeType::Internal(ref i) => {
                write!(f, "{{")?;

                // Output node name and description. First row.
                write!(f,
                       "{{ {} / {} | {} }}",
                       addr,
                       Self::escape(self.name()),
                       Self::escape(&i.description()))?;

                // Output node outputs. Second row.
                write!(f, " | {}", self.fields().join(", \\n"))?;

                // Maybe output node's HAVING conditions. Optional third row.
                // TODO
                // if let Some(conds) = n.node().unwrap().having_conditions() {
                //     let conds = conds.iter()
                //         .map(|c| format!("{}", c))
                //         .collect::<Vec<_>>()
                //         .join(" ∧ ");
                //     write!(f, " | σ({})", escape(&conds))?;
                // }

                write!(f, " }}")
            }
        }?;

        writeln!(f, "\"]")
    }

    fn escape(s: &str) -> String {
        use regex::Regex;

        Regex::new("([\"|{}])")
            .unwrap()
            .replace_all(s, "\\$1")
            .to_string()
    }
}
