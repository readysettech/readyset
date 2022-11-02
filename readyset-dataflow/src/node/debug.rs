use std::collections::HashMap;
use std::fmt;

use itertools::Itertools;
use lazy_static::lazy_static;
use readyset::NodeSize;
use regex::Regex;

use crate::node::{Node, NodeType};
use crate::prelude::*;

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.inner {
            NodeType::Dropped => write!(f, "dropped node"),
            NodeType::Source => write!(f, "source node"),
            NodeType::Ingress => write!(f, "ingress node"),
            NodeType::Egress { .. } => write!(f, "egress node"),
            NodeType::Sharder(ref s) => write!(f, "sharder [{}] node", s.sharded_by()),
            NodeType::Reader(..) => write!(f, "reader node"),
            NodeType::Base(..) => write!(f, "B"),
            NodeType::Internal(ref i) => write!(f, "internal {} node", i.description(true)),
        }
    }
}

fn escape<S>(s: S) -> String
where
    S: ToString,
{
    lazy_static! {
        static ref ESCAPE_RE: Regex = Regex::new("([\"|{}])").unwrap();
    };
    ESCAPE_RE.replace_all(&s.to_string(), "\\$1").into_owned()
}

impl Node {
    pub fn describe(
        &self,
        idx: NodeIndex,
        detailed: bool,
        node_sizes: &HashMap<NodeIndex, NodeSize>,
        materialization_status: MaterializationStatus,
    ) -> String {
        let mut s = String::new();
        let border = match self.sharded_by {
            Sharding::ByColumn(_, _) | Sharding::Random(_) => "filled,dashed",
            _ => "filled",
        };

        if !detailed {
            match self.inner {
                NodeType::Dropped => {
                    s.push_str("[shape=none]\n");
                }
                NodeType::Source | NodeType::Ingress | NodeType::Egress { .. } => {
                    s.push_str("[shape=point]\n");
                }
                NodeType::Base(..) => {
                    s.push_str(&format!(
                        "[style=bold, shape=tab, label=\"{}\"]\n",
                        escape(self.name())
                    ));
                }
                NodeType::Sharder(ref sharder) => {
                    s.push_str(&format!(
                        "[style=bold, shape=Msquare, label=\"shard by {}\"]\n",
                        escape(&self.columns[sharder.sharded_by()].name),
                    ));
                }
                NodeType::Reader(_) => {
                    s.push_str(&format!(
                        "[style=\"bold,filled\", fillcolor=\"{}\", shape=box3d, label=\"{}\"]\n",
                        if let MaterializationStatus::Full = materialization_status {
                            "#0C6FA9"
                        } else {
                            "#5CBFF9"
                        },
                        escape(&self.name().to_string())
                    ));
                }
                NodeType::Internal(ref i) => {
                    s.push_str(&format!(
                        "[label=\"{}\"]\n",
                        escape(&i.description(detailed))
                    ));

                    match materialization_status {
                        MaterializationStatus::Not => {}
                        MaterializationStatus::Full => {
                            s.push_str(&format!(
                                "n{}_m [shape=tab, style=\"bold,filled\", color=\"#AA4444\", fillcolor=\"#AA4444\", label=\"\"]\n\
                                 n{} -> n{}_m {{ dir=none }}\n\
                                 {{rank=same; n{} n{}_m}}\n",
                                idx.index(),
                                idx.index(),
                                idx.index(),
                                idx.index(),
                                idx.index()
                            ));
                        }
                        MaterializationStatus::Partial {
                            beyond_materialization_frontier,
                        } => {
                            s.push_str(&format!(
                                "n{}_m [shape=tab, style=\"bold,filled\", color=\"#AA4444\", {}, label=\"\"]\n\
                                 n{} -> n{}_m {{ dir=none }}\n\
                                 {{rank=same; n{} n{}_m}}\n",
                                idx.index(),
                                if beyond_materialization_frontier {
                                "fillcolor=\"#EEBB99\""
                                } else {
                                "fillcolor=\"#EE9999\""
                                },
                                idx.index(),
                                idx.index(),
                                idx.index(),
                                idx.index()
                            ));
                        }
                    }
                }
            }
        } else {
            s.push_str(&format!(
                " [style=\"{}\", fillcolor={}, label=\"",
                border,
                self.domain
                    .map(|d| -> usize { d.into() })
                    .map(|d| format!("\"/set312/{}\"", (d % 12) + 1))
                    .unwrap_or_else(|| "white".into())
            ));

            let (key_count_str, node_size_str) = match node_sizes.get(&idx) {
                Some(NodeSize { key_count, bytes }) => {
                    (format!("&nbsp;({})", key_count), format!("| {}", bytes))
                }
                _ => ("".to_string(), "".to_string()),
            };

            let materialized = match materialization_status {
                MaterializationStatus::Not => "",
                MaterializationStatus::Partial {
                    beyond_materialization_frontier,
                } => {
                    if beyond_materialization_frontier {
                        "| ◔"
                    } else {
                        "| ◕"
                    }
                }
                MaterializationStatus::Full => "| ●",
            };

            let sharding = match self.sharded_by {
                Sharding::ByColumn(k, w) => {
                    format!("shard ⚷: {} / {}-way", self.columns[k].name, w)
                }
                Sharding::Random(_) => "shard randomly".to_owned(),
                Sharding::None => "unsharded".to_owned(),
                Sharding::ForcedNone => "desharded to avoid SS".to_owned(),
            };

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
                NodeType::Source => s.push_str("(source)"),
                NodeType::Dropped => s.push_str(&format!("{{ {} | dropped }}", addr)),
                NodeType::Base(..) => {
                    s.push_str(&format!(
                        "{{ {{ {} / {} | {} {} {} }} | {} | {} }}",
                        addr,
                        escape(self.name()),
                        "B",
                        materialized,
                        key_count_str,
                        self.columns()
                            .iter()
                            .enumerate()
                            .map(|(i, c)| format!("[{}] {} : {}", i, c.name, c.ty()))
                            .join(", \\n"),
                        sharding
                    ));
                }
                NodeType::Ingress => s.push_str(&format!(
                    "{{ {{ {} {} {} {} }} | (ingress) | {} }}",
                    addr, materialized, key_count_str, node_size_str, sharding
                )),
                NodeType::Egress { .. } => {
                    s.push_str(&format!("{{ {} | (egress) | {} }}", addr, sharding))
                }
                NodeType::Sharder(ref sharder) => s.push_str(&format!(
                    "{{ {} | shard by {} | {} }}",
                    addr,
                    self.columns[sharder.sharded_by()].name,
                    sharding
                )),
                NodeType::Reader(ref r) => {
                    let key = match r.index() {
                        None => String::from("none"),
                        Some(index) => format!("{:?}({:?})", index.index_type, index.columns),
                    };
                    s.push_str(&format!(
                        "{{ {{ {} / {} {} {} {} }} | (reader / ⚷: {}) | {} }}",
                        addr,
                        escape(self.name()),
                        materialized,
                        key_count_str,
                        node_size_str,
                        key,
                        sharding,
                    ))
                }
                NodeType::Internal(ref i) => {
                    s.push('{');

                    // Output node name and description. First row.
                    s.push_str(&format!(
                        "{{ {} / {} | {} {} {} {} }}",
                        addr,
                        escape(self.name()),
                        escape(&i.description(detailed)),
                        materialized,
                        key_count_str,
                        node_size_str,
                    ));

                    // Output node outputs. Second row.
                    s.push_str(&format!(
                        " | {}",
                        self.columns()
                            .iter()
                            .enumerate()
                            .map(|(i, c)| format!("[{}] {} : {}", i, c.name, c.ty()))
                            .join(", \\n"),
                    ));
                    s.push_str(&format!(" | {}", sharding));

                    s.push('}');
                }
            };
            s.push_str("\"]\n");
        }

        s
    }
}
