use node::{Node, NodeType};
use prelude::*;
use std::fmt;

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

impl Node {
    pub fn describe(
        &self,
        idx: NodeIndex,
        detailed: bool,
        materialization_status: MaterializationStatus,
    ) -> String {
        let mut s = String::new();
        let border = match self.sharded_by {
            Sharding::ByColumn(_, _) | Sharding::Random(_) => "filled,dashed",
            _ => {
                if Self::is_security(self.name()) {
                    "filled,rounded"
                } else {
                    "filled"
                }
            }
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
                        Self::escape(self.name())
                    ));
                }
                NodeType::Sharder(ref sharder) => {
                    s.push_str(&format!(
                        "[style=bold, shape=Msquare, label=\"shard by {}\"]\n",
                        Self::escape(&self.fields[sharder.sharded_by()]),
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
                        Self::escape(self.name())
                    ));
                }
                NodeType::Internal(ref i) => {
                    s.push_str(&format!(
                        "[label=\"{}\"]\n",
                        Self::escape(&i.description(detailed))
                    ));

                    match materialization_status {
                        MaterializationStatus::Not => {}
                        MaterializationStatus::Full | MaterializationStatus::Partial => {
                            s.push_str(&format!(
                                "n{}_m [shape=tab, style=\"bold,filled\", color=\"#AA4444\", {}, label=\"\"]\n\
                                 n{} -> n{}_m {{ dir=none }}\n\
                                 {{rank=same; n{} n{}_m}}\n",
                                idx.index(),
                                if let MaterializationStatus::Full = materialization_status {
                                    "fillcolor=\"#AA4444\""
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

            let materialized = match materialization_status {
                MaterializationStatus::Not => "",
                MaterializationStatus::Partial => "| ░",
                MaterializationStatus::Full => "| █",
            };

            let sharding = match self.sharded_by {
                Sharding::ByColumn(k, w) => format!("shard ⚷: {} / {}-way", self.fields[k], w),
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
                        "{{ {{ {} / {} | {} {} }} | {} | {} }}",
                        addr,
                        Self::escape(self.name()),
                        "B",
                        materialized,
                        self.fields().join(", \\n"),
                        sharding
                    ));
                }
                NodeType::Ingress => s.push_str(&format!(
                    "{{ {{ {} {} }} | (ingress) | {} }}",
                    addr, materialized, sharding
                )),
                NodeType::Egress { .. } => {
                    s.push_str(&format!("{{ {} | (egress) | {} }}", addr, sharding))
                }
                NodeType::Sharder(ref sharder) => s.push_str(&format!(
                    "{{ {} | shard by {} | {} }}",
                    addr,
                    self.fields[sharder.sharded_by()],
                    sharding
                )),
                NodeType::Reader(ref r) => {
                    let key = match r.key() {
                        None => String::from("none"),
                        Some(k) => format!("{:?}", k),
                    };
                    s.push_str(&format!(
                        "{{ {{ {} / {} {} }} | (reader / ⚷: {}) | {} }}",
                        addr,
                        Self::escape(self.name()),
                        materialized,
                        key,
                        sharding,
                    ))
                }
                NodeType::Internal(ref i) => {
                    s.push_str("{{");

                    // Output node name and description. First row.
                    s.push_str(&format!(
                        "{{ {} / {} | {} {} }}",
                        addr,
                        Self::escape(self.name()),
                        Self::escape(&i.description(detailed)),
                        materialized
                    ));

                    // Output node outputs. Second row.
                    s.push_str(&format!(" | {}", self.fields().join(", \\n")));
                    s.push_str(&format!(" | {} }}", sharding))
                }
            };
            s.push_str("\"]\n");
        }

        s
    }

    fn is_security(name: &str) -> bool {
        name.starts_with("sp_")
    }

    fn escape(s: &str) -> String {
        use regex::Regex;

        Regex::new("([\"|{}])")
            .unwrap()
            .replace_all(s, "\\$1")
            .to_string()
    }
}
