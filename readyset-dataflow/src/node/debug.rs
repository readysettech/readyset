use std::collections::HashMap;
use std::fmt;

use html_escape::encode_text;
use itertools::Itertools;
use readyset_client::debug::info::NodeSize;

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
            NodeType::Internal(ref i) => write!(f, "internal {} node", i.description()),
        }
    }
}

fn out_header(
    s: &mut String,
    addr: &str,
    materialized: &str,
    key_count: &str,
    node_size: &str,
) -> usize {
    s.push_str(&format!("<tr><td>{}</td> ", addr));
    let mut span = 1;
    if !materialized.is_empty() {
        s.push_str(&format!("<td>{} {}</td> ", materialized, key_count));
        span += 1;
    }
    if !node_size.is_empty() {
        s.push_str(&format!("<td>{}</td> ", node_size));
        span += 1;
    }
    s.push_str("</tr> ");
    span
}

fn out_columns(s: &mut String, span: usize, node: &Node) {
    s.push_str(&format!(
        "<tr><td colspan=\"{}\">{}</td></tr> ",
        span,
        node.columns()
            .iter()
            .enumerate()
            .map(|(i, c)| format!("[{}] {} : {}", i, c.name, c.ty()))
            .join("<br/>"),
    ));
}

fn out_sharding(s: &mut String, span: usize, sharding: &str) {
    s.push_str(&format!(
        "<tr><td colspan=\"{}\">{}</td></tr>",
        span, sharding
    ));
}

impl Node {
    pub fn describe(
        &self,
        idx: NodeIndex,
        node_sizes: &HashMap<NodeIndex, NodeSize>,
        materialization_status: MaterializationStatus,
    ) -> String {
        let mut s = String::new();
        let color = self
            .domain
            .map(|d| format!("/set312/{}", (usize::from(d) % 12) + 1))
            .unwrap_or_else(|| "white".into());

        s.push_str("[label=< <table ");
        s.push_str(&format!(
            r#"cellspacing="0" cellpadding="4" border="0" cellborder="1" bgcolor="{}"> "#,
            color
        ));

        let (key_count, node_size) = match node_sizes.get(&idx) {
            Some(NodeSize { key_count, bytes }) => {
                (format!("({})", key_count), format!("{}", bytes))
            }
            _ => ("".to_string(), "".to_string()),
        };

        let materialized = match materialization_status {
            MaterializationStatus::Not => "",
            MaterializationStatus::Partial {
                beyond_materialization_frontier,
            } => {
                if beyond_materialization_frontier {
                    "◔"
                } else {
                    "◕"
                }
            }
            MaterializationStatus::Full => "●",
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
            Some(ref idx) if idx.has_local() => {
                format!("{} / {}", idx.as_global().index(), **idx)
            }
            Some(ref idx) => {
                format!("{} / -", idx.as_global().index())
            }
            None => format!("{} / -", idx.index()),
        };

        match self.inner {
            NodeType::Source => s.push_str("<tr><td>source</td></tr>"),
            NodeType::Dropped => s.push_str(&format!(
                "<tr><td>{}</td></tr><tr><td>dropped</td></tr>",
                addr
            )),
            NodeType::Base(..) => {
                s.push_str(&format!(
                    "<tr><td>{} / {}</td> <td>base</td> <td>{} {}</td></tr> ",
                    addr,
                    encode_text(&self.name().display_unquoted().to_string()),
                    materialized,
                    key_count,
                ));
                out_columns(&mut s, 3, self);
                out_sharding(&mut s, 3, &sharding);
            }
            NodeType::Ingress => {
                let span = out_header(&mut s, &addr, materialized, &key_count, &node_size);
                s.push_str(&format!("<tr><td colspan=\"{}\">ingress</td></tr> ", span));
                out_sharding(&mut s, span, &sharding);
            }
            NodeType::Egress { .. } => {
                s.push_str(&format!("<tr><td>{}</td></tr> ", addr));
                s.push_str("<tr><td>egress</td></tr> ");
                out_sharding(&mut s, 1, &sharding);
            }
            NodeType::Sharder(ref sharder) => {
                s.push_str(&format!("<tr><td>{}</td></tr> ", addr));
                s.push_str(&format!(
                    "<tr><td>shard by {}</td></tr> ",
                    self.columns[sharder.sharded_by()].name
                ));
                out_sharding(&mut s, 1, &sharding);
            }
            NodeType::Reader(ref r) => {
                let key = match r.index() {
                    None => String::from("none"),
                    Some(index) => format!("{:?}({:?})", index.index_type, index.columns),
                };
                let span = out_header(&mut s, &addr, materialized, &key_count, &node_size);
                s.push_str(&format!(
                    "<tr><td colspan=\"{}\">reader — ⚷: {}</td></tr> ",
                    span, key,
                ));
                out_sharding(&mut s, span, &sharding);
            }
            NodeType::Internal(ref i) => {
                s.push_str(&format!(
                    "<tr><td>{} / {}</td> <td>{}</td> ",
                    addr,
                    encode_text(&self.name().display_unquoted().to_string()),
                    encode_text(&i.description()),
                ));

                let mut span = 2;
                if !materialized.is_empty() {
                    s.push_str(&format!("<td>{}</td> ", materialized));
                    span += 1;
                }
                if !key_count.is_empty() {
                    s.push_str(&format!("<td>{}</td> ", key_count));
                    span += 1;
                }
                s.push_str("</tr> ");

                out_columns(&mut s, span, self);
                out_sharding(&mut s, span, &sharding);
            }
        };
        s.push_str("</table> >]");

        s
    }
}
