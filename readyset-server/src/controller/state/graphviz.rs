use std::collections::HashMap;
use std::fmt::{self, Display, Write};

use dataflow::prelude::{Graph, NodeIndex};
use dataflow::{DomainIndex, NodeMap};
use dataflow_expression::{PostLookup, PostLookupAggregateFunction};
use petgraph::Direction;
use readyset_client::debug::info::NodeSize;

use crate::controller::migrate::materialization::Materializations;

pub(in crate::controller) struct Graphviz<'a> {
    pub graph: &'a Graph,
    pub node_sizes: Option<HashMap<NodeIndex, NodeSize>>,
    pub materializations: &'a Materializations,
    pub domain_nodes: Option<&'a HashMap<DomainIndex, NodeMap<NodeIndex>>>,
    pub reachable_from: Option<(NodeIndex, Direction)>,
}

macro_rules! out {
    ($f:ident, $indent:literal, $fmt:literal $(, $arg:expr)*) => {
        #[allow(clippy::reversed_empty_ranges)]
        for _ in 0..$indent {
            $f.write_str("    ")?;
        }
        writeln!($f, $fmt, $($arg),*)?;
    }
}

/// Builds a graphviz [dot][] representation of the graph
///
/// For more information, see <http://docs/debugging.html#graphviz>
///
/// [dot]: https://graphviz.org/doc/info/lang.html
impl Display for Graphviz<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let node_sizes = self.node_sizes.clone().unwrap_or_default();

        // header.
        out!(f, 0, "digraph {{");

        // global formatting.
        out!(f, 1, "fontsize=10");
        out!(f, 1, "node [shape=none, fontsize=10]");

        let nodes = if let Some((ni, dir)) = self.reachable_from {
            super::reachable_from(self.graph, ni, dir)
        } else {
            self.graph.node_indices().collect()
        };

        let domain_for_node = self
            .domain_nodes
            .iter()
            .flat_map(|m| m.iter())
            .flat_map(|(di, nodes)| nodes.iter().map(|(_, ni)| (*ni, *di)))
            .collect::<HashMap<_, _>>();
        let mut domains_to_nodes = HashMap::new();
        for ni in &nodes {
            let domain = domain_for_node.get(ni).copied();
            domains_to_nodes
                .entry(domain)
                .or_insert_with(Vec::new)
                .push(*ni);
        }

        // node descriptions.
        for (domain, nodes) in domains_to_nodes {
            if let Some(domain) = domain {
                out!(f, 1, "subgraph cluster_d{domain} {{");
                out!(f, 2, "label = \"Domain {domain}\";");
            }
            for index in nodes {
                let node = &self.graph[index];
                if node.is_graph_root() {
                    continue;
                }
                let materialization_status = self.materializations.get_status(index, node);
                out!(
                    f,
                    2,
                    "n{} {}",
                    index.index(),
                    node.describe(index, &node_sizes, materialization_status)
                );
            }
            if domain.is_some() {
                out!(f, 1, "}}");
            }
        }

        // Reader processing pseudo-nodes: rendered outside domain subgraphs so they
        // don't clutter the dataflow layout.  Each pseudo-node is a dashed "note"
        // shape linked to its reader with a gray dashed edge (no arrowhead).
        for &index in &nodes {
            let node = &self.graph[index];
            if let Some(reader) = node.as_reader() {
                let PostLookup {
                    ref order_by,
                    limit,
                    ref aggregates,
                    // Column projection and default rows are omitted from the
                    // pseudo-node as they are less interesting for dataflow debugging.
                    returned_cols: _,
                    default_row: _,
                } = reader.reader_processing().post_processing;
                let has_aggregates = aggregates
                    .as_ref()
                    .is_some_and(|a| !a.group_by.is_empty() || !a.aggregates.is_empty());
                if order_by.is_none() && limit.is_none() && !has_aggregates {
                    continue;
                }
                let id = index.index();
                let mut label = String::from("<b>ReaderProcessing</b>");
                write_reader_processing(
                    &mut label,
                    order_by.as_deref(),
                    limit,
                    aggregates.as_ref(),
                )
                .expect("write to String is infallible");
                out!(
                    f,
                    1,
                    "n{id}_rp [label=< {label} >, shape=note, fontsize=9, \
                     fillcolor=\"#f0f0f0\", style=\"dashed,filled\"]"
                );
                out!(
                    f,
                    1,
                    "n{id} -> n{id}_rp [ style=dashed, arrowhead=none, color=gray ]"
                );
            }
        }

        // edges.
        for edge in self.graph.raw_edges() {
            if self.graph[edge.source()].is_graph_root() {
                continue;
            }
            if !(nodes.contains(&edge.source()) && nodes.contains(&edge.target())) {
                continue;
            }

            out!(
                f,
                1,
                "n{} -> n{} [ {} ]",
                edge.source().index(),
                edge.target().index(),
                if self.graph[edge.source()].is_egress() {
                    "penwidth=4"
                } else {
                    ""
                }
            );
        }

        // replay paths
        for (src, paths) in &self.materializations.paths {
            for (tag, (index, dsts)) in paths {
                let s = "&nbsp;&nbsp;";
                let dst = dsts[0]; // ignore downward path; obvious
                let t = format!("{s}tag: {}{s}\n{s}cols: {:?}{s}", tag, index.columns);
                out!(
                    f,
                    1,
                    "n{} -> n{} [ label=\"{}\", constraint=false, color=IndianRed ];",
                    src.index(),
                    dst.index(),
                    t
                );
            }
        }

        // footer.
        out!(f, 0, "}}");

        Ok(())
    }
}

/// Write post-lookup processing info as HTML content for a graphviz pseudo-node label.
fn write_reader_processing(
    w: &mut impl Write,
    order_by: Option<
        &[(
            usize,
            readyset_sql::ast::OrderType,
            readyset_sql::ast::NullOrder,
        )],
    >,
    limit: Option<usize>,
    aggregates: Option<&dataflow_expression::PostLookupAggregates>,
) -> fmt::Result {
    if let Some(order_by) = order_by {
        if !order_by.is_empty() {
            write!(w, "<br/>ORDER BY ")?;
            for (i, (col, order, null_order)) in order_by.iter().enumerate() {
                if i > 0 {
                    w.write_str(", ")?;
                }
                write!(w, "[{col}] {order} {null_order}")?;
            }
        }
    }

    if let Some(limit) = limit {
        write!(w, "<br/>LIMIT {limit}")?;
    }

    if let Some(aggs) = aggregates {
        if !aggs.group_by.is_empty() {
            write!(w, "<br/>GROUP BY ")?;
            for (i, col) in aggs.group_by.iter().enumerate() {
                if i > 0 {
                    w.write_str(", ")?;
                }
                write!(w, "[{col}]")?;
            }
        }
        for agg in &aggs.aggregates {
            // SQL-standard names for graphviz readability (vs. abbreviated
            // names in PostLookupAggregate::description()).
            let name = match &agg.function {
                PostLookupAggregateFunction::Sum => "SUM",
                PostLookupAggregateFunction::Max => "MAX",
                PostLookupAggregateFunction::Min => "MIN",
                PostLookupAggregateFunction::GroupConcat { .. } => "GROUP_CONCAT",
                PostLookupAggregateFunction::ArrayAgg { .. } => "ARRAY_AGG",
                PostLookupAggregateFunction::StringAgg { .. } => "STRING_AGG",
                PostLookupAggregateFunction::JsonObjectAgg { .. } => "JSON_OBJECT_AGG",
            };
            write!(w, "<br/>{name}([{}])", agg.column)?;
        }
    }

    Ok(())
}
