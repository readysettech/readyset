//! A word of warning.
//! Sadly, since today we don't have centralized MIR graphs (we should strive to try to use a
//! petgraph::Graph or similar solution in the future), the MIR nodes all have references to their
//! parent and child nodes.
//! This affects the [`SqlIncorporator`], which holds the [`MirQuery`] structs, with each of them
//! representing a MIR graph.
//! This means that automatic serde implementation by means of using `#[derive(Serialize,
//! Deserialize)]` is not possible, because we would run into a stack overflow when the serializing
//! algorithm starts resolving the references, getting into a never ending loop (parent -> child ->
//! parent -> child...). Because of that, we are forced to implement our own serialization and
//! deserialization.
//!
//! The [`MirQuery`] structs are converted to [`SerializableMirQuery`] structs, which transforms
//! all the [`MirNodeRef`] into their corresponding [`MirNode`], and then encodes the relationships
//! between those nodes using a [`petgraph::Graph`].
//!
//! A similar transformation occurs for [`SqlToMirConverter`].
//!
//! *NOTE*: This is only possible because all [`MirQuery`] nodes are self-contained within the
//! [`MirQuery`] (meaning, they don't have any references to a different node from a different
//! [`MirQuery`]). The only nodes that may reference a different [`MirQuery`] node are those which
//! use [`MirNodeInner::Reuse`]. *This means that serialization won't work if we are using the reuse
//! feature (!)* TODO(fran): Refactor MIR such that we don't need any of this and reuse can be used
//! again.
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::hash::Hash;
use std::rc::Rc;

use mir::node::MirNode;
use mir::query::MirQuery;
use mir::MirNodeRef;
use nom_sql::{CreateTableStatement, SqlIdentifier};
use noria_errors::{internal_err, invariant, ReadySetError, ReadySetResult};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize, Serializer};

use crate::sql::mir::serde::SerializableSqlToMirConverter;
use crate::sql::mir::SqlToMirConverter;
use crate::sql::query_graph::QueryGraph;
use crate::sql::{Config, SqlIncorporator};

/// A helper structure that holds all the information from a [`MirQuery`],
/// in a serializable form.
#[derive(Serialize, Deserialize)]
struct SerializableMirQuery {
    /// The name field of the [`MirQuery`]
    name: SqlIdentifier,
    /// A graph representing the MIR graph of the [`MirQuery`], but centralized.
    graph: petgraph::Graph<MirNode, ()>,
    /// The root field of the [`MirQuery`], but holding the references to the root nodes in the
    /// graph instead of the references to the node.
    roots: Vec<NodeIndex>,
    /// The leaf field of the [`MirQuery`], but holding the reference to the leaf node in the graph
    /// instead of the reference to the node.
    leaf: NodeIndex,
}

impl<'a> TryFrom<&'a MirQuery> for SerializableMirQuery {
    type Error = ReadySetError;

    fn try_from(mir_query: &'a MirQuery) -> Result<Self, Self::Error> {
        let mut graph = petgraph::Graph::new();
        let mut stack = Vec::new();
        let mut nodes = HashMap::new();
        let mut roots = Vec::new();

        // Find out which are the roots and add them to the petgraph::Graph (and to the stack,
        // which we will use to traverse the MIR graph).
        for root_ref in mir_query.roots.iter() {
            let root = root_ref.borrow();
            invariant!(
                root.ancestors.is_empty(),
                "Mir query {} has a root node {}, which has ancestors",
                mir_query.name,
                root.versioned_name()
            );
            // Add the node to the petgraph::Graph, and get an id.
            let node_id = graph.add_node(root.clone_without_relations());
            // Keep track of the node index assigned to the node.
            nodes.insert((root.name.clone(), root.from_version), node_id);
            // Add the node index for the root to the roots field
            roots.push(node_id);
            // Push the node reference to the stack, to traverse the MIR graph.
            stack.push(root_ref.clone());
        }

        while let Some(node_ref) = stack.pop() {
            let node = node_ref.borrow();
            let node_id = *nodes
                .get(&(node.name.clone(), node.from_version))
                .ok_or_else(|| internal_err("node not found"))?;
            for child_ref in node.children.iter() {
                let child = child_ref.borrow();
                // Create a new node for the child.
                let child_id = graph.add_node(child.clone_without_relations());
                // Keep track of the node index assigned to the node.
                nodes.insert((child.name.clone(), child.from_version), child_id);
                // Add an edge between the parent and the child.
                graph.add_edge(node_id, child_id, ());
                // Add the child reference to the stack.
                stack.push(child_ref.clone());
            }
        }

        let leaf_node_id = {
            let leaf = mir_query.leaf.borrow();
            nodes
                .get(&(leaf.name.clone(), leaf.from_version))
                .ok_or_else(|| internal_err("leaf node not found"))?
        };

        Ok(SerializableMirQuery {
            name: mir_query.name.clone(),
            leaf: *leaf_node_id,
            roots,
            graph,
        })
    }
}

/// A helper structure that holds all the information from a [`SqlIncorporator`],
/// in a serializable form.
#[derive(Serialize, Deserialize)]
struct SerializableSqlIncorporator {
    mir_converter: SerializableSqlToMirConverter,
    leaf_addresses: HashMap<SqlIdentifier, NodeIndex>,

    named_queries: HashMap<SqlIdentifier, u64>,
    query_graphs: HashMap<u64, QueryGraph>,
    base_mir_queries: HashMap<SqlIdentifier, SerializableMirQuery>,
    mir_queries: HashMap<u64, SerializableMirQuery>,
    num_queries: usize,

    base_schemas: HashMap<SqlIdentifier, CreateTableStatement>,
    view_schemas: HashMap<SqlIdentifier, Vec<SqlIdentifier>>,

    pub(crate) config: Config,
}

impl<'a> TryFrom<&'a SqlIncorporator> for SerializableSqlIncorporator {
    type Error = ReadySetError;

    fn try_from(inc: &'a SqlIncorporator) -> Result<Self, Self::Error> {
        let mir_converter = (&inc.mir_converter).try_into()?;

        let base_mir_queries = inc
            .base_mir_queries
            .iter()
            .map(|(name, mir_query)| Ok((name.clone(), SerializableMirQuery::try_from(mir_query)?)))
            .collect::<ReadySetResult<HashMap<SqlIdentifier, SerializableMirQuery>>>()?;
        let mir_queries = inc
            .mir_queries
            .iter()
            .map(|(id, mir_query)| Ok((*id, SerializableMirQuery::try_from(mir_query)?)))
            .collect::<ReadySetResult<HashMap<u64, SerializableMirQuery>>>()?;

        Ok(SerializableSqlIncorporator {
            mir_converter,
            leaf_addresses: inc.leaf_addresses.clone(),
            named_queries: inc.named_queries.clone(),
            query_graphs: inc.query_graphs.clone(),
            base_mir_queries,
            mir_queries,
            num_queries: inc.num_queries,
            base_schemas: inc.base_schemas.clone(),
            view_schemas: inc.view_schemas.clone(),
            config: inc.config.clone(),
        })
    }
}

impl Serialize for SqlIncorporator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SerializableSqlIncorporator::try_from(self)
            .map_err(|e| {
                serde::ser::Error::custom(format!("Could not serialize SqlIncorporator: {}", e))
            })?
            .serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for SqlIncorporator {
    fn deserialize<D>(deserializer: D) -> Result<SqlIncorporator, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        SerializableSqlIncorporator::deserialize(deserializer)
            .and_then(deserialize_into_sql_incorporator)
    }
}

/// Deserializes the given MIR queries.
/// The `all_nodes` params is used to gather all the references to the nodes, as this will be
/// used to deserialize the [`SqlToMirConverter`] field later.
fn deserialize_mir_queries<T, E>(
    mir_queries: HashMap<T, SerializableMirQuery>,
    all_nodes: &mut HashMap<(SqlIdentifier, usize), MirNodeRef>,
) -> Result<HashMap<T, MirQuery>, E>
where
    T: Eq + Hash + Clone,
    E: serde::de::Error,
{
    // Restore the MIR queries first, which have the actual node references.
    let mut result_mir_queries = HashMap::new();
    // Starting from the base MIR queries.
    for (name, mir_query) in mir_queries.into_iter() {
        let mut roots = Vec::new();
        let mut stack = Vec::new();
        for root in mir_query.roots.into_iter() {
            // Create a MirNodeRef for each root.
            let mir_node = Rc::new(RefCell::new(
                mir_query.graph[root].clone_without_relations(),
            ));
            // Keep track of the root nodes.
            roots.push(mir_node.clone());
            // Add the root nodes to the stack, to traverse the graph.
            // We keep the node so we can modify it later (add ancestors/children), and also
            // the index so we can find it in the graph.
            stack.push((mir_node, root));
        }

        let mut leaf_node = None;
        // Traverse the MIR graph
        while let Some((mir_node, node_idx)) = stack.pop() {
            let node_id = {
                let node = mir_node.borrow();
                (node.name.clone(), node.from_version)
            };
            if node_idx == mir_query.leaf {
                leaf_node = Some(mir_node.clone());
            }
            // Add the node to the `all_nodes` map.
            all_nodes.insert(node_id, mir_node.clone());
            for child_idx in mir_query
                .graph
                .neighbors_directed(node_idx, petgraph::Direction::Outgoing)
            {
                let child = mir_query.graph[child_idx].clone_without_relations();
                let child_mir_node = Rc::new(RefCell::new(child));
                stack.push((child_mir_node.clone(), child_idx));
                // Stablish the nodes relationship.
                mir_node.borrow_mut().add_child(child_mir_node.clone());
                child_mir_node.borrow_mut().add_ancestor(mir_node.clone());
            }
        }

        let mir_query = MirQuery {
            name: mir_query.name.clone(),
            roots,
            leaf: leaf_node.ok_or_else(|| E::custom("leaf node not found"))?,
        };
        result_mir_queries.insert(name.clone(), mir_query);
    }
    Ok(result_mir_queries)
}

/// Deserializes the [`SqlToMirConverter`].
/// The `nodes` param must have all known node references, and will be filtered to keep the ones
/// used by the original [`SqlToMirConverter`].
fn deserialize_mir_converter<E>(
    serialized_mir_converter: SerializableSqlToMirConverter,
    mut nodes: HashMap<(SqlIdentifier, usize), MirNodeRef>,
) -> Result<SqlToMirConverter, E> {
    nodes.retain(|node_id, _| serialized_mir_converter.nodes.contains(node_id));
    Ok(SqlToMirConverter {
        config: serialized_mir_converter.config,
        base_schemas: serialized_mir_converter.base_schemas,
        current: serialized_mir_converter.current,
        nodes,
        schema_version: serialized_mir_converter.schema_version,
    })
}

/// Deserialize the [`SqlIncorporator`].
fn deserialize_into_sql_incorporator<E>(
    serialized_inc: SerializableSqlIncorporator,
) -> Result<SqlIncorporator, E>
where
    E: serde::de::Error,
{
    let mut all_nodes = HashMap::new();

    let base_mir_queries =
        deserialize_mir_queries(serialized_inc.base_mir_queries, &mut all_nodes)?;
    let mir_queries = deserialize_mir_queries(serialized_inc.mir_queries, &mut all_nodes)?;

    let mir_converter = deserialize_mir_converter(serialized_inc.mir_converter, all_nodes)?;

    Ok(SqlIncorporator {
        mir_converter,
        leaf_addresses: serialized_inc.leaf_addresses,
        named_queries: serialized_inc.named_queries,
        query_graphs: serialized_inc.query_graphs,
        base_mir_queries,
        mir_queries,
        num_queries: serialized_inc.num_queries,
        base_schemas: serialized_inc.base_schemas,
        view_schemas: serialized_inc.view_schemas,
        config: serialized_inc.config,
    })
}
