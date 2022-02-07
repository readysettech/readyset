//! A word of warning.
//! Sadly, since today we don't have a centralized MIR graph (we should strive to try to use a
//! petgraph::Graph or similar solution), the MIR nodes all have references to their parent and
//! child nodes.
//! This means that automatic serde implementation (by means of using `#[derive(Serialize,
//! Deserialize)]` is not possible, because we would run into a stack overflow when the serializing
//! algorithm starts resolving the references, getting into a never ending loop (parent -> child ->
//! parent -> child...). Because of that, we are forced to implement our own serialization and
//! deserialization.
//!
//! First and foremost, we will replace those [`MirNodeRef`]s with the identifiers of each node,
//! namely the [`MirNode::name`] and [`MirNode::from_version`] fields.
//! Secondly, we will skip serdes for the [`MirNode::children`] and [`MirNode::ancestors`] fields,
//! which will have to be added afterwards (since, again, they are references).
//! A note here: we don't do the same for the [`MirNodeRef`] that might be present in some
//! [`MirNodeInner`], which derives the [`Serialize`] and [`Deserialize`] implementations.
//! We let those fields be serialized as [`MirNode`]s, even if that's not very efficient (we can
//! improve this later). We are guaranteed that we won't be entering an infinite loop, since we are
//! skipping the serdes of the `children` and `ancestors` nodes in the [`MirNode`] fields.
//! This relies heavily on the fact that we SHOULD NEVER EVER HAVE circular references in the
//! [`MirNodeInner`] node reference fields.
//!
//! Then, we will have the following foreign fields in the serialized structure:
//! - all_nodes: This is a map (conceptually at least, since the keys are not strings and we have to
//! serialize it as a vector in the end), which has the tuple (name, version) as keys (the ids of
//! the nodes), and the [`MirNode`] (with empty children and ancestors fields) as the values.
//! This contains ALL the nodes, meaning we have to traverse the whole graph, starting from the ones
//! defined in the [`SqlToMirConverter::nodes`] fields. This will be our source of truth upon
//! deserialization.
//! - children: Another (conceptual) map, again with the tuple (name, version) as keys, and the list
//!   of
//! children ids as values.
//!
//! Also, the [`SqlToMirConverter::nodes`] field gets serialized as a vector of tuples (name,
//! version). For deserialization, read the comments in the [`Deserialize`] impl.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use mir::node::node_inner::MirNodeInner;
use mir::node::MirNode;
use nom_sql::ColumnSpecification;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};

use crate::sql::mir::{Config, SqlToMirConverter};

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash)]
pub(in crate::controller::sql) struct MirNodeId {
    pub(in crate::controller::sql) name: String,
    pub(in crate::controller::sql) version: usize,
}

impl Serialize for SqlToMirConverter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        /// Traverse the whole graph and get all the nodes and a list of node -> children.
        fn extract_nodes(
            stack: &mut Vec<MirNode>,
        ) -> (
            HashMap<MirNodeId, MirNode>,
            HashMap<MirNodeId, Vec<MirNodeId>>,
        ) {
            let mut all_nodes: HashMap<MirNodeId, MirNode> = HashMap::new();
            let mut children: HashMap<MirNodeId, Vec<MirNodeId>> = HashMap::new();
            while let Some(node) = stack.pop() {
                if all_nodes.contains_key(&MirNodeId {
                    name: node.name.clone(),
                    version: node.from_version,
                }) {
                    continue;
                }
                all_nodes.insert(
                    MirNodeId {
                        name: node.name.clone(),
                        version: node.from_version,
                    },
                    node.clone_without_relations(),
                );
                for child in node.children.iter() {
                    let child = child.borrow();
                    all_nodes.insert(
                        MirNodeId {
                            name: child.name.clone(),
                            version: child.from_version,
                        },
                        child.clone_without_relations(),
                    );
                    children
                        .entry(MirNodeId {
                            name: node.name.clone(),
                            version: node.from_version,
                        })
                        .or_insert_with(|| Vec::new())
                        .push(MirNodeId {
                            name: child.name.clone(),
                            version: child.from_version,
                        });
                }
                stack.extend(
                    node.children
                        .iter()
                        .map(|n| n.borrow().clone_without_relations()),
                );
                stack.extend(
                    node.ancestors
                        .iter()
                        .map(|n| n.upgrade().unwrap().borrow().clone_without_relations()),
                );
            }
            (all_nodes, children)
        }
        let mut nodes: Vec<(String, usize)> = Vec::new();
        let mut stack: Vec<MirNode> = Vec::new();
        for ((name, version), node) in self.nodes.iter() {
            let node = node.borrow();
            nodes.push((name.clone(), *version));
            stack.push(node.clone_without_relations());
        }
        let (mut all_nodes, mut children) = extract_nodes(&mut stack);
        let mut state = serializer.serialize_struct("SqlToMirConverter", 7)?;
        state.serialize_field("config", &self.config)?;
        state.serialize_field("base_schemas", &self.base_schemas)?;
        state.serialize_field("current", &self.current)?;
        state.serialize_field("nodes", &nodes)?;
        // Sadly we have to transform the maps into vectors, since the serializers might panic
        // if the key is not a string (i.e., the serde::json).
        state.serialize_field("all_nodes", &all_nodes.drain().collect::<Vec<_>>())?;
        state.serialize_field("children", &children.drain().collect::<Vec<_>>())?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.end()
    }
}
impl<'de> serde::Deserialize<'de> for SqlToMirConverter {
    fn deserialize<D>(deserializer: D) -> Result<SqlToMirConverter, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        enum Field {
            Config,
            BaseSchemas,
            Current,
            Nodes,
            AllNodes,
            Children,
            SchemaVersion,
        }
        struct FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for FieldVisitor {
            type Value = Field;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`config`, `base_schemas`, `current`, `nodes`, `all_nodes`, `children` or `schema_version`")
            }
            fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match val {
                    "config" => Ok(Field::Config),
                    "base_schemas" => Ok(Field::BaseSchemas),
                    "current" => Ok(Field::Current),
                    "nodes" => Ok(Field::Nodes),
                    "all_nodes" => Ok(Field::AllNodes),
                    "children" => Ok(Field::Children),
                    "schema_version" => Ok(Field::SchemaVersion),
                    _ => Err(serde::de::Error::unknown_field(val, FIELDS)),
                }
            }
        }
        impl<'de> serde::Deserialize<'de> for Field {
            #[inline]
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_identifier(FieldVisitor)
            }
        }
        struct SqlToMirConverterVisitor;
        impl<'de> Visitor<'de> for SqlToMirConverterVisitor {
            type Value = SqlToMirConverter;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("struct SqlToMirConverter")
            }
            fn visit_seq<V>(self, mut seq: V) -> Result<SqlToMirConverter, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let config: Config = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let base_schemas: HashMap<String, Vec<(usize, Vec<ColumnSpecification>)>> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let current: HashMap<String, usize> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let nodes: Vec<MirNodeId> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;
                let all_nodes: Vec<(MirNodeId, MirNode)> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(4, &self))?;
                let children: Vec<(MirNodeId, Vec<MirNodeId>)> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(5, &self))?;
                let schema_version: usize = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(6, &self))?;
                deserialize_into_sql_to_mir_converter::<V::Error>(
                    config,
                    base_schemas,
                    current,
                    nodes,
                    all_nodes,
                    children,
                    schema_version,
                )
            }
            fn visit_map<V>(self, mut map: V) -> Result<SqlToMirConverter, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut config = None;
                let mut base_schemas = None;
                let mut current = None;
                let mut nodes = None;
                let mut all_nodes = None;
                let mut children = None;
                let mut schema_version = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Config => {
                            if config.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config = Some(map.next_value()?);
                        }
                        Field::BaseSchemas => {
                            if base_schemas.is_some() {
                                return Err(serde::de::Error::duplicate_field("base_schemas"));
                            }
                            base_schemas = Some(map.next_value()?);
                        }
                        Field::Current => {
                            if current.is_some() {
                                return Err(serde::de::Error::duplicate_field("current"));
                            }
                            current = Some(map.next_value()?);
                        }
                        Field::Nodes => {
                            if nodes.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodes"));
                            }
                            nodes = Some(map.next_value()?);
                        }
                        Field::AllNodes => {
                            if all_nodes.is_some() {
                                return Err(serde::de::Error::duplicate_field("all_nodes"));
                            }
                            all_nodes = Some(map.next_value()?);
                        }
                        Field::Children => {
                            if children.is_some() {
                                return Err(serde::de::Error::duplicate_field("children"));
                            }
                            children = Some(map.next_value()?);
                        }
                        Field::SchemaVersion => {
                            if schema_version.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema_version"));
                            }
                            schema_version = Some(map.next_value()?);
                        }
                    }
                }
                let config = config.ok_or_else(|| serde::de::Error::missing_field("config"))?;
                let base_schemas =
                    base_schemas.ok_or_else(|| serde::de::Error::missing_field("base_schemas"))?;
                let current = current.ok_or_else(|| serde::de::Error::missing_field("current"))?;
                let nodes = nodes.ok_or_else(|| serde::de::Error::missing_field("nodes"))?;
                let all_nodes =
                    all_nodes.ok_or_else(|| serde::de::Error::missing_field("all_nodes"))?;
                let children =
                    children.ok_or_else(|| serde::de::Error::missing_field("children"))?;
                let schema_version = schema_version
                    .ok_or_else(|| serde::de::Error::missing_field("schema_version"))?;
                deserialize_into_sql_to_mir_converter::<V::Error>(
                    config,
                    base_schemas,
                    current,
                    nodes,
                    all_nodes,
                    children,
                    schema_version,
                )
            }
        }
        const FIELDS: &[&str] = &[
            "config",
            "base_schemas",
            "current",
            "nodes",
            "all_nodes",
            "children",
            "schema_version",
        ];
        deserializer.deserialize_struct("SqlToMirConverter", FIELDS, SqlToMirConverterVisitor)
    }
}
// Now, deserialization. This is pretty straightforward.
// We start by taking the `all_nodes` vector and transforming it back into a map.
// We take this opportunity to transform the [`MirNode`]s into [`MirNodeRef`]s.
// We also reconstruct the [`SqlToMirConverter::nodes`] maps simply by iterating the `nodes` field,
// and using the (name, version) to look in the `all_nodes` map for the references. We copy them
// and that's it, we have our `nodes` field.
// Then we traverse the `children` field and we do something similar: we look in the `all_nodes` for
// the actual parent node and each children node, and we call `add_child` on the parent node and
// `add_ancestor` on the child node, cloning the references.
// Finally, we iterate the `nodes` map and we make sure to correctly set all the references in
// any possible [`MirNodeInner`], by taking the [`MirNodeRef`] that they store, getting the real
// [`MirNodeRef`] from the `all_nodes` using the (name, version), and replacing the references.
/// Takes the serialized fields that describe the [`SqlToMirConverter`] and
/// creates an instance of it (thus effectively deserializing it).
fn deserialize_into_sql_to_mir_converter<E>(
    config: Config,
    base_schemas: HashMap<String, Vec<(usize, Vec<ColumnSpecification>)>>,
    current: HashMap<String, usize>,
    mut nodes: Vec<MirNodeId>,
    mut all_nodes: Vec<(MirNodeId, MirNode)>,
    mut children: Vec<(MirNodeId, Vec<MirNodeId>)>,
    schema_version: usize,
) -> Result<SqlToMirConverter, E>
where
    E: serde::de::Error,
{
    // Convert the `all_nodes` vector back into a map and transforming the [`MirNode`]s into
    // [`MirNodeRef`]s. Check.
    let all_nodes = all_nodes
        .drain(0..)
        .map(|(id, node)| (id, Rc::new(RefCell::new(node))))
        .collect::<HashMap<_, _>>();
    // Convert the `nodes` vector back into a map, getting the [`MirNodeRef`]s from the `all_nodes`
    // map. Check.
    let mut new_nodes = HashMap::new();
    for id in nodes.drain(0..) {
        let node = all_nodes
            .get(&id)
            .ok_or_else(|| E::custom("missing node"))?
            .clone();
        new_nodes.insert((id.name, id.version), node);
    }
    // Setting the `children` and `ancestors` fields of all nodes. Check.
    for (id, children) in children.drain(0..) {
        let parent = all_nodes.get(&id).unwrap();
        for child in children {
            let child = all_nodes.get(&child).unwrap();
            parent.borrow_mut().add_child(child.clone());
            child.borrow_mut().add_ancestor(parent.clone());
        }
    }
    // Setting the [`MirNodeInner`] references to the ones we have in the `all_nodes` map. Check.
    for node in new_nodes.values() {
        let mut node = node.borrow_mut();
        match node.inner {
            MirNodeInner::Base {
                adapted_over: Some(ref mut adaptation),
                ..
            } => {
                let (name, version) = {
                    let inner_node = adaptation.over.borrow();
                    (inner_node.name.clone(), inner_node.from_version)
                };
                // Expect is allowed, since the node we are referencing must be in the nodes map.
                // Otherwise, we can assume there was an error during deserialization.
                #[allow(clippy::expect_used)]
                adaptation.over = all_nodes
                    .get(&MirNodeId { name, version })
                    .expect("Reference to node does not exist")
                    .clone();
            }
            MirNodeInner::Reuse { ref mut node } => {
                let (name, version) = {
                    let inner_node = node.borrow();
                    (inner_node.name.clone(), inner_node.from_version)
                };
                // Expect is allowed, since the node we are referencing must be in the nodes map.
                // Otherwise, we can assume there was an error during deserialization.
                #[allow(clippy::expect_used)]
                *node = all_nodes
                    .get(&MirNodeId { name, version })
                    .expect("Reference to node does not exist")
                    .clone();
            }
            MirNodeInner::Leaf { ref mut node, .. } => {
                let (name, version) = {
                    let inner_node = node.borrow();
                    (inner_node.name.clone(), inner_node.from_version)
                };
                // Expect is allowed, since the node we are referencing must be in the nodes map.
                // Otherwise, we can assume there was an error during deserialization.
                #[allow(clippy::expect_used)]
                *node = all_nodes
                    .get(&MirNodeId { name, version })
                    .expect("Reference to node does not exist")
                    .clone();
            }
            _ => (),
        }
    }
    // Voilà!
    Ok(SqlToMirConverter {
        config,
        base_schemas,
        current,
        nodes: new_nodes,
        schema_version,
    })
}
