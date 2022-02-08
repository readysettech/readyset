//! A word of warning.
//! Sadly, since today we don't have a centralized MIR graph (we should strive to try to use a
//! petgraph::Graph or similar solution), the MIR nodes all have references to their parent and
//! child nodes. This affects the [`SqlIncorporator`], because the [`MirQuery`] structs hold those
//! references. This means that automatic serde implementation (by means of using
//! `#[derive(Serialize, Deserialize)]` is not possible, because we would run into a stack overflow
//! when the serializing algorithm starts resolving the references, getting into a never ending loop
//! (parent -> child -> parent -> child...). Because of that, we are forced to implement our own
//! serialization and deserialization.
//!
//! Unlike the serde implementation of [`SqlToMirConverter`], the structure is maintained, but only
//! the `mir_query` and `base_mir_queries` where slightly changed, by replaced the [`MirQuery`] type
//! with [`SerializableMirQuery`].
//!
//! Since serializing here means changing the [`MirNodeRef`] into just the identifiers of the
//! [`MirNode`]s, these changes require that we have access to the real [`MirNodeRef`]s upon
//! deserialization. Those should be available through the [`SqlToMirConverter::nodes()`] method.
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use mir::query::MirQuery;
use mir::MirNodeRef;
use nom_sql::CreateTableStatement;
use petgraph::graph::NodeIndex;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};

use crate::sql::mir::serde::MirNodeId;
use crate::sql::mir::SqlToMirConverter;
use crate::sql::query_graph::QueryGraph;
use crate::sql::{Config, SqlIncorporator};

#[derive(Serialize, Deserialize)]
struct SerializableMirQuery {
    name: String,
    roots: Vec<MirNodeId>,
    leaf: MirNodeId,
}

impl Serialize for SqlIncorporator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        fn serialize_mir_query(mir_query: &MirQuery) -> SerializableMirQuery {
            let roots = mir_query
                .roots
                .iter()
                .map(|r| {
                    let node = r.borrow();
                    MirNodeId {
                        name: node.name.clone(),
                        version: node.from_version,
                    }
                })
                .collect();
            let leaf_node = mir_query.leaf.borrow();
            let leaf = MirNodeId {
                name: leaf_node.name.clone(),
                version: leaf_node.from_version,
            };
            SerializableMirQuery {
                name: mir_query.name.clone(),
                roots,
                leaf,
            }
        }
        let mut base_mir_queries = HashMap::new();
        let mut mir_queries = HashMap::new();
        for (name, query) in self.base_mir_queries.iter() {
            base_mir_queries.insert(name.clone(), serialize_mir_query(query));
        }
        for (qg_hash, query) in self.mir_queries.iter() {
            // We have to transform the map key into a string, since the serializers might panic
            // if the key is not a string (i.e., the serde::json).
            mir_queries.insert(qg_hash.to_string(), serialize_mir_query(query));
        }
        let mut state = serializer.serialize_struct("SqlIncorporator", 10)?;
        state.serialize_field("mir_converter", &self.mir_converter)?;
        state.serialize_field("leaf_addresses", &self.leaf_addresses)?;
        state.serialize_field("named_queries", &self.named_queries)?;
        state.serialize_field("query_graphs", &self.query_graphs)?;
        state.serialize_field("base_mir_queries", &base_mir_queries)?;
        state.serialize_field("mir_queries", &mir_queries)?;
        state.serialize_field("num_queries", &self.num_queries)?;
        state.serialize_field("base_schemas", &self.base_schemas)?;
        state.serialize_field("view_schemas", &self.view_schemas)?;
        state.serialize_field("config", &self.config)?;
        state.end()
    }
}
impl<'de> serde::Deserialize<'de> for SqlIncorporator {
    fn deserialize<D>(deserializer: D) -> Result<SqlIncorporator, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        enum Field {
            MirConverter,
            LeafAddresses,
            NamedQueries,
            QueryGraphs,
            BaseMirQueries,
            MirQueries,
            NumQueries,
            BaseSchemas,
            ViewSchemas,
            Config,
        }
        struct FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for FieldVisitor {
            type Value = Field;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`mir_converter`, `leaf_addresses`, `named_queries`, `query_graphs`, `base_mir_queries`, `mir_queries`, `num_queries`, `base_schemas`, `view_schemas`, `schema_version` or `config`")
            }
            fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match val {
                    "mir_converter" => Ok(Field::MirConverter),
                    "leaf_addresses" => Ok(Field::LeafAddresses),
                    "named_queries" => Ok(Field::NamedQueries),
                    "query_graphs" => Ok(Field::QueryGraphs),
                    "base_mir_queries" => Ok(Field::BaseMirQueries),
                    "mir_queries" => Ok(Field::MirQueries),
                    "num_queries" => Ok(Field::NumQueries),
                    "base_schemas" => Ok(Field::BaseSchemas),
                    "view_schemas" => Ok(Field::ViewSchemas),
                    "config" => Ok(Field::Config),
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
        struct SqlIncorporatorVisitor;
        impl<'de> Visitor<'de> for SqlIncorporatorVisitor {
            type Value = SqlIncorporator;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("struct SqlToMirConverter")
            }
            fn visit_seq<V>(self, mut seq: V) -> Result<SqlIncorporator, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let mir_converter = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let leaf_addresses = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let named_queries = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let query_graphs = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;
                let base_mir_queries = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(4, &self))?;
                let mir_queries = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(5, &self))?;
                let num_queries = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(6, &self))?;
                let base_schemas = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(7, &self))?;
                let view_schemas = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(8, &self))?;
                let config = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(9, &self))?;
                deserialize_into_sql_incorporator::<V::Error>(
                    mir_converter,
                    leaf_addresses,
                    named_queries,
                    query_graphs,
                    base_mir_queries,
                    mir_queries,
                    num_queries,
                    base_schemas,
                    view_schemas,
                    config,
                )
            }
            fn visit_map<V>(self, mut map: V) -> Result<SqlIncorporator, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut mir_converter = None;
                let mut leaf_addresses = None;
                let mut named_queries = None;
                let mut query_graphs = None;
                let mut base_mir_queries = None;
                let mut mir_queries = None;
                let mut num_queries = None;
                let mut base_schemas = None;
                let mut view_schemas = None;
                let mut config = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::MirConverter => {
                            if mir_converter.is_some() {
                                return Err(serde::de::Error::duplicate_field("mir_converter"));
                            }
                            mir_converter = Some(map.next_value()?);
                        }
                        Field::LeafAddresses => {
                            if leaf_addresses.is_some() {
                                return Err(serde::de::Error::duplicate_field("leaf_addresses"));
                            }
                            leaf_addresses = Some(map.next_value()?);
                        }
                        Field::NamedQueries => {
                            if named_queries.is_some() {
                                return Err(serde::de::Error::duplicate_field("named_queries"));
                            }
                            named_queries = Some(map.next_value()?);
                        }
                        Field::QueryGraphs => {
                            if query_graphs.is_some() {
                                return Err(serde::de::Error::duplicate_field("query_graphs"));
                            }
                            query_graphs = Some(map.next_value()?);
                        }
                        Field::BaseMirQueries => {
                            if base_mir_queries.is_some() {
                                return Err(serde::de::Error::duplicate_field("base_mir_queries"));
                            }
                            base_mir_queries = Some(map.next_value()?);
                        }
                        Field::MirQueries => {
                            if mir_queries.is_some() {
                                return Err(serde::de::Error::duplicate_field("mir_queries"));
                            }
                            mir_queries = Some(map.next_value()?);
                        }
                        Field::NumQueries => {
                            if num_queries.is_some() {
                                return Err(serde::de::Error::duplicate_field("num_queries"));
                            }
                            num_queries = Some(map.next_value()?);
                        }
                        Field::BaseSchemas => {
                            if base_schemas.is_some() {
                                return Err(serde::de::Error::duplicate_field("base_schemas"));
                            }
                            base_schemas = Some(map.next_value()?);
                        }
                        Field::ViewSchemas => {
                            if view_schemas.is_some() {
                                return Err(serde::de::Error::duplicate_field("view_schemas"));
                            }
                            view_schemas = Some(map.next_value()?);
                        }
                        Field::Config => {
                            if config.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config = Some(map.next_value()?);
                        }
                    }
                }
                let mir_converter = mir_converter
                    .ok_or_else(|| serde::de::Error::missing_field("mir_converter"))?;
                let leaf_addresses = leaf_addresses
                    .ok_or_else(|| serde::de::Error::missing_field("leaf_addresses"))?;
                let named_queries = named_queries
                    .ok_or_else(|| serde::de::Error::missing_field("named_queries"))?;
                let query_graphs =
                    query_graphs.ok_or_else(|| serde::de::Error::missing_field("query_graphs"))?;
                let base_mir_queries = base_mir_queries
                    .ok_or_else(|| serde::de::Error::missing_field("base_mir_queries"))?;
                let mir_queries =
                    mir_queries.ok_or_else(|| serde::de::Error::missing_field("mir_queries"))?;
                let num_queries =
                    num_queries.ok_or_else(|| serde::de::Error::missing_field("num_queries"))?;
                let base_schemas =
                    base_schemas.ok_or_else(|| serde::de::Error::missing_field("base_schemas"))?;
                let view_schemas =
                    view_schemas.ok_or_else(|| serde::de::Error::missing_field("view_schemas"))?;
                let config = config.ok_or_else(|| serde::de::Error::missing_field("config"))?;
                deserialize_into_sql_incorporator::<V::Error>(
                    mir_converter,
                    leaf_addresses,
                    named_queries,
                    query_graphs,
                    base_mir_queries,
                    mir_queries,
                    num_queries,
                    base_schemas,
                    view_schemas,
                    config,
                )
            }
        }
        const FIELDS: &[&str] = &[
            "mir_converter",
            "leaf_addresses",
            "named_queries",
            "query_graphs",
            "base_mir_queries",
            "mir_queries",
            "num_queries",
            "base_schemas",
            "view_schemas",
            "config",
        ];
        deserializer.deserialize_struct("SqlIncorporator", FIELDS, SqlIncorporatorVisitor)
    }
}

fn extract_mir_query_elements<E>(
    mut mir_query: SerializableMirQuery,
    nodes: &HashMap<(String, usize), MirNodeRef>,
) -> Result<(String, Vec<MirNodeRef>, MirNodeRef), E>
where
    E: serde::de::Error,
{
    let mut roots = Vec::new();
    for root_id in mir_query.roots.drain(0..) {
        roots.push(
            nodes
                .get(&(root_id.name, root_id.version))
                .ok_or_else(|| E::custom("missing root node"))?
                .clone(),
        )
    }
    let leaf_id = mir_query.leaf;
    let leaf = nodes
        .get(&(leaf_id.name, leaf_id.version))
        .ok_or_else(|| E::custom("missing leaf node"))?
        .clone();
    Ok((mir_query.name, roots, leaf))
}

fn deserialize_into_sql_incorporator<E>(
    mir_converter: SqlToMirConverter,
    leaf_addresses: HashMap<String, NodeIndex>,
    named_queries: HashMap<String, u64>,
    query_graphs: HashMap<u64, QueryGraph>,
    mut base_mir_queries: HashMap<String, SerializableMirQuery>,
    mut mir_queries: HashMap<String, SerializableMirQuery>,
    num_queries: usize,
    base_schemas: HashMap<String, CreateTableStatement>,
    view_schemas: HashMap<String, Vec<String>>,
    config: Config,
) -> Result<SqlIncorporator, E>
where
    E: serde::de::Error,
{
    let nodes = mir_converter.nodes();
    let mut final_base_mir_queries = HashMap::new();
    for (name, mir_query) in base_mir_queries.drain() {
        let (query_name, roots, leaf) = extract_mir_query_elements(mir_query, nodes)?;
        final_base_mir_queries.insert(
            name,
            MirQuery {
                name: query_name,
                roots,
                leaf,
            },
        );
    }
    let mut final_mir_queries = HashMap::new();
    for (name, mir_query) in mir_queries.drain() {
        let (query_name, roots, leaf) = extract_mir_query_elements(mir_query, nodes)?;
        final_mir_queries.insert(
            u64::from_str(name.as_str()).unwrap(),
            MirQuery {
                name: query_name,
                roots,
                leaf,
            },
        );
    }
    Ok(SqlIncorporator {
        mir_converter,
        leaf_addresses,
        named_queries,
        query_graphs,
        base_mir_queries: final_base_mir_queries,
        mir_queries: final_mir_queries,
        num_queries,
        base_schemas,
        view_schemas,
        config,
    })
}
