use crate::internal::*;
use crate::MaterializationStatus;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

type DomainMap = HashMap<(DomainIndex, usize), (DomainStats, HashMap<NodeIndex, NodeStats>)>;

/// Statistics about a domain.
///
/// All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct DomainStats {
    /// Total wall-clock time elapsed while processing in this domain.
    pub total_time: u64,
    /// Total thread time elapsed while processing in this domain.
    pub total_ptime: u64,
    /// Total wall-clock time spent processing replays in this domain.
    pub total_replay_time: u64,
    /// Total wall-clock time spent processing forward updates in this domain.
    pub total_forward_time: u64,
    /// Total wall-clock time spent waiting for work in this domain.
    pub wait_time: u64,
}

/// Statistics about a node.
///
/// All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStats {
    /// A textual description of this node.
    pub desc: String,
    /// Total wall-clock time elapsed while processing in this node.
    pub process_time: u64,
    /// Total thread time elapsed while processing in this node.
    pub process_ptime: u64,
    /// Total memory size of this node's state.
    pub mem_size: u64,
    /// The materialization type of this node's state.
    pub materialized: MaterializationStatus,
    /// The value returned from Ingredient::probe.
    pub probe_result: HashMap<String, String>,
}

/// Statistics about the Soup data-flow.
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphStats {
    #[serde(serialize_with = "serialize_domainmap")]
    #[serde(deserialize_with = "deserialize_domainmap")]
    #[doc(hidden)]
    pub domains: DomainMap,
}

use std::ops::Deref;
impl Deref for GraphStats {
    type Target = DomainMap;
    fn deref(&self) -> &Self::Target {
        &self.domains
    }
}

// TODO: probably use https://serde.rs/impl-serialize.html#serializing-a-sequence-or-map instead
fn serialize_domainmap<S: Serializer>(map: &DomainMap, s: S) -> Result<S::Ok, S::Error> {
    map.iter()
        .map(|((di, shard), v)| (format!("{}.{}", di.index(), shard), v))
        .collect::<HashMap<_, _>>()
        .serialize(s)
}

fn deserialize_domainmap<'de, D: Deserializer<'de>>(d: D) -> Result<DomainMap, D::Error> {
    use std::str::FromStr;

    let dm = <HashMap<String, (DomainStats, HashMap<NodeIndex, NodeStats>)>>::deserialize(d)?;
    let mut map = DomainMap::default();
    for (k, v) in dm {
        let di = usize::from_str(&k[..k.find('.').unwrap()]).unwrap().into();
        let shard = usize::from_str(&k[k.find('.').unwrap() + 1..]).unwrap();
        map.insert((di, shard), v);
    }
    Ok(map)
}
