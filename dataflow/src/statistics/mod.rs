use std::collections::HashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use node::MaterializationStatus;
use domain;
use prelude::*;

type DomainMap = HashMap<(domain::Index, usize), (DomainStats, HashMap<NodeIndex, NodeStats>)>;

/// Struct holding statistics about a domain. All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct DomainStats {
    pub total_time: u64,
    pub total_ptime: u64,
    pub wait_time: u64,
}

/// Struct holding statistics about a node. All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStats {
    pub desc: String,
    pub process_time: u64,
    pub process_ptime: u64,
    pub mem_size: u64,
    pub materialized: MaterializationStatus,
}

/// Struct holding statistics about an entire graph.
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphStats {
    #[serde(serialize_with = "serialize_domainmap")]
    #[serde(deserialize_with = "deserialize_domainmap")]
    pub domains: DomainMap,
}

fn serialize_domainmap<S: Serializer>(map: &DomainMap, s: S) -> Result<S::Ok, S::Error> {
    map.iter()
        .map(|((di, shard), v)| (format!("{}.{}", di.index(), shard), v.clone()))
        .collect::<HashMap<_, _>>()
        .serialize(s)
}

fn deserialize_domainmap<'de, D: Deserializer<'de>>(d: D) -> Result<DomainMap, D::Error> {
    use std::str::FromStr;

    let dm = <HashMap<String, (DomainStats, HashMap<NodeIndex, NodeStats>)>>::deserialize(d)?;
    let mut map = DomainMap::default();
    for (k, v) in dm {
        let di = usize::from_str(&k[..k.find(".").unwrap()]).unwrap().into();
        let shard = usize::from_str(&k[k.find(".").unwrap() + 1..]).unwrap();
        map.insert((di, shard), v);
    }
    Ok(map)
}
