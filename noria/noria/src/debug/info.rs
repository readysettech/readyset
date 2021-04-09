use crate::internal::*;
use petgraph::graph::NodeIndex;
use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::ser::{Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

/// [`HashMap`] that has a pair of [`DomainIndex`] and [`usize`] as keys.
/// Useful since it already implements the Serialization/Deserialization traits.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DomainKey(pub DomainIndex, pub usize);
type DomainMap<V> = HashMap<DomainKey, V>;
type WorkersInfo = HashMap<SocketAddr, DomainMap<Vec<NodeIndex>>>;

/// Information about the dataflow graph.
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphInfo {
    #[doc(hidden)]
    pub workers: WorkersInfo,
}

use std::ops::Deref;

impl Deref for GraphInfo {
    type Target = WorkersInfo;
    fn deref(&self) -> &Self::Target {
        &self.workers
    }
}

impl Serialize for DomainKey {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}.{}", self.0.index(), self.1))
    }
}

impl<'de> Deserialize<'de> for DomainKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        struct DomainKeyVisitor;

        impl<'de> Visitor<'de> for DomainKeyVisitor {
            type Value = DomainKey;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("A DomainKey object")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_string(v.to_owned())
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: Error,
            {
                use std::str::FromStr;

                let di = usize::from_str(&v[..v.find('.').unwrap()]).unwrap().into();
                let shard = usize::from_str(&v[v.find('.').unwrap() + 1..]).unwrap();
                Ok(DomainKey(di, shard))
            }
        }

        deserializer.deserialize_map(DomainKeyVisitor)
    }
}
