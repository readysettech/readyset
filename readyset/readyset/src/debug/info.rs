use std::collections::HashMap;

use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

use crate::internal::*;

/// [`HashMap`] that has a pair of [`DomainIndex`] and [`usize`] as keys.
/// Useful since it already implements the Serialization/Deserialization traits.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct DomainKey(pub DomainIndex, pub usize);
type DomainMap<V> = HashMap<DomainKey, V>;
type WorkersInfo = HashMap<Url, DomainMap<Vec<NodeIndex>>>;

/// Information about the dataflow graph.
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphInfo {
    #[doc(hidden)]
    pub workers: WorkersInfo,
}

use std::ops::Deref;

use url::Url;

impl Deref for GraphInfo {
    type Target = WorkersInfo;
    fn deref(&self) -> &Self::Target {
        &self.workers
    }
}
