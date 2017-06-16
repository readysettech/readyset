use std::fmt;
use std::collections::HashMap;
use petgraph::graph::NodeIndex;

use serde::{Serialize, Serializer, Deserialize, Deserializer};

/// A domain-local node identifier.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct LocalNodeIndex {
    id: u32, // not a tuple struct so this field can be made private
}

impl LocalNodeIndex {
    pub unsafe fn make(id: u32) -> LocalNodeIndex {
        LocalNodeIndex { id }
    }

    pub fn id(&self) -> usize {
        self.id as usize
    }
}

impl fmt::Display for LocalNodeIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "l{}", self.id)
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug)]
pub struct IndexPair {
    global: NodeIndex,
    local: Option<LocalNodeIndex>,
}

impl fmt::Display for IndexPair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.global.index())
    }
}

use std::ops::Deref;
impl Deref for IndexPair {
    type Target = LocalNodeIndex;
    fn deref(&self) -> &Self::Target {
        self.local.as_ref().expect(
            "tried to access local node index, which has not yet been assigned",
        )
    }
}

impl From<NodeIndex> for IndexPair {
    fn from(ni: NodeIndex) -> Self {
        IndexPair {
            global: ni,
            local: None,
        }
    }
}

impl IndexPair {
    pub fn set_local(&mut self, li: LocalNodeIndex) {
        assert_eq!(self.local, None);
        self.local = Some(li);
    }

    pub fn has_local(&self) -> bool {
        self.local.is_some()
    }

    pub fn as_global(&self) -> NodeIndex {
        self.global
    }

    pub fn remap(&mut self, mapping: &HashMap<NodeIndex, IndexPair>) {
        *self = *mapping
            .get(&self.global)
            .expect("asked to remap index that is unknown in mapping");
    }
}

#[derive(Serialize, Deserialize)]
struct IndexPairDef {
    global: usize,
    local: Option<LocalNodeIndex>,
}

impl Serialize for IndexPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let def = IndexPairDef {
            global: self.global.index(),
            local: self.local,
        };

        def.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for IndexPair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        IndexPairDef::deserialize(deserializer).map(|def| {
            // what if I put a really long comment rustfmt? then what?
            IndexPair {
                global: NodeIndex::new(def.global),
                local: def.local,
            }
        })
    }
}
