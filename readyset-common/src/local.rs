use std::collections::HashMap;
use std::fmt;

use petgraph::graph::NodeIndex;
use readyset_client::internal::LocalNodeIndex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use test_strategy::Arbitrary;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub src: LocalNodeIndex,
    pub dst: LocalNodeIndex,
}

impl Link {
    pub fn new(src: LocalNodeIndex, dst: LocalNodeIndex) -> Self {
        Link { src, dst }
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} -> {:?}", self.src, self.dst)
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
        self.local
            .as_ref()
            .expect("tried to access local node index, which has not yet been assigned")
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
        *self = *mapping.get(&self.global).unwrap_or_else(|| {
            panic!(
                "asked to remap index {:?} that is unknown in mapping",
                self.global
            )
        });
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

#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, Arbitrary,
)]
#[repr(transparent)]
pub struct Tag(u32);

impl Tag {
    pub fn new(upquery: u32) -> Tag {
        Tag(upquery)
    }
}

impl std::fmt::Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Tag> for u32 {
    fn from(tag: Tag) -> Self {
        tag.0
    }
}
