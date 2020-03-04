use noria::internal::LocalNodeIndex;
use noria::DataType;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct Tag(pub u32);
impl Tag {
    pub fn id(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum KeyType<'a> {
    Single(&'a DataType),
    Double((DataType, DataType)),
    Tri((DataType, DataType, DataType)),
    Quad((DataType, DataType, DataType, DataType)),
    Quin((DataType, DataType, DataType, DataType, DataType)),
    Sex((DataType, DataType, DataType, DataType, DataType, DataType)),
}

impl<'a> KeyType<'a> {
    pub fn from<I>(other: I) -> Self
    where
        I: IntoIterator<Item = &'a DataType>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        let mut other = other.into_iter();
        let len = other.len();
        let mut more = move || other.next().unwrap();
        match len {
            0 => unreachable!(),
            1 => KeyType::Single(more()),
            2 => KeyType::Double((more().clone(), more().clone())),
            3 => KeyType::Tri((more().clone(), more().clone(), more().clone())),
            4 => KeyType::Quad((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            5 => KeyType::Quin((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            6 => KeyType::Sex((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            _ => unimplemented!(),
        }
    }
}
