use noria::internal::LocalNodeIndex;
use noria::DataType;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::ops::{Bound, RangeBounds};
use vec1::Vec1;

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

#[derive(Clone, Debug, Serialize)]
pub enum KeyType<'a> {
    Single(&'a DataType),
    Double((DataType, DataType)),
    Tri((DataType, DataType, DataType)),
    Quad((DataType, DataType, DataType, DataType)),
    Quin((DataType, DataType, DataType, DataType, DataType)),
    Sex((DataType, DataType, DataType, DataType, DataType, DataType)),
    Multi(Vec<DataType>),
}

#[allow(clippy::len_without_is_empty)]
impl<'a> KeyType<'a> {
    pub fn get(&self, idx: usize) -> Option<&DataType> {
        use tuple::TupleElements;

        match self {
            KeyType::Single(x) if idx == 0 => Some(x),
            KeyType::Single(_) => None,
            KeyType::Double(x) => TupleElements::get(x, idx),
            KeyType::Tri(x) => TupleElements::get(x, idx),
            KeyType::Quad(x) => TupleElements::get(x, idx),
            KeyType::Quin(x) => TupleElements::get(x, idx),
            KeyType::Sex(x) => TupleElements::get(x, idx),
            KeyType::Multi(arr) => arr.get(idx),
        }
    }

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
            x => KeyType::Multi((0..x).map(|_| more().clone()).collect()),
        }
    }

    /// Return the length of this key
    ///
    /// # Invariants
    ///
    /// This function will never return 0
    pub fn len(&self) -> usize {
        match self {
            KeyType::Single(_) => 1,
            KeyType::Double(_) => 2,
            KeyType::Tri(_) => 3,
            KeyType::Quad(_) => 4,
            KeyType::Quin(_) => 5,
            KeyType::Sex(_) => 6,
            KeyType::Multi(k) => k.len(),
        }
    }

    /// Return true if any of the elements is null
    pub fn has_null(&self) -> bool {
        match self {
            KeyType::Single(e) => e.is_none(),
            KeyType::Double((e0, e1)) => e0.is_none() || e1.is_none(),
            KeyType::Tri((e0, e1, e2)) => e0.is_none() || e1.is_none() || e2.is_none(),
            KeyType::Quad((e0, e1, e2, e3)) => {
                e0.is_none() || e1.is_none() || e2.is_none() || e3.is_none()
            }
            KeyType::Quin((e0, e1, e2, e3, e4)) => {
                e0.is_none() || e1.is_none() || e2.is_none() || e3.is_none() || e4.is_none()
            }
            KeyType::Sex((e0, e1, e2, e3, e4, e5)) => {
                e0.is_none()
                    || e1.is_none()
                    || e2.is_none()
                    || e3.is_none()
                    || e4.is_none()
                    || e5.is_none()
            }
            KeyType::Multi(k) => k.iter().any(DataType::is_none),
        }
    }
}

#[allow(clippy::type_complexity)]
#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub enum RangeKey<'a> {
    /// Key-length-polymorphic double-unbounded range key
    Unbounded,
    Single((Bound<&'a DataType>, Bound<&'a DataType>)),
    Double(
        (
            Bound<(&'a DataType, &'a DataType)>,
            Bound<(&'a DataType, &'a DataType)>,
        ),
    ),
    Tri(
        (
            Bound<(&'a DataType, &'a DataType, &'a DataType)>,
            Bound<(&'a DataType, &'a DataType, &'a DataType)>,
        ),
    ),
    Quad(
        (
            Bound<(&'a DataType, &'a DataType, &'a DataType, &'a DataType)>,
            Bound<(&'a DataType, &'a DataType, &'a DataType, &'a DataType)>,
        ),
    ),
    Quin(
        (
            Bound<(
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
            )>,
            Bound<(
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
            )>,
        ),
    ),
    Sex(
        (
            Bound<(
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
            )>,
            Bound<(
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
                &'a DataType,
            )>,
        ),
    ),
    Multi((Bound<&'a [DataType]>, Bound<&'a [DataType]>)),
}

#[allow(clippy::len_without_is_empty)]
impl<'a> RangeKey<'a> {
    /// Build a [`RangeKey`] from a type that implements [`RangeBounds`] over a vector of keys.
    ///
    /// # Panics
    ///
    /// Panics if the lengths of the bounds are different, or if the length is greater than 6
    ///
    /// # Examples
    ///
    /// ```rust
    /// use noria_common::RangeKey;
    /// use noria::DataType;
    /// use std::ops::Bound::*;
    /// use vec1::vec1;
    ///
    /// // Can build RangeKeys from regular range expressions
    /// assert_eq!(RangeKey::from(&(..)), RangeKey::Unbounded);
    /// assert_eq!(
    ///   RangeKey::from(&(vec1![DataType::from(0)]..vec1![DataType::from(1)])),
    ///   RangeKey::Single((Included(&(0.into())), Excluded(&(1.into())))
    /// ));
    /// ```
    pub fn from<R>(range: &'a R) -> Self
    where
        R: RangeBounds<Vec1<DataType>>,
    {
        use Bound::*;
        let len = match (range.start_bound(), range.end_bound()) {
            (Unbounded, Unbounded) => return RangeKey::Unbounded,
            (Included(start) | Excluded(start), Included(end) | Excluded(end)) => {
                assert_eq!(start.len(), end.len());
                start.len()
            }
            (Included(start) | Excluded(start), Unbounded) => start.len(),
            (Unbounded, Included(end) | Excluded(end)) => end.len(),
        };

        macro_rules! make {
            ($variant: ident, |$elem: ident| $make_tuple: expr) => {
                RangeKey::$variant((
                    make!(bound, start_bound, $elem, $make_tuple),
                    make!(bound, end_bound, $elem, $make_tuple),
                ))
            };
            (bound, $bound_type: ident, $elem: ident, $make_tuple: expr) => {
                range.$bound_type().map(|key| {
                    let mut key = key.into_iter();
                    let mut $elem = move || key.next().unwrap();
                    $make_tuple
                })
            };
        }

        match len {
            0 => unreachable!("Vec1 cannot be empty"),
            1 => make!(Single, |elem| elem()),
            2 => make!(Double, |elem| (elem(), elem())),
            3 => make!(Tri, |elem| (elem(), elem(), elem())),
            4 => make!(Quad, |elem| (elem(), elem(), elem(), elem())),
            5 => make!(Quin, |elem| (elem(), elem(), elem(), elem(), elem())),
            6 => make!(Sex, |elem| (elem(), elem(), elem(), elem(), elem(), elem())),
            n => panic!(
                "RangeKey cannot be built from keys of length greater than 6 (got {})",
                n
            ),
        }
    }

    /// Return the length of this range key, or None if the key is Unbounded
    pub fn len(&self) -> Option<usize> {
        match self {
            RangeKey::Unbounded | RangeKey::Multi((Bound::Unbounded, Bound::Unbounded)) => None,
            RangeKey::Single(_) => Some(1),
            RangeKey::Double(_) => Some(2),
            RangeKey::Tri(_) => Some(3),
            RangeKey::Quad(_) => Some(4),
            RangeKey::Quin(_) => Some(5),
            RangeKey::Sex(_) => Some(6),
            RangeKey::Multi(
                (Bound::Included(k), _)
                | (Bound::Excluded(k), _)
                | (_, Bound::Included(k))
                | (_, Bound::Excluded(k)),
            ) => Some(k.len()),
        }
    }
}
