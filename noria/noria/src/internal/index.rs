/// Types of (key-value) data structures we can use as indices in Noria.
///
/// See [the design doc][0] for more information
///
/// [0]: https://www.notion.so/readyset/Index-Selection-f91b2a873dda4b63a4b5d9d14bbee266
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum IndexType {
    /// An index backed by a [`HashMap`](std::collections::HashMap).
    HashMap,
    /// An index backed by a [`BTreeMap`](std::collections::BTreeMap)
    BTreeMap,
}

/// A description of an index used on a relation
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Index {
    /// The type of the index
    pub index_type: IndexType,

    /// The column indices in the underlying relation that this index is on
    pub columns: Vec<usize>,
}

impl Index {
    /// Create a new Index with the given index type and column indices
    pub fn new(index_type: IndexType, columns: Vec<usize>) -> Self {
        Self {
            index_type,
            columns,
        }
    }

    /// Construct a new [`HashMap`](IndexType::HashMap) index with the given column indices
    pub fn hash_map(columns: Vec<usize>) -> Self {
        Self::new(IndexType::HashMap, columns)
    }

    /// Construct a new [`BTreeMap`](IndexType::HashMap) index with the given column indices
    pub fn btree_map(columns: Vec<usize>) -> Self {
        Self::new(IndexType::BTreeMap, columns)
    }

    /// Returns the length of this index's key
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

impl std::ops::Index<usize> for Index {
    type Output = usize;

    fn index(&self, index: usize) -> &Self::Output {
        self.columns.get(index).unwrap()
    }
}
