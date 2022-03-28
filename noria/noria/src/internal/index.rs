use std::cmp::Ordering;

use nom_sql::BinaryOperator;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::KeyComparison;

/// Types of (key-value) data structures we can use as indices in Noria.
///
/// See [the design doc][0] for more information
///
/// [0]: https://www.notion.so/readyset/Index-Selection-f91b2a873dda4b63a4b5d9d14bbee266
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash, Arbitrary)]
pub enum IndexType {
    /// An index backed by a [`HashMap`](std::collections::HashMap).
    HashMap,
    /// An index backed by a [`BTreeMap`](std::collections::BTreeMap)
    BTreeMap,
}

/// An index type it₁ is > it₂ iff it₁ can support all lookup operations it₂ can support.
impl Ord for IndexType {
    fn cmp(&self, other: &Self) -> Ordering {
        use IndexType::*;

        match (self, other) {
            (HashMap, HashMap) | (BTreeMap, BTreeMap) => Ordering::Equal,
            (BTreeMap, HashMap) => Ordering::Greater,
            (HashMap, BTreeMap) => Ordering::Less,
        }
    }
}

impl PartialOrd for IndexType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl IndexType {
    /// Return the [`IndexType`] that is best able to satisfy lookups via the given operator, if any
    pub fn for_operator(operator: BinaryOperator) -> Option<Self> {
        use BinaryOperator::*;
        match operator {
            Equal | Is => Some(Self::HashMap),
            Greater | GreaterOrEqual | Less | LessOrEqual => Some(Self::BTreeMap),
            _ => None,
        }
    }

    /// Return the [`IndexType`] that is best able to satisfy lookups for the given `key`
    pub fn best_for_key(key: &KeyComparison) -> Self {
        match key {
            KeyComparison::Equal(_) => Self::HashMap,
            KeyComparison::Range(_) => Self::BTreeMap,
        }
    }

    /// Return a list of all [`IndexType`]s that could possibly satisfy lookups for the given `key`
    pub fn all_for_key(key: &KeyComparison) -> &'static [Self] {
        match key {
            KeyComparison::Equal(_) => &[Self::HashMap, Self::BTreeMap],
            KeyComparison::Range(_) => &[Self::BTreeMap],
        }
    }

    /// Return the [`IndexType`] that is best able to satisfy lookups for all the given `keys`
    pub fn best_for_keys(keys: &[KeyComparison]) -> Self {
        keys.iter()
            .map(Self::best_for_key)
            .max()
            .unwrap_or(Self::HashMap)
    }

    /// Return true if this index type can support lookups for the given `key`
    pub fn supports_key(&self, key: &KeyComparison) -> bool {
        match self {
            IndexType::HashMap => key.is_equal(),
            IndexType::BTreeMap => true,
        }
    }
}

/// A description of an index used on a relation
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct Index {
    /// The type of the index
    pub index_type: IndexType,

    /// The column indices in the underlying relation that this index is on
    /// TODO: Convert to Vec1
    pub columns: Vec<usize>,
}

#[allow(clippy::len_without_is_empty)] // Index can't have an empty set of columns
impl Index {
    /// Create a new Index with the given index type and column indices
    pub fn new(index_type: IndexType, columns: Vec<usize>) -> Self {
        debug_assert!(!columns.is_empty());
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
}

impl std::ops::Index<usize> for Index {
    type Output = usize;

    fn index(&self, index: usize) -> &Self::Output {
        self.columns.get(index).unwrap()
    }
}
