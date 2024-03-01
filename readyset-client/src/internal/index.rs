use std::cmp::Ordering;
use std::hash::Hash;

use nom_sql::BinaryOperator;
use readyset_errors::{internal, ReadySetResult};
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::KeyComparison;

/// Types of (key-value) data structures we can use as indices in ReadySet.
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash, Arbitrary)]
pub enum QueryType {
    Point,
    Range,
    Both,
    None,
}

impl From<&KeyComparison> for QueryType {
    fn from(value: &KeyComparison) -> Self {
        match value {
            KeyComparison::Equal(_) => QueryType::Point,
            KeyComparison::Range(_) => QueryType::Range,
        }
    }
}

/// A description of an index used on a relation
#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
pub struct Index {
    /// The type of the index
    index_type: IndexType,

    /// The column indices in the underlying relation that this index is on
    columns: Vec<usize>,

    /// The type of queries that will be served by this index
    query_type: QueryType,
}

impl PartialEq for Index {
    fn eq(&self, other: &Self) -> bool {
        (self.index_type, &self.columns) == (other.index_type, &other.columns)
    }
}

impl PartialOrd for Index {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Index {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.index_type, &self.columns).cmp(&(other.index_type, &other.columns))
    }
}

impl Hash for Index {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index_type.hash(state);
        self.columns.hash(state);
    }
}

impl Index {
    /// Create a new Index with the given index type and column indices
    pub fn new(
        index_type: IndexType,
        columns: Vec<usize>,
        query_type: QueryType,
    ) -> ReadySetResult<Self> {
        match (index_type, query_type) {
            (IndexType::HashMap, QueryType::Range | QueryType::Both) => {
                internal!("Tried to hashmap index for range query")
            }
            _ => Ok(Self {
                index_type,
                query_type,
                columns,
            }),
        }
    }

    /// Construct a new [`HashMap`](IndexType::HashMap) index with the given column indices
    pub fn hash_map(columns: Vec<usize>) -> Self {
        Self {
            index_type: IndexType::HashMap,
            query_type: QueryType::Point,
            columns,
        }
    }

    /// Construct a new [`BTreeMap`](IndexType::HashMap) index with the given column indices
    pub fn btree_map(columns: Vec<usize>) -> Self {
        Self {
            index_type: IndexType::BTreeMap,
            query_type: QueryType::Range,
            columns,
        }
    }

    pub fn columns(&self) -> &[usize] {
        self.columns.as_slice()
    }

    pub fn index_type(&self) -> IndexType {
        self.index_type
    }

    pub fn query_type(&self) -> QueryType {
        self.query_type
    }

    /// Returns the length of this index's key
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns true if this index is indexing on no columns
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
