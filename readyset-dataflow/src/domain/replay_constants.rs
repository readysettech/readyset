//! Reading a base table by the constants of the filter node that follows it. A replay from the
//! table looks up each key with those constants in front, so it skips rows the filter would drop.

use std::fmt;

use itertools::Itertools;
use readyset_client::{KeyComparison, ReplayKeys};
use readyset_util::ranges::Bound;
use serde::{Deserialize, Serialize};
use vec1::Vec1;

use crate::prelude::*;

/// The constants a replay path puts in front of each key when it reads its source, a base table
/// followed by a filter node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayConstants {
    /// The index to read, on some of the filter's constant columns followed by the key columns
    index: Index,
    /// The values to put in front of each key, one combination of the constants per lookup
    prefixes: Box<[Box<[DfValue]>]>,
}

impl ReplayConstants {
    /// The index the replay reads
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// How many lookups each key turns into
    pub(super) fn lookups_per_key(&self) -> usize {
        self.prefixes.len()
    }

    /// Each key in `keys` once per prefix, with the prefix in front. A BTreeMap index only answers
    /// range lookups, so point keys become ranges over a single key there.
    pub(super) fn prefixed_keys(&self, keys: &ReplayKeys) -> ReplayKeys {
        let mut prefixed = ReplayKeys::new();
        for prefix in self.prefixes.iter() {
            let with_prefix = |key: &Vec1<DfValue>| {
                let mut k = Vec::with_capacity(prefix.len() + key.len());
                k.extend(prefix.iter().chain(&key[..key.len() - 1]).cloned());
                Vec1::from_vec_push(k, key.last().clone())
            };
            for key in keys.equals().map(with_prefix) {
                prefixed.insert(match self.index.index_type {
                    IndexType::HashMap => KeyComparison::Equal(key),
                    IndexType::BTreeMap => {
                        KeyComparison::Range((Bound::Included(key.clone()), Bound::Included(key)))
                    }
                });
            }
            for (lower, upper) in keys.ranges() {
                let range = (
                    lower.as_ref().map(with_prefix),
                    upper.as_ref().map(with_prefix),
                );
                prefixed.insert(KeyComparison::Range(range));
            }
        }
        prefixed
    }
}

impl fmt::Display for ReplayConstants {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefixes = self
            .prefixes
            .iter()
            .map(|p| format!("({})", p.iter().join(", ")));
        write!(f, "{} by {}", self.index, prefixes.format(", "))
    }
}

#[cfg(test)]
mod tests {
    use readyset_data::Collation;

    use super::*;

    #[test]
    fn prefixed_keys_put_each_prefix_in_front() {
        let text = |s: &str| DfValue::from_str_and_collation(s, Collation::Utf8Shifted);
        let (a, b) = (text("a"), text("b"));
        let constants = |index| ReplayConstants {
            index,
            prefixes: Box::from([
                Box::from([2.into(), a.clone()]),
                Box::from([2.into(), b.clone()]),
            ]),
        };
        let keys = |keys: Vec<KeyComparison>| keys.into_iter().collect::<ReplayKeys>();

        let point = constants(Index::hash_map(vec![2, 3, 1]));
        assert_eq!(point.to_string(), "HashMap[2, 3, 1] by (2, a), (2, b)");
        assert_eq!(
            point.prefixed_keys(&keys(vec![KeyComparison::Equal(vec1![5.into()])])),
            keys(vec![
                KeyComparison::Equal(vec1![2.into(), a.clone(), 5.into()]),
                KeyComparison::Equal(vec1![2.into(), b.clone(), 5.into()]),
            ])
        );

        // On a BTreeMap index, point keys become single-key ranges and ranges keep both bounds
        let range = constants(Index::btree_map(vec![2, 3, 1, 4]));
        let (k, m, max) = (text("k"), text("m"), DfValue::Max);
        let single = |key: Vec1<DfValue>| {
            KeyComparison::Range((Bound::Included(key.clone()), Bound::Included(key)))
        };
        let after = |prefix: &[DfValue]| {
            let bound =
                |last: DfValue| Vec1::try_from_vec([prefix, &[5.into(), last]].concat()).unwrap();
            KeyComparison::Range((
                Bound::Excluded(bound(k.clone())),
                Bound::Included(bound(max.clone())),
            ))
        };
        assert_eq!(
            range.prefixed_keys(&keys(vec![
                KeyComparison::Equal(vec1![5.into(), m.clone()]),
                after(&[])
            ])),
            keys(vec![
                single(vec1![2.into(), a.clone(), 5.into(), m.clone()]),
                single(vec1![2.into(), b.clone(), 5.into(), m]),
                after(&[2.into(), a]),
                after(&[2.into(), b]),
            ])
        );
    }
}
