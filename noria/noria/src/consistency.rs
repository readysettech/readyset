//! Primitives and structs related to maintaining different consistency
//! models within the Noria dataflow graph.
use std::collections::HashMap;

use proptest::arbitrary::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::LocalNodeIndex;

/// The timestamp maps a each base table to a monotonically increasing
/// identifier, the transaction id of the last transaction executed on the
/// table. Timestamps may call `satisfies` to verify if another timestamp
/// is sufficiently up to date to satisfy read-your-write guarentees.
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct Timestamp {
    /// A map from  a base table's LocalNodeIndex to timestamp.
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    pub map: HashMap<LocalNodeIndex, u64>,
}

impl Arbitrary for Timestamp {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Timestamp>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;

        any::<Vec<u32>>()
            .prop_map(|ts| {
                let mut timestamp = Timestamp::default();
                // Keep local node indices contiguous.
                for (i, t) in ts.into_iter().enumerate() {
                    timestamp
                        .map
                        .insert(unsafe { LocalNodeIndex::make(i as u32) }, t as u64);
                }
                timestamp
            })
            .boxed()
    }
}

// TODO(justin): Remove #[allow(dead_code)] when the impl functions are being used.
#[allow(dead_code)]
impl Timestamp {
    /// Combine a set of timestamps, returning a timestamp with the minimum
    /// base table timestamp across all timestamps in `t`.
    pub fn min(timestamps: &[&Timestamp]) -> Timestamp {
        // Iterate over each timestamps maps. Keep the minimum value seen in
        // the returned map.
        let mut ret = Timestamp::default();
        for t in timestamps {
            for (table, value) in t.map.iter() {
                if let Some(current_value) = ret.map.get_mut(table) {
                    // Set current_value to the minimum across all the maps.
                    if &*current_value > value {
                        *current_value = *value;
                    }
                } else {
                    ret.map.insert(*table, *value);
                }
            }
        }

        ret
    }

    /// Join two timestamps, returning the maximum timestamp value across all
    /// timestamps in `t1` and `t2`. A join of a timestamp with an empty timestamp
    /// Timestamp::default(), returns the unchanged timestamp.
    pub fn join(t1: &Timestamp, t2: &Timestamp) -> Timestamp {
        // Iterate over the set of keys, for each key take the maximum timestamp
        // value of t1[key], t2[key]. This iterates over both t1 and t2's keys.
        let mut ret = Timestamp::default();
        for key in t1.map.keys().chain(t2.map.keys()) {
            ret.map.entry(*key).or_insert_with(|| {
                let v1 = t1.map.get(key);
                let v2 = t2.map.get(key);
                match v1 {
                    None => match v2 {
                        None => unreachable!("One of the maps must have the key"),
                        Some(v2) => *v2,
                    },
                    Some(v1) => match v2 {
                        None => *v1,
                        Some(v2) => {
                            if v1 > v2 {
                                *v1
                            } else {
                                *v2
                            }
                        }
                    },
                }
            });
        }

        ret
    }

    /// A timestamp `self` satisfies read-your-write consistency for `t2` if:
    /// Every base table timestamp in `self` is greater than the respective
    /// base table timestamp in `t2`. A base table timestamp without a value
    /// in either timestamp is consdiered a value less than any valid timestamp.
    pub fn satisfies(&self, t2: &Timestamp) -> bool {
        for (table, timestamp) in t2.map.iter() {
            // If `self` does not have a timestamp for a base table in `self` then the '
            // timestamp `self` cannot satisfy `t2`.
            if !self.map.contains_key(table) || timestamp > self.map.get(table).unwrap() {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;
    use crate::internal::LocalNodeIndex;

    fn create_timestamp(t: Vec<(LocalNodeIndex, u64)>) -> Timestamp {
        Timestamp {
            map: t.into_iter().collect(),
        }
    }

    #[test]
    fn timestamp_equal_satisfies() {
        // SAFETY: The local node indices are safe as they are 0-indexed
        // and contiguous.
        let b1 = unsafe { LocalNodeIndex::make(0) };
        let b2 = unsafe { LocalNodeIndex::make(1) };
        let t1 = create_timestamp(vec![(b1, 2), (b2, 2)]);

        let t2 = create_timestamp(vec![(b1, 2), (b2, 2)]);
        assert!(t2.satisfies(&t1));
    }

    #[test]
    fn timestamp_table_greater_satisfies() {
        // SAFETY: The local node indices are safe as they are 0-indexed
        // and contiguous.
        let b1 = unsafe { LocalNodeIndex::make(0) };
        let b2 = unsafe { LocalNodeIndex::make(1) };
        let b3 = unsafe { LocalNodeIndex::make(2) };
        let t1 = create_timestamp(vec![(b1, 2), (b2, 2)]);

        let t2 = create_timestamp(vec![(b1, 3), (b2, 2), (b3, 1)]);
        assert!(t2.satisfies(&t1));
    }

    #[test]
    fn timestamp_table_less_not_satisfies() {
        // SAFETY: The local node indices are safe as they are 0-indexed
        // and contiguous.
        let b1 = unsafe { LocalNodeIndex::make(0) };
        let b2 = unsafe { LocalNodeIndex::make(1) };
        let b3 = unsafe { LocalNodeIndex::make(2) };
        let t1 = create_timestamp(vec![(b1, 2), (b2, 2)]);

        let t2 = create_timestamp(vec![(b1, 1), (b2, 2), (b3, 1)]);
        assert!(!t2.satisfies(&t1));
    }

    #[test]
    fn timestamp_table_missing_not_satisfies() {
        // SAFETY: The local node indices are safe as they are 0-indexed
        // and contiguous.
        let b1 = unsafe { LocalNodeIndex::make(0) };
        let b2 = unsafe { LocalNodeIndex::make(1) };
        let t1 = create_timestamp(vec![(b1, 2), (b2, 2)]);

        let t2 = create_timestamp(vec![(b2, 2)]);
        assert!(!t2.satisfies(&t1));
    }

    #[proptest]
    fn min_with_empty(t: Timestamp) {
        assert_eq!(&t, &Timestamp::min(&[&t, &Timestamp::default()]));
    }

    #[proptest]
    fn min_associative(t1: Timestamp, t2: Timestamp, t3: Timestamp) {
        assert_eq!(
            &Timestamp::min(&[&Timestamp::min(&[&t1, &t2]), &t3]),
            &Timestamp::min(&[&t1, &Timestamp::min(&[&t2, &t3])])
        );
    }

    #[proptest]
    fn min_commutative(t1: Timestamp, t2: Timestamp) {
        assert_eq!(&Timestamp::min(&[&t1, &t2]), &Timestamp::min(&[&t2, &t1]));
    }

    #[proptest]
    fn min_idempotent(t: Timestamp) {
        assert_eq!(&t, &Timestamp::min(&[&t, &t]));
    }

    #[test]
    fn min_multiple_vectors() {
        // SAFETY: The local node indices are safe as they are 0-indexed
        // and contiguous.
        let b1 = unsafe { LocalNodeIndex::make(0) };
        let b2 = unsafe { LocalNodeIndex::make(1) };

        let t1 = create_timestamp(vec![(b1, 1), (b2, 2)]);
        let t2 = create_timestamp(vec![(b1, 2), (b2, 1)]);

        let min = Timestamp::min(&[&t1, &t2]);
        let expected = create_timestamp(vec![(b1, 1), (b2, 1)]);
        assert_eq!(&min, &expected);
    }

    #[proptest]
    fn join_with_empty(t: Timestamp) {
        assert_eq!(&t, &Timestamp::join(&t, &Timestamp::default()));
    }

    #[proptest]
    fn join_associative(t1: Timestamp, t2: Timestamp, t3: Timestamp) {
        assert_eq!(
            &Timestamp::join(&Timestamp::join(&t1, &t2), &t3),
            &Timestamp::join(&t1, &Timestamp::join(&t2, &t3))
        );
    }

    #[proptest]
    fn join_commutative(t1: Timestamp, t2: Timestamp) {
        assert_eq!(&Timestamp::join(&t1, &t2), &Timestamp::join(&t2, &t1));
    }

    #[proptest]
    fn join_idempotent(t: Timestamp) {
        assert_eq!(&t, &Timestamp::join(&t, &t));
    }

    #[test]
    fn join_calculates_max() {
        // SAFETY: The local node indices are safe as they are 0-indexed
        // and contiguous.
        let b1 = unsafe { LocalNodeIndex::make(0) };
        let b2 = unsafe { LocalNodeIndex::make(1) };

        let t1 = create_timestamp(vec![(b1, 2), (b2, 1)]);
        let t2 = create_timestamp(vec![(b1, 1), (b2, 2)]);

        let join = Timestamp::join(&t1, &t2);
        let expected = create_timestamp(vec![(b1, 2), (b2, 2)]);
        assert_eq!(&join, &expected);
    }
}
