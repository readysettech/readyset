//! Primitives and structs related to maintaining different consistency
//! models within the Noria dataflow graph.
use crate::map::Map;

/// The timestamp maps a each base table to a monotonically increasing
/// identifier, the transaction id of the last transaction executed on the
/// table. Timestamps may call `satisfies` to verify if another timestamp
/// is sufficiently up to date to satisfy read-your-write guarentees.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Timestamp {
    map: Map<u64>,
}

// TODO(justin): Remove #[allow(dead_code)] when the impl functions are being used.
// TODO(justin/andrew): Implementation of these functions.
#[allow(dead_code)]
impl Timestamp {
    /// Combine a set of timestamps, returning a timestamp with the minimum
    /// base table timestamp across all timestamps in `t`.
    pub fn min(_t: &[&Timestamp]) -> Timestamp {
        todo!("Implementation");
    }

    /// Join two timestamps, returning the maximum timestamp value across all
    /// timestamps in `t1` and `t2`.
    pub fn join(_t1: &Timestamp, _t2: &Timestamp) -> Timestamp {
        todo!("Implementation");
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
}
