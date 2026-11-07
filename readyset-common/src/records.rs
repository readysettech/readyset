use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::iter::FromIterator;
use std::mem::discriminant;
use std::ops::{Deref, DerefMut};

use readyset_data::DfValue;
use serde::{Deserialize, Serialize};

/// A record is a single positive or negative data record with an associated time stamp.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
#[warn(variant_size_differences)]
pub enum Record {
    Positive(Vec<DfValue>),
    Negative(Vec<DfValue>),
}

impl Record {
    pub fn rec(&self) -> &[DfValue] {
        match *self {
            Record::Positive(ref v) | Record::Negative(ref v) => &v[..],
        }
    }

    pub fn is_positive(&self) -> bool {
        matches!(self, Record::Positive(..))
    }

    pub fn extract(self) -> (Vec<DfValue>, bool) {
        match self {
            Record::Positive(v) => (v, true),
            Record::Negative(v) => (v, false),
        }
    }

    /// Return a reference to the underlying row for this Record, whether negative or positive
    pub fn row(&self) -> &Vec<DfValue> {
        match self {
            Record::Positive(v) | Record::Negative(v) => v,
        }
    }

    /// Convert this Record into its underlying row
    pub fn into_row(self) -> Vec<DfValue> {
        match self {
            Record::Positive(v) | Record::Negative(v) => v,
        }
    }
}

impl Deref for Record {
    type Target = Vec<DfValue>;

    fn deref(&self) -> &Self::Target {
        match *self {
            Record::Positive(ref r) | Record::Negative(ref r) => r,
        }
    }
}

impl DerefMut for Record {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Record::Positive(ref mut r) | Record::Negative(ref mut r) => r,
        }
    }
}

impl From<Vec<DfValue>> for Record {
    fn from(other: Vec<DfValue>) -> Self {
        Record::Positive(other)
    }
}

impl From<(Vec<DfValue>, bool)> for Record {
    fn from(other: (Vec<DfValue>, bool)) -> Self {
        if other.1 {
            Record::Positive(other.0)
        } else {
            Record::Negative(other.0)
        }
    }
}

impl From<Records> for Vec<Record> {
    fn from(val: Records) -> Vec<Record> {
        val.0
    }
}

impl FromIterator<Record> for Records {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Record>,
    {
        Records(iter.into_iter().collect())
    }
}

impl FromIterator<Vec<DfValue>> for Records {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Vec<DfValue>>,
    {
        Records(iter.into_iter().map(Record::Positive).collect())
    }
}

impl IntoIterator for Records {
    type Item = Record;
    type IntoIter = ::std::vec::IntoIter<Record>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Records {
    type Item = &'a Record;
    type IntoIter = ::std::slice::Iter<'a, Record>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Clone, Default, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Records(Vec<Record>);

impl Records {
    pub fn has<Q>(&self, q: &Q, positive: bool) -> bool
    where
        Vec<DfValue>: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.iter().any(|r| match r {
            Record::Positive(r) if positive => r.borrow() == q,
            Record::Negative(r) if !positive => r.borrow() == q,
            _ => false,
        })
    }

    pub fn has_positive<Q>(&self, q: &Q) -> bool
    where
        Vec<DfValue>: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.has(q, true)
    }

    pub fn has_negative<Q>(&self, q: &Q) -> bool
    where
        Vec<DfValue>: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.has(q, false)
    }

    // This function checks every Negative record and ensures that there isn't a Positive record
    // before it that matches its content. If there is, then both the Negative and Positive
    // records are removed. This will prevent unnecessary writes to RocksDB.
    //
    // Each negative cancels the latest outstanding positive with the same row. Hash buckets avoid
    // scanning unrelated rows; compaction preserves the order and multiplicity of surviving records.
    pub fn remove_deleted(&mut self) {
        let Some(first) = self.0.first() else {
            return;
        };
        if self
            .0
            .iter()
            .all(|r| r.is_positive() == first.is_positive())
        {
            return;
        }

        // Cross-type DfValue equality can disagree with hashing. Only fingerprint columns with a
        // consistent scalar representation and collation throughout the batch. Full row equality
        // resolves collisions, including batches whose columns cannot contribute to the hash.
        let hash_columns: Vec<_> = first
            .iter()
            .enumerate()
            .filter_map(|(col, _)| {
                let first = self
                    .0
                    .iter()
                    .filter_map(|r| r.get(col))
                    .find(|v| !v.is_none())?;
                self.0
                    .iter()
                    .all(|r| {
                        r.get(col).is_some_and(|v| match (first, v) {
                            (_, DfValue::None) => true,
                            (DfValue::Array(_), _) => false,
                            (
                                DfValue::Text(_) | DfValue::TinyText(_),
                                DfValue::Text(_) | DfValue::TinyText(_),
                            ) => first.collation() == v.collation(),
                            _ => discriminant(first) == discriminant(v),
                        })
                    })
                    .then_some(col)
            })
            .collect();
        let hash_builder = RandomState::new();
        let mut keep = vec![true; self.0.len()];
        let mut positives: HashMap<u64, Vec<usize>> = HashMap::new();
        for (i, record) in self.0.iter().enumerate() {
            let mut hasher = hash_builder.build_hasher();
            record.len().hash(&mut hasher);
            for &col in &hash_columns {
                match &record[col] {
                    // Decimal hashing preserves precision and normalizes equal scales and zeros.
                    DfValue::Numeric(n) => n.hash(&mut hasher),
                    v => v.hash(&mut hasher),
                }
            }
            let hash = hasher.finish();
            match record {
                Record::Positive(_) => positives.entry(hash).or_default().push(i),
                Record::Negative(row) => {
                    if let Some(bucket) = positives.get_mut(&hash)
                        && let Some(pos) = bucket.iter().rposition(|&j| self.0[j].rec() == row)
                    {
                        let j = bucket.remove(pos);
                        keep[i] = false;
                        keep[j] = false;
                    }
                }
            }
        }

        let mut keep = keep.into_iter();
        self.0
            .retain(|_| keep.next().expect("one retention flag per record"));
    }
}

impl Deref for Records {
    type Target = Vec<Record>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Records {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Record> for Records {
    fn from(val: Record) -> Records {
        Records(vec![val])
    }
}

impl From<Vec<Record>> for Records {
    fn from(val: Vec<Record>) -> Records {
        Records(val)
    }
}

impl From<Vec<Vec<DfValue>>> for Records {
    fn from(val: Vec<Vec<DfValue>>) -> Records {
        Records(val.into_iter().map(Into::into).collect())
    }
}

impl From<Vec<(Vec<DfValue>, bool)>> for Records {
    fn from(val: Vec<(Vec<DfValue>, bool)>) -> Records {
        Records(val.into_iter().map(Into::into).collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use proptest::prelude::*;
    use readyset_data::{Array, Collation, Text};

    use super::*;

    // Certain operators (see e.g. [note: topk-record-ordering]) rely on the fact that positive
    // records compare less than negative records when the actual record is the same.
    #[test]
    fn positive_less_than_negative() {
        assert!(
            Record::Positive(vec![1.into(), 2.into()]) < Record::Negative(vec![1.into(), 2.into()])
        )
    }

    // Transactions sometimes include records that negate each other. The following test
    // ensures that the simplify function handles them correctly.
    #[test]
    fn test_simplify() {
        let mut records: Records = vec![
            Record::Positive(vec![1.into(), "2".into(), 3.into()]),
            Record::Negative(vec![1.into(), "2".into(), 3.into()]),
            Record::Positive(vec![4.into(), "5".into(), 6.into()]),
            Record::Negative(vec![4.into(), "5".into(), 6.into()]),
            Record::Positive(vec!["last".into(), 8.into(), 9.into()]),
        ]
        .into();

        records.remove_deleted();

        let mut result: Records =
            vec![Record::Positive(vec!["last".into(), 8.into(), 9.into()])].into();

        assert_eq!(records, result);

        records = vec![
            Record::Positive(vec![1.into(), "2".into(), 3.into()]),
            Record::Negative(vec![9.into(), "2".into(), 3.into()]),
            Record::Positive(vec![7.into(), "5".into(), 6.into()]),
            Record::Negative(vec![1.into(), "2".into(), 3.into()]),
            Record::Positive(vec!["last".into(), 8.into(), 9.into()]),
        ]
        .into();

        records.remove_deleted();

        result = vec![
            Record::Negative(vec![9.into(), "2".into(), 3.into()]),
            Record::Positive(vec![7.into(), "5".into(), 6.into()]),
            Record::Positive(vec!["last".into(), 8.into(), 9.into()]),
        ]
        .into();

        assert_eq!(records, result);

        records = vec![
            Record::Positive(vec![1.into(), "2".into(), 3.into()]),
            Record::Negative(vec![1.into(), "2".into(), 3.into()]),
        ]
        .into();

        records.remove_deleted();

        assert!(records.is_empty());
    }

    /// A direct quadratic form of the rule `remove_deleted` follows, used as the test oracle.
    fn remove_deleted_reference(records: &mut Vec<Record>) {
        let mut i = 0;
        while i < records.len() {
            let cancelled = match &records[i] {
                Record::Negative(row) => (0..i)
                    .rev()
                    .find(|&j| matches!(&records[j], Record::Positive(p) if p == row)),
                Record::Positive(_) => None,
            };
            match cancelled {
                Some(j) => {
                    records.remove(i);
                    records.remove(j);
                    i -= 1;
                }
                None => i += 1,
            }
        }
    }

    fn numeric(value: &str) -> DfValue {
        DfValue::Numeric(Arc::new(value.parse().unwrap()))
    }

    #[test]
    fn remove_deleted_interleaved_cancellations() {
        let mut records: Records = vec![
            (vec![3.into()], true),
            (vec![1.into()], true),
            (vec![3.into()], false),
            (vec![1.into()], false),
        ]
        .into();
        records.remove_deleted();
        assert!(records.is_empty());
    }

    #[test]
    fn remove_deleted_preserves_order_and_multiplicity() {
        let a = vec![1.into()];
        let b = vec![2.into()];
        let mut records: Records = vec![
            (a.clone(), false),
            (a.clone(), true),
            (b.clone(), true),
            (a.clone(), true),
            (a.clone(), false),
            (b.clone(), false),
        ]
        .into();
        records.remove_deleted();
        assert_eq!(records, Records::from(vec![(a.clone(), false), (a, true)]));
    }

    #[test]
    fn remove_deleted_repeated_updates() {
        let mut records: Records = vec![
            (vec![1.into(), "old".into()], false),
            (vec![1.into(), "middle".into()], true),
            (vec![1.into(), "middle".into()], false),
            (vec![1.into(), "new".into()], true),
        ]
        .into();
        let expected = Records::from(vec![records[0].clone(), records[3].clone()]);
        records.remove_deleted();
        assert_eq!(records, expected);
    }

    #[test]
    fn remove_deleted_cross_type_equality() {
        let pairs = [
            (1.into(), numeric("1")),
            (DfValue::UnsignedInt(1), 1.into()),
            (DfValue::Float(1.0), numeric("1")),
            (DfValue::Double(1.0), DfValue::Float(1.0)),
            (numeric("1.00"), numeric("1")),
            (
                DfValue::Text(Text::from_str_with_collation("a", Collation::Utf8Ci)),
                DfValue::Text(Text::from_str_with_collation("A", Collation::Utf8Ci)),
            ),
            (numeric("-0.00"), numeric("0")),
            (
                DfValue::from(Array::from(vec![1.into()])),
                DfValue::from(Array::from(vec![numeric("1")])),
            ),
        ];
        for (a, b) in pairs {
            assert_eq!(a, b);
            let mut records: Records = vec![(vec![a], true), (vec![b], false)].into();
            records.remove_deleted();
            assert!(records.is_empty(), "{records:?}");
        }
    }

    #[test]
    fn remove_deleted_uses_latest_matching_row() {
        // Cross-type equality is not transitive: the integer equals the decimal, and the
        // decimal equals the float, but the integer does not equal the float.
        let mut records: Records = vec![
            (vec![1.into()], true),
            (vec![numeric("1")], true),
            (vec![DfValue::Float(1.0)], false),
        ]
        .into();
        records.remove_deleted();
        assert_eq!(records, Records::from(vec![vec![DfValue::Int(1)]]));
    }

    fn arb_records_from(
        values: impl Strategy<Value = DfValue>,
    ) -> impl Strategy<Value = Vec<Record>> {
        proptest::collection::vec(
            (proptest::collection::vec(values, 0..4), any::<bool>())
                .prop_map(|(row, positive)| (row, positive).into()),
            0..40,
        )
    }

    /// Values that are equal across types or collations, which the hashing has to respect.
    fn arb_records_from_menu() -> impl Strategy<Value = Vec<Record>> {
        let values = vec![
            DfValue::None,
            0.into(),
            1.into(),
            DfValue::UnsignedInt(1),
            DfValue::Float(1.0),
            DfValue::Double(1.0),
            DfValue::Double(f64::NAN),
            numeric("1"),
            numeric("1.00"),
            numeric("-0.00"),
            numeric("0"),
            numeric("999999999999999999"),
            "a".into(),
            "A".into(),
            DfValue::Text(Text::from_str_with_collation("a", Collation::Utf8)),
            DfValue::Text(Text::from_str_with_collation("a", Collation::Utf8Ci)),
            DfValue::Text(Text::from_str_with_collation("A", Collation::Utf8Ci)),
            DfValue::from(Array::from(vec![1.into()])),
            DfValue::from(Array::from(vec![numeric("1")])),
        ];
        arb_records_from(proptest::sample::select(values))
    }

    /// Arbitrary values drawn from a small pool, so rows repeat and negatives find positives.
    fn arb_records_from_arbitrary() -> impl Strategy<Value = Vec<Record>> {
        proptest::collection::vec(any::<DfValue>(), 1..6)
            .prop_flat_map(|pool| arb_records_from(proptest::sample::select(pool)))
    }

    fn check_remove_deleted(records: Vec<Record>) -> Result<(), TestCaseError> {
        let mut expected = records.clone();
        remove_deleted_reference(&mut expected);
        let mut actual = Records::from(records);
        actual.remove_deleted();
        prop_assert_eq!(actual.0, expected);
        Ok(())
    }

    proptest! {
        #[test]
        fn remove_deleted_matches_reference(records in arb_records_from_menu()) {
            check_remove_deleted(records)?;
        }

        #[test]
        fn remove_deleted_matches_reference_on_arbitrary_values(
            records in arb_records_from_arbitrary()
        ) {
            check_remove_deleted(records)?;
        }
    }
}
