use std::borrow::Borrow;
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

use std::iter::FromIterator;
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
    pub fn has<Q: ?Sized>(&self, q: &Q, positive: bool) -> bool
    where
        Vec<DfValue>: Borrow<Q>,
        Q: Eq,
    {
        self.iter().any(|r| match r {
            Record::Positive(ref r) if positive => r.borrow() == q,
            Record::Negative(ref r) if !positive => r.borrow() == q,
            _ => false,
        })
    }

    pub fn has_positive<Q: ?Sized>(&self, q: &Q) -> bool
    where
        Vec<DfValue>: Borrow<Q>,
        Q: Eq,
    {
        self.has(q, true)
    }

    pub fn has_negative<Q: ?Sized>(&self, q: &Q) -> bool
    where
        Vec<DfValue>: Borrow<Q>,
        Q: Eq,
    {
        self.has(q, false)
    }

    // This function checks every Negative record and ensures that there isn't a Positive record
    // before it that matches its content. If there is, then both the Negative and Positive
    // records are removed. This will prevent unnecessary writes to RocksDB.
    pub fn remove_deleted(&mut self) {
        let mut i = 0;
        while i < self.0.len() {
            if let Record::Negative(val) = &self.0[i] {
                for j in (0..i).rev() {
                    if let Record::Positive(pos_val) = &self.0[j] {
                        if pos_val == val {
                            self.0.remove(j);
                            i -= 1;
                            self.0.remove(i); // index decreased due to previous removal
                            break;
                        }
                    }
                }
            }
            i += 1;
        }
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
    use super::*;

    /// Certain operators (see eg [note: topk-record-ordering]) rely on the fact that positive
    /// records compare less than negative records when the actual record is the same - this checks
    /// that that is actually the case to prevent us from accidentally changing it.
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
}
