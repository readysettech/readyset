use noria::DataType;
use std::borrow::Borrow;
use std::ops::{Deref, DerefMut};

/// A record is a single positive or negative data record with an associated time stamp.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
#[warn(variant_size_differences)]
pub enum Record {
    Positive(Vec<DataType>),
    Negative(Vec<DataType>),
}

impl Record {
    pub fn rec(&self) -> &[DataType] {
        match *self {
            Record::Positive(ref v) | Record::Negative(ref v) => &v[..],
        }
    }

    pub fn is_positive(&self) -> bool {
        matches!(self, Record::Positive(..))
    }

    pub fn extract(self) -> (Vec<DataType>, bool) {
        match self {
            Record::Positive(v) => (v, true),
            Record::Negative(v) => (v, false),
        }
    }

    /// Return a reference to the underlying row for this Record, whether negative or positive
    pub fn row(&self) -> &Vec<DataType> {
        match self {
            Record::Positive(v) | Record::Negative(v) => v,
        }
    }

    /// Convert this Record into its underlying row
    pub fn into_row(self) -> Vec<DataType> {
        match self {
            Record::Positive(v) | Record::Negative(v) => v,
        }
    }
}

impl Deref for Record {
    type Target = Vec<DataType>;
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

impl From<Vec<DataType>> for Record {
    fn from(other: Vec<DataType>) -> Self {
        Record::Positive(other)
    }
}

impl From<(Vec<DataType>, bool)> for Record {
    fn from(other: (Vec<DataType>, bool)) -> Self {
        if other.1 {
            Record::Positive(other.0)
        } else {
            Record::Negative(other.0)
        }
    }
}

impl Into<Vec<Record>> for Records {
    fn into(self) -> Vec<Record> {
        self.0
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
impl FromIterator<Vec<DataType>> for Records {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Vec<DataType>>,
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

#[derive(Clone, Default, PartialEq, Debug, Serialize, Deserialize)]
pub struct Records(Vec<Record>);

impl Records {
    pub fn has<Q: ?Sized>(&self, q: &Q, positive: bool) -> bool
    where
        Vec<DataType>: Borrow<Q>,
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
        Vec<DataType>: Borrow<Q>,
        Q: Eq,
    {
        self.has(q, true)
    }

    pub fn has_negative<Q: ?Sized>(&self, q: &Q) -> bool
    where
        Vec<DataType>: Borrow<Q>,
        Q: Eq,
    {
        self.has(q, false)
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

impl Into<Records> for Record {
    fn into(self) -> Records {
        Records(vec![self])
    }
}

impl Into<Records> for Vec<Record> {
    fn into(self) -> Records {
        Records(self)
    }
}

impl Into<Records> for Vec<Vec<DataType>> {
    fn into(self) -> Records {
        Records(self.into_iter().map(Into::into).collect())
    }
}

impl Into<Records> for Vec<(Vec<DataType>, bool)> {
    fn into(self) -> Records {
        Records(self.into_iter().map(Into::into).collect())
    }
}
