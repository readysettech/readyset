pub mod base;
pub mod grouped;
pub mod join;
pub mod latest;
pub mod permute;
pub mod union;
pub mod identity;
#[cfg(test)]
pub mod gatedid;

use query;

use std::ops::{Deref, DerefMut};

/// A record is a single positive or negative data record with an associated time stamp.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record {
    Positive(Vec<query::DataType>),
    Negative(Vec<query::DataType>),
}

impl Record {
    pub fn rec(&self) -> &[query::DataType] {
        match *self {
            Record::Positive(ref v) |
            Record::Negative(ref v) => &v[..],
        }
    }

    pub fn is_positive(&self) -> bool {
        if let Record::Positive(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn extract(self) -> (Vec<query::DataType>, bool) {
        match self {
            Record::Positive(v) => (v, true),
            Record::Negative(v) => (v, false),
        }
    }
}

impl Deref for Record {
    type Target = Vec<query::DataType>;
    fn deref(&self) -> &Self::Target {
        match *self {
            Record::Positive(ref r) |
            Record::Negative(ref r) => r,
        }
    }
}

impl DerefMut for Record {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Record::Positive(ref mut r) |
            Record::Negative(ref mut r) => r,
        }
    }
}

impl From<Vec<query::DataType>> for Record {
    fn from(other: Vec<query::DataType>) -> Self {
        Record::Positive(other)
    }
}

/// Update is the smallest unit of data transmitted over edges in a data flow graph.
#[derive(Clone)]
pub enum Update {
    /// This update holds a set of records.
    Records(Vec<Record>),
}

impl From<Vec<Record>> for Update {
    fn from(others: Vec<Record>) -> Self {
        Update::Records(others)
    }
}

impl From<Record> for Update {
    fn from(other: Record) -> Self {
        Update::Records(vec![other])
    }
}

impl From<Vec<query::DataType>> for Update {
    fn from(other: Vec<query::DataType>) -> Self {
        Update::Records(vec![other.into()])
    }
}

pub type Datas = Vec<Vec<query::DataType>>;
