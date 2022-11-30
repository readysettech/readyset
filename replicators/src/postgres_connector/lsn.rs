use std::fmt::{Debug, Display, Formatter, Result};

#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default)]
pub struct Lsn(pub i64);

impl Display for Lsn {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffffffff)
    }
}

impl Debug for Lsn {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self)
    }
}

impl From<i64> for Lsn {
    fn from(i: i64) -> Self {
        Self(i)
    }
}
