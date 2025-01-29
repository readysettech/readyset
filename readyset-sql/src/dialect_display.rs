use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;

use crate::Dialect;

pub trait DialectDisplay {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_;
}

#[derive(Debug)]
pub struct CommaSeparatedList<'a, T: DialectDisplay>(&'a Vec<T>);

impl<'a, T> From<&'a Vec<T>> for CommaSeparatedList<'a, T>
where
    T: DialectDisplay,
{
    fn from(value: &'a Vec<T>) -> Self {
        CommaSeparatedList(value)
    }
}

impl<T> DialectDisplay for CommaSeparatedList<'_, T>
where
    T: DialectDisplay,
{
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "{}",
                self.0.iter().map(|i| i.display(dialect)).join(", ")
            )
        })
    }
}

impl<T> DialectDisplay for Vec<T>
where
    T: DialectDisplay,
{
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        self.iter().map(|i| i.display(dialect)).join(", ")
    }
}
