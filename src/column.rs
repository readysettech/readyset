use std::cmp::Ordering;
use std::str;

use common::FieldExpression;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum FunctionExpression {
    Avg(FieldExpression),
    Count(FieldExpression),
    Sum(FieldExpression),
    Max(FieldExpression),
    Min(FieldExpression),
    GroupConcat(FieldExpression, String),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Column {
    pub name: String,
    pub alias: Option<String>,
    pub table: Option<String>,
    pub function: Option<FunctionExpression>,
}

impl<'a> From<&'a str> for Column {
    fn from(c: &str) -> Column {
        match c.find(".") {
            None => {
                Column {
                    name: String::from(c),
                    alias: None,
                    table: None,
                    function: None,
                }
            }
            Some(i) => {
                Column {
                    name: String::from(&c[i + 1..]),
                    alias: None,
                    table: Some(String::from(&c[0..i])),
                    function: None,
                }
            }
        }
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Column) -> Ordering {
        if self.table.is_some() && other.table.is_some() {
            match self.table.cmp(&other.table) {
                Ordering::Equal => self.name.cmp(&other.name),
                x => x,
            }
        } else {
            self.name.cmp(&other.name)
        }
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Column) -> Option<Ordering> {
        if self.table.is_some() && other.table.is_some() {
            match self.table.cmp(&other.table) {
                Ordering::Equal => Some(self.name.cmp(&other.name)),
                x => Some(x),
            }
        } else if self.table.is_none() && other.table.is_none() {
            Some(self.name.cmp(&other.name))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_from_str() {
        let s = "table.col";
        let c = Column::from(s);

        assert_eq!(c,
                   Column {
                       name: String::from("col"),
                       alias: None,
                       table: Some(String::from("table")),
                       function: None,
                   });
    }
}
