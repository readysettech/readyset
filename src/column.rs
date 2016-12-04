use std::str;

use common::FieldExpression;

#[derive(Clone, Debug, PartialEq)]
pub enum AggregationExpression {
    Avg(FieldExpression),
    Count(FieldExpression),
    Sum(FieldExpression),
    Max(FieldExpression),
    Min(FieldExpression),
    GroupConcat(FieldExpression),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub table: Option<String>,
    pub aggregation: Option<AggregationExpression>,
}

impl<'a> From<&'a str> for Column {
    fn from(c: &str) -> Column {
        match c.find(".") {
            None => {
                Column {
                    name: String::from(c),
                    table: None,
                    aggregation: None,
                }
            }
            Some(i) => {
                Column {
                    name: String::from(&c[i + 1..]),
                    table: Some(String::from(&c[0..i])),
                    aggregation: None,
                }
            }
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
                       table: Some(String::from("table")),
                       aggregation: None,
                   });
    }
}
