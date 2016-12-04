use std::str;

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub table: Option<String>,
}

impl<'a> From<&'a str> for Column {
    fn from(c: &str) -> Column {
        match c.find(".") {
            None => {
                Column {
                    name: String::from(c),
                    table: None,
                }
            }
            Some(i) => {
                Column {
                    name: String::from(&c[i + 1..]),
                    table: Some(String::from(&c[0..i])),
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
                   });
    }
}
