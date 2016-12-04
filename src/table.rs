use std::str;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Table {
    pub name: String,
    pub alias: Option<String>,
}

impl<'a> From<&'a str> for Table {
    fn from(t: &str) -> Table {
        Table {
            name: String::from(t),
            alias: None,
        }
    }
}
