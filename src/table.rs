use std::str;
use std::fmt;

#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub alias: Option<String>,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(ref alias) = self.alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
}

impl<'a> From<&'a str> for Table {
    fn from(t: &str) -> Table {
        Table {
            name: String::from(t),
            alias: None,
        }
    }
}
