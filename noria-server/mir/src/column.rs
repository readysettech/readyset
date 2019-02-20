use std::cmp::Ordering;

use nom_sql::{self, FunctionExpression};

#[derive(Clone, Debug, Hash)]
pub struct Column {
    pub table: Option<String>,
    pub name: String,
    pub function: Option<Box<FunctionExpression>>,
    pub aliases: Vec<Column>,
}

impl Column {
    pub fn new(table: Option<&str>, name: &str) -> Self {
        Column {
            table: table.map(|t| t.to_owned()),
            name: name.to_owned(),
            function: None,
            aliases: vec![],
        }
    }

    pub fn add_alias(&mut self, alias: &Column) {
        self.aliases.push(alias.clone());
    }
}

impl From<nom_sql::Column> for Column {
    fn from(c: nom_sql::Column) -> Column {
        Column {
            aliases: match &c.alias {
                Some(_) => vec![Column::from(nom_sql::Column {
                    name: c.name.to_owned(),
                    table: c.table.clone(),
                    alias: None,
                    function: c.function.clone(),
                })],
                None => vec![],
            },
            table: c.table,
            name: match c.alias {
                Some(a) => a,
                None => c.name,
            },
            function: c.function,
        }
    }
}

impl<'a> From<&'a nom_sql::Column> for Column {
    fn from(c: &'a nom_sql::Column) -> Column {
        Column {
            aliases: match c.alias {
                Some(_) => vec![Column::from(nom_sql::Column {
                    name: c.name.to_owned(),
                    table: c.table.clone(),
                    alias: None,
                    function: c.function.clone(),
                })],
                None => vec![],
            },
            table: c.table.clone(),
            name: match c.alias {
                Some(ref a) => a.clone(),
                None => c.name.clone(),
            },
            function: c.function.clone(),
        }
    }
}

#[cfg(test)]
impl<'a> From<&'a str> for Column {
    fn from(c: &str) -> Column {
        match c.find('.') {
            None => Column {
                table: None,
                name: String::from(c),
                function: None,
                aliases: vec![],
            },
            Some(i) => Column {
                name: String::from(&c[i + 1..]),
                table: Some(String::from(&c[0..i])),
                function: None,
                aliases: vec![],
            },
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Column) -> bool {
        (self.name == other.name && self.table == other.table)
            || self.aliases.contains(other)
            || other.aliases.contains(self)
    }
}

impl Eq for Column {}

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
    fn column_from_nomsql_column() {
        let nsc = nom_sql::Column::from("t.col");
        assert_eq!(
            Column::from(nsc),
            Column {
                table: Some("t".into()),
                name: "col".into(),
                function: None,
                aliases: vec![],
            }
        );

        let nsc_with_alias = nom_sql::Column {
            name: "colwa".into(),
            table: Some("t".into()),
            alias: Some("al".into()),
            function: None,
        };
        assert_eq!(
            Column::from(nsc_with_alias),
            Column {
                table: Some("t".into()),
                name: "al".into(),
                function: None,
                aliases: vec![Column {
                    table: Some("t".into()),
                    name: "colwa".into(),
                    function: None,
                    aliases: vec![],
                }],
            }
        );
    }

    #[test]
    fn column_equality() {
        let c = Column::new(Some("t"), "col");
        let ac = Column {
            table: Some("t".into()),
            name: "al".into(),
            function: None,
            aliases: vec![c.clone()],
        };

        // column is equal to itself
        assert_eq!(c, c);

        // column is equal to its aliases
        assert_eq!(ac, c);

        // column with aliases is equal to the same column without aliases set
        let ac_no_alias = Column::new(Some("t"), "al");
        assert_eq!(ac, ac_no_alias);
    }
}
