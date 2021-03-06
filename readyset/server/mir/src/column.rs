use std::cmp::Ordering;
use std::mem;

use nom_sql::{self, SqlIdentifier};
use serde::{Deserialize, Serialize};

// FIXME: this is _not_ okay! malte knows about it
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    pub table: Option<SqlIdentifier>,
    pub name: SqlIdentifier,
    pub aliases: Vec<Column>,
}

impl Column {
    pub fn new(table: Option<&str>, name: impl Into<SqlIdentifier>) -> Self {
        Column {
            table: table.map(Into::into),
            name: name.into(),
            aliases: vec![],
        }
    }

    pub fn named<S>(name: S) -> Self
    where
        S: Into<SqlIdentifier>,
    {
        Self {
            table: None,
            name: name.into(),
            aliases: vec![],
        }
    }

    pub fn add_alias(&mut self, alias: &Column) {
        self.aliases.push(alias.clone());
    }

    #[must_use]
    pub fn aliased_as(mut self, alias: SqlIdentifier) -> Self {
        let name = mem::replace(&mut self.name, alias);
        self.aliases.push(Column {
            name,
            table: self.table.clone(),
            aliases: vec![],
        });
        self
    }

    /// Return a column that is the same as `self`, but has an additional alias with the same name
    /// as `self`, but the provided table name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use readyset_mir::Column;
    /// let column = Column::new(Some("table_a"), "col").aliased_as_table("table_b");
    ///
    /// // The original column is preserved...
    /// assert_eq!(column, Column::new(Some("table_a"), "col"));
    ///
    /// // ...but an alias for the table is added
    /// assert_eq!(column, Column::new(Some("table_b"), "col"));
    /// ```
    #[must_use]
    pub fn aliased_as_table<T>(mut self, table: T) -> Self
    where
        T: Into<SqlIdentifier>,
    {
        self.aliases.push(Column {
            table: Some(table.into()),
            name: self.name.clone(),
            aliases: vec![],
        });
        self
    }

    pub(crate) fn has_table(&self, table: &SqlIdentifier) -> bool {
        self.table.iter().any(|t| t == table) || self.aliases.iter().any(|c| c.has_table(table))
    }
}

impl From<nom_sql::Column> for Column {
    fn from(c: nom_sql::Column) -> Column {
        Column {
            table: c.table.map(Into::into),
            aliases: vec![],
            name: c.name,
        }
    }
}

impl<'a> From<&'a nom_sql::Column> for Column {
    fn from(c: &'a nom_sql::Column) -> Column {
        Column {
            table: c.table.as_deref().map(Into::into),
            aliases: vec![],
            name: c.name.clone(),
        }
    }
}

#[cfg(test)]
impl<'a> From<&'a str> for Column {
    fn from(c: &str) -> Column {
        match c.find('.') {
            None => Column {
                table: None,
                name: c.into(),
                aliases: vec![],
            },
            Some(i) => Column {
                name: c[i + 1..].into(),
                table: Some(c[0..i].into()),
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

impl PartialEq<nom_sql::Column> for Column {
    fn eq(&self, other: &nom_sql::Column) -> bool {
        (self.name == other.name && self.table.as_deref() == other.table.as_deref())
            || self.aliases.iter().any(|c| c == other)
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
                aliases: vec![],
            }
        );
    }

    #[test]
    fn column_equality() {
        let c = Column::new(Some("t"), "col");
        let ac = Column {
            table: Some("t".into()),
            name: "al".into(),
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
