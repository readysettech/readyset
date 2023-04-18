use std::cmp::Ordering;
use std::fmt::Display;
use std::{fmt, mem};

use itertools::Itertools;
use nom_sql::{self, Relation, SqlIdentifier};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    pub table: Option<Relation>,
    pub name: SqlIdentifier,
    pub aliases: Vec<Column>,
}

impl Column {
    pub fn new<T, N>(table: Option<T>, name: N) -> Self
    where
        T: Into<Relation>,
        N: Into<SqlIdentifier>,
    {
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

    pub fn add_table_alias<T>(&mut self, table: T)
    where
        T: Into<Relation>,
    {
        self.aliases.push(Column {
            table: Some(table.into()),
            name: self.name.clone(),
            aliases: vec![],
        });
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
        T: Into<Relation>,
    {
        self.add_table_alias(table);
        self
    }

    pub(crate) fn has_table(&self, table: &Relation) -> bool {
        self.table.iter().any(|t| t == table) || self.aliases.iter().any(|c| c.has_table(table))
    }
}

impl From<nom_sql::Column> for Column {
    fn from(c: nom_sql::Column) -> Column {
        Column {
            table: c.table,
            aliases: vec![],
            name: c.name,
        }
    }
}

impl<'a> From<&'a nom_sql::Column> for Column {
    fn from(c: &'a nom_sql::Column) -> Column {
        Column {
            table: c.table.clone(),
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
        (self.name == other.name && self.table == other.table)
            || self.aliases.iter().any(|c| c == other)
    }
}

impl Eq for Column {}

// NOTE: This technically violates the transitive property. This is not the case after the
// implied_tables rewrite pass but could get us into trouble if called before then
impl Ord for Column {
    fn cmp(&self, other: &Column) -> Ordering {
        match (self.table.as_ref(), other.table.as_ref()) {
            (Some(s), Some(o)) => (s, &self.name).cmp(&(o, &other.name)),
            _ => self.name.cmp(&other.name),
        }
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Column) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.", table.display_unquoted())?;
        }

        write!(f, "{}", self.name)?;

        if f.alternate() && !self.aliases.is_empty() {
            write!(f, " (")?;
            write!(f, "{:#}", self.aliases.iter().join(","))?;
            write!(f, ")")?;
        }

        Ok(())
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
