use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::str;

use common::{Literal, SqlType};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum FunctionExpression {
    Avg(Column, bool),
    Count(Column, bool),
    CountStar,
    Sum(Column, bool),
    Max(Column),
    Min(Column),
    GroupConcat(Column, String),
}

impl Display for FunctionExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FunctionExpression::Avg(ref col, d) if d => {
                write!(f, "avg(distinct {})", col.name.as_str())
            }
            FunctionExpression::Count(ref col, d) if d => {
                write!(f, "count(distinct {})", col.name.as_str())
            }
            FunctionExpression::Sum(ref col, d) if d => {
                write!(f, "sum(distinct {})", col.name.as_str())
            }

            FunctionExpression::Avg(ref col, _) => write!(f, "avg({})", col.name.as_str()),
            FunctionExpression::Count(ref col, _) => write!(f, "count({})", col.name.as_str()),
            FunctionExpression::CountStar => write!(f, "count(all)"),
            FunctionExpression::Sum(ref col, _) => write!(f, "sum({})", col.name.as_str()),
            FunctionExpression::Max(ref col) => write!(f, "max({})", col.name.as_str()),
            FunctionExpression::Min(ref col) => write!(f, "min({})", col.name.as_str()),
            FunctionExpression::GroupConcat(ref col, ref s) => {
                write!(f, "group_concat({}, {})", col.name.as_str(), s)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub alias: Option<String>,
    pub table: Option<String>,
    pub function: Option<Box<FunctionExpression>>,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref table) = self.table {
            write!(f, "{}.{}", table, self.name)?;
        } else {
            write!(f, "{}", self.name)?;
        }
        if let Some(ref alias) = self.alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ColumnConstraint {
    NotNull,
    DefaultValue(Literal),
    AutoIncrement,
}

impl fmt::Display for ColumnConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ColumnConstraint::NotNull => write!(f, "NOT NULL"),
            ColumnConstraint::DefaultValue(ref literal) => {
                write!(f, "DEFAULT {}", literal.to_string())
            }
            ColumnConstraint::AutoIncrement => write!(f, "AUTOINCREMENT"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ColumnSpecification {
    pub column: Column,
    pub sql_type: SqlType,
    pub constraints: Vec<ColumnConstraint>,
}

impl fmt::Display for ColumnSpecification {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.column, self.sql_type)?;
        for constraint in self.constraints.iter() {
            write!(f, " {}", constraint)?;
        }
        Ok(())
    }
}

impl ColumnSpecification {
    pub fn new(c: Column, t: SqlType) -> ColumnSpecification {
        ColumnSpecification {
            column: c,
            sql_type: t,
            constraints: vec![],
        }
    }

    pub fn with_constraints(
        c: Column,
        t: SqlType,
        ccs: Vec<ColumnConstraint>,
    ) -> ColumnSpecification {
        ColumnSpecification {
            column: c,
            sql_type: t,
            constraints: ccs,
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

        assert_eq!(
            c,
            Column {
                name: String::from("col"),
                alias: None,
                table: Some(String::from("table")),
                function: None,
            }
        );
    }
}
