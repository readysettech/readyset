use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::str;

use common::{Literal, SqlType};
use keywords::escape_if_keyword;
use condition::ConditionExpression;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum FunctionExpression {
    Avg(Column, bool),
    Count(Column, bool),
    CountStar,
    CountFilter(Column, Option<Column>, ConditionExpression),
    Sum(Column, bool),
    SumFilter(Column, Option<Column>, ConditionExpression),
    Max(Column),
    Min(Column),
    GroupConcat(Column, String),
}

impl Display for FunctionExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FunctionExpression::Avg(ref col, d) if d => write!(f, "avg(distinct {})", col),
            FunctionExpression::Count(ref col, d) if d => write!(f, "count(distinct {})", col),
            FunctionExpression::Sum(ref col, d) if d => write!(f, "sum(distinct {})", col),

            FunctionExpression::Avg(ref col, _) => write!(f, "avg({})", col),
            FunctionExpression::Count(ref col, _) => write!(f, "count({})", col),
            FunctionExpression::CountStar => write!(f, "count(*)"),
            FunctionExpression::Sum(ref col, _) => write!(f, "sum({})", col),
            FunctionExpression::CountFilter(ref col, ref else_col, _) => {
                match else_col {
                    Some(ecol) => write!(f, "count(filter {} {})", col, ecol),
                    None => write!(f, "count(filter {})", col),
                }
            }
            FunctionExpression::SumFilter(ref col, ref else_col, _) => {
                match else_col {
                    Some(ecol) => write!(f, "sum(filter {} {})", col, ecol),
                    None => write!(f, "sum(filter {})", col),
                }
            }
            FunctionExpression::Max(ref col) => write!(f, "max({})", col),
            FunctionExpression::Min(ref col) => write!(f, "min({})", col),
            FunctionExpression::GroupConcat(ref col, ref s) => {
                write!(f, "group_concat({}, {})", col, s)
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
            write!(
                f,
                "{}.{}",
                escape_if_keyword(table),
                escape_if_keyword(&self.name)
            )?;
        } else if let Some(ref function) = self.function {
            write!(f, "{}", *function)?;
        } else {
            write!(f, "{}", escape_if_keyword(&self.name))?;
        }
        if let Some(ref alias) = self.alias {
            write!(f, " AS {}", escape_if_keyword(alias))?;
        }
        Ok(())
    }
}

impl<'a> From<&'a str> for Column {
    fn from(c: &str) -> Column {
        match c.find(".") {
            None => Column {
                name: String::from(c),
                alias: None,
                table: None,
                function: None,
            },
            Some(i) => Column {
                name: String::from(&c[i + 1..]),
                alias: None,
                table: Some(String::from(&c[0..i])),
                function: None,
            },
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
    CharacterSet(String),
    Collation(String),
    DefaultValue(Literal),
    AutoIncrement,
    PrimaryKey,
    Unique,
}

impl fmt::Display for ColumnConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ColumnConstraint::NotNull => write!(f, "NOT NULL"),
            ColumnConstraint::CharacterSet(ref charset) => write!(f, "CHARACTER SET {}", charset),
            ColumnConstraint::Collation(ref collation) => write!(f, "COLLATE {}", collation),
            ColumnConstraint::DefaultValue(ref literal) => {
                write!(f, "DEFAULT {}", literal.to_string())
            }
            ColumnConstraint::AutoIncrement => write!(f, "AUTO_INCREMENT"),
            ColumnConstraint::PrimaryKey => write!(f, "PRIMARY KEY"),
            ColumnConstraint::Unique => write!(f, "UNIQUE"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ColumnSpecification {
    pub column: Column,
    pub sql_type: SqlType,
    pub constraints: Vec<ColumnConstraint>,
    pub comment: Option<String>,
}

impl fmt::Display for ColumnSpecification {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {}",
            escape_if_keyword(&self.column.name),
            self.sql_type
        )?;
        for constraint in self.constraints.iter() {
            write!(f, " {}", constraint)?;
        }
        if let Some(ref comment) = self.comment {
            write!(f, " COMMENT '{}'", comment)?;
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
            comment: None,
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
            comment: None,
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

    #[test]
    fn print_function_column() {
        let c1 = Column {
            name: "".into(), // must be present, but will be ignored
            alias: Some("foo".into()),
            table: None,
            function: Some(Box::new(FunctionExpression::CountStar)),
        };
        let c2 = Column {
            name: "".into(), // must be present, but will be ignored
            alias: None,
            table: None,
            function: Some(Box::new(FunctionExpression::CountStar)),
        };
        let c3 = Column {
            name: "".into(), // must be present, but will be ignored
            alias: None,
            table: None,
            function: Some(Box::new(FunctionExpression::Sum(
                Column::from("mytab.foo"),
                false,
            ))),
        };

        assert_eq!(format!("{}", c1), "count(*) AS foo");
        assert_eq!(format!("{}", c2), "count(*)");
        assert_eq!(format!("{}", c3), "sum(mytab.foo)");
    }

}
