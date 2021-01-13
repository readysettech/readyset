//! AST for `sqllogictest` files. See the [SQLite documentation][1] for more information.
//!
//! [1]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::convert::{TryFrom, TryInto};
use std::fmt::Display;

use anyhow::bail;
use ascii_utils::Check;
use derive_more::{From, TryInto};

/// The expected result of a statement
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum StatementResult {
    /// The statement should succeed
    Ok,
    /// The statement should fail
    Error,
}

/// A conditional for either a [`Statement`] or a [`Query`]. Can be used to omit or include tests on
/// specific database engines
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Conditional {
    /// Skip this [`Statement`] or [`Query`] on the database engine with the given name.
    SkipIf(String),
    /// Only run this [`Statement`] or [`Query`] on the database engine with the given name.
    OnlyIf(String),
}

/// Run a statement against the database engine
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Statement {
    /// The expected result of the statement
    pub result: StatementResult,
    /// The SQL string to run
    pub command: String,
    /// Optional list of [`Conditional`]s for the statement
    pub conditionals: Vec<Conditional>,
}

/// The type of a column in the result set of a [`Query`]
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Type {
    Text,
    Integer,
    Real,
}

impl Type {
    pub fn of_mysql_value(val: &mysql::Value) -> Option<Self> {
        use mysql::Value::*;
        match val {
            Bytes(_) => Some(Self::Text),
            Int(_) => Some(Self::Real),
            UInt(_) => Some(Self::Real),
            Float(_) => Some(Self::Real),
            Double(_) => Some(Self::Real),
            _ => None,
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text => write!(f, "Text"),
            Self::Integer => write!(f, "Integer"),
            Self::Real => write!(f, "Real"),
        }
    }
}

/// Result set sorting mode of a query
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum SortMode {
    /// nosort - the default value. In nosort mode, the results appear in exactly the order in which
    /// they were received from the database engine. The nosort mode should only be used on queries
    /// that have an ORDER BY clause or which only have a single row of result, since otherwise the
    /// order of results is undefined and might vary from one database engine to another.
    NoSort,

    /// The "rowsort" mode gathers all output from the database engine then sorts it by rows on the
    /// client side. Sort comparisons use strcmp() on the rendered ASCII text representation of the
    /// values. Hence, "9" sorts after "10", not before.
    RowSort,

    /// The "valuesort" mode works like rowsort except that it does not honor row groupings. Each
    /// individual result value is sorted on its own.
    ValueSort,
}

impl Default for SortMode {
    /// Returns [`Self::NoSort`]
    fn default() -> Self {
        Self::NoSort
    }
}

/// An expected result value from a query
#[derive(Debug, Eq, PartialEq, Clone, TryInto, From)]
pub enum ResultValue {
    Text(String),
    Integer(i64),
    Real(i64, u32),
    Null,
}

impl TryFrom<mysql::Value> for ResultValue {
    type Error = anyhow::Error;

    fn try_from(value: mysql::Value) -> Result<Self, Self::Error> {
        use mysql::Value::*;
        match value {
            NULL => Ok(Self::Null),
            Bytes(bs) => Ok(Self::Text(String::from_utf8(bs)?)),
            Int(i) => Ok(Self::Integer(i)),
            UInt(i) => Ok(Self::Integer(i.try_into()?)),
            Float(f) => Self::try_from(Double(f as f64)),
            Double(f) => {
                if !f.is_finite() {
                    bail!("Invalid infinite float value");
                }
                Ok(Self::Real(
                    f.trunc() as i64,
                    (f.fract() * 1_000_000_000.0).round() as u32,
                ))
            }
            Date(_, _, _, _, _, _, _) => bail!("Invalid column value of type Date"),
            Time(_, _, _, _, _, _) => bail!("Invalid column value of type Time"),
        }
    }
}

impl Display for ResultValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text(s) => {
                if s.is_empty() {
                    write!(f, "(empty)")
                } else {
                    for chr in s.chars() {
                        if chr.is_printable() {
                            write!(f, "{}", chr)?;
                        } else {
                            write!(f, "@")?;
                        }
                    }
                    Ok(())
                }
            }
            Self::Integer(i) => write!(f, "{}", i),
            Self::Real(whole, frac) => write!(f, "{}.{}", whole, &format!("{}", frac)[..3]),
            Self::Null => write!(f, "NULL"),
        }
    }
}

impl PartialOrd for ResultValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResultValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        format!("{}", self).cmp(&format!("{}", other))
    }
}

impl ResultValue {
    pub fn typ(&self) -> Option<Type> {
        match self {
            Self::Text(_) => Some(Type::Text),
            Self::Integer(_) => Some(Type::Integer),
            Self::Real(_, _) => Some(Type::Real),
            Self::Null => None,
        }
    }

    pub fn from_mysql_value_with_type(
        val: mysql::Value,
        typ: &Type,
    ) -> anyhow::Result<ResultValue> {
        if val == mysql::Value::NULL {
            return Ok(Self::Null);
        }
        match typ {
            Type::Text => Ok(Self::Text(mysql::from_value_opt(val)?)),
            Type::Integer => Ok(Self::Integer(mysql::from_value_opt(val)?)),
            Type::Real => {
                let f: f64 = mysql::from_value_opt(val)?;
                Ok(Self::Real(
                    f.trunc() as i64,
                    (f.fract() * 1_000_000_000.0).round() as u32,
                ))
            }
        }
    }

    pub fn hash_results(results: &[Self]) -> md5::Digest {
        let mut context = md5::Context::new();
        for result in results {
            context.consume(&format!("{}", result));
            context.consume("\n");
        }
        context.compute()
    }
}

/// The expected results of a query. Past a [`HashThreshold`][Record::HashThreshold], an [`md5`] sum
/// of the results will be computed and compared.
#[derive(Debug, Eq, PartialEq, Clone, TryInto, From)]
pub enum QueryResults {
    Hash { count: usize, digest: md5::Digest },
    Results(Vec<ResultValue>),
}

/// Run a query against the database engine and check the results against an expected result set
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Query {
    pub label: Option<String>,
    pub column_types: Vec<Type>,
    pub sort_mode: Option<SortMode>,
    pub conditionals: Vec<Conditional>,
    pub query: String,
    pub results: QueryResults,
}

/// Top level expression in a sqllogictest test script
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Record {
    Statement(Statement),
    Query(Query),

    /// The "hash-threshold" record sets a limit on the number of values that can appear in a result
    /// set. If the number of values exceeds this, then instead of recording each individual value
    /// in the full test script, an MD5 hash of all values is computed in stored. This makes the
    /// full test scripts much shorter, but at the cost of obscuring the results. If the
    /// hash-threshold is 0, then results are never hashed. A hash-threshold of 10 or 20 is
    /// recommended. During debugging, it is advantage to set the hash-threshold to zero so that all
    /// results can be seen.
    HashThreshold(usize),

    /// Stop testing and halt immediately. Useful when debugging.
    Halt,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_result_value() {
        assert_eq!(format!("{}", ResultValue::Text("\0".to_string())), "@");
    }

    #[test]
    fn compare_result_value() {
        assert!(ResultValue::Integer(9) > ResultValue::Integer(10));
    }
}
