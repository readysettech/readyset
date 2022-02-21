use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use nom_sql::SqlIdentifier;
use noria_data::DataType;

/// A result set from a Noria query.
#[derive(PartialEq, Eq)]
pub struct Results {
    results: Vec<Vec<DataType>>,
    columns: Arc<[SqlIdentifier]>,
}

impl Results {
    // NOTE: should be pub(crate), but that triggers:
    // https://github.com/rust-lang/rust/issues/69785
    #[doc(hidden)]
    pub fn new(results: Vec<Vec<DataType>>, columns: Arc<[SqlIdentifier]>) -> Self {
        Self { results, columns }
    }

    /// Iterate over references to the returned rows.
    pub fn iter(&self) -> ResultIter<'_> {
        self.into_iter()
    }
}

impl From<Results> for Vec<Vec<DataType>> {
    fn from(val: Results) -> Self {
        val.results
    }
}

impl PartialEq<[Vec<DataType>]> for Results {
    fn eq(&self, other: &[Vec<DataType>]) -> bool {
        self.results == other
    }
}

impl PartialEq<Vec<Vec<DataType>>> for Results {
    fn eq(&self, other: &Vec<Vec<DataType>>) -> bool {
        &self.results == other
    }
}

impl PartialEq<&'_ Vec<Vec<DataType>>> for Results {
    fn eq(&self, other: &&Vec<Vec<DataType>>) -> bool {
        &self.results == *other
    }
}

/// A reference to a row in a result set.
///
/// You can access fields either by numerical index or by field index.
/// If you want to also perform type conversion, use [`ResultRow::get`].
#[derive(PartialEq, Eq)]
pub struct ResultRow<'a> {
    result: &'a [DataType],
    columns: &'a [SqlIdentifier],
}

impl<'a> ResultRow<'a> {
    fn new(row: &'a [DataType], columns: &'a [SqlIdentifier]) -> Self {
        Self {
            result: row,
            columns,
        }
    }
}

impl std::ops::Index<usize> for ResultRow<'_> {
    type Output = DataType;
    fn index(&self, index: usize) -> &Self::Output {
        self.result.get(index).unwrap()
    }
}

impl<'a> ResultRow<'a> {
    /// Retrieve the field of the result by the given name.
    ///
    /// Returns `None` if the given field does not exist.
    pub fn get(&self, field: &str) -> Option<&DataType> {
        let index = self.columns.iter().position(|col| *col == field)?;
        self.result.get(index)
    }
}

impl PartialEq<[DataType]> for ResultRow<'_> {
    fn eq(&self, other: &[DataType]) -> bool {
        self.result == other
    }
}

impl PartialEq<Vec<DataType>> for ResultRow<'_> {
    fn eq(&self, other: &Vec<DataType>) -> bool {
        self.result == other
    }
}

impl PartialEq<&'_ Vec<DataType>> for ResultRow<'_> {
    fn eq(&self, other: &&Vec<DataType>) -> bool {
        &self.result == other
    }
}

impl fmt::Debug for ResultRow<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entries(self.columns.iter().zip(self.result.iter()))
            .finish()
    }
}

impl fmt::Debug for Results {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_list()
            .entries(self.results.iter().map(|r| ResultRow {
                result: r,
                columns: &self.columns,
            }))
            .finish()
    }
}

impl Deref for Results {
    type Target = [Vec<DataType>];
    fn deref(&self) -> &Self::Target {
        &self.results
    }
}

impl AsRef<[Vec<DataType>]> for Results {
    fn as_ref(&self) -> &[Vec<DataType>] {
        &self.results
    }
}

pub struct ResultIter<'a> {
    results: std::slice::Iter<'a, Vec<DataType>>,
    columns: &'a [SqlIdentifier],
}

impl<'a> IntoIterator for &'a Results {
    type Item = ResultRow<'a>;
    type IntoIter = ResultIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        ResultIter {
            results: self.results.iter(),
            columns: &self.columns,
        }
    }
}

impl<'a> Iterator for ResultIter<'a> {
    type Item = ResultRow<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(ResultRow::new(self.results.next()?, self.columns))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.results.size_hint()
    }
}

impl ExactSizeIterator for ResultIter<'_> {}
impl DoubleEndedIterator for ResultIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(ResultRow::new(self.results.next_back()?, self.columns))
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        Some(ResultRow::new(self.results.nth_back(n)?, self.columns))
    }
}

pub struct ResultIntoIter {
    results: std::vec::IntoIter<Vec<DataType>>,
    columns: Arc<[SqlIdentifier]>,
}

impl IntoIterator for Results {
    type Item = Row;
    type IntoIter = ResultIntoIter;
    fn into_iter(self) -> Self::IntoIter {
        ResultIntoIter {
            results: self.results.into_iter(),
            columns: self.columns,
        }
    }
}

impl Iterator for ResultIntoIter {
    type Item = Row;
    fn next(&mut self) -> Option<Self::Item> {
        Some(Row::new(self.results.next()?, &self.columns))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.results.size_hint()
    }
}

impl ExactSizeIterator for ResultIntoIter {}
impl DoubleEndedIterator for ResultIntoIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(Row::new(self.results.next_back()?, &self.columns))
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        Some(Row::new(self.results.nth_back(n)?, &self.columns))
    }
}

/// A single row from a result set.
///
/// You can access fields either by numerical index or by field index.
/// If you want to also perform type conversion, use [`Row::get`].
#[derive(PartialEq, Eq)]
pub struct Row {
    row: Vec<DataType>,
    columns: Arc<[SqlIdentifier]>,
}

impl Row {
    fn new(row: Vec<DataType>, columns: &Arc<[SqlIdentifier]>) -> Self {
        Self {
            row,
            columns: Arc::clone(columns),
        }
    }
}

impl From<Row> for Vec<DataType> {
    fn from(val: Row) -> Vec<DataType> {
        val.row
    }
}

impl PartialEq<[DataType]> for Row {
    fn eq(&self, other: &[DataType]) -> bool {
        self.row == other
    }
}

impl PartialEq<Vec<DataType>> for Row {
    fn eq(&self, other: &Vec<DataType>) -> bool {
        &self.row == other
    }
}

impl PartialEq<&'_ Vec<DataType>> for Row {
    fn eq(&self, other: &&Vec<DataType>) -> bool {
        &self.row == *other
    }
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entries(self.columns.iter().zip(self.row.iter()))
            .finish()
    }
}

impl IntoIterator for Row {
    type Item = DataType;
    type IntoIter = std::vec::IntoIter<DataType>;
    fn into_iter(self) -> Self::IntoIter {
        self.row.into_iter()
    }
}

impl AsRef<[DataType]> for Row {
    fn as_ref(&self) -> &[DataType] {
        &self.row[..]
    }
}

impl Deref for Row {
    type Target = [DataType];
    fn deref(&self) -> &Self::Target {
        &self.row[..]
    }
}

impl std::ops::Index<usize> for Row {
    type Output = DataType;
    fn index(&self, index: usize) -> &Self::Output {
        self.row.get(index).unwrap()
    }
}

impl Row {
    /// Retrieve the field of the result by the given name.
    ///
    /// Returns `None` if the given field does not exist.
    pub fn get(&self, field: &str) -> Option<&DataType> {
        let index = self.columns.iter().position(|col| *col == field)?;
        self.row.get(index)
    }

    /// Remove the value for the field of the result by the given name.
    ///
    /// Returns `None` if the given field does not exist.
    pub fn take(&mut self, field: &str) -> Option<DataType> {
        let index = self.columns.iter().position(|col| *col == field)?;
        self.row
            .get_mut(index)
            .map(|r| std::mem::replace(r, DataType::None))
    }
}
