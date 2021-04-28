use crate::errors::ReadySetError;
use crate::{data::*, ReadySetResult};
use std::convert::TryInto;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

/// A result set from a Noria query.
#[derive(PartialEq, Eq)]
pub struct Results {
    results: Vec<Vec<DataType>>,
    columns: Arc<[String]>,
}

impl Results {
    // NOTE: should be pub(crate), but that triggers:
    // https://github.com/rust-lang/rust/issues/69785
    #[doc(hidden)]
    pub fn new(results: Vec<Vec<DataType>>, columns: Arc<[String]>) -> Self {
        Self { results, columns }
    }

    /// Iterate over references to the returned rows.
    pub fn iter(&self) -> ResultIter<'_> {
        self.into_iter()
    }
}

impl Into<Vec<Vec<DataType>>> for Results {
    fn into(self) -> Vec<Vec<DataType>> {
        self.results
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
    columns: &'a [String],
}

impl<'a> ResultRow<'a> {
    fn new(row: &'a [DataType], columns: &'a [String]) -> Self {
        Self {
            result: row,
            columns,
        }
    }
}

impl std::ops::Index<usize> for ResultRow<'_> {
    type Output = DataType;
    fn index(&self, index: usize) -> &Self::Output {
        &self.result[index]
    }
}

impl std::ops::Index<&'_ str> for ResultRow<'_> {
    type Output = DataType;
    fn index(&self, index: &'_ str) -> &Self::Output {
        let index = self.columns.iter().position(|col| col == index).unwrap();
        &self.result[index]
    }
}

impl<'a> ResultRow<'a> {
    /// Retrieve the field of the result by the given name as a `T`.
    ///
    /// Returns `None` if the given field does not exist.
    pub fn get<T>(&self, field: &str) -> Option<ReadySetResult<T>>
    where
        &'a DataType: TryInto<T, Error = ReadySetError>,
    {
        let index = self.columns.iter().position(|col| col == field)?;
        Some((&self.result[index]).try_into())
    }
}

impl PartialEq<[DataType]> for ResultRow<'_> {
    fn eq(&self, other: &[DataType]) -> bool {
        &self.result[..] == other
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
    columns: &'a [String],
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
        Some(ResultRow::new(self.results.next()?, &self.columns))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.results.size_hint()
    }
}

impl ExactSizeIterator for ResultIter<'_> {}
impl DoubleEndedIterator for ResultIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(ResultRow::new(self.results.next_back()?, &self.columns))
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        Some(ResultRow::new(self.results.nth_back(n)?, &self.columns))
    }
}

pub struct ResultIntoIter {
    results: std::vec::IntoIter<Vec<DataType>>,
    columns: Arc<[String]>,
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
    columns: Arc<[String]>,
}

impl Row {
    fn new(row: Vec<DataType>, columns: &Arc<[String]>) -> Self {
        Self {
            row,
            columns: Arc::clone(columns),
        }
    }
}

impl Into<Vec<DataType>> for Row {
    fn into(self) -> Vec<DataType> {
        self.row
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
        &self.row[index]
    }
}

impl std::ops::Index<&'_ str> for Row {
    type Output = DataType;
    fn index(&self, index: &'_ str) -> &Self::Output {
        let index = self.columns.iter().position(|col| col == index).unwrap();
        &self.row[index]
    }
}

impl Row {
    /// Retrieve the field of the result by the given name as a `T`.
    ///
    /// Returns `None` if the given field does not exist.
    ///
    /// Panics if the value at the given field cannot be turned into a `T`.
    pub fn get<T>(&self, field: &str) -> Option<T>
    where
        for<'a> &'a DataType: Into<T>,
    {
        let index = self.columns.iter().position(|col| col == field)?;
        Some((&self.row[index]).into())
    }

    /// Remove the value for the field of the result by the given name.
    ///
    /// Returns `None` if the given field does not exist.
    pub fn take(&mut self, field: &str) -> Option<DataType> {
        let index = self.columns.iter().position(|col| col == field)?;
        Some(std::mem::replace(&mut self.row[index], DataType::None))
    }
}
