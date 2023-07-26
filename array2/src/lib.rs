//! A fixed-width, immutable, two-dimensional array
//!
//! This crate provides the [`Array2`] type, which provides efficient storage of a fixed-width,
//! two-dimensional vector.
//!
//! # Examples
//!
//! [`Array2`] can be constructed from a list of equal-length rows:
//!
//! ```rust
//! use array2::Array2;
//!
//! let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
//! assert_eq!(my_array2.num_rows(), 2);
//! assert_eq!(my_array2.row_size(), 3);
//! ```
//!
//! # Internals
//!
//! Internally, values are stored in a single continuous allocation row-first, alongside the length
//! of the row.

#![feature(core_intrinsics, int_roundings)]
use std::fmt::Debug;
use std::intrinsics::unlikely;
use std::ops::{Index, IndexMut};
use std::usize;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur when constructing a [`Array2`]
#[derive(Debug, Error)]
pub enum Error {
    /// An empty vector was passed to [`Array2::try_from_rows`]
    #[error("Empty vector passed when constructing a Array2")]
    Empty,

    /// Rows of different size were passed to [`Array2::try_from_rows`]
    #[error(
        "Inconsistent row size at row {row_index}; expected {row_size}, but got {actual_size}"
    )]
    InconsistentRowSize {
        row_index: usize,
        row_size: usize,
        actual_size: usize,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// A fixed-width two-dimensional vector.
///
/// See [the crate documentation][crate] for more information.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Array2<T> {
    row_size: usize,
    cells: Box<[T]>,
}

impl<T: Debug> Debug for Array2<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.rows()).finish()
    }
}

impl<T> Array2<T> {
    /// Construct a [`Array2`] from a vector of rows
    ///
    /// # Panics
    ///
    /// * Panics if passed an empty vector
    /// * Panics if any of the rows are a different size
    #[must_use]
    #[inline]
    #[track_caller]
    pub fn from_rows(rows: Vec<Vec<T>>) -> Self {
        match Self::try_from_rows(rows) {
            Ok(r) => r,
            Err(e) => panic!("{}", e),
        }
    }

    /// Construct a [`Array2`] from a vector of rows, returning an error instead of panicking if
    /// passed an empty vector or if the rows are a different size.
    #[inline]
    pub fn try_from_rows(rows: Vec<Vec<T>>) -> Result<Self> {
        let row_size = rows.first().ok_or(Error::Empty)?.len();
        let mut elems = Vec::with_capacity(row_size * rows.len());

        for (row_index, row) in rows.into_iter().enumerate() {
            if unlikely(row.len() != row_size) {
                return Err(Error::InconsistentRowSize {
                    row_index,
                    row_size,
                    actual_size: row.len(),
                });
            }
            elems.extend(row);
        }

        Ok(Self::from_cells_and_row_size(elems, row_size))
    }

    /// Construct an [`Array2`] from a vector of cells and row size.
    ///
    /// # Panics
    ///
    /// Panics if the number of cells is not divisible by the row size.
    #[inline]
    pub fn from_cells_and_row_size(cells: impl Into<Box<[T]>>, row_size: usize) -> Self {
        let cells = cells.into();
        assert_eq!(cells.len() % row_size, 0);
        Self { cells, row_size }
    }

    /// Returns the total number of cells (rows times row size) in this [`Array2`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// assert_eq!(my_array2.num_cells(), 6);
    /// ```
    #[must_use]
    #[inline]
    pub fn num_cells(&self) -> usize {
        self.cells.len()
    }

    /// Returns the total number of rows in this [`Array2`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// assert_eq!(my_array2.num_rows(), 2);
    /// ```
    #[must_use]
    #[inline]
    pub fn num_rows(&self) -> usize {
        debug_assert_eq!(self.num_cells() % self.row_size, 0);
        self.num_cells() / self.row_size
    }

    /// Returns the length of the rows in this [`Array2`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// assert_eq!(my_array2.row_size(), 3);
    /// ```
    #[must_use]
    #[inline]
    pub fn row_size(&self) -> usize {
        self.row_size
    }

    /// Returns the shape of this [`Array2`], represented as a tuple of the row size and number of
    /// rows
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// assert_eq!(my_array2.shape(), (3, 2));
    /// ```
    #[must_use]
    #[inline]
    pub fn shape(&self) -> (usize, usize) {
        (self.row_size, self.num_rows())
    }

    /// Construct an iterator over slices of the rows in this [`Array2`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// assert_eq!(
    ///     my_array2.rows().collect::<Vec<_>>(),
    ///     vec![vec![1, 2, 3], vec![4, 5, 6]]
    /// );
    /// ```
    #[inline]
    pub fn rows(
        &self,
    ) -> impl Iterator<Item = &[T]> + ExactSizeIterator + DoubleEndedIterator + '_ {
        self.cells.chunks(self.row_size)
    }

    /// Construct an iterator over pairs of `((row, column), &value)` in this [`Array2`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2], vec![3, 4]]);
    ///
    /// assert_eq!(
    ///     my_array2.entries().collect::<Vec<_>>(),
    ///     vec![((0, 0), &1), ((0, 1), &2), ((1, 0), &3), ((1, 1), &4)]
    /// )
    /// ```
    #[inline]
    pub fn entries(&self) -> impl Iterator<Item = ((usize, usize), &T)> + ExactSizeIterator + '_ {
        self.cells.iter().enumerate().map(move |(i, v)| {
            let row = i.div_floor(self.row_size);
            let col = i % self.row_size;
            ((row, col), v)
        })
    }

    /// Construct an iterator over pairs of `((row, column), value)` in this [`Array2`], consuming
    /// `self`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2], vec![3, 4]]);
    ///
    /// assert_eq!(
    ///     my_array2.into_entries().collect::<Vec<_>>(),
    ///     vec![((0, 0), 1), ((0, 1), 2), ((1, 0), 3), ((1, 1), 4)]
    /// )
    /// ```
    #[inline]
    pub fn into_entries(self) -> impl Iterator<Item = ((usize, usize), T)> + ExactSizeIterator {
        self.cells
            .into_vec()
            .into_iter()
            .enumerate()
            .map(move |(i, v)| {
                let row = i.div_floor(self.row_size);
                let col = i % self.row_size;
                ((row, col), v)
            })
    }

    /// Returns a slice of the cells in this [`Array2`], in row-first order.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// assert_eq!(my_array2.cells(), &[1, 2, 3, 4, 5, 6]);
    /// ```
    #[must_use]
    #[inline]
    pub fn cells(&self) -> &[T] {
        &self.cells
    }

    /// Returns a mutable slice of the cells in this [`Array2`], in row-first order.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let mut my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// my_array2.cells_mut()[3] *= 10;
    /// assert_eq!(my_array2[(1, 0)], 40);
    /// ```
    #[must_use]
    #[inline]
    pub fn cells_mut(&mut self) -> &mut [T] {
        &mut self.cells
    }

    /// Convert this [`Array2`] into an owned vector of cells
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// assert_eq!(my_array2.into_cells(), vec![1, 2, 3, 4, 5, 6]);
    /// ```
    #[must_use]
    #[inline]
    pub fn into_cells(self) -> Vec<T> {
        self.cells.into_vec()
    }

    /// Transform this [`Array2`] by mapping a function which can return an error over all the
    /// entries in the [`Array2`], represented as tuples of `((row, column), value)`. If the
    /// function returns an error for any entry, this entire method will return an error
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2], vec![3, 4]]);
    ///
    /// let res1 = my_array2
    ///     .clone()
    ///     .try_map_cells(|(_, i)| -> Result<i32, String> { Ok(i + 1) });
    /// assert_eq!(
    ///     res1.unwrap(),
    ///     Array2::from_rows(vec![vec![2, 3], vec![4, 5]])
    /// );
    ///
    /// let res2 = my_array2.try_map_cells(|(_, i)| -> Result<i32, String> {
    ///     if i == 3 {
    ///         Err("I hate the number three".into())
    ///     } else {
    ///         Ok(i + 1)
    ///     }
    /// });
    /// assert_eq!(res2.unwrap_err(), "I hate the number three");
    /// ```
    pub fn try_map_cells<F, R, E>(self, f: F) -> std::result::Result<Array2<R>, E>
    where
        F: FnMut(((usize, usize), T)) -> std::result::Result<R, E>,
    {
        let row_size = self.row_size;
        Ok(Array2::from_cells_and_row_size(
            self.into_entries()
                .map(f)
                .collect::<std::result::Result<Vec<_>, _>>()?,
            row_size,
        ))
    }

    /// Returns a reference to a row or cell depending on the type of the index, or `None` if the
    /// index is out of bounds.
    ///
    /// * If given a single index, returns a reference to the row at that index
    /// * If given a pair of indexes, treats that pair as a row index and column index respectively
    ///   and returns a reference to that cell.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// assert_eq!(my_array2.get(0), Some(&[1, 2, 3][..]));
    /// assert_eq!(my_array2.get(1), Some(&[4, 5, 6][..]));
    /// assert_eq!(my_array2.get(2), None);
    ///
    /// assert_eq!(my_array2.get((0, 1)), Some(&2));
    /// assert_eq!(my_array2.get((1, 2)), Some(&6));
    /// assert_eq!(my_array2.get((2, 0)), None);
    /// ```
    #[must_use]
    #[inline]
    pub fn get<I>(&self, index: I) -> Option<&<I as Array2Index<T>>::Output>
    where
        I: Array2Index<T>,
    {
        index.get(self)
    }

    /// Returns a reference to a row or cell depending on the type of the index (see [`get`]).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let mut my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// if let Some(elem) = my_array2.get_mut((0, 1)) {
    ///     *elem = 42
    /// }
    ///
    /// assert_eq!(my_array2[(0, 1)], 42)
    /// ```
    #[must_use]
    #[inline]
    pub fn get_mut<I>(&mut self, index: I) -> Option<&mut <I as Array2Index<T>>::Output>
    where
        I: Array2Index<T>,
    {
        index.get_mut(self)
    }

    /// Construct an iterator over to the [`Column`]s of this [`Array2`], which themselves are
    /// iterators over references to cells.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let mut my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// let transposed = my_array2
    ///     .columns()
    ///     .map(|col| col.copied().collect())
    ///     .collect::<Vec<Vec<_>>>();
    ///
    /// assert_eq!(transposed, vec![vec![1, 4], vec![2, 5], vec![3, 6]]);
    /// ```
    #[must_use]
    #[inline]
    pub fn columns(&self) -> Columns<T> {
        Columns { col: 0, arr2: self }
    }

    /// Return a reference to a single column of this [`Array2`], which is itself an iterator over
    /// references to the cells in that column
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let mut my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// let col = my_array2.get_column(1).unwrap().collect::<Vec<_>>();
    /// assert_eq!(col, vec![&2, &5]);
    /// ```
    #[must_use]
    #[inline]
    pub fn get_column(&self, col: usize) -> Option<Column<T>> {
        if col >= self.row_size() {
            None
        } else {
            Some(Column {
                row: 0,
                col,
                arr2: self,
            })
        }
    }

    /// Construct a new [`Array2`] of the same shape as `self` but with a different value type, by
    /// mapping a function over all the values
    ///
    /// # Examples
    ///
    /// ```rust
    /// use array2::Array2;
    ///
    /// let mut my_array2: Array2<i32> = Array2::from_rows(vec![vec![1, 2, 3], vec![4, 5, 6]]);
    ///
    /// let res: Array2<String> = my_array2.map(|i| i.to_string());
    /// assert_eq!(
    ///     res,
    ///     Array2::from_rows(vec![
    ///         vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
    ///         vec!["4".to_owned(), "5".to_owned(), "6".to_owned()]
    ///     ])
    /// );
    /// ```
    #[inline]
    pub fn map<F, R>(&self, f: F) -> Array2<R>
    where
        F: Fn(&T) -> R,
    {
        Array2 {
            row_size: self.row_size,
            cells: self.cells.iter().map(f).collect(),
        }
    }
}

mod private {
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for (usize, usize) {}
}

/// Trait for types which can be used to index into a [`Array2`].
///
/// This trait is used to implement [`Array2::get`], [`Array2::get_mut`], and the implementations of
/// the [`Index`] and [`IndexMut`] traits for [`Array2`]
pub trait Array2Index<T>: private::Sealed + Sized {
    type Output: ?Sized;
    fn get(self, array2: &Array2<T>) -> Option<&Self::Output>;
    fn get_mut(self, array2: &mut Array2<T>) -> Option<&mut Self::Output>;

    #[inline]
    #[track_caller]
    fn index(self, array2: &Array2<T>) -> &Self::Output {
        self.get(array2)
            .unwrap_or_else(|| panic!("Array2 index out of bounds"))
    }

    #[inline]
    #[track_caller]
    fn index_mut(self, array2: &mut Array2<T>) -> &mut Self::Output {
        self.get_mut(array2)
            .unwrap_or_else(|| panic!("Array2 index out of bounds"))
    }
}

impl<T> Array2Index<T> for usize {
    type Output = [T];

    #[inline]
    fn get(self, array2: &Array2<T>) -> Option<&Self::Output> {
        if self >= array2.num_rows() {
            None
        } else {
            Some(&array2.cells[(array2.row_size * self)..(array2.row_size * (self + 1))])
        }
    }

    #[inline]
    fn get_mut(self, array2: &mut Array2<T>) -> Option<&mut Self::Output> {
        if self >= array2.num_rows() {
            None
        } else {
            Some(&mut array2.cells[(array2.row_size * self)..(array2.row_size * (self + 1))])
        }
    }
}

impl<T> Array2Index<T> for (usize, usize) {
    type Output = T;

    #[inline]
    fn get(self, array2: &Array2<T>) -> Option<&Self::Output> {
        let (row, col) = self;
        if row >= array2.num_rows() || col >= array2.row_size {
            None
        } else {
            Some(&array2.cells[array2.row_size * row + col])
        }
    }

    #[inline]
    fn get_mut(self, array2: &mut Array2<T>) -> Option<&mut Self::Output> {
        let (row, col) = self;
        if row >= array2.num_rows() || col >= array2.row_size {
            None
        } else {
            Some(&mut array2.cells[array2.row_size * row + col])
        }
    }
}

impl<T, I> Index<I> for Array2<T>
where
    I: Array2Index<T>,
{
    type Output = <I as Array2Index<T>>::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        index.index(self)
    }
}

impl<T, I> IndexMut<I> for Array2<T>
where
    I: Array2Index<T>,
{
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        index.index_mut(self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Columns<'a, T> {
    col: usize,
    arr2: &'a Array2<T>,
}

#[derive(Debug, Clone, Copy)]
pub struct Column<'a, T> {
    row: usize,
    col: usize,
    arr2: &'a Array2<T>,
}

/// An iterator over the [`Column`]s of an [`Array2`]. Constructed via [`Array2::columns`].
impl<'a, T> Iterator for Columns<'a, T> {
    type Item = Column<'a, T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let res = if self.col >= self.arr2.row_size {
            None
        } else {
            Some(Column {
                row: 0,
                col: self.col,
                arr2: self.arr2,
            })
        };
        self.col += 1;
        res
    }
}

/// An iterator over the cells in a single column of an [`Array2`]. Yielded as the iterated item by
/// [`Columns`].
impl<'a, T> Iterator for Column<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.arr2.get((self.row, self.col));
        self.row += 1;
        res
    }
}
