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
//! Internally, values are stored in a single continous allocation row-first, alongside the length
//! of the row.

#![feature(core_intrinsics)]
use std::fmt::Debug;
use std::intrinsics::unlikely;
use std::ops::{Index, IndexMut};

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

        Ok(Self {
            row_size,
            cells: elems.into(),
        })
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
