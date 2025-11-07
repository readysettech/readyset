//! This crate provides miscellaneous utilities and extensions to the Rust standard library, for use
//! in all crates in this workspace.

#![deny(missing_docs, rustdoc::missing_crate_level_docs)]

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::mem::size_of_val;
use std::sync::Arc;

#[cfg(feature = "failure_injection")]
pub mod failpoints;

pub mod arbitrary;
pub mod display;
pub mod fmt;
pub mod futures;
pub mod graphviz;
pub mod hash;
pub mod indexmap;
pub mod intervals;
pub mod iter;
pub mod logging;
pub mod math;
pub mod progress;
pub mod properties;
pub mod ranges;
pub mod redacted;
pub mod shared_cache;
pub mod shutdown;

mod time_scope;
pub use time_scope::time_scope;

/// Error (returned by [`Indices::indices`] and [`Indices::cloned_indices`]) for an out-of-bounds
/// index access
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct IndexOutOfBounds<Idx>(Idx);

/// Extension trait to add the [`indices`] and [`cloned_indices`] methods to all types that
/// implement [`Index`]
pub trait Indices<'idx, Idx: 'idx> {
    /// The type of values in self, returned as elements of the Vec returned by both [`indices`] and
    /// [`cloned_indices`]
    type Output;

    /// Return a vector of references to all the values in self corresponding to the indices in
    /// `indices`, or, if any of the indices were out of bounds, an error indicating the first such
    /// out-of-bound index
    ///
    /// # Examples
    ///
    /// ```rust
    /// use readyset_util::Indices;
    ///
    /// let v = vec![0, 1, 2, 3, 4];
    /// let res = v.indices(vec![1, 2, 3]).unwrap();
    /// assert_eq!(res, vec![&1, &2, &3]);
    /// ```
    fn indices<'a, I>(&'a self, indices: I) -> Result<Vec<&'a Self::Output>, IndexOutOfBounds<Idx>>
    where
        I: IntoIterator<Item = Idx> + 'idx;

    /// Return a vector of clones of all the values in self corresponding to the indices in
    /// `indices`, or, if any of the indices were out of bounds, an error indicating the first such
    /// out-of-bound index
    ///
    /// # Examples
    ///
    /// ```rust
    /// use readyset_util::Indices;
    ///
    /// let v = vec![0, 1, 2, 3, 4];
    /// let res = v.cloned_indices(vec![1, 2, 3]).unwrap();
    /// assert_eq!(res, vec![1, 2, 3]);
    /// ```
    fn cloned_indices<I>(&self, indices: I) -> Result<Vec<Self::Output>, IndexOutOfBounds<Idx>>
    where
        I: IntoIterator<Item = Idx> + 'idx,
        Self::Output: Clone;
}

impl<A> Indices<'_, usize> for [A] {
    type Output = A;

    fn indices<I>(&self, indices: I) -> Result<Vec<&Self::Output>, IndexOutOfBounds<usize>>
    where
        I: IntoIterator<Item = usize>,
    {
        indices
            .into_iter()
            .map(|i| self.get(i).ok_or(IndexOutOfBounds(i)))
            .collect()
    }

    fn cloned_indices<I>(&self, indices: I) -> Result<Vec<Self::Output>, IndexOutOfBounds<usize>>
    where
        I: IntoIterator<Item = usize>,
        A: Clone,
    {
        indices
            .into_iter()
            .map(|i| self.get(i).cloned().ok_or(IndexOutOfBounds(i)))
            .collect()
    }
}

impl<'idx, K, Q, V> Indices<'idx, &'idx Q> for HashMap<K, V>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash,
{
    type Output = V;

    fn indices<I>(&self, indices: I) -> Result<Vec<&Self::Output>, IndexOutOfBounds<&'idx Q>>
    where
        I: IntoIterator<Item = &'idx Q>,
    {
        indices
            .into_iter()
            .map(|i| self.get(i).ok_or(IndexOutOfBounds(i)))
            .collect()
    }

    fn cloned_indices<I>(&self, indices: I) -> Result<Vec<Self::Output>, IndexOutOfBounds<&'idx Q>>
    where
        I: IntoIterator<Item = &'idx Q>,
        V: Clone,
    {
        indices
            .into_iter()
            .map(|i| self.get(i).cloned().ok_or(IndexOutOfBounds(i)))
            .collect()
    }
}

impl<'idx, K, Q, V> Indices<'idx, &'idx Q> for BTreeMap<K, V>
where
    K: Eq + Ord + Borrow<Q>,
    Q: Eq + Ord,
{
    type Output = V;

    fn indices<I>(&self, indices: I) -> Result<Vec<&Self::Output>, IndexOutOfBounds<&'idx Q>>
    where
        I: IntoIterator<Item = &'idx Q>,
    {
        indices
            .into_iter()
            .map(|i| self.get(i).ok_or(IndexOutOfBounds(i)))
            .collect()
    }

    fn cloned_indices<I>(&self, indices: I) -> Result<Vec<Self::Output>, IndexOutOfBounds<&'idx Q>>
    where
        I: IntoIterator<Item = &'idx Q>,
        V: Clone,
    {
        indices
            .into_iter()
            .map(|i| self.get(i).cloned().ok_or(IndexOutOfBounds(i)))
            .collect()
    }
}

/// Trait for types that can calculate their deep memory size.
///
/// This trait provides methods to calculate the total memory footprint of a value,
/// including heap-allocated data.
pub trait SizeOf {
    /// Returns the deep size of this value in bytes, including heap allocations.
    fn deep_size_of(&self) -> usize;

    /// Returns whether this value should be considered empty for memory tracking purposes.
    fn is_empty(&self) -> bool;
}

impl<T> SizeOf for Vec<T>
where
    T: SizeOf,
{
    fn deep_size_of(&self) -> usize {
        size_of_val(self) + self.iter().map(|x| x.deep_size_of()).sum::<usize>()
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<T> SizeOf for Box<[T]>
where
    T: SizeOf,
{
    fn deep_size_of(&self) -> usize {
        size_of_val(self) + self.iter().map(|x| x.deep_size_of()).sum::<usize>() + 8
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<T> SizeOf for Arc<T>
where
    T: SizeOf,
{
    fn deep_size_of(&self) -> usize {
        (**self).deep_size_of()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }
}

impl<H, T> SizeOf for triomphe::ThinArc<H, T> {
    fn deep_size_of(&self) -> usize {
        size_of::<Self>() + size_of_val(&self.slice)
    }

    fn is_empty(&self) -> bool {
        self.slice.is_empty()
    }
}

impl SizeOf for &str {
    fn deep_size_of(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for String {
    fn deep_size_of(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        false
    }
}

macro_rules! sizeof_integer {
    ($t:ty) => {
        impl SizeOf for $t {
            fn deep_size_of(&self) -> usize {
                size_of::<$t>()
            }

            fn is_empty(&self) -> bool {
                false
            }
        }
    };
}

sizeof_integer!(u8);
sizeof_integer!(u16);
sizeof_integer!(u32);
sizeof_integer!(u64);
sizeof_integer!(u128);
sizeof_integer!(i8);
sizeof_integer!(i16);
sizeof_integer!(i32);
sizeof_integer!(i64);
sizeof_integer!(i128);

impl<A, B> SizeOf for (A, B)
where
    A: SizeOf,
    B: SizeOf,
{
    fn deep_size_of(&self) -> usize {
        size_of::<A>() + size_of::<B>()
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<T> SizeOf for Option<T>
where
    T: SizeOf,
{
    fn deep_size_of(&self) -> usize {
        size_of::<Self>() + self.as_ref().map_or(0, |v| v.deep_size_of())
    }

    fn is_empty(&self) -> bool {
        self.is_none()
    }
}
