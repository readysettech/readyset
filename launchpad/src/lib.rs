#![warn(clippy::dbg_macro)]
//! This crate provides miscellanious utilities and extensions to the Rust standard library, for use
//! in all crates in this workspace.
#![deny(missing_docs, missing_crate_level_docs)]
#![feature(or_patterns, step_trait, step_trait_ext, bound_as_ref)]

use std::ops::Index;

pub mod arbitrary;
pub mod hash;
pub mod intervals;

/// If `a` is `Some`, wrap it in `Ok`. If `a` is `None`, run `b` and return it.
pub fn or_else_result<T, E, F>(a: Option<T>, b: F) -> Result<Option<T>, E>
where
    F: FnOnce() -> Result<Option<T>, E>,
{
    match a {
        Some(a) => Ok(Some(a)),
        None => b(),
    }
}

/// Extension trait to add the [`indices`] and [`cloned_indices`] methods to all types that
/// implement [`Index`]
pub trait Indices<Idx>: Index<Idx> {
    /// Return a vector of references to all the values in self corresponding to the indices in
    /// `indices`
    ///
    /// # Panics
    ///
    /// Panics if the Index operation panics, eg if any of the indices are out of bounds
    ///
    /// # Examples
    ///
    /// ```rust
    /// use launchpad::Indices;
    ///
    /// let v = vec![0, 1, 2, 3, 4];
    /// let res = v.indices(vec![1, 2, 3]);
    /// assert_eq!(res, vec![&1, &2, &3]);
    /// ```
    fn indices<I>(&self, indices: I) -> Vec<&<Self as Index<Idx>>::Output>
    where
        I: IntoIterator<Item = Idx>;

    /// Return a vector of clones of all the values in self corresponding to the indices in
    /// `indices`
    ///
    /// # Panics
    ///
    /// Panics if the Index operation panics, eg if any of the indices are out of bounds
    ///
    /// # Examples
    ///
    /// ```rust
    /// use launchpad::Indices;
    ///
    /// let v = vec![0, 1, 2, 3, 4];
    /// let res = v.cloned_indices(vec![1, 2, 3]);
    /// assert_eq!(res, vec![1, 2, 3]);
    /// ```
    fn cloned_indices<I>(&self, indices: I) -> Vec<<Self as Index<Idx>>::Output>
    where
        I: IntoIterator<Item = Idx>,
        <Self as Index<Idx>>::Output: Clone;
}

impl<A, Idx> Indices<Idx> for A
where
    A: Index<Idx> + ?Sized,
{
    fn indices<I>(&self, indices: I) -> Vec<&<A as Index<Idx>>::Output>
    where
        I: IntoIterator<Item = Idx>,
    {
        indices.into_iter().map(|i| &self[i]).collect()
    }

    fn cloned_indices<I>(&self, indices: I) -> Vec<<A as Index<Idx>>::Output>
    where
        I: IntoIterator<Item = Idx>,
        <A as Index<Idx>>::Output: Clone,
    {
        indices.into_iter().map(|i| self[i].clone()).collect()
    }
}
