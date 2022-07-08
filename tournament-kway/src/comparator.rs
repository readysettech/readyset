use core::cmp::Ordering;
use std::marker::PhantomData;

/// A [`Comparator`] can compare two items in the tournament to decide which is the winner.
pub trait Comparator<I: ?Sized> {
    /// Compare two results in the tournament, the winner is the smaller of the two as decide
    /// by [`Ordering::Less`]. [`Ordering::Equal`] is concidered a draw, and either contestant
    /// may be chosen as the winner.
    fn cmp(&self, a: &I, b: &I) -> Ordering;
}

/// A [`Comparator`] that choses the smaller result of the two
#[derive(Copy)]
pub struct LessComparator<I: ?Sized + Ord> {
    _p: PhantomData<I>,
}

impl<I: ?Sized + Ord> Default for LessComparator<I> {
    #[inline(always)]
    fn default() -> Self {
        LessComparator { _p: PhantomData }
    }
}

impl<I: ?Sized + Ord> Clone for LessComparator<I> {
    #[inline(always)]
    fn clone(&self) -> Self {
        LessComparator { _p: PhantomData }
    }
}

impl<I: ?Sized + Ord> Comparator<I> for LessComparator<I> {
    #[inline(always)]
    fn cmp(&self, a: &I, b: &I) -> Ordering {
        a.cmp(b)
    }
}

/// A [`Comparator`] that choses the larger result of the two
#[derive(Copy)]
pub struct GreaterComparator<I: ?Sized + Ord> {
    _p: PhantomData<I>,
}

impl<I: ?Sized + Ord> Default for GreaterComparator<I> {
    #[inline(always)]
    fn default() -> Self {
        GreaterComparator { _p: PhantomData }
    }
}

impl<I: ?Sized + Ord> Clone for GreaterComparator<I> {
    #[inline(always)]
    fn clone(&self) -> Self {
        GreaterComparator { _p: PhantomData }
    }
}

impl<I: ?Sized + Ord> Comparator<I> for GreaterComparator<I> {
    #[inline(always)]
    fn cmp(&self, a: &I, b: &I) -> Ordering {
        b.cmp(a)
    }
}
