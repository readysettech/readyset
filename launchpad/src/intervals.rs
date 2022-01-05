//! Utilities for dealing with intervals ([`Bound`][std::ops::Bound]s and instances of
//! [`RangeBounds`][std::ops::RangeBounds]).

use std::cmp::{max_by, min_by, Ordering};
use std::iter::{self, Step};
use std::mem;
use std::ops::{Bound, RangeBounds};
use Bound::*;
use Ordering::*;

/// Converts a `Bound<A>` into an `Option<A>`, which is `None` if the `Bound` is `Unbounded`
/// and `Some` otherwise.
pub fn into_bound_endpoint<A>(bound: Bound<A>) -> Option<A> {
    match bound {
        Unbounded => None,
        Included(x) => Some(x),
        Excluded(x) => Some(x),
    }
}

/// Returns true if the `outer` range fully contains the `inner` range, inclusively.
///
/// Concretely, this is true if the start of `outer` is less than the start of `inner`, and the end
/// of `outer` is greater than the end of `inner`.
///
/// # Examples
///
/// ```rust
/// use std::ops::Bound::*;
/// use launchpad::intervals::covers;
///
/// assert!(covers(&(1..10), &(2..3)));
/// assert!(covers(&(1..=10), &(8..=10)));
/// assert!(covers(&(1..=10), &(8..10)));
/// assert!(covers(
///     &(Excluded(2), Included(5)),
///     &(Excluded(2), Included(4))
/// ));
/// assert!(!covers(&(1..10), &(8..=10)));
/// assert!(!covers(&(1..=10), &(8..11)));
/// assert!(!covers(&(1..10), &(11..=12)));
/// assert!(!covers(&(1..10), &(5..=12)));
/// assert!(!covers(&(0..56), &(56..56)));
/// assert!(!covers(&(56..57), &(56..56)));
/// ```
pub fn covers<Q, R, S>(outer: &R, inner: &S) -> bool
where
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
    S: RangeBounds<Q>,
{
    // empty inner ranges can give false positives for covers using the checks below - specifically
    // if we have outer = x..y and inner = y..y, the end bound of outer is equal to the end bound of
    // inner, even though outer does not cover inner. We check for specifically that case here
    if is_empty(inner)
        && (matches!((outer.end_bound(), inner.start_bound()), (Excluded(x), Included(y)) if x == y)
            || matches!(
                (outer.start_bound(), inner.end_bound()),
                (Included(x), Excluded(y)) if x == y
            ))
    {
        return false;
    }

    match (outer.start_bound(), inner.start_bound()) {
        (Excluded(x), Included(y)) if x >= y => return false,
        (Excluded(x) | Included(x), Excluded(y) | Included(y)) if x > y => return false,
        (x, Unbounded) if x != Unbounded => return false,
        _ => {}
    }

    match (outer.end_bound(), inner.end_bound()) {
        (Excluded(x), Included(y)) if x <= y => return false,
        (Excluded(x) | Included(x), Excluded(y) | Included(y)) if x < y => return false,
        (x, Unbounded) if x != Unbounded => return false,
        _ => {}
    }

    true
}
/// Returns true if the two ranges overlap in any way.
///
/// Concretely, this is true if the start of the first range is less than or equal to the end of the
/// second, *and* the end of the first range is less than or equal to the start of the second.
///
/// This provides a relation that is commutative and reflexive, but not transitive
///
/// # Examples:
///
/// ```rust
/// use launchpad::intervals::overlaps;
///
/// assert!(overlaps(&(0..=0), &(0..)));
/// assert!(overlaps(&(..=0), &(0..10)));
/// assert!(overlaps(&(0..10), &(..=0)));
/// assert!(!overlaps(&(0..10), &(10..)));
/// ```
pub fn overlaps<Q, R, S>(r1: &R, r2: &S) -> bool
where
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
    S: RangeBounds<Q>,
{
    // start lte end
    (match (r1.start_bound(), r2.end_bound()) {
        (Included(x), Excluded(y)) if x >= y => false,
        (Excluded(x), Included(y)) if x >= y => false,
        (Excluded(x) | Included(x), Excluded(y) | Included(y)) if x > y => false,
        _ => true,
    }) && (
        // and end gte start
        match (r1.end_bound(), r2.start_bound()) {
            (Included(x), Excluded(y)) if x <= y => false,
            (Excluded(x), Included(y)) if x <= y => false,
            (Excluded(x) | Included(x), Excluded(y) | Included(y)) if x < y => false,
            _ => true,
        }
    )
}

/// Returns true if the given range is empty.
///
/// # Examples
///
/// ```rust
/// use launchpad::intervals;
/// use std::ops::Bound::*;
///
/// assert!(intervals::is_empty(&(1..1)));
/// assert!(intervals::is_empty(&(Excluded(1), Included(1))));
/// assert!(!intervals::is_empty(&(1..=1)));
/// ```
pub fn is_empty<Q, R>(r: &R) -> bool
where
    Q: Eq + ?Sized,
    R: RangeBounds<Q>,
{
    matches!(
        (r.start_bound(), r.end_bound()),
        (Included(x), Excluded(y)) | (Excluded(x), Included(y))
            if x == y)
}

/// Compare two bounds that are at the start of an interval
///
/// This is necessary rather than just having a single compare method on bounds because at the [end
/// of an interval][cmp_endbound], [`Excluded(x)`][Bound::Excluded] is *less* than
/// [`Included(x)`][Bound::Included], whereas at the start of an interval it's *greater*:
///
/// ```rust
/// use std::ops::Bound::*;
/// use launchpad::intervals::cmp_startbound;
/// use std::cmp::Ordering;
///
/// assert_eq!(cmp_startbound(Excluded(&10), Included(&10)), Ordering::Greater);
/// ```
pub fn cmp_startbound<Q>(b1: Bound<&Q>, b2: Bound<&Q>) -> Ordering
where
    Q: Ord + ?Sized,
{
    let r1_min = match b1 {
        Included(x) => Some((x, 1)),
        Excluded(x) => Some((x, 2)),
        Unbounded => None,
    };
    let r2_min = match b2 {
        Included(x) => Some((x, 1)),
        Excluded(x) => Some((x, 2)),
        Unbounded => None,
    };

    match (r1_min, r2_min) {
        (None, None) => Equal,
        (None, Some(_)) => Less,
        (Some(_), None) => Greater,
        (Some(r1), Some(ref r2)) => r1.cmp(r2),
    }
}

/// Compare two bounds that are at the end of an interval
///
/// This is necessary rather than just having a single compare method on bounds because at the end
/// of an interval, [`Excluded(x)`][Bound::Excluded] is *less* than
/// [`Included(x)`][Bound::Included], whereas at the [start of an interval][cmp_startbound] it's
/// *greater*:
///
/// ```rust
/// use std::ops::Bound::*;
/// use launchpad::intervals::cmp_endbound;
/// use std::cmp::Ordering;
///
/// assert_eq!(cmp_endbound(Excluded(&10), Included(&10)), Ordering::Less);
/// ```
pub fn cmp_endbound<Q>(e1: Bound<&Q>, e2: Bound<&Q>) -> Ordering
where
    Q: Ord + ?Sized,
{
    // Based on the encoding idea used in `cmp_startbound`.
    // Note that we have inversed the 2nd value in the tuple,
    // as the Included/Excluded rules are flipped for the upper bound.
    let e1 = match e1 {
        Included(x) => Some((x, 2)),
        Excluded(x) => Some((x, 1)),
        Unbounded => None,
    };
    let e2 = match e2 {
        Included(x) => Some((x, 2)),
        Excluded(x) => Some((x, 1)),
        Unbounded => None,
    };

    match (e1, e2) {
        (None, None) => Equal,
        (None, Some(_)) => Greater,
        (Some(_), None) => Less,
        (Some(r1), Some(ref r2)) => r1.cmp(r2),
    }
}

/// Compare a bound that is at the start of an interval with one at the end of an interval
///
/// This is necessary rather than just having a single compare method on bounds because
/// [`Unbounded`] is positive infinity at the end of an interval, whereas it's *negative* infinity
/// at the start of an interval:
///
/// ```rust
/// use std::ops::Bound::*;
/// use launchpad::intervals::cmp_start_end;
/// use std::cmp::Ordering;
///
/// assert_eq!(cmp_start_end::<i32>(Unbounded, Unbounded), Ordering::Less);
/// ```
pub fn cmp_start_end<Q>(b1: Bound<&Q>, b2: Bound<&Q>) -> Ordering
where
    Q: Ord + ?Sized,
{
    let r1_min = match b1 {
        Included(x) => Some((x, 1)),
        Excluded(x) => Some((x, 2)),
        Unbounded => None,
    };
    let r2_min = match b2 {
        Included(x) => Some((x, 1)),
        Excluded(x) => Some((x, 2)),
        Unbounded => None,
    };

    match (r1_min, r2_min) {
        (None, None) => Less,
        (None, Some(_)) => Less,
        (Some(_), None) => Less,
        (Some(r1), Some(ref r2)) => r1.cmp(r2),
    }
}

/// Compare a bound that is at the end of an interval with one at the start of an interval
///
/// This is necessary rather than just having a single compare method on bounds because
/// [`Unbounded`] is positive infinity at the end of an interval, whereas it's *negative* infinity
/// at the start of an interval:
///
/// ```rust
/// use std::ops::Bound::*;
/// use launchpad::intervals::cmp_end_start;
/// use std::cmp::Ordering;
///
/// assert_eq!(cmp_end_start::<i32>(Unbounded, Unbounded), Ordering::Greater);
/// ```
pub fn cmp_end_start<Q>(e1: Bound<&Q>, e2: Bound<&Q>) -> Ordering
where
    Q: Ord + ?Sized,
{
    // Based on the encoding idea used in `cmp_startbound`.
    // Note that we have inversed the 2nd value in the tuple,
    // as the Included/Excluded rules are flipped for the upper bound.
    let e1 = match e1 {
        Included(x) => Some((x, 2)),
        Excluded(x) => Some((x, 1)),
        Unbounded => None,
    };
    let e2 = match e2 {
        Included(x) => Some((x, 2)),
        Excluded(x) => Some((x, 1)),
        Unbounded => None,
    };

    match (e1, e2) {
        (None, None) => Greater,
        (None, Some(_)) => Greater,
        (Some(_), None) => Greater,
        (Some(r1), Some(ref r2)) => r1.cmp(r2),
    }
}

/// A pair of (lower, upper) [bound][0]s on `T`
///
/// [0]: std::ops::Bound
pub type BoundPair<T> = (Bound<T>, Bound<T>);

/// An iterator over all the values of `T` between a pair of [`Bound`]s. Constructed by
/// [`IterBoundPair::into_iter`].
#[derive(Clone)]
pub struct BoundPairIter<T> {
    current: T,
    upper: Bound<T>,
}

impl<T> Iterator for BoundPairIter<T>
where
    T: Step + Ord + Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.upper.as_ref().map(|upper| upper.clone()) {
            Excluded(upper) if self.current >= upper => None,
            Included(upper) if self.current > upper => None,
            _ => {
                let new = Step::forward(self.current.clone(), 1);
                Some(mem::replace(&mut self.current, new))
            }
        }
    }
}

/// Extension trait to provide an `into_iter` method on a pair of [`Bound`]s
pub trait IterBoundPair<T>
where
    T: Step,
{
    /// Construct an iterator over all the values of `T` between the lower and upper bound of a
    /// [`BoundPair`].
    ///
    /// Will return None if the lower bound is [`Unbounded`]
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::*;
    /// use launchpad::intervals::IterBoundPair;
    ///
    /// assert_eq!(
    ///     (Excluded(4_u32), Included(6_u32)).into_iter().unwrap().collect::<Vec<u32>>(),
    ///     vec![5_u32, 6_u32]
    /// );
    /// ```
    fn into_iter(self) -> Option<BoundPairIter<T>>;
}

impl<T> IterBoundPair<T> for BoundPair<T>
where
    T: Step,
{
    fn into_iter(self) -> Option<BoundPairIter<T>> {
        Some(BoundPairIter {
            current: match self.0 {
                Included(x) => x,
                Excluded(x) => Step::forward(x, 1),
                Unbounded => return None,
            },
            upper: self.1,
        })
    }
}

/// An iterator over the result(s) of removing one interval from another
#[allow(clippy::type_complexity)]
pub enum DifferenceIterator<'a, Q, R> {
    /// Fully removed, for the case when the removed interval fully covers the operand
    Empty,
    /// Unchanged, for the case when the intervals dont overlap each other at all
    Unchanged(iter::Once<&'a R>),
    /// The case where the second interval overlaps the beginning of the first
    Single(iter::Once<(Bound<&'a Q>, Bound<&'a Q>)>),
    /// The case where the first interval fully covers the second (so we need to yield two ranges)
    Double(
        iter::Chain<
            iter::Once<(Bound<&'a Q>, Bound<&'a Q>)>,
            iter::Once<(Bound<&'a Q>, Bound<&'a Q>)>,
        >,
    ),
}

impl<'a, Q, R> Iterator for DifferenceIterator<'a, Q, R>
where
    R: RangeBounds<Q>,
{
    type Item = (Bound<&'a Q>, Bound<&'a Q>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::Unchanged(inner) => inner.next().map(|r| (r.start_bound(), r.end_bound())),
            Self::Single(inner) => inner.next(),
            Self::Double(inner) => inner.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Empty => (0, Some(0)),
            Self::Unchanged(_) => (1, Some(1)),
            Self::Single(_) => (1, Some(1)),
            Self::Double(_) => (2, Some(2)),
        }
    }
}

impl<'a, Q, R> ExactSizeIterator for DifferenceIterator<'a, Q, R> where R: RangeBounds<Q> {}

/// Returns an iterator over the result(s) of removing the second interval from the first.
///
/// The result of this is either zero, one, or two intervals:
///   * zero, if the second interval fully covers the first,
///   * one, if the second interval overlaps the start or end of the first, or if the intervals do
///     not overlap at all
///   * two, if the first interval fully covers the second
///
/// # Examples
///
/// ```rust
/// use launchpad::intervals::difference;
/// use std::ops::Bound::*;
///
/// // When the intervals don't overlap at all, the first interval is returned unchanged
/// assert_eq!(
///     difference(&(1..2), &(3..4)).collect::<Vec<_>>(),
///     vec![(Included(&1), Excluded(&2))]
/// );
/// ```
pub fn difference<'a, Q, R, S>(r1: &'a R, r2: &'a S) -> DifferenceIterator<'a, Q, R>
where
    R: RangeBounds<Q>,
    S: RangeBounds<Q>,
    Q: Ord,
{
    if !overlaps(r1, r2) {
        DifferenceIterator::Unchanged(iter::once(r1))
    } else {
        let below = if matches!(
            cmp_startbound(r2.start_bound(), r1.start_bound()),
            Less | Equal
        ) {
            None
        } else {
            Some((
                r1.start_bound(),
                match r2.start_bound() {
                    Included(x) => Excluded(x),
                    Excluded(x) => Included(x),
                    // This should be impossible (since -inf would always get hit by the `matches!`
                    // above), but best to cover it anyway
                    Unbounded => Unbounded,
                },
            ))
        };

        let above = if matches!(
            cmp_endbound(r2.end_bound(), r1.end_bound()),
            Greater | Equal
        ) {
            None
        } else {
            Some((
                match r2.end_bound() {
                    Included(x) => Excluded(x),
                    Excluded(x) => Included(x),
                    // This should be impossible (since +inf would always get hit by the `matches!`
                    // above), but best to cover it anyway
                    Unbounded => Unbounded,
                },
                r1.end_bound(),
            ))
        };

        match (below, above) {
            (Some(below), Some(above)) => {
                DifferenceIterator::Double(iter::once(below).chain(iter::once(above)))
            }
            (Some(r), None) | (None, Some(r)) => DifferenceIterator::Single(iter::once(r)),
            (None, None) => DifferenceIterator::Empty,
        }
    }
}

/// Returns the (non-empty) intersection of the two ranges, if any exists
///
/// # Examples
///
/// ```rust
/// use launchpad::intervals::intersection;
/// use std::ops::Bound::*;
///
/// // If the intervals don't intersect, returns None
/// assert_eq!(intersection(&(1..3), &(4..8)), None);
///
/// assert_eq!(intersection(&(1..8), &(7..10)), Some((Included(&7), Excluded(&8))));
///
/// // If one interval fully covers another, the smaller interval will be returned unchanged
/// assert_eq!(intersection(&(1..10), &(2..9)), Some((Included(&2), Excluded(&9))));
/// ```
pub fn intersection<'a, Q, R, S>(r1: &'a R, r2: &'a S) -> Option<(Bound<&'a Q>, Bound<&'a Q>)>
where
    R: RangeBounds<Q>,
    S: RangeBounds<Q>,
    Q: Ord,
{
    if cmp_end_start(r1.end_bound(), r2.start_bound()) == Less
        || cmp_end_start(r2.end_bound(), r1.start_bound()) == Less
    {
        return None;
    }

    let lower = max_by(r1.start_bound(), r2.start_bound(), |x, y| {
        cmp_startbound(*x, *y)
    });
    let upper = min_by(r1.end_bound(), r2.end_bound(), |x, y| cmp_endbound(*x, *y));

    if cmp_start_end(lower, upper) == Ordering::Greater {
        None
    } else {
        Some((lower, upper))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cmp_endbound_works() {
        assert_eq!(cmp_endbound(Included(&1), Excluded(&1)), Greater)
    }

    mod covers {
        use super::*;
        use test_strategy::proptest;

        #[proptest]
        fn covers_reflexive(x: (Bound<i32>, Bound<i32>)) {
            assert!(covers(&x, &x))
        }

        #[proptest]
        fn covers_transitive(
            r1: (Bound<i32>, Bound<i32>),
            r2: (Bound<i32>, Bound<i32>),
            r3: (Bound<i32>, Bound<i32>),
        ) {
            if covers(&r1, &r2) && covers(&r2, &r3) {
                assert!(covers(&r1, &r3))
            }
        }

        #[proptest]
        fn infinite_covers_all(r: (Bound<i8>, Bound<i8>)) {
            assert!(covers(&(..), &r));
        }
    }

    mod overlaps {
        use super::*;
        use proptest::prelude::*;
        use std::collections::HashSet;
        use std::ops::Range;
        use test_strategy::proptest;

        #[proptest]
        fn overlaps_is_collect_intersects(r1: Range<i8>, r2: Range<i8>) {
            prop_assume!(!r1.is_empty());
            prop_assume!(!r2.is_empty());
            assert_eq!(
                overlaps(&r1, &r2),
                r1 == r2
                    || r1
                        .collect::<HashSet<_>>()
                        .intersection(&r2.collect::<HashSet<_>>())
                        .count()
                        > 0
            );
        }

        #[proptest]
        fn overlaps_commutative(r1: Range<i8>, r2: Range<i8>) {
            assert_eq!(overlaps(&r1, &r2), overlaps(&r2, &r1));
        }

        #[proptest]
        fn covers_implies_overlaps(r1: Range<i8>, r2: Range<i8>) {
            if covers(&r1, &r2) {
                assert!(overlaps(&r1, &r2))
            }
        }
    }

    mod iter_bound_pair {
        use super::*;

        macro_rules! bound_pair_iter_test {
            ($name: ident, $bound_pair: expr => $results: expr) => {
                #[test]
                fn $name() {
                    assert_eq!(
                        $bound_pair.into_iter().unwrap().collect::<Vec<_>>(),
                        $results.into_iter().collect::<Vec<u32>>()
                    );
                }
            };
        }

        bound_pair_iter_test!(incl_excl, (Included(0u32), Excluded(7)) => (0..7));
        bound_pair_iter_test!(incl_incl, (Included(0u32), Included(7)) => (0..=7));
        bound_pair_iter_test!(excl_incl, (Excluded(0u32), Included(3)) => vec![1, 2, 3]);
        bound_pair_iter_test!(excl_excl, (Excluded(0u32), Excluded(3)) => vec![1, 2]);
        bound_pair_iter_test!(excl_excl_equal, (Excluded(0u32), Excluded(0)) => vec![]);
        bound_pair_iter_test!(excl_incl_equal, (Excluded(0u32), Included(0)) => vec![]);
        bound_pair_iter_test!(excl_incl_backwards, (Excluded(3u32), Included(0)) => vec![]);
    }

    mod difference {
        use super::*;
        use test_strategy::proptest;

        #[test]
        fn empty() {
            assert_eq!(difference(&(5..7), &(1..10)).collect::<Vec<_>>(), vec![])
        }

        #[test]
        fn overlap_beginning() {
            assert_eq!(
                difference(&(5..10), &(1..7)).collect::<Vec<_>>(),
                vec![(Included(&7), Excluded(&10))]
            )
        }

        #[test]
        fn overlap_end() {
            assert_eq!(
                difference(&(5..10), &(7..15)).collect::<Vec<_>>(),
                vec![(Included(&5), Excluded(&7))]
            )
        }

        #[test]
        fn two_intervals() {
            assert_eq!(
                difference(&(1..10), &(5..7)).collect::<Vec<_>>(),
                vec![(Included(&1), Excluded(&5)), (Included(&7), Excluded(&10))]
            )
        }

        #[proptest]
        fn idempotent(r1: (Bound<i8>, Bound<i8>), r2: (Bound<i8>, Bound<i8>)) {
            let once: Vec<_> = difference(&r1, &r2).collect();
            let twice: Vec<_> = once.iter().flat_map(|r| difference(r, &r2)).collect();
            assert_eq!(once, twice);
        }
    }

    mod intersection {
        use super::*;
        use proptest::prop_assume;
        use std::ops::Range;
        use test_strategy::proptest;

        #[proptest]
        fn commutative(r1: Range<i8>, r2: Range<i8>) {
            assert_eq!(intersection(&r1, &r2), intersection(&r2, &r1));
        }

        #[proptest]
        fn is_some_implies_overlaps(r1: Range<i8>, r2: Range<i8>) {
            prop_assume!(!r1.is_empty());
            let int = intersection(&r1, &r2);
            assert_eq!(int.is_some(), overlaps(&r1, &r2));
        }
    }
}
