//! Utilities for dealing with intervals ([`Bound`][std::ops::Bound]s and instances of
//! [`RangeBounds`][std::ops::RangeBounds]).

use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};
use Bound::*;
use Ordering::*;

/// Allow `map` on a [`Bound`]
///
/// please sir, can I have some GATs?
pub trait BoundFunctor {
    /// The type parameter for this bound
    type Inner;

    /// Map a function over the endpoint of this bound
    ///
    /// ```rust
    /// use launchpad::intervals::BoundFunctor;
    /// use std::ops::Bound::*;
    ///
    /// assert_eq!(Included(1).map(|x: i32| x + 1), Included(2));
    /// assert_eq!(Excluded(1).map(|x: i32| x + 1), Excluded(2));
    /// assert_eq!(Unbounded.map(|x: i32| x + 1), Unbounded);
    /// ```
    fn map<F, B>(self, f: F) -> Bound<B>
    where
        F: FnMut(Self::Inner) -> B;
}

impl<A> BoundFunctor for Bound<A> {
    type Inner = A;
    fn map<F, B>(self, mut f: F) -> Bound<B>
    where
        F: FnMut(Self::Inner) -> B,
    {
        match self {
            Included(a) => Included(f(a)),
            Excluded(a) => Excluded(f(a)),
            Unbounded => Unbounded,
        }
    }
}

impl<'a, A> BoundFunctor for &'a Bound<A> {
    type Inner = &'a A;
    fn map<F, B>(self, mut f: F) -> Bound<B>
    where
        F: FnMut(Self::Inner) -> B,
    {
        match self {
            Included(ref a) => Included(f(a)),
            Excluded(ref a) => Excluded(f(a)),
            Unbounded => Unbounded,
        }
    }
}

/// Provide `as_ref` for [`Bound`]
///
/// NOTE: This has been submitted upstream to the standard library at
/// https://github.com/rust-lang/rust/pull/80444, so should be removed once that lands
pub trait BoundAsRef<A> {
    /// Convert a [`&Bound<A>`] into a `Bound<&A>`
    ///
    /// ```rust
    /// use launchpad::intervals::BoundAsRef;
    /// use std::ops::Bound;
    /// use Bound::*;
    ///
    /// let bound: Bound<i32> = Included(3);
    /// let _: Bound<&i32> = bound.as_ref();
    /// ```
    fn as_ref(&self) -> Bound<&A>;

    /// Convert a [`&mut Bound<A>`] into a `Bound<&mut A>`
    ///
    /// ```rust
    /// use launchpad::intervals::BoundAsRef;
    /// use std::ops::Bound;
    /// use Bound::*;
    ///
    /// let mut bound: Bound<i32> = Included(3);
    /// let _: Bound<&mut i32> = bound.as_mut();
    /// ```
    fn as_mut(&mut self) -> Bound<&mut A>;
}

impl<A> BoundAsRef<A> for Bound<A> {
    fn as_ref(&self) -> Bound<&A> {
        match self {
            Unbounded => Unbounded,
            Included(ref x) => Included(x),
            Excluded(ref x) => Excluded(x),
        }
    }

    fn as_mut(&mut self) -> Bound<&mut A> {
        match self {
            Unbounded => Unbounded,
            Included(ref mut x) => Included(x),
            Excluded(ref mut x) => Excluded(x),
        }
    }
}

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
    Q: Ord,
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
    Q: Ord,
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
    Q: Eq,
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
    Q: Ord,
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
    Q: Ord,
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
    Q: Ord,
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
    Q: Ord,
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;
    use std::ops::Range;

    #[test]
    fn cmp_endbound_works() {
        assert_eq!(cmp_endbound(Included(&1), Excluded(&1)), Greater)
    }

    proptest! {
        #[test]
        fn covers_reflexive(x: (Bound<i32>, Bound<i32>)) {
            assert!(covers(&x, &x))
        }

        #[test]
        fn covers_transitive(
            r1: (Bound<i32>, Bound<i32>),
            r2: (Bound<i32>, Bound<i32>),
            r3: (Bound<i32>, Bound<i32>)
        ) {
            if covers(&r1, &r2) && covers(&r2, &r3) {
                assert!(covers(&r1, &r3))
            }
        }

        #[test]
        fn infinite_covers_all(r: (Bound<i8>, Bound<i8>)) {
            assert!(covers(&(..), &r));
        }
    }

    proptest! {
        #[test]
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

        #[test]
        fn overlaps_commutative(r1: Range<i8>, r2: Range<i8>) {
            assert_eq!(overlaps(&r1, &r2), overlaps(&r2, &r1));
        }

        #[test]
        fn covers_implies_overlaps(r1: Range<i8>, r2: Range<i8>) {
            if covers(&r1, &r2) {
                assert!(overlaps(&r1, &r2))
            }
        }
    }
}
