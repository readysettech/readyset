//! Utilities for working with intervals

use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};
use Bound::*;
use Ordering::*;

/// Returns true if the `outer` range fully contains the `inner` range, inclusively
pub(crate) fn covers<Q, R, S>(outer: &R, inner: &S) -> bool
where
    Q: Ord,
    R: RangeBounds<Q>,
    S: RangeBounds<Q>,
{
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
/// Returns true if the two ranges overlap in any way
pub(crate) fn overlaps<Q, R, S>(r1: &R, r2: &S) -> bool
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

pub(crate) fn cmp<Q, R, S>(r1: &R, r2: &S) -> Ordering
where
    Q: Ord,
    R: RangeBounds<Q>,
    S: RangeBounds<Q>,
{
    // Sorting by lower bound, then by upper bound.
    //   -> Unbounded is the smallest lower bound.
    //   -> Unbounded is the biggest upper bound.
    //   -> Included(x) < Excluded(x) for a lower bound.
    //   -> Included(x) > Excluded(x) for an upper bound.

    let start = cmp_startbound(r1.start_bound(), r2.start_bound());

    if start == Equal {
        // Both left-bounds are equal, we have to
        // compare the right-bounds as a tie-breaker.
        cmp_endbound(r1.end_bound(), r2.end_bound())
    } else {
        start
    }
}

pub(crate) fn cmp_startbound<Q>(b1: Bound<&Q>, b2: Bound<&Q>) -> Ordering
where
    Q: Ord,
{
    // Unpacking from a Bound is annoying, so let's map it to an Option<Q>.  Let's use this
    // transformation to encode the Included/Excluded rules at the same time.  Note that
    // lexicographic order is used during comparison, so if r1 and r2 have the same `x`, only then
    // will the 2nd element of the tuple serve as a tie-breaker.
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

pub(crate) fn cmp_start_end<Q>(b1: Bound<&Q>, b2: Bound<&Q>) -> Ordering
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

pub(crate) fn cmp_endbound<Q>(e1: Bound<&Q>, e2: Bound<&Q>) -> Ordering
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

pub(crate) fn cmp_end_start<Q>(e1: Bound<&Q>, e2: Bound<&Q>) -> Ordering
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

pub(crate) fn bound_ref<A>(bound: &Bound<A>) -> Bound<&A> {
    match bound {
        Included(ref v) => Included(v),
        Excluded(ref v) => Excluded(v),
        Unbounded => Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;
    use std::ops::Range;

    #[test]
    fn cmp_works_as_expected() {
        let key0 = ..20;
        let key1 = 1..=5;
        let key2 = 1..7;
        let key3 = 1..=7;
        let key4 = (Excluded(5), Excluded(9));
        let key5 = (Included(7), Included(8));
        let key_str1 = (Included("abc"), Excluded("def"));
        let key_str2 = (Included("bbc"), Included("bde"));
        let key_str3: (_, Bound<&str>) = (Included("bbc"), Unbounded);

        assert_eq!(cmp(&key1, &key1), Equal);
        assert_eq!(cmp(&key1, &key2), Less);
        assert_eq!(cmp(&key2, &key3), Less);
        assert_eq!(cmp(&key0, &key1), Less);
        assert_eq!(cmp(&key4, &key5), Less);
        assert_eq!(cmp::<&str, _, _>(&key_str1, &key_str2), Less);
        assert_eq!(cmp::<&str, _, _>(&key_str2, &key_str3), Less);
    }

    #[test]
    fn cmp_endbound_works() {
        assert_eq!(cmp_endbound(Included(&1), Excluded(&1)), Greater)
    }

    #[test]
    fn covers_works() {
        assert!(covers(&(1..10), &(2..3)));
        assert!(covers(&(1..=10), &(8..=10)));
        assert!(covers(&(1..=10), &(8..10)));
        assert!(covers(
            &(Excluded(2), Included(5)),
            &(Excluded(2), Included(4))
        ));
        assert!(!covers(&(1..10), &(8..=10)));
        assert!(!covers(&(1..=10), &(8..11)));
        assert!(!covers(&(1..10), &(11..=12)));
        assert!(!covers(&(1..10), &(5..=12)));
    }

    proptest! {
        #[test]
        fn covers_equal(x: (Bound<i32>, Bound<i32>)) {
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

    #[test]
    fn overlaps_works() {
        assert!(overlaps(&(0..=0), &(0..)));
        assert!(overlaps(&(..=0), &(0..10)));
        assert!(overlaps(&(0..10), &(..=0)));
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
