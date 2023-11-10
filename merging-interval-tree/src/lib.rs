//! Implementation of an interval tree, that may contains only nonoverlapping intervals, and no
//! additional data. Whenever an overlapping interval is added, it gets merged with any intervals it
//! overlaps with. This sort of data structure is useful when one wants to keep track of total
//! interval coverage, without differentiating between individual intervals.
//!
//! # Implementation
//!
//! Internally the [`IntervalTreeSet`] is simply a [`BTreeSet`] of bounds. For each contiguous
//! interval the tree contains the entries for its lower-bound and its upper-bound.  The tree
//! differentiates between lower/upper and exclusive/inclusive/unbounded bounds. The tree upholds
//! two invariants: any two bounds must be different, and every lower bound must be followed by its
//! closing upper bound (this follows from intervals being non-overlapping).
//! This structure allows all operations to be performed with the compexity of the underlying
//! B-Tree.
//!
//! Sadly using the std library [`BTreeSet`] comes with a significant challenge to this
//! implementation: ideally lookups into the set can with anything that implements [`Borrow`], for
//! example if the tree contains intervals of [`String`] if should be possible to perform lookups
//! using [`str`], however a bound can't borrow the borrowed version of itself, i.e. it is
//! impossible to implement a borrow of the form `fn borrow(self: &Bound<T>) -> &Borrow<B> where T:
//! Borrow<B>>`. To get around this limitation some unsafe code is required as described in the
//! code.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::btree_set::Range;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hint::unreachable_unchecked;
use std::ops::{Bound, Deref, RangeBounds};

/// A set for storing non-overlapping intervals.
///
/// See [crate documentation](crate) for more details.
#[derive(Clone, PartialEq, Eq)]
pub struct IntervalTreeSet<T> {
    /// The internal storage of [`Endpoint`], where for each non-overlapping interval two
    /// [`Endpoint`]s are stored: the lower-bound endpoint and the upper-bound endpoint, which are
    /// of distinct variants.
    bound_set: BTreeSet<Endpoint<T>>,
}

impl<T> Debug for IntervalTreeSet<T>
where
    T: Ord + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.intervals()).finish()
    }
}

impl<T> Default for IntervalTreeSet<T> {
    fn default() -> Self {
        IntervalTreeSet {
            bound_set: BTreeSet::new(),
        }
    }
}

/// An [`Endpoint`] is the bound endpoint of an interval stored in the tree. For each interval in
/// the tree, two [`Endpoint`]s are stored. [`Endpoint`] has the same layout as [`BorrowedEndpoint`]
/// and can be safely transmuted into it as long as the [`OwnedOrPtr::Ptr`] variant is never
/// constructed, as described in [`OwnedOrPtr`].
#[repr(u8)]
#[derive(Clone, PartialEq, Eq, Debug)]
enum Endpoint<T> {
    LowerUnbounded,
    LowerInclusive(OwnedOrPtr<T>),
    LowerExclusive(OwnedOrPtr<T>),
    UpperExclusive(OwnedOrPtr<T>),
    UpperInclusive(OwnedOrPtr<T>),
    UpperUnbounded,
}

/// This is the borrowed version of [`Endpoint<T>`]. Importantly it has the same layout as
/// [`Endpoint<T>`], guaranteed by using `#[repr(u8)]`
/// https://doc.rust-lang.org/nomicon/other-reprs.html#repru-repri
#[repr(u8)]
#[derive(PartialEq, Eq, Debug)]
enum BorrowedEndpoint<'a, T, B>
where
    T: Borrow<B>,
    B: ?Sized,
{
    LowerUnbounded,
    LowerInclusive(OwnedOrBorrowed<'a, T, B>),
    LowerExclusive(OwnedOrBorrowed<'a, T, B>),
    UpperExclusive(OwnedOrBorrowed<'a, T, B>),
    UpperInclusive(OwnedOrBorrowed<'a, T, B>),
    UpperUnbounded,
}

/// [`OwnedOrPtr`] is the crux of the tree's ability to use borrowed representation for lookups.
/// There are two layout equivalent structures [`OwnedOrPtr<T>`] and [`OwnedOrPtr<T,B>`] that are
/// guaranteed to have the same size and layout by using `#[repr(u8)]` and variants that have the
/// same size.
///
/// The [`OwnedOrPtr`] only ever uses the [`OwnedOrPtr::Owned`] variant and always contains owned
/// data. It is crucial to never construct the [`OwnedOrPtr::Ptr`] variant or the safety breaks.
///
/// On the other hand [`OwnedOrBorrowed`] is essentially equivalent to [`std::borrow::Cow`] except
/// that it has to have the same layout as [`OwnedOrPtr`]. As long as only the [`OwnedOrPtr::Owned`]
/// variant is constructed, [`OwnedOrPtr<T>`] can be transmuted to any [`OwnedOrBorrowed<T, B>`]
/// safely, and a reference to it can be transmuted into a reference to [`OwnedOrBorrowed<T, B>`],
/// which is done in the implementation for [`Borrow`].
///
/// The reason for this is so that the tree can compare instances of [`OwnedOrBorrowed`] when
/// performing lookups into the tree using borrowed forms (i.e. looking up [`String`] intervals with
/// [`str`]).
#[repr(u8)]
#[derive(Clone, PartialEq, Eq, PartialOrd, Debug)]
enum OwnedOrPtr<T> {
    Owned(T),
    #[allow(dead_code)]
    Ptr(*const T),
}

#[repr(u8)]
#[derive(PartialEq, Eq, PartialOrd, Debug)]
enum OwnedOrBorrowed<'a, T, B>
where
    T: Borrow<B>,
    B: ?Sized,
{
    #[allow(dead_code)]
    Owned(T),
    Ref(&'a B),
}

/// SAFETY: Those are safe to implement for a given T, because the `Ptr` variant is never
/// constructed
unsafe impl<T> Send for OwnedOrPtr<T> where T: Send {}
unsafe impl<T> Sync for OwnedOrPtr<T> where T: Sync {}

impl<T> Deref for OwnedOrPtr<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            OwnedOrPtr::Owned(v) => v,
            // SAFETY: This variant is never constructed
            OwnedOrPtr::Ptr(_) => {
                debug_assert_eq!(0, 1);
                unsafe { unreachable_unchecked() }
            }
        }
    }
}

impl<'a, T, B> Deref for OwnedOrBorrowed<'a, T, B>
where
    T: Borrow<B>,
    B: ?Sized,
{
    type Target = B;

    fn deref(&self) -> &Self::Target {
        match self {
            OwnedOrBorrowed::Owned(v) => v.borrow(),
            OwnedOrBorrowed::Ref(p) => p,
        }
    }
}

macro_rules! impl_ord {
    ( $x:ty ) => {
        /// The bounds are compared as follows:
        /// `LowerUnbounded` < `LowerInclusive(x)` < `UpperExclusive(x)` < `LowerExclusive(x)` <
        /// UpperInclusive(x)` < `UpperUnbounded`.
        fn cmp(&self, other: &Self) -> Ordering {
            use $x::*;
            match self {
                LowerUnbounded => match other {
                    LowerUnbounded => Ordering::Equal,
                    _ => Ordering::Less,
                },
                LowerInclusive(s) => match other {
                    LowerUnbounded => Ordering::Greater,
                    LowerInclusive(o) => s.cmp(o),
                    UpperExclusive(o) => s.cmp(o).then(Ordering::Less),
                    LowerExclusive(o) => s.cmp(o).then(Ordering::Less),
                    UpperInclusive(o) => s.cmp(o).then(Ordering::Less),
                    UpperUnbounded => Ordering::Less,
                },
                UpperExclusive(s) => match other {
                    LowerUnbounded => Ordering::Greater,
                    LowerInclusive(o) => s.cmp(o).then(Ordering::Greater),
                    UpperExclusive(o) => s.cmp(o),
                    LowerExclusive(o) => s.cmp(o).then(Ordering::Less),
                    UpperInclusive(o) => s.cmp(o).then(Ordering::Less),
                    UpperUnbounded => Ordering::Less,
                },
                LowerExclusive(s) => match other {
                    LowerUnbounded => Ordering::Greater,
                    LowerInclusive(o) => s.cmp(o).then(Ordering::Greater),
                    UpperExclusive(o) => s.cmp(o).then(Ordering::Greater),
                    LowerExclusive(o) => s.cmp(o),
                    UpperInclusive(o) => s.cmp(o).then(Ordering::Less),
                    UpperUnbounded => Ordering::Less,
                },
                UpperInclusive(s) => match other {
                    LowerUnbounded => Ordering::Greater,
                    LowerInclusive(o) => s.cmp(o).then(Ordering::Greater),
                    UpperExclusive(o) => s.cmp(o).then(Ordering::Greater),
                    LowerExclusive(o) => s.cmp(o).then(Ordering::Greater),
                    UpperInclusive(o) => s.cmp(o),
                    UpperUnbounded => Ordering::Less,
                },
                UpperUnbounded => match other {
                    UpperUnbounded => Ordering::Equal,
                    _ => Ordering::Greater,
                },
            }
        }
    };
}

impl<T> Ord for Endpoint<T>
where
    T: Ord,
{
    impl_ord! { Endpoint }
}

impl<T> PartialOrd for Endpoint<T>
where
    T: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, T, B> Ord for BorrowedEndpoint<'a, T, B>
where
    T: Borrow<B> + Ord,
    B: Ord + ?Sized,
{
    impl_ord! { BorrowedEndpoint }
}

impl<'a, T, B> PartialOrd for BorrowedEndpoint<'a, T, B>
where
    T: Borrow<B> + Ord,
    B: Ord + ?Sized,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, T, B> Borrow<BorrowedEndpoint<'a, T, B>> for Endpoint<T>
where
    T: Borrow<B> + Ord,
    B: Ord + ?Sized + 'a,
{
    fn borrow(&self) -> &BorrowedEndpoint<'a, T, B> {
        // Make sure the two structs really do have the same size and alignment. This will be
        // optimized out.
        assert_eq!(
            std::mem::size_of::<Endpoint<T>>(),
            std::mem::size_of::<BorrowedEndpoint<T, B>>()
        );
        assert_eq!(
            std::mem::size_of::<OwnedOrPtr<T>>(),
            std::mem::size_of::<OwnedOrBorrowed<T, B>>()
        );
        assert_eq!(
            std::mem::align_of::<Endpoint<T>>(),
            std::mem::align_of::<BorrowedEndpoint<T, B>>()
        );
        assert_eq!(
            std::mem::align_of::<OwnedOrPtr<T>>(),
            std::mem::align_of::<OwnedOrBorrowed<T, B>>()
        );
        // SAFETY: transmuting an `Endpoint::XXX(OwnedOrPtr::Owned(v))` into a
        // `BorrowedEndpoint::XXX(OwnedOrBorrowed::Owned(v))` is safe, because they have the same
        // layout and containt identical enum variants with owned data. The kind is always `Owned`
        // because the other variant is not constructed. Refer to [`OwnedOrPtr`] for more
        // details.
        unsafe { std::mem::transmute(self) }
    }
}

impl<T> Endpoint<T> {
    fn is_lower(&self) -> bool {
        matches!(
            self,
            Endpoint::LowerUnbounded | Endpoint::LowerInclusive(_) | Endpoint::LowerExclusive(_)
        )
    }

    fn is_upper(&self) -> bool {
        !self.is_lower()
    }

    /// Construct a new [`Endpoint`] from a starting [`Bound`] by converting it into an owned value.
    fn lower<B>(bound: Bound<&B>) -> Endpoint<T>
    where
        B: ToOwned<Owned = T> + ?Sized,
    {
        match bound {
            Bound::Included(v) => Endpoint::LowerInclusive(OwnedOrPtr::Owned(v.to_owned())),
            Bound::Excluded(v) => Endpoint::LowerExclusive(OwnedOrPtr::Owned(v.to_owned())),
            Bound::Unbounded => Endpoint::LowerUnbounded,
        }
    }

    /// Construct a new [`Endpoint`] from an ending [`Bound`] by converting it into an owned value.
    fn upper<B>(bound: Bound<&B>) -> Endpoint<T>
    where
        B: ToOwned<Owned = T> + ?Sized,
    {
        match bound {
            Bound::Included(v) => Endpoint::UpperInclusive(OwnedOrPtr::Owned(v.to_owned())),
            Bound::Excluded(v) => Endpoint::UpperExclusive(OwnedOrPtr::Owned(v.to_owned())),
            Bound::Unbounded => Endpoint::UpperUnbounded,
        }
    }

    /// Construct a new complementary [`Endpoint`] from a starting [`Bound`] by converting it into
    /// an owned value.
    fn lower_complement<B>(bound: Bound<&B>) -> Endpoint<T>
    where
        B: ToOwned<Owned = T> + ?Sized,
    {
        match bound {
            Bound::Included(v) => Endpoint::LowerExclusive(OwnedOrPtr::Owned(v.to_owned())),
            Bound::Excluded(v) => Endpoint::LowerInclusive(OwnedOrPtr::Owned(v.to_owned())),
            Bound::Unbounded => Endpoint::LowerUnbounded,
        }
    }

    /// Construct a new complementary [`Endpoint`] from an ending [`Bound`] by converting it into an
    /// owned value.
    fn upper_complement<B>(bound: Bound<&B>) -> Endpoint<T>
    where
        B: ToOwned<Owned = T> + ?Sized,
    {
        match bound {
            Bound::Included(v) => Endpoint::UpperExclusive(OwnedOrPtr::Owned(v.to_owned())),
            Bound::Excluded(v) => Endpoint::UpperInclusive(OwnedOrPtr::Owned(v.to_owned())),
            Bound::Unbounded => Endpoint::UpperUnbounded,
        }
    }

    /// Convert the [`Endpoint`] to a [`Bound`].
    fn as_bound<B>(&self) -> Bound<&B>
    where
        T: Borrow<B>,
        B: ?Sized,
    {
        match self {
            Endpoint::LowerUnbounded => Bound::Unbounded,
            Endpoint::UpperExclusive(v) | Endpoint::LowerExclusive(v) => {
                Bound::Excluded((**v).borrow())
            }
            Endpoint::UpperInclusive(v) | Endpoint::LowerInclusive(v) => {
                Bound::Included((**v).borrow())
            }
            Endpoint::UpperUnbounded => Bound::Unbounded,
        }
    }
}

/// Panics if the range is empty or decreasing, otherwise returns the bounds of the range.
#[track_caller]
fn check_range<'a, B, R>(range: &'a R) -> (Bound<&'a B>, Bound<&'a B>)
where
    B: Ord + ?Sized + 'a,
    R: RangeBounds<B>,
{
    let (start, end) = (range.start_bound(), range.end_bound());
    match (start, end) {
        (Bound::Excluded(s), Bound::Excluded(e))
        | (Bound::Excluded(s), Bound::Included(e))
        | (Bound::Included(s), Bound::Excluded(e))
            if s == e =>
        {
            panic!("range start and end are equal and not inclusive in IntervalTreeSet")
        }
        (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e))
            if s > e =>
        {
            panic!("range start is greater than range end in IntervalTreeSet")
        }
        _ => {}
    }

    (range.start_bound(), range.end_bound())
}

impl<'a, T, B> BorrowedEndpoint<'a, T, B>
where
    T: Borrow<B>,
    B: Ord + ?Sized + 'a,
{
    /// Construct a new pair of [`BorrowedEndpoint`] from [`RangeBounds`]. If a range would be empty
    /// (i.e. (0..0)) then it panics
    #[track_caller]
    fn pair<R>(range: &'a R) -> (BorrowedEndpoint<'a, T, B>, BorrowedEndpoint<'a, T, B>)
    where
        R: RangeBounds<B>,
    {
        let (start, end) = check_range(range);

        let lo = match start {
            Bound::Included(v) => BorrowedEndpoint::LowerInclusive(OwnedOrBorrowed::Ref(v)),
            Bound::Excluded(v) => BorrowedEndpoint::LowerExclusive(OwnedOrBorrowed::Ref(v)),
            Bound::Unbounded => BorrowedEndpoint::LowerUnbounded,
        };

        let hi = match end {
            Bound::Included(v) => BorrowedEndpoint::UpperInclusive(OwnedOrBorrowed::Ref(v)),
            Bound::Excluded(v) => BorrowedEndpoint::UpperExclusive(OwnedOrBorrowed::Ref(v)),
            Bound::Unbounded => BorrowedEndpoint::UpperUnbounded,
        };

        (lo, hi)
    }

    /// Convert the [`BorrowedEndpoint`] to a [`Bound`].
    fn as_bound(&self) -> Bound<&B> {
        match self {
            BorrowedEndpoint::LowerUnbounded => Bound::Unbounded,
            BorrowedEndpoint::UpperExclusive(v) | BorrowedEndpoint::LowerExclusive(v) => {
                Bound::Excluded(v.deref())
            }
            BorrowedEndpoint::UpperInclusive(v) | BorrowedEndpoint::LowerInclusive(v) => {
                Bound::Included(v.deref())
            }
            BorrowedEndpoint::UpperUnbounded => Bound::Unbounded,
        }
    }
}

impl<T> IntervalTreeSet<T> {
    /// Returns `true` if the tree contains no intervals
    pub fn is_empty(&self) -> bool {
        self.bound_set.is_empty()
    }

    /// Remove all intervals from the tree
    pub fn clear(&mut self) {
        self.bound_set.clear()
    }
}

impl<T> IntervalTreeSet<T>
where
    T: Ord,
{
    /// Inserts an interval `range` into the interval tree. This method does not preserve the
    /// "uniqueness" of existing intervals, and will merge stored intervals if possible.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut int_tree: merging_interval_tree::IntervalTreeSet<i32> = Default::default();
    ///
    /// int_tree.insert_interval(5..9);
    /// int_tree.insert_interval(..=10);
    ///
    /// assert!(int_tree.contains_point(&10));
    ///
    /// let mut str_tree: merging_interval_tree::IntervalTreeSet<&str> = Default::default();
    ///
    /// str_tree.insert_interval("Tree"..);
    ///
    /// assert!(str_tree.contains_point("Treebeard"));
    /// ```
    pub fn insert_interval<B, R>(&mut self, range: R)
    where
        T: Borrow<B> + Clone,
        B: Ord + ?Sized + ToOwned<Owned = T>,
        R: RangeBounds<B>,
    {
        let (lo, hi) = BorrowedEndpoint::pair(&range);

        // Find and remove any bounds that fall fully within the new range, this results in a
        // "merge" of existing intervals.
        // Unfortunately as of yet there is no `remove_range` function in `BTreeSet`, therefore
        // removal takes two steps: find the nodes that needs removal, then clone and perform the
        // actual removal. This is suboptimal, but the alternative is to use `drain` which
        // requires a linear scan of the entire tree and is asymptotically slower (although faster
        // in practice in many cases).
        let mut next_is_upper = false;
        let range = self.bound_set.range::<BorrowedEndpoint<T, _>, _>(&lo..);
        let remove_range = range.take_while(|lim| {
            // Mark if the limit that follows the last removed range is a upper limit.
            next_is_upper = lim.is_upper();
            hi.cmp((*lim).borrow()).is_ge()
        });

        let removed_nodes = remove_range.cloned().collect::<Vec<_>>();
        for v in &removed_nodes {
            self.bound_set.remove(v);
        }

        // If the first removed limit was a lower limit, or if no limit was removed and the next
        // limit is not an upper limit, then the inserted interval begins in a gap between
        // intervals, which means a new bound needs to be inserted. Otherwise if is followed
        // by a lower limit, and therefore already falls within an existing interval.
        match removed_nodes.first() {
            maybe_elem if maybe_elem.map(|e| e.is_upper()).unwrap_or(next_is_upper) => false,
            _ => self.bound_set.insert(Endpoint::lower(lo.as_bound())),
        };

        // If the last removed limit was an upper limit, or if no limit was removed and the next
        // limit is not an upper limit, then the inserted interval ends in a gap between intervals,
        // which means a new bound needs to be inserted. Otherwise if is followed by an
        // upper limit, and therefore already falls within an existing interval.
        match removed_nodes.last() {
            maybe_elem if maybe_elem.map(|e| e.is_lower()).unwrap_or(next_is_upper) => false,
            _ => self.bound_set.insert(Endpoint::upper(hi.as_bound())),
        };
    }

    /// Inserts a single point into the interval tree.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut int_tree = merging_interval_tree::IntervalTreeSet::default();
    ///
    /// int_tree.insert_point(1);
    /// assert!(int_tree.contains_point(&1));
    /// ```
    pub fn insert_point(&mut self, p: T)
    where
        T: Clone,
    {
        self.insert_interval((Bound::Included(&p), Bound::Included(&p)))
    }

    /// Remove an interval from the interval tree or part thereof if the interval is not fully
    /// covered by the tree.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut tree = merging_interval_tree::IntervalTreeSet::default();
    ///
    /// tree.insert_interval(5..=10);
    /// assert!(tree.contains_point(&6));
    /// assert!(tree.covers_interval(&(7..9)));
    ///
    /// tree.remove_interval(&(5..9));
    /// assert!(!tree.covers_interval(&(5..9)));
    /// assert!(!tree.covers_interval(&(7..=8)));
    /// assert!(tree.covers_interval(&(9..=10)));
    /// ```
    pub fn remove_interval<B, R>(&mut self, range: &R)
    where
        T: Borrow<B> + Clone,
        B: Ord + ?Sized + ToOwned<Owned = T>,
        R: RangeBounds<B>,
    {
        let (lo, hi) = BorrowedEndpoint::pair(range);

        // Find and remove any bounds that fall fully within the new range. This requires cloning,
        // same as in `insert_interval`.
        let mut next_is_upper = false;
        let range = self.bound_set.range::<BorrowedEndpoint<_, _>, _>(&lo..);
        let remove_range = range.take_while(|lim| {
            // Mark if the limit that follows the last removed range is an upper limit.
            next_is_upper = lim.is_upper();
            hi.cmp((*lim).borrow()).is_ge()
        });

        let removed_nodes = remove_range.cloned().collect::<Vec<_>>();
        for v in &removed_nodes {
            self.bound_set.remove(v);
        }

        // If the first removed limit was an upper limit or if no limits were removed but the next
        // limit is an upper limit, it means the removed interval begins in a middle of an existing
        // interval. Therefore a new upper limit is required marking a new end to that existing
        // interval.
        match removed_nodes.first() {
            maybe_elem if maybe_elem.map(|e| e.is_upper()).unwrap_or(next_is_upper) => self
                .bound_set
                .insert(Endpoint::upper_complement(lo.as_bound())),
            _ => false,
        };

        // If the last removed limit was a lower limit or if no limits were removed but the next
        // limit is an upper limit, it means the removed interval ends in a middle of an existing
        // interval. Therefore a new lower limit is required marking a new beginning of that
        // existing interval.
        match removed_nodes.last() {
            maybe_elem if maybe_elem.map(|e| e.is_lower()).unwrap_or(next_is_upper) => self
                .bound_set
                .insert(Endpoint::lower_complement(hi.as_bound())),
            _ => false,
        };
    }

    /// Remove a single point from the interval tree
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut tree: merging_interval_tree::IntervalTreeSet<usize> = Default::default();
    ///
    /// tree.insert_interval(5..=10);
    /// tree.remove_point(&7);
    ///
    /// assert!(!tree.contains_point(&7));
    /// assert!(tree.covers_interval(&(5..7)));
    /// assert!(tree.covers_interval(&(8..=10)));
    /// ````
    pub fn remove_point<B>(&mut self, p: &B)
    where
        T: Borrow<B> + Clone,
        B: Ord + ?Sized + ToOwned<Owned = T>,
    {
        self.remove_interval(&(Bound::Included(p), Bound::Included(p)))
    }

    /// Check whether or not a point is covered by the intervals in the tree.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::ops::Bound::{Excluded, Unbounded};
    ///
    /// let mut tree: merging_interval_tree::IntervalTreeSet<String> = Default::default();
    ///
    /// tree.insert_interval((Excluded("Tree".to_string()), Unbounded));
    ///
    /// assert!(tree.contains_point("Zebra"));
    /// assert!(!tree.contains_point("Tree"));
    /// ```
    pub fn contains_point<B>(&self, p: &B) -> bool
    where
        T: Borrow<B>,
        B: Ord + ?Sized,
    {
        // A point is contained in the interval tree, if there is an interval with a upper-endpoint
        // that is greater-equal than the point (that is either UpperUnbounded, UpperExclusive with
        // value greater than the point, or UpperInclusive with a value greater or equal
        // than the point) and a lower-endpoint that is smaller-equal than the point. The
        // method therefore looks for the upper-endpoint of such an interval, if present, while
        // skipping the lower-endpoint itself.
        match self
            .bound_set
            .range((
                Bound::Excluded(BorrowedEndpoint::LowerInclusive(OwnedOrBorrowed::Ref(
                    p.borrow(),
                ))),
                Bound::Unbounded,
            ))
            .next()
        {
            // Unbounded includes all points
            Some(Endpoint::UpperUnbounded) => true,
            // An open endpoint only includes points that are strictly smaller
            Some(Endpoint::UpperExclusive(l)) => (**l).borrow().cmp(p).is_gt(),
            // A closed endpoint includes points that are smaller or equal
            Some(Endpoint::UpperInclusive(l)) => (**l).borrow().cmp(p).is_ge(),
            // If no endpoint was found, it means no interval above the point exists, if a lower
            // endpoint was found, it means the point falls inbetween intervals, and not contained.
            _ => false,
        }
    }

    /// Check whether or not an interval is fully covered by the intervals in the tree.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::ops::Bound::{Excluded, Included, Unbounded};
    ///
    /// let mut tree = merging_interval_tree::IntervalTreeSet::default();
    ///
    /// tree.insert_interval((Included(20), Included(30)));
    /// tree.insert_interval((Excluded(30), Excluded(50)));
    ///
    /// assert!(tree.covers_interval(&(Included(20), Included(40))));
    /// assert!(!tree.covers_interval(&(Included(30), Included(50))));
    /// ```
    pub fn covers_interval<B, R>(&self, range: &R) -> bool
    where
        T: Borrow<B> + Ord,
        B: Ord + ?Sized,
        R: RangeBounds<B>,
    {
        let (lo, hi) = BorrowedEndpoint::pair(range);

        match self
            .bound_set
            .range((Bound::Excluded(lo), Bound::Unbounded))
            .next()
        {
            // The check is pretty simple as well, just compare the upper intervals
            Some(Endpoint::UpperUnbounded) => true,
            Some(limit) if limit.is_upper() => {
                let b: &BorrowedEndpoint<T, B> = limit.borrow();
                b.cmp(&hi).is_ge()
            }
            _ => false,
        }
    }

    /// Returns an iterator over ordered subintervals in `range` that are not covered by the tree.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::ops::Bound::{Excluded, Included, Unbounded};
    ///
    /// let mut tree = merging_interval_tree::IntervalTreeSet::default();
    ///
    /// tree.insert_interval((Included(0), Excluded(10)));
    /// tree.insert_interval((Excluded(10), Included(30)));
    /// tree.insert_interval((Excluded(50), Unbounded));
    ///
    /// assert_eq!(
    ///     tree.get_interval_difference(&(Included(-5), Included(30)))
    ///         .collect::<Vec<_>>(),
    ///     vec![
    ///         (Included(&-5), Excluded(&0)),
    ///         (Included(&10), Included(&10))
    ///     ]
    /// );
    /// assert_eq!(
    ///     tree.get_interval_difference(&(Unbounded, Excluded(10)))
    ///         .collect::<Vec<_>>(),
    ///     vec![(Unbounded, Excluded(&0))]
    /// );
    /// assert!(tree
    ///     .get_interval_difference(&(Included(100), Unbounded))
    ///     .next()
    ///     .is_none());
    /// ```
    pub fn get_interval_difference<'a, B, R>(&'a self, range: &'a R) -> IntervalDiffIter<'a, T, B>
    where
        T: Borrow<B> + Ord,
        B: Ord + ?Sized + 'a,
        R: RangeBounds<B>,
    {
        let (lower_bound, upper_bound) = check_range(range);

        let lo = match &lower_bound {
            Bound::Included(v) => Bound::Included(BorrowedEndpoint::UpperExclusive(
                OwnedOrBorrowed::Ref(v.borrow()),
            )),
            Bound::Excluded(v) => Bound::Excluded(BorrowedEndpoint::UpperInclusive(
                OwnedOrBorrowed::Ref(v.borrow()),
            )),
            Bound::Unbounded => Bound::Excluded(BorrowedEndpoint::LowerUnbounded),
        };

        IntervalDiffIter {
            lower_bound: Some(lower_bound),
            upper_bound: Some(upper_bound),
            inner: self.bound_set.range((lo, Bound::Unbounded)),
        }
    }

    /// Returns an iterator over ordered subintervals in `range` that are covered by the tree.
    /// This can be seen as a complement to `get_interval_difference`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::ops::Bound::{Excluded, Included, Unbounded};
    ///
    /// let mut tree = merging_interval_tree::IntervalTreeSet::default();
    ///
    /// tree.insert_interval((Included(0), Included(5)));
    /// tree.insert_interval((Included(7), Excluded(10)));
    ///
    /// assert_eq!(
    ///     tree.get_interval_overlaps(&(Included(-5), Excluded(7)))
    ///         .collect::<Vec<_>>(),
    ///     vec![(Included(&0), Included(&5))]
    /// );
    /// assert!(tree
    ///     .get_interval_overlaps(&(Included(10), Unbounded))
    ///     .next()
    ///     .is_none());
    /// ```
    pub fn get_interval_overlaps<'a, B, R>(&'a self, range: &'a R) -> IntervalOverlapIter<'a, T, B>
    where
        T: Borrow<B> + Ord,
        B: Ord + ?Sized + 'a,
        R: RangeBounds<B>,
    {
        let (lower_bound, upper_bound) = check_range(range);
        // Begin the search from a complimentary bound, so to not accidentally include a
        // upper-closed bound for a previous interval
        let lo = match &lower_bound {
            Bound::Included(v) => {
                BorrowedEndpoint::UpperExclusive(OwnedOrBorrowed::Ref(v.borrow()))
            }
            Bound::Excluded(v) => {
                BorrowedEndpoint::UpperInclusive(OwnedOrBorrowed::Ref(v.borrow()))
            }
            Bound::Unbounded => BorrowedEndpoint::LowerUnbounded,
        };

        let range = self
            .bound_set
            .range((Bound::Excluded(lo), Bound::Unbounded));

        IntervalOverlapIter {
            lower_bound: Some(lower_bound),
            upper_bound: Some(upper_bound),
            inner: range,
        }
    }

    /// Returns the list of all continuous intervals in the tree.
    pub fn intervals(&self) -> impl Iterator<Item = (Bound<&T>, Bound<&T>)> {
        let iter = self.bound_set.range::<Endpoint<T>, _>(..);

        let mut lo: Option<&Endpoint<T>> = None;
        iter.filter_map(move |next| match lo.take() {
            // If the previous match is empty, the next endpoint is a lower endpoint, otherwise the
            // next endpoint is a upper endpoint
            Some(lo) => Some((lo.as_bound(), next.as_bound())),
            None => {
                lo = Some(next);
                None
            }
        })
    }
}

/// An [`Iterator`] over interval differences, created with the method
/// [`IntervalTreeSet::get_interval_difference`].
pub struct IntervalDiffIter<'a, T, B>
where
    B: ?Sized + 'a,
{
    lower_bound: Option<Bound<&'a B>>,
    upper_bound: Option<Bound<&'a B>>,
    inner: Range<'a, Endpoint<T>>,
}

/// An [`Iterator`] over interval overlaps, created with the method
/// [`IntervalTreeSet::get_interval_overlaps`].
pub struct IntervalOverlapIter<'a, T, B>
where
    B: ?Sized + 'a,
{
    lower_bound: Option<Bound<&'a B>>,
    upper_bound: Option<Bound<&'a B>>,
    inner: Range<'a, Endpoint<T>>,
}

impl<'a, T, B> IntervalDiffIter<'a, T, B>
where
    T: Borrow<B> + Ord,
    B: Ord + ?Sized + 'a,
{
    /// Return the smaller of own `upper_bound` or `other_bound`. Once own `upper_bound` is
    /// returned, the method will keep returning [`None`]. The `max` in the name refers to the
    /// maximum limit the iterator stops at, and once it is reached the method returns
    /// [`None`].
    fn max_bound(&mut self, other_bound: Bound<&'a B>) -> Option<Bound<&'a B>> {
        let cur_upper = self.upper_bound.as_ref()?;

        match cur_upper {
            Bound::Unbounded => match other_bound {
                Bound::Unbounded => None,
                Bound::Included(oth) => Some(Bound::Excluded(oth)),
                Bound::Excluded(oth) => Some(Bound::Included(oth)),
            },
            Bound::Included(cur) => match other_bound {
                Bound::Included(oth) if oth.cmp(cur).is_lt() => Some(Bound::Excluded(oth)),
                Bound::Excluded(oth) if oth.cmp(cur).is_lt() => Some(Bound::Included(oth)),
                Bound::Excluded(oth) if oth.cmp(cur).is_eq() => Some(Bound::Included(cur)),
                Bound::Included(oth) if oth.cmp(cur).is_eq() => {
                    self.upper_bound.take().map(|b| match b {
                        Bound::Included(b) => Bound::Excluded(b),
                        _ => unreachable!("Checked in match"),
                    })
                }
                _ => self.upper_bound.take(),
            },
            Bound::Excluded(cur) => match other_bound {
                Bound::Included(oth) if oth.cmp(cur).is_lt() => Some(Bound::Excluded(oth)),
                Bound::Excluded(oth) if oth.cmp(cur).is_lt() => Some(Bound::Included(oth)),
                _ => self.upper_bound.take(),
            },
        }
    }
}

impl<'a, T, B> IntervalOverlapIter<'a, T, B>
where
    T: Borrow<B> + Ord,
    B: Ord + ?Sized + 'a,
{
    /// Return the smaller of own `upper_bound` or `other_bound`. Once own `upper_bound` is
    /// returned, the method will keep returning [`None`]. The `max` in the name refers to the
    /// maximum limit the iterator stops at, and once it is reached the method returns
    /// [`None`].
    fn max_bound(&mut self, other_bound: Bound<&'a B>) -> Option<Bound<&'a B>> {
        let cur_upper = self.upper_bound.as_ref()?;

        match cur_upper {
            Bound::Unbounded => match other_bound {
                Bound::Unbounded => Some(other_bound),
                _ => None,
            },
            Bound::Included(cur) => match other_bound {
                Bound::Included(oth) if oth.cmp(cur).is_le() => Some(other_bound),
                Bound::Excluded(oth) if oth.cmp(cur).is_lt() => Some(other_bound),
                _ => self.upper_bound.take(),
            },
            Bound::Excluded(cur) => match other_bound {
                Bound::Included(oth) if oth.cmp(cur).is_lt() => Some(other_bound),
                Bound::Excluded(oth) if oth.cmp(cur).is_lt() => Some(other_bound),
                _ => self.upper_bound.take(),
            },
        }
    }
}

impl<'a, T, B> Iterator for IntervalDiffIter<'a, T, B>
where
    T: Borrow<B> + Ord,
    B: Ord + ?Sized + 'a,
{
    type Item = (Bound<&'a B>, Bound<&'a B>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            // Iterate over pairs of (lower_bound, upper_bound), because those are the "holes" in
            // the range covered by the tree, but if the first element in the iterator is actually
            // lower bound, consume it just once, and then do the happy loop.
            Some(lim) if lim.is_lower() => {
                // This basically means that the range from the initial lower bound till here was
                // not covered, and will be the first returned difference
                let lo = self.lower_bound.take().expect("First iteration");
                let hi = self.max_bound(lim.as_bound())?;
                Some((lo, hi))
            }
            Some(lim) => {
                // If the iterator got here it has to be an upper limit, so the iterator will return
                // it, together with the next lower limit, or failing that the maximum upper limit.
                let lo = self.max_bound(lim.as_bound())?;
                let hi = if let Some(lo) = self.inner.next() {
                    self.max_bound(lo.as_bound())
                } else {
                    self.upper_bound.take()
                }?;
                Some((lo, hi))
            }
            None => {
                if let (Some(lo), Some(hi)) = (self.lower_bound.take(), self.upper_bound.take()) {
                    Some((lo, hi))
                } else {
                    None
                }
            }
        }
    }
}

impl<'a, T, B> Iterator for IntervalOverlapIter<'a, T, B>
where
    T: Borrow<B> + Ord,
    B: Ord + ?Sized + 'a,
{
    type Item = (Bound<&'a B>, Bound<&'a B>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            // Iterate over pairs of (lower_bound, upper_bound), because those are the "overlaps" in
            // the range covered by the tree, but if the first element in the iterator is actually
            // a upper bound, consume it just once, and then do the happy loop.
            Some(lim) if lim.is_upper() => {
                // This basically means that the range from the initial lower bound till here was
                // overlapping, and will be the first returned overlap
                let lo = self.lower_bound.take().expect("First iteration");
                let hi = self.max_bound(lim.as_bound())?;
                Some((lo, hi))
            }
            Some(lim) => {
                // If the iterator got here it has to be a lower limit, so the iterator will return
                // it, together with the next lower limit, or failing that the maximum upper limit.
                let lo = self.max_bound(lim.as_bound())?;
                let hi = if let Some(lo) = self.inner.next() {
                    self.max_bound(lo.as_bound())
                } else {
                    self.upper_bound.take()
                }?;
                Some((lo, hi))
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeBounds};

    use test_strategy::proptest;

    use crate::{Endpoint, IntervalTreeSet};

    #[track_caller]
    pub(crate) fn assert_invariant<T: Ord + Clone>(tree: &IntervalTreeSet<T>) {
        let mut was_lower = false;

        for edge in tree.bound_set.range::<Endpoint<T>, _>(..) {
            assert!(
                was_lower && edge.is_upper() || !was_lower && edge.is_lower(),
                "Tree invariant violated"
            );
            was_lower = edge.is_lower();
        }
    }

    #[track_caller]
    fn test_contains_ranges<R: RangeBounds<usize>>(tree: &IntervalTreeSet<usize>, ranges: &[R]) {
        assert_invariant(tree);
        for i in 0..10000 {
            if ranges.iter().any(|r| r.contains(&i)) {
                assert!(tree.contains_point(&i), "Should contain {i}");
            } else {
                assert!(!tree.contains_point(&i), "Should not contain {i}");
            }
        }
    }

    fn check_interval<T: Ord>(int: &(Bound<T>, Bound<T>)) -> bool {
        match (&int.0, &int.1) {
            (Bound::Excluded(s), Bound::Excluded(e))
            | (Bound::Excluded(s), Bound::Included(e))
            | (Bound::Included(s), Bound::Excluded(e))
                if s == e =>
            {
                false
            }
            (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e))
                if s > e =>
            {
                false
            }
            _ => true,
        }
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn insert_interval() {
        let mut tree: IntervalTreeSet<usize> = Default::default();

        tree.insert_interval((Bound::Excluded(5), Bound::Excluded(15)));
        test_contains_ranges(&tree, &[6..15]);

        tree.insert_interval((Bound::Included(5), Bound::Excluded(16)));
        test_contains_ranges(&tree, &[5..16]);

        tree.insert_interval((Bound::Included(4), Bound::Included(16)));
        test_contains_ranges(&tree, &[4..=16]);

        tree.insert_interval((Bound::Excluded(19), Bound::Included(34)));
        test_contains_ranges(&tree, &[4..=16, 20..=34]);

        tree.insert_interval((Bound::Included(1), Bound::Included(100)));
        test_contains_ranges(&tree, &[1..=100]);

        tree.insert_interval((Bound::Excluded(200), Bound::Unbounded));
        test_contains_ranges(&tree, &[1..=100, 201..=usize::MAX]);

        tree.insert_interval((Bound::Unbounded, Bound::Included(150)));
        test_contains_ranges(&tree, &[0..=150, 201..=usize::MAX]);
    }

    #[test]
    fn string_intervals() {
        let mut tree: IntervalTreeSet<String> = Default::default();

        tree.insert_interval((
            Bound::Included("a".to_string()),
            Bound::Excluded("b".to_string()),
        ));

        assert!(tree.contains_point("a"));
        assert!(tree.covers_interval::<str, _>(&(
            Bound::Included("a"),
            Bound::Excluded("asdasdasdsadsasd")
        )));
        assert!(!tree.covers_interval::<String, _>(&(
            Bound::Included(&"a".to_string()),
            Bound::Excluded(&"bb".to_string())
        )));

        tree.remove_interval::<String, _>(&(
            Bound::Included("aa".to_string()),
            Bound::Excluded("aaa".to_string()),
        ));
        assert!(!tree.covers_interval::<str, _>(&(Bound::Included("aa"), Bound::Excluded("aaaa"))));
        assert!(tree.covers_interval::<str, _>(&(
            Bound::Included("ab"),
            Bound::Excluded("asdasdasdsadsasd")
        )));
        assert_invariant(&tree);
    }

    #[test]
    fn insert_interval_boxed_slice() {
        let mut tree: IntervalTreeSet<Box<[u8]>> = Default::default();

        tree.insert_interval((
            Bound::Included(b"a".to_vec().into_boxed_slice()),
            Bound::Excluded(b"b".to_vec().into_boxed_slice()),
        ));

        assert!(tree.contains_point(b"a".as_slice()));
        assert!(tree.covers_interval::<[u8], _>(&(
            Bound::Included(&[b'a'][..]),
            Bound::Excluded(&[b'b'][..])
        )));
        assert_invariant(&tree);
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn remove_interval() {
        let mut tree: IntervalTreeSet<usize> = Default::default();

        tree.insert_interval((Bound::Included(1), Bound::Included(100)));
        assert!(tree.covers_interval(&(Bound::Included(1), Bound::Included(100))));
        test_contains_ranges(&tree, &[1..=100]);

        // Remove a range that is a subset of an existing range
        tree.remove_interval(&(Bound::Included(15), Bound::Excluded(50)));
        assert!(!tree.covers_interval(&(Bound::Included(15), Bound::Excluded(50))));
        test_contains_ranges(&tree, &[1..=14, 50..=100]);

        // Remove a range that intersects with some current range
        tree.remove_interval(&(Bound::Excluded(70), Bound::Unbounded));
        test_contains_ranges(&tree, &[1..=14, 50..=70]);

        // Remove a range that intersects with two ranges
        tree.remove_interval(&(Bound::Excluded(13), Bound::Included(60)));
        test_contains_ranges(&tree, &[1..=13, 61..=70]);

        tree.remove_interval(&(Bound::Excluded(65), Bound::Excluded(68)));
        test_contains_ranges(&tree, &[1..=13, 61..=65, 68..=70]);

        // Remove a range that overlaps three ranges
        tree.remove_interval(&(Bound::Excluded(1), Bound::Included(80)));
        test_contains_ranges(&tree, &[1..2]);

        // Remove non existing interval
        tree.remove_interval(&(Bound::Excluded(1), Bound::Included(80)));
        test_contains_ranges(&tree, &[1..2]);

        // Remove all
        tree.remove_interval::<usize, (Bound<usize>, Bound<usize>)>(&(
            Bound::Unbounded,
            Bound::Unbounded,
        ));
        test_contains_ranges::<std::ops::Range<usize>>(&tree, &[]);

        // Remove from empty
        tree.remove_interval(&(Bound::Excluded(1), Bound::Included(80)));
        test_contains_ranges::<std::ops::Range<usize>>(&tree, &[]);
    }

    #[test]
    #[should_panic]
    fn remove_interval2() {
        let mut tree: IntervalTreeSet<usize> = Default::default();
        tree.remove_interval(&(Bound::Excluded(15), Bound::Excluded(15)));
    }

    #[test]
    #[should_panic]
    fn remove_interval3() {
        let mut tree: IntervalTreeSet<i32> = Default::default();
        tree.insert_point(-64);
        tree.remove_interval(&(Bound::Excluded(20), Bound::Excluded(-64)));

        assert!(tree.contains_point(&-64));
        assert!(!tree.contains_point(&-14));
    }

    #[test]
    fn insert_remove_point() {
        let mut tree: IntervalTreeSet<usize> = Default::default();

        tree.insert_interval((Bound::Included(1), Bound::Included(100)));
        tree.remove_point(&0);
        tree.remove_point(&15);
        tree.remove_point(&40);
        tree.remove_point(&100);
        tree.remove_point(&200);
        test_contains_ranges(&tree, &[1..15, 16..40, 41..100]);

        tree.remove_point(&99);
        tree.remove_point(&98);
        tree.remove_point(&1);
        tree.remove_point(&3);
        tree.remove_point(&2);
        test_contains_ranges(&tree, &[4..15, 16..40, 41..98]);

        tree.insert_point(1);
        tree.insert_point(2);
        tree.insert_point(3);
        tree.insert_point(99);
        tree.insert_point(98);
        tree.insert_point(97);
        tree.insert_point(50);
        test_contains_ranges(&tree, &[1..15, 16..40, 41..100]);
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn remove_range_from_unbounded() {
        let mut tree: IntervalTreeSet<usize> = Default::default();

        tree.insert_interval(..);
        tree.remove_interval(&(Bound::Included(15), Bound::Unbounded));
        test_contains_ranges(&tree, &[0..15]);

        tree.remove_interval(&(Bound::Unbounded, Bound::Included(13)));
        test_contains_ranges(&tree, &[14..15]);

        tree.insert_interval(..);
        tree.remove_interval(&(Bound::Unbounded, Bound::Excluded(13)));
        test_contains_ranges(&tree, &[13..usize::MAX]);
    }

    #[test]
    fn covers_interval() {
        let mut tree: IntervalTreeSet<usize> = Default::default();

        tree.insert_interval(..);
        assert!(tree.covers_interval(&(100..=19999)));
        assert!(tree.covers_interval(&(100..19999)));
        assert!(tree.covers_interval(&(12..3534)));

        tree.remove_interval(&(Bound::Included(60), Bound::Excluded(100)));
        assert!(tree.covers_interval(&(&10..&60)));
        assert!(tree.covers_interval(&(10..60)));
        assert!(!tree.covers_interval(&(10..=60)));
        assert!(!tree.covers_interval(&(70..=80)));
        assert!(!tree.covers_interval(&(99..200)));
        assert!(tree.covers_interval(&(100..200)));
        assert!(!tree.covers_interval(&(40..200)));

        let mut tree: IntervalTreeSet<usize> = Default::default();
        tree.insert_interval((Bound::Unbounded, Bound::Excluded(60)));
        tree.insert_interval((Bound::Included(60), Bound::Unbounded));
        assert!(tree.covers_interval(&(0..60)));
        assert!(tree.covers_interval(&(0..usize::MAX)));

        let mut tree: IntervalTreeSet<usize> = Default::default();
        tree.insert_interval((Bound::Unbounded, Bound::Included(60)));
        tree.insert_interval((Bound::Excluded(60), Bound::Unbounded));
        assert!(tree.covers_interval(&(0..60)));
        assert!(tree.covers_interval(&(0..usize::MAX)));
    }

    #[test]
    fn range_diff() {
        use Bound::*;

        let mut tree: IntervalTreeSet<usize> = Default::default();
        tree.insert_interval((Unbounded, Excluded(200)));
        tree.insert_interval((Included(300), Included(400)));

        let intervals = tree.get_interval_difference(&(Unbounded, Included(200)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Included(&200), Included(&200))]
        );

        let intervals = tree.get_interval_difference(&(Unbounded, Excluded(200)));
        assert_eq!(intervals.collect::<Vec<_>>(), vec![]);

        let intervals = tree.get_interval_difference(&(Included(50), Unbounded));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![
                (Included(&200), Excluded(&300)),
                (Excluded(&400), Unbounded)
            ]
        );

        let intervals = tree.get_interval_difference(&(Included(250), Unbounded));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![
                (Included(&250), Excluded(&300)),
                (Excluded(&400), Unbounded)
            ]
        );

        let intervals = tree.get_interval_difference(&(Included(200), Included(500)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![
                (Included(&200), Excluded(&300)),
                (Excluded(&400), Included(&500))
            ]
        );

        let intervals = tree.get_interval_difference(&(Included(250), Included(500)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![
                (Included(&250), Excluded(&300)),
                (Excluded(&400), Included(&500))
            ]
        );

        let intervals = tree.get_interval_difference(&(Included(400), Included(500)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Excluded(&400), Included(&500))]
        );

        let intervals = tree.get_interval_difference(&(Included(400), Included(400)));
        assert_eq!(intervals.collect::<Vec<_>>(), vec![]);

        let intervals = tree.get_interval_difference(&(Included(200), Included(200)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Included(&200), Included(&200)),]
        );

        let intervals = tree.get_interval_difference(&(Bound::<usize>::Unbounded, Unbounded));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![
                (Included(&200), Excluded(&300)),
                (Excluded(&400), Unbounded)
            ]
        );
    }

    #[test]
    fn range_diff2() {
        use Bound::*;

        let mut tree: IntervalTreeSet<usize> = Default::default();
        tree.insert_interval((Excluded(200), Included(599)));
        tree.insert_interval((Included(800), Included(900)));
        tree.insert_interval((Excluded(1000), Excluded(2000)));

        let intervals = tree.get_interval_difference(&(Unbounded, Included(200)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Unbounded, Included(&200))]
        );

        let intervals = tree.get_interval_difference(&(Unbounded, Included(300)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Unbounded, Included(&200))]
        );

        let intervals = tree.get_interval_difference(&(Unbounded, Excluded(200)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Unbounded, Excluded(&200))]
        );

        let intervals = tree.get_interval_difference(&(Included(100), Included(1500)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![
                (Included(&100), Included(&200)),
                (Excluded(&599), Excluded(&800)),
                (Excluded(&900), Included(&1000)),
            ]
        );
    }

    #[test]
    fn range_diff3() {
        use Bound::*;

        let mut tree: IntervalTreeSet<usize> = Default::default();
        tree.insert_interval((Excluded(1), Unbounded));

        let mut intervals = tree.get_interval_difference(&(Excluded(1), Unbounded));
        assert!(intervals.next().is_none());
    }

    #[test]
    fn range_diff4() {
        use Bound::*;

        let mut tree: IntervalTreeSet<usize> = Default::default();
        tree.insert_interval((Included(200), Included(900)));

        let intervals = tree.get_interval_difference(&(Included(100), Included(200)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Included(&100), Excluded(&200)),]
        );
    }

    #[test]
    fn range_diff5() {
        use Bound::*;

        let mut tree: IntervalTreeSet<usize> = Default::default();
        tree.insert_interval((Unbounded, Excluded(200)));
        tree.insert_interval((Included(300), Included(400)));

        let intervals = tree.get_interval_difference(&(Included(250), Included(300)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Included(&250), Excluded(&300))]
        );
    }

    #[test]
    fn range_diff6() {
        use Bound::*;

        let mut tree: IntervalTreeSet<usize> = Default::default();

        tree.insert_interval((Included(25797), Included(25929)));
        tree.insert_interval((Included(25969), Included(26023)));

        let intervals = tree.get_interval_difference(&(Excluded(25929), Included(25969)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Excluded(&25929), Excluded(&25969))],
        );
    }

    #[test]
    fn range_diff7() {
        use Bound::*;

        let mut tree: IntervalTreeSet<usize> = Default::default();

        tree.insert_interval((Excluded(19), Excluded(21)));
        tree.remove_point(&20);

        assert!(!tree.contains_point(&20));

        let intervals = tree.get_interval_difference(&(Included(20), Included(20)));
        assert_eq!(
            intervals.collect::<Vec<_>>(),
            vec![(Included(&20), Included(&20))],
        );
        assert!(tree
            .get_interval_overlaps(&(Included(20), Included(20)))
            .next()
            .is_none());
    }

    #[proptest]
    fn insert_interval_is_in_tree(intervals: Vec<(Bound<u16>, Bound<u16>)>) {
        let mut tree = IntervalTreeSet::default();
        let mut all_intervals = std::collections::HashSet::new();

        for interval in intervals {
            // Skip invalid intervals
            if !check_interval(&interval) {
                continue;
            }

            all_intervals.insert(interval);
            tree.insert_interval(interval);

            for int in all_intervals.iter() {
                assert_invariant(&tree);
                // Every previously added interval should still be covered
                assert!(tree.covers_interval(int));
                // There should be no interval differences, because interval is covered
                assert!(tree.get_interval_difference(int).next().is_none());
                // There should be a single full overlap, since interval is covered
                let mut overlaps = tree.get_interval_overlaps(int);
                assert_eq!(
                    overlaps
                        .next()
                        .map(|(s, e)| (s.cloned(), e.cloned()))
                        .unwrap(),
                    *int
                );
                assert!(overlaps.next().is_none());
            }
        }
    }

    #[proptest]
    fn remove_interval_not_in_tree(intervals: Vec<(Bound<u16>, Bound<u16>)>) {
        let mut tree = IntervalTreeSet::<u16>::default();
        let mut all_intervals = std::collections::HashSet::new();
        tree.insert_interval(..);

        for interval in intervals {
            // Skip invalid intervals
            if !check_interval(&interval) {
                continue;
            }

            all_intervals.insert(interval);
            tree.remove_interval(&interval);

            for int in all_intervals.iter() {
                assert_invariant(&tree);
                // Every previously removed interval should still be missing
                assert!(!tree.covers_interval(int));
                // The interval difference should be the entire interval
                let mut diffs = tree.get_interval_difference(int);
                assert_eq!(
                    diffs.next().map(|(s, e)| (s.cloned(), e.cloned())).unwrap(),
                    *int
                );
                assert!(diffs.next().is_none());
                // There should be no overlaps
                assert!(tree.get_interval_overlaps(int).next().is_none());
            }
        }
    }
}
