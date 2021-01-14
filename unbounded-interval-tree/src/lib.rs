//! Implementation of an interval tree that works with inclusive/exclusive
//! bounds, as well as unbounded intervals. It is based on the
//! data structure described in Cormen et al.
//! (2009, Section 14.3: Interval trees, pp. 348–354). It provides methods
//! for "stabbing queries" (as in "is point `p` or an interval `i` contained in any intervals
//! in the tree of intervals?"), as well as helpers to get the difference between a queried
//! interval and the database (in order to find subsegments not covered), and the list of
//! intervals in the database overlapping a queried interval.
//!
//! # Implementation
//!
//! The [`IntervalTree`] is implemented as a binary search tree of intervals (which are represented
//! as a pair of [`Bound`]s), ordered by the lower bound of the interval and with each node
//! augmented with the maximum upper bound of all of its descendants
//!
//! Explicitly:
//!
//! - A tree is either an empty, or contains a single root [`Node`]
//! - Every [`Node`] contains an [interval](Node::key) and a single [max bound](Node::value)
//! - A node's [left child](Node::left) contains the interval in the tree with the greatest left
//!   bound less than or equal to the node's left bound
//! - A node's [right child](Node::right) contains the interval in the tree with the lowest left
//!   bound greater than or equal to the node's left bound
//! - The node's [max bound](Node::value) contains the maximum upper bound of all its children
//!
//! ## Invariants
//!
//! - Every range's lower bound is less than or equal to its upper bound
//! - Every node's upper bound is less than or equal to its parent's max bound
//! - The max bound of each node is equal to the upper bound of either that node, or one of that
//!   node's descendants
#![feature(bound_cloned, or_patterns)]

use std::cmp::Ordering;
use std::fmt;
use std::mem;
use std::ops::{Bound, RangeBounds};
use Bound::*;
use Ordering::*;

use launchpad::intervals::{
    cmp_end_start, cmp_endbound, cmp_start_end, cmp_startbound, covers, overlaps, BoundAsRef,
};
use proptest::arbitrary::Arbitrary;

/// A tree for storing intervals
///
/// See [the module documentation](crate) for more information
#[derive(Clone, Debug, PartialEq)]
pub struct IntervalTree<Q: Ord + Clone> {
    root: Option<Box<Node<Q>>>,
}

/// An inorder interator through the interval tree.
pub struct IntervalTreeIter<'a, Q: Ord + Clone> {
    to_visit: Vec<&'a Node<Q>>,
    curr: &'a Option<Box<Node<Q>>>,
}

impl<Q> fmt::Display for IntervalTree<Q>
where
    Q: Ord + Clone + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.root {
            Some(ref root) => write!(f, "{}", root),
            None => write!(f, "Empty tree"),
        }
    }
}

impl<Q> Default for IntervalTree<Q>
where
    Q: Ord + Clone,
{
    fn default() -> IntervalTree<Q> {
        IntervalTree { root: None }
    }
}

impl<Q> Arbitrary for IntervalTree<Q>
where
    Q: Ord + Clone + Arbitrary,
{
    type Parameters = <Vec<(Bound<Q>, Bound<Q>)> as Arbitrary>::Parameters;
    #[allow(clippy::type_complexity)]
    type Strategy = proptest::strategy::Map<
        <Vec<(Bound<Q>, Bound<Q>)> as Arbitrary>::Strategy,
        fn(Vec<(Bound<Q>, Bound<Q>)>) -> Self,
    >;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;

        any_with::<Vec<(Bound<Q>, Bound<Q>)>>(params).prop_map(|intervals| {
            let mut tree = Self::default();
            for (mut lower, mut upper) in intervals {
                if let (
                    Bound::Included(ref mut lower) | Bound::Excluded(ref mut lower),
                    Bound::Included(ref mut upper) | Bound::Excluded(ref mut upper),
                ) = (&mut lower, &mut upper)
                {
                    if lower > upper {
                        std::mem::swap(lower, upper);
                    }
                }
                tree.insert((lower, upper));
            }
            tree
        })
    }
}
impl<Q> IntervalTree<Q>
where
    Q: Ord + Clone,
{
    /// Produces an inorder iterator for the interval tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::Included;
    ///
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// tree.insert((Included(0), Included(10)));
    /// tree.insert((Included(-5), Included(-1)));
    /// tree.insert((Included(20), Included(30)));
    ///
    /// let mut iter = tree.iter();
    /// assert_eq!(iter.next(), Some(&(Included(-5), Included(-1))));
    /// assert_eq!(iter.next(), Some(&(Included(0), Included(10))));
    /// assert_eq!(iter.next(), Some(&(Included(20), Included(30))));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter(&self) -> IntervalTreeIter<'_, Q> {
        IntervalTreeIter {
            to_visit: vec![],
            curr: &self.root,
        }
    }

    /// Inserts an interval `range` into the interval tree. Insertions respect the
    /// binary search properties of this tree. An improvement to come is to rebalance
    /// the tree (following an AVL or a red-black scheme).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::{Included, Excluded, Unbounded};
    ///
    /// let mut int_tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// int_tree.insert((Included(5), Excluded(9)));
    /// int_tree.insert((Unbounded, Included(10)));
    ///
    /// let mut str_tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// str_tree.insert("Noria"..);
    /// ```
    pub fn insert<R>(&mut self, range: R)
    where
        R: RangeBounds<Q> + Clone,
    {
        self.insert_node(Box::new(Node::new(range)));
    }

    fn insert_node(&mut self, node: Box<Node<Q>>) {
        // If the tree is empty, put new node at the root.
        if self.root.is_none() {
            self.root = Some(node);
            return;
        }

        // Otherwise, walk down the tree and insert when we reach leaves.
        // TODO(jonathangb): Rotate tree?
        let mut curr = self.root.as_mut().unwrap();
        loop {
            curr.maybe_update_value(node.value.as_ref());

            match cmp(&curr.key, &node.key) {
                Equal => return, // Don't insert a redundant key.
                Less => {
                    match curr.right {
                        None => {
                            curr.right = Some(node);
                            return;
                        }
                        Some(ref mut n) => curr = n,
                    };
                }
                Greater => {
                    match curr.left {
                        None => {
                            curr.left = Some(node);
                            return;
                        }
                        Some(ref mut n) => curr = n,
                    };
                }
            };
        }
    }

    /// Inserts a single point into the interval tree.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut int_tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// int_tree.insert_point(1);
    /// assert!(int_tree.contains_point(&1));
    /// ```
    pub fn insert_point(&mut self, point: Q) {
        self.insert((Included(&point), Included(&point)));
    }

    /// Remove an interval from the interval tree
    ///
    /// # Examples
    ///
    /// ```
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// tree.insert(5..=10);
    /// assert!(tree.contains_point(&6));
    /// assert!(tree.contains_interval((7..9)));
    ///
    /// tree.remove(&(5..9));
    /// assert!(!tree.contains_interval((5..9)));
    /// assert!(!tree.contains_interval((7..=8)));
    /// ```
    pub fn remove<R>(&mut self, range: &R)
    where
        R: RangeBounds<Q>,
    {
        // A high-level explanation of the algorithm follows. For the sake of illustration, we'll
        // also be working with an example instance of deletion with an example tree. At the
        // beginning, our tree looks like this:
        //
        //                                   [5, 10], 11]
        //
        //                     [2, 3], 11]                  (6, 7], 9]
        //
        //          (0, 11], 11]    (2, 5), 5)                      [8, 9], 9]
        //
        //   [0, 3), 3)
        //
        // And we're going to be removing the interval [4, 7]
        //
        //
        // - we recursively walk down the tree, skipping subtrees that are guaranteed to be
        //   completely outside of `range`
        //
        //   - concretely, we can skip:
        //     - all nodes to the right of nodes whose lower bound is past the end of `range`.
        //     - all nodes whose max value is below the start of `range`.
        //
        //   In our example, this allows us to skip the intervals [0, 3) and [8, 9]
        //
        // - for each node in the tree, first we check to see if it or any of its children are
        //   *completely* covered by `range` (this is `maybe_replace_node`'s job)
        //   - if it is, we replace that node with its smallest child (or delete it if it has no
        //     children), and add the other child (if any) into a queue of nodes to re-insert once
        //     we're done walking the tree
        //   - we then recurse on the new node until we stop encountering fully covered nodes
        //
        //   In our example, we are able to replace (6, 7] with its child [8, 9]. That interval is
        //   not fully covered, so we don't have to recurse.
        //
        // - if we haven't replaced the node but its interval overlaps with `range`, we subtract
        //   `range` from its interval, resulting in either one or two ranges. We then replace the
        //   node's interval with the lower of the two and put the upper in the re-insert queue
        //
        //   - if, as a result of this, we've increased the lower bound past the lower bound of the
        //     node's right child, we have to split that child off and put it in the queue to
        //     re-insert later
        //   - after recursively walking the node's children, we re-calculate the node's max by
        //     taking the max of its left child's max, its right child's max, and its own upper
        //     bound.
        //
        //   In our example we have three nodes whose intervals overlap with but are not covered by
        //   our range:
        //
        //   - the root node, [5, 10], with [4, 7] removed results in two ranges, [5, 4) and (7,
        //     10]. We replace [5, 10] with [5, 4) and add (7, 10] to the queue to re-insert
        //
        //   - (2, 5) with [4, 7] removed results in a single range, (2, 4), so we're able to
        //     replace the node direcly. The node has no children, so we don't need to split
        //     anything off or recurse, so the node's max value is recalculated at 4).
        //
        //   - (0, 11] with [4, 7] removed results in two ranges, (0, 4) and (7, 11]. We replace (0,
        //     11] with (0, 4) and put (7, 11] in the queue to re-insert. The lower bound hasn't
        //     changed, so we don't need to do anything with the children, so we recalculate the
        //     node's max value at 4), and recursively recalculate the parent and grandparent node's
        //     max values at 4).
        //
        // - once we're done walking the tree, we run through the list of split-off nodes and walk
        //   each of *their* subtrees, then reinsert back into the tree, and keep going until we
        //   haven't split anything off on the last iteration
        //
        //   In our example our queue contains two nodes - (7, 10] and (7, 11]. Neither of these
        //   nodes have to walk, so we just reinsert them below our new [8, 9] node, recalculate the
        //   max values, and we're done.
        //   The resulting tree looks like this:
        //
        //                               [5, 4), 11]
        //
        //                     [2, 3], 4)            [8, 9], 11]
        //
        //          (0, 4), 4)    (2, 4), 4)     (7, 10] 11]
        //
        //   [0, 3), 3)                                  (7, 11] 11]

        /// If a node is entirely covered by a bound, replace it with its own child (collecting the
        /// other child if it exists into `to_insert` and recurse.
        fn replace_node_if_covered<Q, R>(
            node: &mut Option<Box<Node<Q>>>,
            range: &R,
            to_insert: &mut Vec<Box<Node<Q>>>,
        ) where
            Q: Ord + Clone,
            R: RangeBounds<Q>,
        {
            if node.iter().any(|n| covers(range, &n.key)) {
                let n = node.as_mut().unwrap();
                // we're gonna throw the node away anyway, so just steal the left and right
                // separately
                let right = mem::replace(&mut n.right, None);
                let left = mem::replace(&mut n.left, None);
                match (left, right) {
                    (None, None) => {
                        *node = None;
                        return;
                    }
                    (Some(left), None) => *node = Some(left),
                    (None, Some(right)) => *node = Some(right),
                    (Some(left), Some(right)) => {
                        *node = Some(left);
                        to_insert.push(right);
                    }
                }
                replace_node_if_covered(node, range, to_insert);
            }
        }

        fn walk_tree<Q, R>(node: &mut Node<Q>, range: &R, to_insert: &mut Vec<Box<Node<Q>>>)
        where
            Q: Ord + Clone,
            R: RangeBounds<Q>,
        {
            if cmp_end_start(node.value.as_ref(), range.start_bound()) == Less {
                // the upper bound of all of this node's descendants is less than the start bound of
                // the range we care about, so we can skip it entirely
                return;
            }

            if overlaps(&node.key, range) {
                // split node's range on the argument range, yielding one or two new ranges
                let below = if matches!(
                    cmp_startbound(range.start_bound(), node.key.start_bound()),
                    Less | Equal
                ) {
                    None
                } else {
                    Some((
                        node.key.0.clone(),
                        match range.start_bound() {
                            Included(x) => Excluded(x.clone()),
                            Excluded(x) => Included(x.clone()),
                            Unbounded => Unbounded,
                        },
                    ))
                };

                let above = if matches!(
                    cmp_endbound(range.end_bound(), node.key.end_bound()),
                    Greater | Equal
                ) {
                    None
                } else {
                    Some((
                        match range.end_bound() {
                            Included(x) => Excluded(x.clone()),
                            Excluded(x) => Included(x.clone()),
                            Unbounded => Unbounded,
                        },
                        node.key.1.clone(),
                    ))
                };

                match (below, above) {
                    (None, Some(new)) | (Some(new), None) => {
                        node.key = new;
                    }
                    (Some(below), Some(above)) => {
                        node.key = below;
                        to_insert.push(Box::new(Node::new(above)));
                    }
                    (None, None) => unreachable!(
                        "fully covered node should have already been removed in maybe_replace_node",
                    ),
                }

                // if we've increased the lower bound past the lower bound of our right
                // child, we have to split that child off and re-insert it later
                //
                // We can only increase our lower bound, never decrease, because we're
                // always shrinking the range
                let right_needs_split = node.right.iter().any(|right| {
                    cmp_startbound(node.key.start_bound(), right.key.start_bound()) == Greater
                });
                if right_needs_split {
                    let right = mem::replace(&mut node.right, None).unwrap();
                    to_insert.push(right);
                }
            }

            // now walk the children

            // first, the left
            replace_node_if_covered(&mut node.left, range, to_insert);
            if let Some(ref mut left) = node.left {
                walk_tree(left, range, to_insert);
            }

            // now, if the node's start is not above our end (which would mean all children to the
            // right are irrelevant), we walk to the right
            if matches!(
                cmp_start_end(node.key.start_bound(), range.end_bound()),
                Less | Equal
            ) {
                replace_node_if_covered(&mut node.right, range, to_insert);
                if let Some(ref mut right) = node.right {
                    walk_tree(right, range, to_insert);
                } else {
                    node.value = node.key.1.clone();
                }
            }

            // Finally, for each node, we recalculate its upper bound by  taking the max of its left
            // child's value, its right child's value, and its own upper bound
            node.value = std::iter::once(node.key.end_bound())
                .chain(node.left.iter().map(|n| n.value.as_ref()))
                .chain(node.right.iter().map(|n| n.value.as_ref()))
                .max_by(|l, r| cmp_endbound(*l, *r))
                .unwrap()
                .cloned();
        }

        // Collection of nodes we've split off that we have to re-insert later
        let mut to_insert = Vec::new();

        // First, maybe replace the root node and promote its children
        replace_node_if_covered(&mut self.root, range, &mut to_insert);

        if let Some(ref mut root) = self.root {
            walk_tree(root, range, &mut to_insert);
        }

        // Now, keep walking and inserting the contents of to_insert until it empties
        while !to_insert.is_empty() {
            let mut new_to_insert = Vec::new();
            for node in to_insert {
                let mut opt_node = Some(node);
                replace_node_if_covered(&mut opt_node, range, &mut new_to_insert);
                // If we've split a node off, we haven't walked it to remove the range yet, so we do
                // that here
                if let Some(mut node) = opt_node {
                    walk_tree(&mut node, range, &mut new_to_insert);
                    self.insert_node(node);
                }
            }
            to_insert = new_to_insert
        }
    }

    /// Remove a single point from the interval tree
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// tree.insert(5..=10);
    /// tree.remove_point(&7);
    ///
    /// assert!(!tree.contains_point(&7));
    /// assert!(tree.contains_interval(5..7));
    /// assert!(tree.contains_interval(8..=10));
    /// ````
    pub fn remove_point(&mut self, point: &Q) {
        self.remove(&(Included(point), Included(point)))
    }

    /// A "stabbing query" in the jargon: returns whether or not a point `q`
    /// is contained in any of the intervals stored in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::{Excluded, Unbounded};
    ///
    /// let mut int_tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// int_tree.insert((Excluded(5), Unbounded));
    ///
    /// assert!(int_tree.contains_point(&100));
    /// assert!(!int_tree.contains_point(&5));
    /// ```
    ///
    /// Note that we can work with any type that implements the `Ord+Clone` traits, so
    /// we are not limited to just integers.
    ///
    /// ```
    /// use std::ops::Bound::{Excluded, Unbounded};
    ///
    /// let mut str_tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// str_tree.insert((Excluded("Noria"), Unbounded));
    ///
    /// assert!(str_tree.contains_point(&"Zebra"));
    /// assert!(!str_tree.contains_point(&"Noria"));
    /// ```
    pub fn contains_point(&self, q: &Q) -> bool {
        self.contains_interval((Included(q), Included(q)))
    }

    /// An alternative "stabbing query": returns whether or not an interval `q`
    /// is fully covered by the intervals stored in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::{Included, Excluded, Unbounded};
    ///
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// tree.insert((Included(20), Included(30)));
    /// tree.insert((Excluded(30), Excluded(50)));
    ///
    /// assert!(tree.contains_interval((Included(20), Included(40))));
    /// assert!(!tree.contains_interval((Included(30), Included(50))));
    /// ```
    pub fn contains_interval<R>(&self, q: R) -> bool
    where
        R: RangeBounds<Q> + Clone,
    {
        self.get_interval_difference(q).is_empty()
    }

    /// Returns the inorder list of all intervals stored in the tree that overlaps
    /// with a given range `q` (partially or completely).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::{Included, Excluded, Unbounded};
    ///
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// tree.insert((Included(0), Included(5)));
    /// tree.insert((Included(7), Excluded(10)));
    ///
    /// assert_eq!(tree.get_interval_overlaps((Included(-5), Excluded(7))),
    ///            vec![&(Included(0), Included(5))]);
    /// assert!(tree.get_interval_overlaps((Included(10), Unbounded)).is_empty());
    /// ```
    pub fn get_interval_overlaps<R>(&self, q: R) -> Vec<&(Bound<Q>, Bound<Q>)>
    where
        R: RangeBounds<Q>,
    {
        let curr = &self.root;
        let mut acc = Vec::new();

        Self::get_interval_overlaps_rec(curr, &q, &mut acc);
        acc
    }

    /// Returns the ordered list of subintervals in `q` that are not covered by the tree.
    /// This is useful to compute what subsegments of `q` that are not covered by the intervals
    /// stored in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::{Included, Excluded, Unbounded};
    ///
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// tree.insert((Included(0), Excluded(10)));
    /// tree.insert((Excluded(10), Included(30)));
    /// tree.insert((Excluded(50), Unbounded));
    ///
    /// assert_eq!(tree.get_interval_difference((Included(-5), Included(30))),
    ///            vec![(Included(-5), Excluded(0)),
    ///                 (Included(10), Included(10))]);
    /// assert_eq!(tree.get_interval_difference((Unbounded, Excluded(10))),
    ///            vec![(Unbounded, Excluded(0))]);
    /// assert!(tree.get_interval_difference((Included(100), Unbounded)).is_empty());
    /// ```
    pub fn get_interval_difference<R>(&self, q: R) -> Vec<(Bound<Q>, Bound<Q>)>
    where
        R: RangeBounds<Q> + Clone,
    {
        let overlaps = self.get_interval_overlaps(q.clone());

        // If there is no overlap, then the difference is the query `q` itself.
        if overlaps.is_empty() {
            return vec![(q.start_bound().cloned(), q.end_bound().cloned())];
        }

        let mut acc = Vec::new();
        let first = *overlaps.first().unwrap();

        // If q.min < first.min, we have a difference to append.
        match (q.start_bound(), first.start_bound()) {
            (Unbounded, Included(first_min)) => acc.push((Unbounded, Excluded(first_min.clone()))),
            (Unbounded, Excluded(first_min)) => acc.push((Unbounded, Included(first_min.clone()))),
            (Included(q_min), Included(first_min)) if q_min < first_min => {
                acc.push((Included(q_min.clone()), Excluded(first_min.clone())))
            }
            (Excluded(q_min), Included(first_min)) if q_min < first_min => {
                acc.push((Excluded(q_min.clone()), Excluded(first_min.clone())))
            }
            (Excluded(q_min), Excluded(first_min)) if q_min < first_min => {
                acc.push((Excluded(q_min.clone()), Included(first_min.clone())))
            }
            (Included(q_min), Excluded(first_min)) if q_min <= first_min => {
                acc.push((Included(q_min.clone()), Included(first_min.clone())))
            }
            _ => {}
        };

        // If the max is unbounded, there can't be any difference going forward.
        if first.end_bound() == Unbounded {
            return acc;
        }

        // keeps track of the maximum of a contiguous interval.
        let mut contiguous = first.end_bound();
        for overlap in overlaps.iter().skip(1) {
            // If contiguous < overlap.min:
            //   1. We have a difference between contiguous -> overlap.min to fill.
            //     1.1: Note: the endpoints of the difference appended are the opposite,
            //          that is if contiguous was Included, then the difference must
            //          be Excluded, and vice versa.
            //   2. We need to update contiguous to be the new contiguous max.
            // Note: an Included+Excluded at the same point still is contiguous!
            match (contiguous, overlap.start_bound()) {
                (Included(contiguous_max), Included(overlap_min))
                    if contiguous_max < overlap_min =>
                {
                    acc.push((
                        Excluded(contiguous_max.clone()),
                        Excluded(overlap_min.clone()),
                    ));
                    contiguous = overlap.end_bound();
                }
                (Included(contiguous_max), Excluded(overlap_min))
                    if contiguous_max < overlap_min =>
                {
                    acc.push((
                        Excluded(contiguous_max.clone()),
                        Included(overlap_min.clone()),
                    ));
                    contiguous = overlap.end_bound();
                }
                (Excluded(contiguous_max), Included(overlap_min))
                    if contiguous_max < overlap_min =>
                {
                    acc.push((
                        Included(contiguous_max.clone()),
                        Excluded(overlap_min.clone()),
                    ));
                    contiguous = overlap.end_bound();
                }
                (Excluded(contiguous_max), Excluded(overlap_min))
                    if contiguous_max <= overlap_min =>
                {
                    acc.push((
                        Included(contiguous_max.clone()),
                        Included(overlap_min.clone()),
                    ));
                    contiguous = overlap.end_bound();
                }
                _ => {}
            }

            // If contiguous.max < overlap.max, we set contiguous to the new max.
            match (contiguous, overlap.end_bound()) {
                (_, Unbounded) => return acc,
                (Included(contiguous_max), Included(overlap_max))
                | (Excluded(contiguous_max), Excluded(overlap_max))
                | (Included(contiguous_max), Excluded(overlap_max))
                    if contiguous_max < overlap_max =>
                {
                    contiguous = overlap.end_bound()
                }
                (Excluded(contiguous_max), Included(overlap_max))
                    if contiguous_max <= overlap_max =>
                {
                    contiguous = overlap.end_bound()
                }
                _ => {}
            };
        }

        // If contiguous.max < q.max, we have a difference to append.
        match (contiguous, q.end_bound()) {
            (Included(contiguous_max), Included(q_max)) if contiguous_max < q_max => {
                acc.push((Excluded(contiguous_max.clone()), Included(q_max.clone())))
            }
            (Included(contiguous_max), Excluded(q_max)) if contiguous_max < q_max => {
                acc.push((Excluded(contiguous_max.clone()), Excluded(q_max.clone())))
            }
            (Excluded(contiguous_max), Excluded(q_max)) if contiguous_max < q_max => {
                acc.push((Included(contiguous_max.clone()), Excluded(q_max.clone())))
            }
            (Excluded(contiguous_max), Included(q_max)) if contiguous_max <= q_max => {
                acc.push((Included(contiguous_max.clone()), Included(q_max.clone())))
            }
            _ => {}
        };

        acc
    }

    fn get_interval_overlaps_rec<'a, R>(
        curr: &'a Option<Box<Node<Q>>>,
        q: &R,
        acc: &mut Vec<&'a (Bound<Q>, Bound<Q>)>,
    ) where
        R: RangeBounds<Q>,
    {
        // If we reach None, stop recursing along this subtree.
        let node = match curr {
            None => return,
            Some(node) => node,
        };

        // See if subtree.max < q.min. If that is the case, there is no point
        // in visiting the rest of the subtree (we know that the rest of the intervals
        // will necessarily be smaller than `q`).
        // ~ Recall the ordering rules (as defined in `fn cmp` below). ~
        // -> If subtree.max is Unbounded, subtree.max < q.min is impossible.
        // -> If q.min is Unbounded, subtree.max < q.min is impossible.
        // -> If they are equal, we have 4 cases:
        //  * subtree.max: Included(x) / q.min: Included(x) -> =, we keep visiting the subtree
        //  * subtree.max: Included(x) / q.min: Excluded(x) -> <, condition satisfied
        //  * subtree.max: Excluded(x) / q.min: Included(x) -> <, condition satisfied
        //  * subtree.max: Excluded(x) / q.min: Excluded(x) -> <, condition satisfied
        let max_subtree = match &node.value {
            Included(x) => Some((x, 2)),
            Excluded(x) => Some((x, 1)),
            Unbounded => None,
        };
        let min_q = match q.start_bound() {
            Included(x) => Some((x, 2)),
            Excluded(x) => Some((x, 3)),
            Unbounded => None,
        };
        match (max_subtree, min_q) {
            (Some(max_subtree), Some(min_q)) if max_subtree < min_q => return,
            _ => {}
        };

        // Search left subtree.
        Self::get_interval_overlaps_rec(&node.left, q, acc);

        // Visit this node.
        // If node.min <= q.max AND node.max >= q.min, we have an intersection.
        // Let's start with the first inequality, node.min <= q.max.
        // -> If node.min is Unbounded, node.min <= q.max is a tautology.
        // -> If q.max is Unbounded, node.min <= q.max is a tautology.
        // -> If they are equal, we have 4 cases:
        //  * node.min: Included(x) / q.max: Included(x) -> =, we go to 2nd inequality
        //  * node.min: Included(x) / q.max: Excluded(x) -> >, 1st inequality not satisfied
        //  * node.min: Excluded(x) / q.max: Included(x) -> >, 1st inequality not satisfied
        //  * node.min: Excluded(x) / q.max: Excluded(x) -> >, 1st inequality not satisfied
        //
        // Notice that after we visit the node, we should visit the right subtree. However,
        // if node.min > q.max, we can skip right visiting the right subtree.
        // -> If node.min is Unbounded, node.min > q.max is impossible.
        // -> If q.max is Unbounded, node.min > q.max is impossible.
        //
        // It just so happens that we already do this check in the match to satisfy
        // the previous first condition. Hence, we decided to add an early return
        // in there, rather than repeat the logic afterwards.
        let min_node = match node.key.start_bound() {
            Included(x) => Some((x, 2)),
            Excluded(x) => Some((x, 3)),
            Unbounded => None,
        };
        let max_q = match q.end_bound() {
            Included(x) => Some((x, 2)),
            Excluded(x) => Some((x, 1)),
            Unbounded => None,
        };
        match (min_node, max_q) {
            // If the following condition is met, we do not have an intersection.
            // On top of that, we know that we can skip visiting the right subtree,
            // so we can return eagerly.
            (Some(min_node), Some(max_q)) if min_node > max_q => return,
            _ => {
                // Now we are at the second inequality, node.max >= q.min.
                // -> If node.max is Unbounded, node.max >= q.min is a tautology.
                // -> If q.min is Unbounded, node.max >= q.min is a tautology.
                // -> If they are equal, we have 4 cases:
                //  * node.max: Included(x) / q.min: Included(x) -> =, 2nd inequality satisfied
                //  * node.max: Included(x) / q.min: Excluded(x) -> <, 2nd inequality not satisfied
                //  * node.max: Excluded(x) / q.min: Included(x) -> <, 2nd inequality not satisfied
                //  * node.max: Excluded(x) / q.min: Excluded(x) -> <, 2nd inequality not satisfied
                let max_node = match &node.key.1 {
                    Included(x) => Some((x, 2)),
                    Excluded(x) => Some((x, 1)),
                    Unbounded => None,
                };

                match (max_node, min_q) {
                    (Some(max_node), Some(min_q)) if max_node < min_q => {}
                    _ => acc.push(&node.key),
                };
            }
        };

        // Search right subtree.
        Self::get_interval_overlaps_rec(&node.right, q, acc);
    }

    /// Removes a random leaf from the tree,
    /// and returns the range stored in the said node.
    /// The returned value will be `None` if the tree is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::{Included, Excluded, Unbounded};
    ///
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// tree.insert((Included(5), Excluded(9)));
    /// tree.insert((Unbounded, Included(10)));
    ///
    /// assert!(tree.contains_point(&10));
    /// assert!(tree.contains_point(&6));
    ///
    /// let deleted = tree.remove_random_leaf();
    /// assert!(deleted.is_some());
    /// assert!(!tree.contains_point(&10));
    /// assert!(tree.contains_point(&6));
    ///
    /// let deleted = tree.remove_random_leaf();
    /// assert!(deleted.is_some());
    /// assert!(!tree.contains_point(&6));
    ///
    /// let deleted = tree.remove_random_leaf();
    /// assert!(deleted.is_none());
    /// ```
    pub fn remove_random_leaf(&mut self) -> Option<(Bound<Q>, Bound<Q>)> {
        use rand::random;

        // If interval tree is empty, just return None.
        self.root.as_ref()?;

        let mut curr = self.root.as_mut().unwrap();

        // If we only have one node, delete it right away.
        if curr.left.is_none() && curr.right.is_none() {
            let root = mem::replace(&mut self.root, None).unwrap();
            return Some(root.key);
        }

        // Keep track of visited nodes, because we will need to walk up
        // the tree after deleting the leaf in order to possibly update
        // their value stored.
        // The first element of the tuple is a &mut to the value of the node,
        // whilst the second element is the new potential value to store, based
        // on the non-visited path (recall that this is a BST). It
        // is very much possible that both elements are equal: that would imply that the
        // current value depends solely on the non-visited path, hence the deleted
        // node will have no impact up the tree, at least from the current point.
        let mut path: Vec<(_, _)> = Vec::new();

        // Used to keep track of the direction taken from a node.
        enum Direction {
            LEFT,
            RIGHT,
        }

        // Traverse the tree until we find a leaf.
        let (deleted, new_max) = loop {
            // Note that at this point in the loop, `curr` can't be a leaf.
            // Indeed, we traverse the tree such that `curr` is always an
            // internal node, so that it is easy to replace a leaf from `curr`.
            #[allow(clippy::if_same_then_else)]
            let direction = if curr.left.is_none() {
                Direction::RIGHT
            } else if curr.right.is_none() {
                Direction::LEFT
            } else if random() {
                Direction::LEFT
            } else {
                Direction::RIGHT
            };
            // End-bound of the current node.
            let curr_end = curr.key.end_bound();

            // LEFT and RIGHT paths are somewhat repetitive, but this way
            // was the only way to satisfy the borrowchecker...
            match direction {
                Direction::LEFT => {
                    // If we go left and the right path is `None`,
                    // then the right path has no impact towards
                    // the value stored by the current node.
                    // Otherwise, the current node's value might change
                    // to the other branch's max value once we remove the
                    // leaf, so let's keep track of that.
                    let max_other = if curr.right.is_none() {
                        curr_end
                    } else {
                        let other_value = curr.right.as_ref().unwrap().value.as_ref();
                        match cmp_endbound(curr_end, other_value) {
                            Greater | Equal => curr_end,
                            Less => other_value,
                        }
                    };

                    // Check if the next node is a leaf. If it is, then we want to
                    // stop traversing, and remove the leaf.
                    let next = curr.left.as_ref().unwrap();
                    if next.is_leaf() {
                        curr.value = max_other.cloned();
                        break (mem::replace(&mut curr.left, None).unwrap(), max_other);
                    }

                    // If the next node is *not* a leaf, then we can update the visited path
                    // with the current values, and move on to the next node.
                    path.push((&mut curr.value, max_other));
                    curr = curr.left.as_mut().unwrap();
                }
                Direction::RIGHT => {
                    let max_other = if curr.left.is_none() {
                        curr_end
                    } else {
                        let other_value = curr.left.as_ref().unwrap().value.as_ref();
                        match cmp_endbound(curr_end, other_value) {
                            Greater | Equal => curr_end,
                            Less => other_value,
                        }
                    };

                    let next = curr.right.as_ref().unwrap();
                    if next.is_leaf() {
                        curr.value = max_other.cloned();
                        break (mem::replace(&mut curr.right, None).unwrap(), max_other);
                    }

                    path.push((&mut curr.value, max_other));
                    curr = curr.right.as_mut().unwrap();
                }
            };
        };

        // We have removed the leaf. Now, we bubble-up the visited path.
        // If the removed node's value impacted its ancestors, then we update
        // the ancestors' value so that they store the new max value in their
        // respective subtree.
        while let Some((value, max_other)) = path.pop() {
            if cmp_endbound(value.as_ref(), max_other) == Equal {
                break;
            }

            match cmp_endbound(value.as_ref(), new_max) {
                Equal => break,
                Greater => *value = new_max.cloned(),
                Less => unreachable!("Can't have a new max that is bigger"),
            };
        }

        Some(deleted.key.clone())
    }

    /// Returns the number of ranges stored in the interval tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::{Included, Excluded, Unbounded};
    ///
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// assert_eq!(tree.len(), 0);
    ///
    /// tree.insert((Included(5), Excluded(9)));
    /// tree.insert((Unbounded, Included(10)));
    ///
    /// assert_eq!(tree.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.root.as_ref().map_or(0, |n| n.size())
    }

    /// Returns `true` if the map contains no element.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::{Included, Excluded, Unbounded};
    ///
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// assert!(tree.is_empty());
    ///
    /// tree.insert((Included(5), Excluded(9)));
    ///
    /// assert!(!tree.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear the interval tree, removing all values stored.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound::{Included, Excluded, Unbounded};
    ///
    /// let mut tree = unbounded_interval_tree::IntervalTree::default();
    ///
    /// tree.insert((Included(5), Unbounded));
    /// tree.clear();
    ///
    /// assert!(tree.is_empty());
    /// ```
    pub fn clear(&mut self) {
        self.root = None;
    }
}

impl<'a, Q> Iterator for IntervalTreeIter<'a, Q>
where
    Q: Ord + Clone,
{
    type Item = &'a (Bound<Q>, Bound<Q>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr.is_none() && self.to_visit.is_empty() {
            return None;
        }

        while self.curr.is_some() {
            self.to_visit.push(self.curr.as_ref().unwrap());
            self.curr = &self.curr.as_ref().unwrap().left;
        }

        let visited = self.to_visit.pop();
        self.curr = &visited.as_ref().unwrap().right;
        Some(&visited.unwrap().key)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Node<Q: Ord + Clone> {
    key: (Bound<Q>, Bound<Q>),
    /// Max end-point.
    value: Bound<Q>,
    left: Option<Box<Node<Q>>>,
    right: Option<Box<Node<Q>>>,
}

impl<Q> fmt::Display for Node<Q>
where
    Q: Ord + Clone + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let start = match self.key.0 {
            Included(ref x) => format!("[{}", x),
            Excluded(ref x) => format!("]{}", x),
            Unbounded => String::from("]-∞"),
        };
        let end = match self.key.1 {
            Included(ref x) => format!("{}]", x),
            Excluded(ref x) => format!("{}[", x),
            Unbounded => "∞[".to_string(),
        };
        let value = match self.value {
            Included(ref x) => format!("{}]", x),
            Excluded(ref x) => format!("{}[", x),
            Unbounded => String::from("∞"),
        };

        if self.left.is_none() && self.right.is_none() {
            write!(f, " {{ {},{} ({}) }} ", start, end, value)
        } else if self.left.is_none() {
            write!(
                f,
                " {{ {},{} ({}) right:{}}} ",
                start,
                end,
                value,
                self.right.as_ref().unwrap()
            )
        } else if self.right.is_none() {
            write!(
                f,
                " {{ {},{} ({}) left:{}}} ",
                start,
                end,
                value,
                self.left.as_ref().unwrap()
            )
        } else {
            write!(
                f,
                " {{ {},{} ({}) left:{}right:{}}} ",
                start,
                end,
                value,
                self.left.as_ref().unwrap(),
                self.right.as_ref().unwrap()
            )
        }
    }
}

impl<Q> Node<Q>
where
    Q: Ord + Clone,
{
    pub fn new<R>(range: R) -> Node<Q>
    where
        R: RangeBounds<Q>,
    {
        let max = range.end_bound().cloned();

        Node {
            key: (range.start_bound().cloned(), max.clone()),
            value: max,
            left: None,
            right: None,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.left.is_none() && self.right.is_none()
    }

    pub fn maybe_update_value(&mut self, inserted_max: Bound<&Q>) {
        let self_max_q = match &self.value {
            Included(x) => Some((x, 2)),
            Excluded(x) => Some((x, 1)),
            Unbounded => None,
        };
        let inserted_max_q = match inserted_max {
            Included(x) => Some((x, 2)),
            Excluded(x) => Some((x, 1)),
            Unbounded => None,
        };
        match (self_max_q, inserted_max_q) {
            (None, _) => {}
            (_, None) => self.value = Unbounded,
            (Some(self_max_q), Some(inserted_max_q)) => {
                if self_max_q < inserted_max_q {
                    self.value = inserted_max.cloned();
                }
            }
        };
    }

    pub fn size(&self) -> usize {
        1 + self.left.as_ref().map_or(0, |n| n.size()) + self.right.as_ref().map_or(0, |n| n.size())
    }
}

fn cmp<Q, R, S>(r1: &R, r2: &S) -> Ordering
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::ops::Bound;

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

    fn node_children<Q: Clone + Ord>(node: &Node<Q>) -> Vec<Node<Q>> {
        let mut res = vec![node.clone()];
        if let Some(left) = &node.left {
            res.extend(node_children(&left));
        }

        if let Some(right) = &node.right {
            res.extend(node_children(&right));
        }
        res
    }

    fn check_invariants<Q>(tree: &IntervalTree<Q>)
    where
        Q: Ord + Clone + std::fmt::Debug,
    {
        // returns child_contains_max  or true if max is None
        fn check_node_invariants<Q>(node: &Node<Q>, max: Option<&Bound<Q>>)
        where
            Q: Ord + Clone + std::fmt::Debug,
        {
            // range is ordered
            if let (
                Bound::Included(lower) | Bound::Excluded(lower),
                Bound::Included(upper) | Bound::Excluded(upper),
            ) = &node.key
            {
                assert!(lower <= upper);
            }

            // our upper bound is <= parent's max
            if let Some(max) = max {
                assert!(
                    matches!(
                        cmp_endbound(node.key.end_bound(), max.as_ref()),
                        Ordering::Less | Ordering::Equal
                    ),
                    "end bound of node {:?} outside parent's max of {:?}",
                    node,
                    max
                );
            }

            // us or one of our children has our max as the upper bound
            assert!(
                std::iter::once(node)
                    .chain(node_children(node).iter())
                    .any(|n| n.key.1 == node.value),
                "node has invalid upper bound: {:?}",
                node
            );

            if let Some(ref left) = node.left {
                check_node_invariants(left, Some(&node.value));
            }
            if let Some(ref right) = node.right {
                check_node_invariants(right, Some(&node.value));
            }
        }

        if let Some(ref root) = tree.root {
            check_node_invariants(root, None);
        }
    }

    #[test]
    fn it_inserts_root() {
        let mut tree = IntervalTree::default();
        assert!(tree.root.is_none());

        let key = (Included(1), Included(3));

        tree.insert(key);
        assert!(tree.root.is_some());
        assert_eq!(tree.root.as_ref().unwrap().key, key);
        assert_eq!(tree.root.as_ref().unwrap().value, key.1);
        assert!(tree.root.as_ref().unwrap().left.is_none());
        assert!(tree.root.as_ref().unwrap().right.is_none());
    }

    #[test]
    fn it_inserts_left_right_node() {
        let mut tree = IntervalTree::default();

        let root_key = (Included(2), Included(3));
        let left_key = (Included(0), Included(1));
        let left_right_key = (Excluded(1), Unbounded);

        tree.insert(root_key);
        assert!(tree.root.is_some());
        assert!(tree.root.as_ref().unwrap().left.is_none());

        tree.insert(left_key);
        assert!(tree.root.as_ref().unwrap().right.is_none());
        assert!(tree.root.as_ref().unwrap().left.is_some());
        assert_eq!(
            tree.root.as_ref().unwrap().left.as_ref().unwrap().value,
            left_key.1
        );

        tree.insert(left_right_key);
        assert!(tree
            .root
            .as_ref()
            .unwrap()
            .left
            .as_ref()
            .unwrap()
            .right
            .is_some());
    }

    #[test]
    fn it_updates_value() {
        let mut tree = IntervalTree::default();

        let root_key = (Included(2), Included(3));
        let left_key = (Included(0), Included(1));
        let left_left_key = (Included(-5), Excluded(10));
        let right_key = (Excluded(3), Unbounded);

        tree.insert(root_key);
        assert_eq!(tree.root.as_ref().unwrap().value, root_key.1);

        tree.insert(left_key);
        assert_eq!(tree.root.as_ref().unwrap().value, root_key.1);
        assert!(tree.root.as_ref().unwrap().left.is_some());
        assert_eq!(
            tree.root.as_ref().unwrap().left.as_ref().unwrap().value,
            left_key.1
        );

        tree.insert(left_left_key);
        assert_eq!(tree.root.as_ref().unwrap().value, left_left_key.1);
        assert_eq!(
            tree.root.as_ref().unwrap().left.as_ref().unwrap().value,
            left_left_key.1
        );
        assert!(tree
            .root
            .as_ref()
            .unwrap()
            .left
            .as_ref()
            .unwrap()
            .left
            .is_some());
        assert_eq!(
            tree.root
                .as_ref()
                .unwrap()
                .left
                .as_ref()
                .unwrap()
                .left
                .as_ref()
                .unwrap()
                .value,
            left_left_key.1
        );

        tree.insert(right_key);
        assert_eq!(tree.root.as_ref().unwrap().value, right_key.1);
        assert!(tree.root.as_ref().unwrap().right.is_some());
        assert_eq!(
            tree.root.as_ref().unwrap().left.as_ref().unwrap().value,
            left_left_key.1
        );
        assert_eq!(
            tree.root.as_ref().unwrap().right.as_ref().unwrap().value,
            right_key.1
        );
    }

    #[test]
    fn overlap_works_as_expected() {
        let mut tree: IntervalTree<i32> = IntervalTree::default();

        let root_key = (Included(2), Included(3));
        let left_key = (Included(0), Included(1));
        let left_left_key = (Included(-5), Excluded(10));
        let right_key = (Excluded(3), Unbounded);

        tree.insert(root_key);
        tree.insert(left_key);
        assert_eq!(tree.get_interval_overlaps(root_key), vec![&root_key]);

        tree.insert(left_left_key);
        assert_eq!(
            tree.get_interval_overlaps((Unbounded::<i32>, Unbounded)),
            vec![&left_left_key, &left_key, &root_key]
        );
        assert_eq!(
            tree.get_interval_overlaps((Included(100), Unbounded)),
            Vec::<&(Bound<i32>, Bound<i32>)>::new()
        );

        tree.insert(right_key);
        assert_eq!(
            tree.get_interval_overlaps(root_key),
            vec![&left_left_key, &root_key]
        );
        assert_eq!(
            tree.get_interval_overlaps((Unbounded::<i32>, Unbounded)),
            vec![&left_left_key, &left_key, &root_key, &right_key]
        );
        assert_eq!(
            tree.get_interval_overlaps((Included(100), Unbounded)),
            vec![&right_key]
        );
        assert_eq!(
            tree.get_interval_overlaps((Included(3), Excluded(10))),
            vec![&left_left_key, &root_key, &right_key]
        );
        assert_eq!(
            tree.get_interval_overlaps((Excluded(3), Excluded(10))),
            vec![&left_left_key, &right_key]
        );
        assert_eq!(
            tree.get_interval_overlaps((Unbounded, Excluded(2))),
            vec![&left_left_key, &left_key]
        );
        assert_eq!(
            tree.get_interval_overlaps((Unbounded, Included(2))),
            vec![&left_left_key, &left_key, &root_key]
        );
        assert_eq!(
            tree.get_interval_overlaps((Unbounded, Included(3))),
            vec![&left_left_key, &left_key, &root_key]
        );
    }

    #[test]
    fn difference_and_overlaps_with_tuple_works_as_expected() {
        let mut tree = IntervalTree::default();

        let root_key = (Included((1, 2)), Excluded((1, 4)));
        let right_key = (Included((5, 10)), Included((5, 20)));

        tree.insert(root_key);
        tree.insert(right_key);

        assert!(tree
            .get_interval_overlaps((Included((2, 0)), Included((2, 30))))
            .is_empty());
        assert_eq!(
            tree.get_interval_overlaps((Included((1, 3)), Included((1, 5)))),
            vec![&root_key]
        );
        assert_eq!(
            tree.get_interval_difference((Excluded((1, 1)), Included((1, 5)))),
            vec![
                (Excluded((1, 1)), Excluded((1, 2))),
                (Included((1, 4)), Included((1, 5)))
            ]
        );
    }

    #[test]
    fn difference_works_as_expected() {
        let mut tree = IntervalTree::default();

        let key1 = (Included(2), Excluded(10));
        let key2 = (Included(4), Included(6));
        let key3 = (Excluded(10), Excluded(20));
        let key4 = (Excluded(30), Included(35));
        let key5 = (Included(30), Included(40));
        let key6 = (Included(30), Included(35));
        let key7 = (Excluded(45), Unbounded);
        let key8 = (Included(60), Included(70));

        tree.insert(key1);
        tree.insert(key2);
        tree.insert(key3);
        tree.insert(key4);
        tree.insert(key5);
        tree.insert(key6);
        tree.insert(key7);
        tree.insert(key8);

        assert_eq!(
            tree.get_interval_difference((Excluded(0), Included(100))),
            vec![
                (Excluded(0), Excluded(2)),
                (Included(10), Included(10)),
                (Included(20), Excluded(30)),
                (Excluded(40), Included(45))
            ]
        );
        assert_eq!(
            tree.get_interval_difference((Included(19), Included(40))),
            vec![(Included(20), Excluded(30))]
        );
        assert_eq!(
            tree.get_interval_difference((Included(20), Included(40))),
            vec![(Included(20), Excluded(30))]
        );
        assert_eq!(
            tree.get_interval_difference((Included(20), Included(45))),
            vec![(Included(20), Excluded(30)), (Excluded(40), Included(45))]
        );
        assert_eq!(
            tree.get_interval_difference((Included(20), Excluded(45))),
            vec![(Included(20), Excluded(30)), (Excluded(40), Excluded(45))]
        );
        assert_eq!(
            tree.get_interval_difference((Included(2), Included(10))),
            vec![(Included(10), Included(10))]
        );
    }

    #[test]
    fn consecutive_excluded_non_contiguous_difference_works_as_expected() {
        let mut tree = IntervalTree::default();

        let key1 = (Included(10), Excluded(20));
        let key2 = (Excluded(30), Excluded(40));

        tree.insert(key1);
        tree.insert(key2);

        assert_eq!(
            tree.get_interval_difference((Included(0), Included(40))),
            vec![
                (Included(0), Excluded(10)),
                (Included(20), Included(30)),
                (Included(40), Included(40))
            ]
        );
    }

    #[test]
    fn contains_point_works_as_expected() {
        let mut tree = IntervalTree::default();

        let key1 = (Included(10), Excluded(20));
        let key2 = (Excluded(30), Excluded(40));
        let key3 = (Included(40), Unbounded);

        tree.insert(key1);
        tree.insert(key2);
        tree.insert(key3);

        assert!(tree.contains_point(&10));
        assert!(!tree.contains_point(&20));
        assert!(tree.contains_point(&40));
        assert!(tree.contains_point(&100));
    }

    #[test]
    fn contains_works_as_expected() {
        let mut tree = IntervalTree::default();

        let key1 = (Included(10), Excluded(20));
        let key2 = (Excluded(30), Excluded(40));
        let key3 = (Included(40), Unbounded);

        tree.insert(key1);
        tree.insert(key2);
        tree.insert(key3);

        assert!(tree.contains_interval(key1));
        assert!(!tree.contains_interval((Included(10), Included(20))));
        assert!(!tree.contains_interval((Unbounded, Included(0))));
        assert!(tree.contains_interval((Included(35), Included(37))));
    }

    #[test]
    fn iter_works_as_expected() {
        let mut tree = IntervalTree::default();

        assert_eq!(tree.iter().next(), None);

        let key1 = (Included(10), Excluded(20));
        let key2 = (Included(40), Unbounded);
        let key3 = (Excluded(30), Excluded(40));
        let key4 = (Unbounded, Included(50));
        let key5 = (Excluded(-10), Included(-5));
        let key6 = (Included(-10), Included(-4));

        tree.insert(key1);
        tree.insert(key2);
        tree.insert(key3);
        tree.insert(key4);
        tree.insert(key5);
        tree.insert(key6);

        let inorder = vec![&key4, &key6, &key5, &key1, &key3, &key2];
        for (idx, interval) in tree.iter().enumerate() {
            assert_eq!(interval, inorder[idx]);
        }

        assert_eq!(tree.iter().count(), inorder.len());
    }

    #[test]
    fn remove_random_leaf_empty_tree_works_as_expected() {
        let mut tree: IntervalTree<i32> = IntervalTree::default();

        assert_eq!(tree.remove_random_leaf(), None);
    }

    #[test]
    fn remove_random_leaf_one_node_tree_works_as_expected() {
        let mut tree = IntervalTree::default();

        let key1 = (Included(10), Excluded(20));
        tree.insert(key1);

        let deleted = tree.remove_random_leaf();
        assert!(deleted.is_some());
        assert_eq!(deleted.unwrap(), key1);

        assert!(tree.remove_random_leaf().is_none());
    }

    #[test]
    fn remove_random_leaf_works_as_expected() {
        let mut tree = IntervalTree::default();

        let key1 = (Included(16), Unbounded);
        let key2 = (Included(8), Excluded(9));
        let key3 = (Included(5), Excluded(8));
        let key4 = (Excluded(15), Included(23));
        let key5 = (Included(0), Included(3));
        let key6 = (Included(13), Excluded(26));

        tree.insert(key1);
        tree.insert(key2);
        tree.insert(key3);
        tree.insert(key4);
        tree.insert(key5);
        tree.insert(key6);

        let mut tree_deleted_key5 = IntervalTree::default();

        let key1_deleted5 = (Included(16), Unbounded);
        let key2_deleted5 = (Included(8), Excluded(9));
        let key3_deleted5 = (Included(5), Excluded(8));
        let key4_deleted5 = (Excluded(15), Included(23));
        let key6_deleted5 = (Included(13), Excluded(26));

        tree_deleted_key5.insert(key1_deleted5);
        tree_deleted_key5.insert(key2_deleted5);
        tree_deleted_key5.insert(key3_deleted5);
        tree_deleted_key5.insert(key4_deleted5);
        tree_deleted_key5.insert(key6_deleted5);

        let mut tree_deleted_key6 = IntervalTree::default();

        let key1_deleted6 = (Included(16), Unbounded);
        let key2_deleted6 = (Included(8), Excluded(9));
        let key3_deleted6 = (Included(5), Excluded(8));
        let key4_deleted6 = (Excluded(15), Included(23));
        let key5_deleted6 = (Included(0), Included(3));

        tree_deleted_key6.insert(key1_deleted6);
        tree_deleted_key6.insert(key2_deleted6);
        tree_deleted_key6.insert(key3_deleted6);
        tree_deleted_key6.insert(key4_deleted6);
        tree_deleted_key6.insert(key5_deleted6);

        use std::collections::HashSet;
        let mut all_deleted = HashSet::new();
        let num_of_leaves = 2; // Key5 & Key6

        // This loop makes sure that the deletion is random.
        // We delete and reinsert leaves until we have deleted
        // all possible leaves in the tree.
        while all_deleted.len() < num_of_leaves {
            let deleted = tree.remove_random_leaf();
            assert!(deleted.is_some());
            let deleted = deleted.unwrap();

            // Check that the new tree has the right shape,
            // and that the value stored in the various nodes are
            // correctly updated following the removal of a leaf.
            if deleted == key5 {
                assert_eq!(tree, tree_deleted_key5);
            } else if deleted == key6 {
                assert_eq!(tree, tree_deleted_key6);
            } else {
                unreachable!();
            }

            // Keep track of deleted nodes, and reinsert the
            // deleted node in the tree so we come back to
            // the initial state every iteration.
            all_deleted.insert(deleted);
            tree.insert(deleted);
        }
    }

    #[test]
    fn len_and_is_empty_works_as_expected() {
        let mut tree = IntervalTree::default();

        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());

        let key1 = (Included(16), Unbounded);
        let key2 = (Included(8), Excluded(9));

        tree.insert(key1);

        assert_eq!(tree.len(), 1);
        assert!(!tree.is_empty());

        tree.insert(key2);

        assert_eq!(tree.len(), 2);
        assert!(!tree.is_empty());

        tree.remove_random_leaf();

        assert_eq!(tree.len(), 1);
        assert!(!tree.is_empty());

        tree.remove_random_leaf();

        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());
    }

    #[test]
    fn clear_works_as_expected() {
        let mut tree = IntervalTree::default();

        tree.clear();

        let key1 = (Included(16), Unbounded);
        let key2 = (Included(8), Excluded(9));

        tree.insert(key1);
        tree.insert(key2);

        assert_eq!(tree.len(), 2);

        tree.clear();

        assert!(tree.is_empty());
        assert_eq!(tree.root, None);
    }

    mod remove {
        use super::*;

        fn arbitrary_ordered_bound() -> impl Strategy<Value = (Bound<i32>, Bound<i32>)> {
            any::<(Bound<i32>, Bound<i32>)>().prop_map(|mut bounds| {
                if let (
                    Bound::Included(ref mut lower) | Bound::Excluded(ref mut lower),
                    Bound::Included(ref mut upper) | Bound::Excluded(ref mut upper),
                ) = bounds
                {
                    if lower > upper {
                        std::mem::swap(lower, upper);
                    }
                }
                bounds
            })
        }

        #[test]
        fn solo_root_node() {
            let mut tree = IntervalTree::default();
            tree.insert(0..10);
            assert!(tree.contains_interval(0..10));
            tree.remove(&(0..10));
            check_invariants(&tree);
            assert!(!tree.contains_interval(0..10));
            assert!(
                tree.get_interval_difference(0..10)
                    == vec![(Bound::Included(0), Bound::Excluded(10))]
            );
        }

        #[test]
        fn root_node_with_left() {
            let mut tree = IntervalTree::default();
            tree.insert(0..10);
            tree.insert(-2..=-1);

            tree.remove(&(0..10));
            check_invariants(&tree);
            assert!(!tree.contains_interval(0..10));
            assert!(tree.contains_interval(-2..=-1));
        }

        #[test]
        fn root_node_with_right() {
            let mut tree = IntervalTree::default();
            tree.insert(0..10);
            tree.insert(11..=12);
            tree.remove(&(0..10));

            check_invariants(&tree);
            assert!(!tree.contains_interval(0..10));
            assert!(tree.contains_interval(11..=12));
        }

        #[test]
        fn root_node_with_left_and_right() {
            let mut tree = IntervalTree::default();
            tree.insert(0..10);
            tree.insert(-2..=-1);
            tree.insert(11..=12);
            tree.remove(&(0..10));

            check_invariants(&tree);
            assert!(!tree.contains_interval(0..10));
            assert!(tree.contains_interval(-2..=-1));
            assert!(tree.contains_interval(11..=12));
        }

        proptest! {
            #[test]
            fn then_contains(mut tree: IntervalTree<i32>, to_remove in arbitrary_ordered_bound()) {
                check_invariants(&tree);
                tree.remove(&to_remove);
                check_invariants(&tree);
                assert!(!tree.contains_interval(to_remove));
                assert!(tree.get_interval_difference(to_remove) == vec![to_remove]);
            }

            #[test]
            fn preserves_other_disjoint(
                mut tree: IntervalTree<i32>,
                to_remove in arbitrary_ordered_bound(),
                other in arbitrary_ordered_bound()
            ) {
                prop_assume!(!overlaps(&to_remove, &other));
                check_invariants(&tree);

                tree.insert(other);
                check_invariants(&tree);


                tree.remove(&to_remove);
                check_invariants(&tree);

                assert!(tree.contains_interval(other));
            }
        }
    }
}
