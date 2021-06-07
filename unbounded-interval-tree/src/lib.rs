#![warn(clippy::dbg_macro)]
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
#![feature(bound_cloned, or_patterns, bound_as_ref)]

use std::cmp::{max, Ordering};
use std::fmt;
use std::mem;
use std::ops::{Bound, RangeBounds};
use Bound::*;
use Ordering::*;

use launchpad::intervals::{
    cmp_end_start, cmp_endbound, cmp_start_end, cmp_startbound, covers, overlaps,
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

impl<Q: fmt::Display + Ord + Clone> IntervalTree<Q> {
    pub fn graphviz(&self) -> String {
        fn describe_range<Q: fmt::Display>(r: &(Bound<Q>, Bound<Q>)) -> String {
            format!(
                "{},{}",
                match &r.0 {
                    Unbounded => "(-∞".to_owned(),
                    Included(x) => format!("[{}", x),
                    Excluded(x) => format!("({}", x),
                },
                match &r.1 {
                    Unbounded => "∞)".to_owned(),
                    Included(x) => format!("{}]", x),
                    Excluded(x) => format!("{})", x),
                }
            )
        }

        fn walk<Q: fmt::Display + Ord + Clone>(
            node: &Node<Q>,
            nodes: &mut String,
            edges: &mut String,
            invisible_edges: &mut String,
            ni: &mut u32,
        ) -> u32 {
            *ni += 1;
            let me = *ni;
            nodes.push_str(&format!(
                "{} [label=\"{}\"]\n",
                ni,
                describe_range(&node.key)
            ));

            let left_i = node
                .left
                .as_ref()
                .map(|n| walk(n, nodes, edges, invisible_edges, ni));
            let right_i = node
                .right
                .as_ref()
                .map(|n| walk(n, nodes, edges, invisible_edges, ni));

            if let Some(left_i) = left_i {
                edges.push_str(&format!("{} -> {};\n", me, left_i))
            }
            if let Some(right_i) = right_i {
                edges.push_str(&format!("{} -> {};\n", me, right_i))
            }

            if let (Some(left_i), Some(right_i)) = (left_i, right_i) {
                invisible_edges.push_str(&format!("{}:e -> {}:w;\n", left_i, right_i));
                edges.push_str(&format!("{{ rank=same; {} {} }}", left_i, right_i));
            }

            me
        }

        let mut nodes = String::new();
        let mut edges = String::new();
        let mut invisible_edges = String::new();
        let mut ni = 0u32;

        let root = match &self.root {
            Some(n) => n,
            None => return "".to_owned(),
        };

        walk(root, &mut nodes, &mut edges, &mut invisible_edges, &mut ni);

        format!(
            "digraph {{\n{}\n\n{}\n\n{{edge[style=invis];\n{}\n}}\n}}",
            nodes, edges, invisible_edges
        )
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

    pub fn drain(&mut self) -> Drain<Q> {
        Drain {
            to_visit: vec![],
            curr: self.root.take(),
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
        let node = Box::new(Node::new(range));
        let node_key = node.key.clone();

        // If the tree is empty, put new node at the root.
        if self.root.is_none() {
            self.root = Some(node);
            return;
        }

        // Otherwise, walk down the tree and insert when we reach leaves.
        let mut nodes: Vec<*mut Option<Box<Node<Q>>>> = vec![&mut self.root];
        let mut curr = self.root.as_mut().unwrap();
        loop {
            curr.maybe_update_value(node.value.as_ref());

            match cmp(&curr.key, &node.key) {
                Equal => return, // Don't insert a redundant key.
                Less => {
                    let right: *mut _ = &mut curr.right;
                    match curr.right {
                        None => {
                            curr.right = Some(node);
                            break;
                        }
                        Some(ref mut n) => {
                            nodes.push(right);
                            curr = n
                        }
                    };
                }
                Greater => {
                    let left: *mut _ = &mut curr.left;
                    match curr.left {
                        None => {
                            curr.left = Some(node);
                            break;
                        }
                        Some(ref mut n) => {
                            nodes.push(left);
                            curr = n
                        }
                    };
                }
            };
        }

        // now, walk back *up* the tree, updating heights and rebalancing on our way up
        while let Some(curr) = nodes.pop() {
            // SAFETY: at this point in the operation, these pointers are the only *live* references
            // to the nodes they point to - and as we are walk back *up* the tree here, we
            // successively discard potentially-overlapping pointers to nodes.
            let curr: &mut Option<Box<Node<Q>>> = unsafe { &mut *curr };
            curr.as_mut().unwrap().update_height();
            let balance_factor = curr.as_ref().map(|n| n.balance_factor()).unwrap();

            let left_cmp = curr
                .as_ref()
                .unwrap()
                .left
                .as_ref()
                .map(|left| cmp(&node_key, &left.key));

            let right_cmp = curr
                .as_ref()
                .unwrap()
                .right
                .as_ref()
                .map(|right| cmp(&node_key, &right.key));

            if balance_factor > 1 && left_cmp.unwrap() == Less {
                Node::rotate_right(curr);
            } else if balance_factor < -1 && right_cmp.unwrap() == Greater {
                Node::rotate_left(curr);
            } else if balance_factor > 1 && left_cmp.unwrap() == Greater {
                Node::rotate_left(&mut curr.as_mut().unwrap().left);
                Node::rotate_right(curr);
            } else if balance_factor < -1 && right_cmp.unwrap() == Less {
                Node::rotate_right(&mut curr.as_mut().unwrap().right);
                Node::rotate_left(curr);
            }
        }
    }

    fn insert_node(&mut self, node: Box<Node<Q>>) {
        for r in node.drain() {
            self.insert(r);
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
        //   `range` from its interval, resulting in either one or two ranges. We then put both of
        //   the node's children in the queue to re-insert, and remove the node entirely
        //
        // - after recursively walking the node's children, we re-calculate the node's max by
        //   taking the max of its left child's max, its right child's max, and its own upper
        //    bound.
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
                let right = n.right.take();
                let left = n.left.take();
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

        // If this function returns true, the node it was passed should be removed entirely
        // afterwards
        #[must_use]
        fn walk_tree<Q, R>(node: &mut Node<Q>, range: &R, to_insert: &mut Vec<Box<Node<Q>>>) -> bool
        where
            Q: Ord + Clone,
            R: RangeBounds<Q>,
        {
            if cmp_end_start(node.value.as_ref(), range.start_bound()) == Less {
                // the upper bound of all of this node's descendants is less than the start bound of
                // the range we care about, so we can skip it entirely
                return false;
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
                    (None, Some(r)) | (Some(r), None) => {
                        to_insert.push(Box::new(Node::new(r)));
                    }
                    (Some(below), Some(above)) => {
                        to_insert.push(Box::new(Node::new(below)));
                        to_insert.push(Box::new(Node::new(above)));
                    }
                    (None, None) => unreachable!(
                        "fully covered node should have already been removed in maybe_replace_node",
                    ),
                }

                if let Some(left) = node.left.take() {
                    to_insert.push(left)
                }

                if let Some(right) = node.right.take() {
                    to_insert.push(right)
                }

                return true;
            }

            // now walk the children

            // first, the left
            replace_node_if_covered(&mut node.left, range, to_insert);
            let needs_removal = if let Some(ref mut left) = node.left {
                walk_tree(left, range, to_insert)
            } else {
                false
            };

            if needs_removal {
                node.left = None;
            }

            // now, if the node's start is not above the end of the range to remove (which would
            // mean all children to the right are irrelevant), we walk to the right
            if matches!(
                cmp_start_end(node.key.start_bound(), range.end_bound()),
                Less | Equal
            ) {
                replace_node_if_covered(&mut node.right, range, to_insert);
                let needs_removal = if let Some(ref mut right) = node.right {
                    walk_tree(right, range, to_insert)
                } else {
                    node.value = node.key.1.clone();
                    false
                };

                if needs_removal {
                    node.right = None;
                }
            }

            // Finally, for each node, we recalculate its upper bound and height
            node.update_value();
            node.update_height();
            false
        }

        // Collection of nodes we've split off that we have to re-insert later
        let mut to_insert = Vec::new();

        // First, maybe replace the root node and promote its children
        replace_node_if_covered(&mut self.root, range, &mut to_insert);

        let needs_removal = if let Some(ref mut root) = self.root {
            walk_tree(root, range, &mut to_insert)
        } else {
            false
        };
        if needs_removal {
            self.root = None;
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
                    if !walk_tree(&mut node, range, &mut new_to_insert) {
                        self.insert_node(node);
                    }
                }
            }
            to_insert = new_to_insert;
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

pub struct Drain<Q: Ord + Clone> {
    to_visit: Vec<Box<Node<Q>>>,
    curr: Option<Box<Node<Q>>>,
}

impl<Q: Ord + Clone> Iterator for Drain<Q> {
    type Item = (Bound<Q>, Bound<Q>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr.is_none() {
            if let Some(next) = self.to_visit.pop() {
                self.curr = Some(next);
            }
        }

        self.curr.take().map(|mut n| {
            self.curr = n.left.take();
            if let Some(right) = n.right {
                self.to_visit.push(right)
            }

            n.key
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Node<Q: Ord + Clone> {
    key: (Bound<Q>, Bound<Q>),
    /// Max end-point.
    value: Bound<Q>,
    left: Option<Box<Node<Q>>>,
    right: Option<Box<Node<Q>>>,
    height: i32,
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
            height: 1,
        }
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

    fn rotate_right(node: &mut Option<Box<Self>>) {
        if !node.as_ref().map_or(false, |n| n.left.is_some()) {
            return;
        }

        let mut n = node.take().unwrap();
        *node = n.left.take().map(|mut new_top| {
            let new_right = n;
            let new_left = mem::replace(&mut new_top.right, Some(new_right));
            let new_self = new_top.right.as_mut().unwrap();
            new_self.left = new_left;
            new_self.update_height();
            new_self.update_value();
            if cmp_endbound(new_top.value.as_ref(), new_self.value.as_ref()) == Less {
                new_top.value = new_self.value.clone();
            }
            new_top.update_height();
            new_top
        });
    }

    fn rotate_left(node: &mut Option<Box<Self>>) {
        if !node.as_ref().map_or(false, |n| n.right.is_some()) {
            return;
        }

        let mut n = node.take().unwrap();
        *node = n.right.take().map(|mut new_top| {
            let new_left = n;
            let new_right = mem::replace(&mut new_top.left, Some(new_left));
            let new_self = new_top.left.as_mut().unwrap();
            new_self.right = new_right;
            new_self.update_height();
            new_self.update_value();
            if cmp_endbound(new_top.value.as_ref(), new_self.value.as_ref()) == Less {
                new_top.value = new_self.value.clone();
            }
            new_top.update_height();
            new_top
        })
    }

    fn update_height(&mut self) {
        self.height = 1 + max(
            self.left.as_ref().map_or(0, |n| n.height),
            self.right.as_ref().map_or(0, |n| n.height),
        );
    }

    fn update_value(&mut self) {
        self.value = std::iter::once(self.key.end_bound())
            .chain(self.left.iter().map(|n| n.value.as_ref()))
            .chain(self.right.iter().map(|n| n.value.as_ref()))
            .max_by(|l, r| cmp_endbound(*l, *r))
            .unwrap()
            .cloned();
    }

    fn balance_factor(&self) -> i32 {
        self.left.as_ref().map_or(0, |n| n.height) - self.right.as_ref().map_or(0, |n| n.height)
    }

    fn drain(self: Box<Self>) -> Drain<Q> {
        Drain {
            curr: Some(self),
            to_visit: vec![],
        }
    }
}

impl<Q> Arbitrary for Node<Q>
where
    Q: Ord + Clone + Arbitrary,
{
    type Parameters = <IntervalTree<Q> as Arbitrary>::Parameters;
    #[allow(clippy::type_complexity)]
    type Strategy = proptest::strategy::Map<
        <IntervalTree<Q> as Arbitrary>::Strategy,
        fn(IntervalTree<Q>) -> Self,
    >;

    fn arbitrary_with((_, elem_params): Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;

        any_with::<IntervalTree<Q>>(((1usize..100).into(), elem_params))
            .prop_map(|tree| *tree.root.unwrap())
    }
}

#[derive(Eq, PartialEq)]
struct OrdRange<Q>((Bound<Q>, Bound<Q>));

impl<Q: Ord> PartialOrd for OrdRange<Q> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Q: Ord> Ord for OrdRange<Q> {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp(&self.0, &other.0)
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
    use std::hash::Hash;
    use std::ops::{Bound, Range};

    fn arbitrary_ordered_bound<Q: Arbitrary + Ord>() -> impl Strategy<Value = (Bound<Q>, Bound<Q>)>
    {
        any::<(Bound<Q>, Bound<Q>)>()
            .prop_map(|mut bounds| {
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
            .prop_filter("Empty range", |(start, end)| {
                cmp_start_end(start.as_ref(), end.as_ref()) == Less
                    && !matches!((start, end), (Included(x), Excluded(y)) if x == y)
            })
    }

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

    fn check_invariants<Q>(tree: &IntervalTree<Q>, check_balance: bool)
    where
        Q: Ord + Clone + Hash + std::fmt::Debug,
    {
        use itertools::Itertools;

        // returns child_contains_max or true if max is None
        fn check_node_invariants<Q>(node: &Node<Q>, max: Option<&Bound<Q>>, check_balance: bool)
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

            // all our left descendants are below us
            assert!(node
                .left
                .as_ref()
                .map_or(vec![], |l| node_children(l))
                .iter()
                .all(|n| cmp(&n.key, &node.key) == Less));

            // all our right descendants are above us
            assert!(node
                .right
                .as_ref()
                .map_or(vec![], |r| node_children(r))
                .iter()
                .all(|n| cmp(&n.key, &node.key) == Greater));

            // our height is one more than the max of our childrens' heights
            assert_eq!(
                node.height,
                std::cmp::max(
                    node.left.as_ref().map(|n| n.height).unwrap_or(0),
                    node.right.as_ref().map(|n| n.height).unwrap_or(0)
                ) + 1,
                "node has invalid height: {:?}",
                node
            );

            // our balance factor is between -1 and 1 (aka, we are an avl tree!)
            if check_balance {
                assert!(
                    matches!(node.balance_factor(), -1 | 0 | 1),
                    "imbalanced node {:#?}: balance_factor = {}",
                    node,
                    node.balance_factor()
                );
            }

            if let Some(ref left) = node.left {
                check_node_invariants(left, Some(&node.value), check_balance);
            }
            if let Some(ref right) = node.right {
                check_node_invariants(right, Some(&node.value), check_balance);
            }
        }

        if let Some(ref root) = tree.root {
            check_node_invariants(root, None, check_balance);
        }

        assert_eq!(
            tree.iter().unique().count(),
            tree.len(),
            "tree not unique: {:#?}",
            tree
        );
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
        assert!(tree.root.as_ref().unwrap().right.is_some());
    }

    proptest! {
        #[test]
        fn tree_construction(mut ranges: Vec<Range<u8>>) {
            let mut tree = IntervalTree::default();
            for range in ranges.drain(..) {
                tree.insert(range);
                check_invariants(&tree, true);
            }
        }


        #[test]
        fn insert_preserves_invariants(mut tree: IntervalTree<u8>, range: Range<u8>) {
            check_invariants(&tree, true);
            tree.insert(range);
            check_invariants(&tree, true);
        }

        #[test]
        fn insert_node_preserves_invariants(mut tree: IntervalTree<u8>, node: Node<u8>) {
            check_invariants(&tree, true);
            tree.insert_node(Box::new(node));
            check_invariants(&tree, true);
        }

        #[test]
        fn insert_node_inserts_all_children(
            mut tree: IntervalTree<u8>,
            ranges in proptest::collection::vec(
                arbitrary_ordered_bound::<u8>(),
                proptest::collection::SizeRange::default()
            )
        ) {
            let node = {
                let mut tree = IntervalTree::default();
                for range in ranges.clone() {
                    tree.insert(range);
                }
                tree.root
            };
            prop_assume!(node.is_some());
            let node = node.unwrap();
            tree.insert_node(node);
            for range in ranges {
                assert!(tree.contains_interval(range));
            }
        }

        #[test]
        fn insert_then_contains(
            mut tree: IntervalTree<u8>,
            range in arbitrary_ordered_bound::<u8>()
        ) {
            prop_assume!(cmp_start_end(range.start_bound(), range.end_bound()) == Less);
            tree.insert(range);
            assert!(tree.contains_interval(range));
            assert_eq!(tree.get_interval_difference(range), vec![]);
        }
    }

    #[test]
    fn insert_node_balance_example() {
        let mut tree = IntervalTree::default();
        tree.insert(0..=0);
        let node = {
            let mut t = IntervalTree::default();
            t.insert(0..=1);
            t.insert(0..=2);
            t.root.unwrap()
        };

        check_invariants(&tree, true);
        tree.insert_node(node);
        check_invariants(&tree, true);
    }

    #[test]
    fn insert_into_unbalanced() {
        let mut tree = IntervalTree {
            root: Some(Box::new(Node {
                key: (Excluded(-86), Included(0)),
                value: Included(1),
                left: Some(Box::new(Node {
                    key: (Excluded(-86), Excluded(0)),
                    value: Included(0),
                    left: Some(Box::new(Node {
                        key: (Included(-86), Excluded(0)),
                        value: Included(0),
                        left: Some(Box::new(Node {
                            key: (Unbounded, Included(0)),
                            value: Included(0),
                            left: Some(Box::new(Node {
                                key: (Unbounded, Included(-1)),
                                value: Included(-1),
                                left: None,
                                right: None,
                                height: 1,
                            })),
                            right: None,
                            height: 2,
                        })),
                        right: Some(Box::new(Node {
                            key: (Included(-86), Included(0)),
                            value: (Included(0)),
                            left: None,
                            right: None,
                            height: 1,
                        })),
                        height: 3,
                    })),
                    right: None,
                    height: 4,
                })),
                right: Some(Box::new(Node {
                    key: (Excluded(-42), Included(0)),
                    value: (Included(1)),
                    left: Some(Box::new(Node {
                        key: (Excluded(-43), Included(1)),
                        value: (Included(1)),
                        left: None,
                        right: None,
                        height: 1,
                    })),
                    right: None,
                    height: 2,
                })),
                height: 5,
            })),
        };
        check_invariants(&tree, false);
        tree.insert((Excluded(2), Unbounded));
        check_invariants(&tree, false);

        assert_eq!(tree.len(), 9);
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

        assert!(tree.root.as_ref().unwrap().left.is_some());
        assert_eq!(
            tree.root.as_ref().unwrap().left.as_ref().unwrap().value,
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

    proptest! {
        #[test]
        fn drain_empties(mut tree: IntervalTree<i8>) {
            tree.drain();
        }

        #[test]
        fn drain_returns_ranges(mut ranges: Vec<Range<u8>>) {
            use std::collections::HashSet;

            let mut tree = IntervalTree::default();
            for range in ranges.clone() {
                tree.insert(range);
            }
            let result = tree.drain().collect::<HashSet<_>>();
            for range in ranges {
                assert!(result.contains(&(range.start_bound().cloned(), range.end_bound().cloned())))
            }
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

        tree.remove(&(8..9));

        assert_eq!(tree.len(), 1);
        assert!(!tree.is_empty());

        tree.remove(&(16..));

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

        #[test]
        fn solo_root_node() {
            let mut tree = IntervalTree::default();
            tree.insert(0..10);
            assert!(tree.contains_interval(0..10));
            tree.remove(&(0..10));
            check_invariants(&tree, false);
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
            check_invariants(&tree, false);
            assert!(!tree.contains_interval(0..10));
            assert!(tree.contains_interval(-2..=-1));
        }

        #[test]
        fn root_node_with_right() {
            let mut tree = IntervalTree::default();
            tree.insert(0..10);
            tree.insert(11..=12);
            tree.remove(&(0..10));

            check_invariants(&tree, false);
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

            check_invariants(&tree, false);
            assert!(!tree.contains_interval(0..10));
            assert!(tree.contains_interval(-2..=-1));
            assert!(tree.contains_interval(11..=12));
        }

        proptest! {
            #[test]
            fn then_contains(
                mut tree: IntervalTree<i8>,
                to_remove in arbitrary_ordered_bound::<i8>()
            ) {
                check_invariants(&tree, false);
                tree.remove(&to_remove);
                check_invariants(&tree, false);
                assert!(!tree.contains_interval(to_remove));
                assert_eq!(tree.get_interval_difference(to_remove), vec![to_remove]);
            }

            #[test]
            fn preserves_other_disjoint(
                mut tree: IntervalTree<i8>,
                to_remove in arbitrary_ordered_bound::<i8>(),
                other in arbitrary_ordered_bound::<i8>()
            ) {
                prop_assume!(!overlaps(&to_remove, &other));
                check_invariants(&tree, false);

                tree.insert(other);
                check_invariants(&tree, false);

                tree.remove(&to_remove);
                check_invariants(&tree, false);

                assert!(tree.contains_interval(other));
            }
        }
    }

    mod balancing {
        use super::*;
        use pretty_assertions::assert_eq;
        use test_strategy::proptest;

        #[test]
        fn rotate_right() {
            let mut node = Some(Box::new(Node {
                key: (Included(7), Excluded(8)),
                value: Excluded(10),
                left: Some(Box::new(Node {
                    key: (Included(3), Excluded(4)),
                    value: (Excluded(6)),
                    left: Some(Box::new(Node {
                        key: (Included(1), Excluded(2)),
                        value: (Excluded(2)),
                        left: None,
                        right: None,

                        height: 1,
                    })),
                    right: Some(Box::new(Node {
                        key: (Included(5), Excluded(6)),
                        value: (Excluded(6)),
                        left: None,
                        right: None,
                        height: 1,
                    })),
                    height: 2,
                })),
                right: Some(Box::new(Node {
                    key: (Included(9), Excluded(10)),
                    value: (Excluded(10)),
                    left: None,
                    right: None,
                    height: 1,
                })),
                height: 3,
            }));

            Node::rotate_right(&mut node);

            assert_eq!(
                node.unwrap(),
                Box::new(Node {
                    key: (Included(3), Excluded(4)),
                    value: (Excluded(10)),
                    left: Some(Box::new(Node {
                        key: (Included(1), Excluded(2)),
                        value: (Excluded(2)),
                        left: None,
                        right: None,
                        height: 1,
                    })),
                    right: Some(Box::new(Node {
                        key: (Included(7), Excluded(8)),
                        value: Excluded(10),
                        left: Some(Box::new(Node {
                            key: (Included(5), Excluded(6)),
                            value: (Excluded(6)),
                            left: None,
                            right: None,
                            height: 1,
                        })),
                        right: Some(Box::new(Node {
                            key: (Included(9), Excluded(10)),
                            value: (Excluded(10)),
                            left: None,
                            right: None,
                            height: 1,
                        })),
                        height: 2,
                    })),
                    height: 3,
                })
            );
        }

        #[test]
        fn rotate_left() {
            let mut node = Some(Box::new(Node {
                key: (Included(3), Excluded(4)),
                value: (Excluded(10)),
                left: Some(Box::new(Node {
                    key: (Included(1), Excluded(2)),
                    value: (Excluded(2)),
                    left: None,
                    right: None,
                    height: 1,
                })),
                right: Some(Box::new(Node {
                    key: (Included(7), Excluded(8)),
                    value: Excluded(10),
                    left: Some(Box::new(Node {
                        key: (Included(5), Excluded(6)),
                        value: (Excluded(6)),
                        left: None,
                        right: None,
                        height: 1,
                    })),
                    right: Some(Box::new(Node {
                        key: (Included(9), Excluded(10)),
                        value: (Excluded(10)),
                        left: None,
                        right: None,
                        height: 1,
                    })),
                    height: 2,
                })),
                height: 3,
            }));

            Node::rotate_left(&mut node);

            assert_eq!(
                node.unwrap(),
                Box::new(Node {
                    key: (Included(7), Excluded(8)),
                    value: Excluded(10),
                    left: Some(Box::new(Node {
                        key: (Included(3), Excluded(4)),
                        value: (Excluded(6)),
                        left: Some(Box::new(Node {
                            key: (Included(1), Excluded(2)),
                            value: (Excluded(2)),
                            left: None,
                            right: None,

                            height: 1,
                        })),
                        right: Some(Box::new(Node {
                            key: (Included(5), Excluded(6)),
                            value: (Excluded(6)),
                            left: None,
                            right: None,
                            height: 1,
                        })),
                        height: 2,
                    })),
                    right: Some(Box::new(Node {
                        key: (Included(9), Excluded(10)),
                        value: (Excluded(10)),
                        left: None,
                        right: None,
                        height: 1,
                    })),
                    height: 3,
                })
            );
        }

        // Currently, both left and right rotation no-op if the child in the opposite direction is
        // None - this is only necessary because due to removal not rebalancing, we sometimes need
        // to insert into an unbalanced tree, and attempting to rotate an unbalanced tree might
        // result in your tree emptying out from under you. Once we rebalance on removal, this test
        // can come back
        #[proptest]
        #[ignore]
        fn rotation_inverse(node: Box<Node<u8>>) {
            let mut result = Some(node.clone());
            Node::rotate_right(&mut result);
            prop_assume!(result.is_some());
            Node::rotate_left(&mut result);
            prop_assume!(result.is_some());
            assert_eq!(result.unwrap(), node);
        }
    }
}
