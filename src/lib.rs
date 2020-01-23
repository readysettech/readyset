//! Implementation of an interval tree that works with inclusive/exclusive
//! bounds, as well as unbounded intervals. It is based on the
//! data structure described in Cormen et al. 
//! (2009, Section 14.3: Interval trees, pp. 348–354). It provides methods
//! for "stabbing queries" (as in "is point `p` or an interval `i` contained in any intervals
//! in the tree of intervals?"), as well as helpers to get the difference between a queried
//! interval and the database (in order to find subsegments not covered), and the list of
//! intervals in the database overlapping a queried interval.

use std::cmp::Ordering;
use std::cmp::Ordering::*;
use std::ops::Bound;
use std::ops::Bound::*;
use std::fmt;
use std::mem;

type Range<Q> = (Bound<Q>, Bound<Q>);

/// The interval tree storing all the underlying intervals.
#[derive(Clone, Debug, PartialEq)]
pub struct IntervalTree<Q: Ord + Clone> {
    root: Option<Box<Node<Q>>>,
    size: usize,
}

/// An inorder interator through the interval tree.
pub struct IntervalTreeIter<'a, Q: Ord + Clone> {
    to_visit: Vec<&'a Box<Node<Q>>>,
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
        IntervalTree { root: None, size: 0 }
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
    pub fn iter<'a>(&'a self) -> IntervalTreeIter<'a, Q> {
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
    /// str_tree.insert((Included("Noria"), Unbounded));
    /// ```
    pub fn insert(&mut self, range: Range<Q>) {
        self.size += 1;

        // If the tree is empty, put new node at the root.
        if self.root.is_none() {
            self.root = Some(Box::new(Node::new(range)));
            return;
        }

        // Otherwise, walk down the tree and insert when we reach leaves.
        // TODO(jonathangb): Rotate tree?
        let mut curr = self.root.as_mut().unwrap();
        loop {
            curr.maybe_update_value(&range.1);

            match cmp(&curr.key, &range) {
                Equal => return, // Don't insert a redundant key.
                Less => {
                    match curr.right {
                        None => {
                            curr.right = Some(Box::new(Node::new(range)));
                            return;
                        }
                        Some(ref mut node) => curr = node,
                    };
                }
                Greater => {
                    match curr.left {
                        None => {
                            curr.left = Some(Box::new(Node::new(range)));
                            return;
                        }
                        Some(ref mut node) => curr = node,
                    };
                }
            };
        }
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
    /// assert!(int_tree.contains_point(100));
    /// assert!(!int_tree.contains_point(5));
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
    pub fn contains_point(&self, q: Q) -> bool {
        self.contains_interval((Included(q.clone()), Included(q.clone())))
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
    pub fn contains_interval(&self, q: Range<Q>) -> bool {
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
    /// assert_eq!(tree.get_interval_overlaps(&(Included(-5), Excluded(7))),
    ///            vec![&(Included(0), Included(5))]);
    /// assert!(tree.get_interval_overlaps(&(Included(10), Unbounded)).is_empty());
    /// ```
    pub fn get_interval_overlaps(&self, q: &Range<Q>) -> Vec<&Range<Q>> {
        let curr = &self.root;
        let mut acc = Vec::new();

        Self::get_interval_overlaps_rec(curr, q, &mut acc);
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
    pub fn get_interval_difference(&self, q: Range<Q>) -> Vec<Range<Q>> {
        let overlaps = self.get_interval_overlaps(&q);

        // If there is no overlap, then the difference is the query `q` itself.
        if overlaps.is_empty() {
            let min = match q.0 {
                Included(x) => Included(x),
                Excluded(x) => Excluded(x),
                Unbounded => Unbounded,
            };
            let max = match q.1 {
                Included(x) => Included(x),
                Excluded(x) => Excluded(x),
                Unbounded => Unbounded,
            };
            return vec![(min, max)];
        }

        let mut acc = Vec::new();
        let first = &overlaps.first().unwrap();

        // If q.min < first.min, we have a difference to append.
        match (&q.0, &first.0) {
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
        if first.1 == Unbounded {
            return acc;
        }

        let mut contiguous = &first.1; // keeps track of the maximum of a contiguous interval.
        for overlap in overlaps.iter().skip(1) {
            // If contiguous < overlap.min:
            //   1. We have a difference between contiguous -> overlap.min to fill.
            //     1.1: Note: the endpoints of the difference appended are the opposite,
            //          that is if contiguous was Included, then the difference must
            //          be Excluded, and vice versa.
            //   2. We need to update contiguous to be the new contiguous max.
            // Note: an Included+Excluded at the same point still is contiguous!
            match (&contiguous, &overlap.0) {
                (Included(contiguous_max), Included(overlap_min))
                    if contiguous_max < overlap_min =>
                {
                    acc.push((Excluded(contiguous_max.clone()), Excluded(overlap_min.clone())));
                    contiguous = &overlap.1;
                }
                (Included(contiguous_max), Excluded(overlap_min))
                    if contiguous_max < overlap_min =>
                {
                    acc.push((Excluded(contiguous_max.clone()), Included(overlap_min.clone())));
                    contiguous = &overlap.1;
                }
                (Excluded(contiguous_max), Included(overlap_min))
                    if contiguous_max < overlap_min =>
                {
                    acc.push((Included(contiguous_max.clone()), Excluded(overlap_min.clone())));
                    contiguous = &overlap.1;
                }
                (Excluded(contiguous_max), Excluded(overlap_min))
                    if contiguous_max <= overlap_min =>
                {
                    acc.push((Included(contiguous_max.clone()), Included(overlap_min.clone())));
                    contiguous = &overlap.1;
                }
                _ => {}
            }

            // If contiguous.max < overlap.max, we set contiguous to the new max.
            match (&contiguous, &overlap.1) {
                (_, Unbounded) => return acc,
                (Included(contiguous_max), Included(overlap_max))
                | (Excluded(contiguous_max), Excluded(overlap_max))
                | (Included(contiguous_max), Excluded(overlap_max))
                    if contiguous_max < overlap_max =>
                {
                    contiguous = &overlap.1
                }
                (Excluded(contiguous_max), Included(overlap_max))
                    if contiguous_max <= overlap_max =>
                {
                    contiguous = &overlap.1
                }
                _ => {}
            };
        }

        // If contiguous.max < q.max, we have a difference to append.
        match (&contiguous, &q.1) {
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

    fn get_interval_overlaps_rec<'a>(
        curr: &'a Option<Box<Node<Q>>>,
        q: &Range<Q>,
        acc: &mut Vec<&'a Range<Q>>,
    ) {
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
        let min_q = match &q.0 {
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
        let min_node = match &node.key.0 {
            Included(x) => Some((x, 2)),
            Excluded(x) => Some((x, 3)),
            Unbounded => None,
        };
        let max_q = match &q.1 {
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
    /// assert!(tree.contains_point(10));
    /// assert!(tree.contains_point(6));
    /// 
    /// let deleted = tree.remove_random_leaf();
    /// assert!(deleted.is_some());
    /// assert!(!tree.contains_point(10));
    /// assert!(tree.contains_point(6));
    /// 
    /// let deleted = tree.remove_random_leaf();
    /// assert!(deleted.is_some());
    /// assert!(!tree.contains_point(6));
    /// 
    /// let deleted = tree.remove_random_leaf();
    /// assert!(deleted.is_none());
    /// ```
    pub fn remove_random_leaf(&mut self) -> Option<Range<Q>> {
        use rand::random;

        // If interval tree is empty, just return None.
        if self.root.is_none() {
            return None;
        }

        self.size -= 1;

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
        let mut path : Vec<(_, _)> = Vec::new();

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
            let direction = if curr.left.is_none() { Direction::RIGHT }
                else if curr.right.is_none() { Direction::LEFT }
                else if random() { Direction::LEFT }
                else { Direction::RIGHT };
            // End-bound of the current node.
            let curr_end = &curr.key.1;

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
                        let other_value = &curr.right.as_ref().unwrap().value;
                        match cmp_endbound(curr_end, other_value) {
                            Greater | Equal => curr_end,
                            Less => other_value,
                        }
                    };

                    // Check if the next node is a leaf. If it is, then we want to
                    // stop traversing, and remove the leaf.
                    let next = curr.left.as_ref().unwrap();
                    if next.is_leaf() {
                        curr.value = max_other.clone();
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
                        let other_value = &curr.left.as_ref().unwrap().value;
                        match cmp_endbound(curr_end, other_value) {
                            Greater | Equal => curr_end,
                            Less => other_value,
                        }
                    };

                    let next = curr.right.as_ref().unwrap();
                    if next.is_leaf() {
                        curr.value = max_other.clone();
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
            if cmp_endbound(value, max_other) == Equal {
                break;
            }

            match cmp_endbound(value, new_max) {
                Equal => break,
                Greater => *value = new_max.clone(),
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
        self.size
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
}

impl<'a, Q> Iterator for IntervalTreeIter<'a, Q>
where
    Q: Ord + Clone,
{
    type Item = &'a Range<Q>;

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
    key: Range<Q>,
    value: Bound<Q>, // Max end-point.
    left: Option<Box<Node<Q>>>,
    right: Option<Box<Node<Q>>>,
}

impl<Q> fmt::Display for Node<Q>
where
    Q: Ord + Clone + fmt::Display
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
            Unbounded => format!("∞["),
        };
        let value = match self.value {
            Included(ref x) => format!("{}]", x),
            Excluded(ref x) => format!("{}[", x),
            Unbounded => String::from("∞"),
        };

        if self.left.is_none() && self.right.is_none() {
            write!(f, " {{ {},{} ({}) }} ", start, end, value)
        } else if self.left.is_none() {
            write!(f, " {{ {},{} ({}) right:{}}} ", start, end, value, self.right.as_ref().unwrap())
        } else if self.right.is_none() {
            write!(f, " {{ {},{} ({}) left:{}}} ", start, end, value, self.left.as_ref().unwrap())
        } else {
            write!(f, " {{ {},{} ({}) left:{}right:{}}} ", start, end, value, self.left.as_ref().unwrap(), self.right.as_ref().unwrap())
        }
    }
}

impl<Q> Node<Q>
where
    Q: Ord + Clone,
{
    pub fn new(range: Range<Q>) -> Node<Q> {
        let max = range.1.clone();

        Node {
            key: range,
            value: max,
            left: None,
            right: None,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.left.is_none() && self.right.is_none()
    }

    pub fn maybe_update_value(&mut self, inserted_max: &Bound<Q>) {
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
                    self.value = inserted_max.clone();
                }
            }
        };
    }
}

fn cmp<Q>(r1: &Range<Q>, r2: &Range<Q>) -> Ordering
where
    Q: Ord,
{
    // Sorting by lower bound, then by upper bound.
    //   -> Unbounded is the smallest lower bound.
    //   -> Unbounded is the biggest upper bound.
    //   -> Included(x) < Excluded(x) for a lower bound.
    //   -> Included(x) > Excluded(x) for an upper bound.

    // Unpacking from a Bound is annoying, so let's map it to an Option<Q>.
    // Let's use this transformation to encode the Included/Excluded rules at the same time.
    // Note that topological order is used during comparison, so if r1 and r2 have the same `x`,
    // only then will the 2nd element of the tuple serve as a tie-breaker.
    let r1_min = match &r1.0 {
        Included(x) => Some((x, 1)),
        Excluded(x) => Some((x, 2)),
        Unbounded => None,
    };
    let r2_min = match &r2.0 {
        Included(x) => Some((x, 1)),
        Excluded(x) => Some((x, 2)),
        Unbounded => None,
    };

    match (r1_min, r2_min) {
        (None, None) => {} // Left-bounds are equal, we can't return yet.
        (None, Some(_)) => return Less,
        (Some(_), None) => return Greater,
        (Some(r1), Some(ref r2)) => {
            match r1.cmp(r2) {
                Less => return Less,
                Greater => return Greater,
                Equal => {} // Left-bounds are equal, we can't return yet.
            };
        }
    };

    // Both left-bounds are equal, we have to 
    // compare the right-bounds as a tie-breaker.
    cmp_endbound(&r1.1, &r2.1)
}

fn cmp_endbound<Q>(e1: &Bound<Q>, e2: &Bound<Q>) -> Ordering
where
    Q: Ord,
{
    // Based on the encoding idea used in `cmp`.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_inserts_root() {
        let mut tree = IntervalTree::default();
        assert!(tree.root.is_none());

        let key = (Included(1), Included(3));

        tree.insert(key.clone());
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

        tree.insert(root_key.clone());
        assert!(tree.root.is_some());
        assert!(tree.root.as_ref().unwrap().left.is_none());

        tree.insert(left_key.clone());
        assert!(tree.root.as_ref().unwrap().right.is_none());
        assert!(tree.root.as_ref().unwrap().left.is_some());
        assert_eq!(
            tree.root.as_ref().unwrap().left.as_ref().unwrap().value,
            left_key.1
        );

        tree.insert(left_right_key.clone());
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

        tree.insert(root_key.clone());
        assert_eq!(tree.root.as_ref().unwrap().value, root_key.1);

        tree.insert(left_key.clone());
        assert_eq!(tree.root.as_ref().unwrap().value, root_key.1);
        assert!(tree.root.as_ref().unwrap().left.is_some());
        assert_eq!(
            tree.root.as_ref().unwrap().left.as_ref().unwrap().value,
            left_key.1
        );

        tree.insert(left_left_key.clone());
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

        tree.insert(right_key.clone());
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
    fn cmp_works_as_expected() {
        let key0 = (Unbounded, Excluded(20));
        let key1 = (Included(1), Included(5));
        let key2 = (Included(1), Excluded(7));
        let key3 = (Included(1), Included(7));
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
        assert_eq!(cmp(&key_str1, &key_str2), Less);
        assert_eq!(cmp(&key_str2, &key_str3), Less);
    }

    #[test]
    fn overlap_works_as_expected() {
        let mut tree = IntervalTree::default();

        let root_key = (Included(2), Included(3));
        let left_key = (Included(0), Included(1));
        let left_left_key = (Included(-5), Excluded(10));
        let right_key = (Excluded(3), Unbounded);

        tree.insert(root_key.clone());
        tree.insert(left_key.clone());
        assert_eq!(tree.get_interval_overlaps(&root_key), vec![&root_key]);

        tree.insert(left_left_key.clone());
        assert_eq!(
            tree.get_interval_overlaps(&(Unbounded, Unbounded)),
            vec![&left_left_key, &left_key, &root_key]
        );
        assert_eq!(
            tree.get_interval_overlaps(&(Included(100), Unbounded)),
            Vec::<&Range<i32>>::new()
        );

        tree.insert(right_key);
        assert_eq!(
            tree.get_interval_overlaps(&root_key),
            vec![&left_left_key, &root_key]
        );
        assert_eq!(
            tree.get_interval_overlaps(&(Unbounded, Unbounded)),
            vec![&left_left_key, &left_key, &root_key, &right_key]
        );
        assert_eq!(
            tree.get_interval_overlaps(&(Included(100), Unbounded)),
            vec![&right_key]
        );
        assert_eq!(
            tree.get_interval_overlaps(&(Included(3), Excluded(10))),
            vec![&left_left_key, &root_key, &right_key]
        );
        assert_eq!(
            tree.get_interval_overlaps(&(Excluded(3), Excluded(10))),
            vec![&left_left_key, &right_key]
        );
        assert_eq!(
            tree.get_interval_overlaps(&(Unbounded, Excluded(2))),
            vec![&left_left_key, &left_key]
        );
        assert_eq!(
            tree.get_interval_overlaps(&(Unbounded, Included(2))),
            vec![&left_left_key, &left_key, &root_key]
        );
        assert_eq!(
            tree.get_interval_overlaps(&(Unbounded, Included(3))),
            vec![&left_left_key, &left_key, &root_key]
        );
    }

    #[test]
    fn difference_and_overlaps_with_tuple_works_as_expected() {
        let mut tree = IntervalTree::default();

        let root_key = (Included((1, 2)), Excluded((1, 4)));
        let right_key = (Included((5, 10)), Included((5, 20)));

        tree.insert(root_key.clone());
        tree.insert(right_key.clone());

        assert!(tree
            .get_interval_overlaps(&(Included((2, 0)), Included((2, 30))))
            .is_empty());
        assert_eq!(
            tree.get_interval_overlaps(&(Included((1, 3)), Included((1, 5)))),
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
            vec![
                (Included(20), Excluded(30)),
                (Excluded(40), Included(45))
            ]
        );
        assert_eq!(
            tree.get_interval_difference((Included(20), Excluded(45))),
            vec![
                (Included(20), Excluded(30)),
                (Excluded(40), Excluded(45))
            ]
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

        assert!(tree.contains_point(10));
        assert!(!tree.contains_point(20));
        assert!(tree.contains_point(40));
        assert!(tree.contains_point(100));
    }

    #[test]
    fn contains_works_as_expected() {
        let mut tree = IntervalTree::default();

        let key1 = (Included(10), Excluded(20));
        let key2 = (Excluded(30), Excluded(40));
        let key3 = (Included(40), Unbounded);

        tree.insert(key1.clone());
        tree.insert(key2.clone());
        tree.insert(key3.clone());

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

        tree.insert(key1.clone());
        tree.insert(key2.clone());
        tree.insert(key3.clone());
        tree.insert(key4.clone());
        tree.insert(key5.clone());
        tree.insert(key6.clone());

        let inorder = vec![&key4, &key6, &key5, &key1, &key3, &key2];
        for (idx, interval) in tree.iter().enumerate() {
            assert_eq!(interval, inorder[idx]);
        }

        assert_eq!(tree.iter().count(), inorder.len());
    }

    #[test]
    fn remove_random_leaf_empty_tree_works_as_expected() {
        let mut tree : IntervalTree<i32> = IntervalTree::default();

        assert_eq!(tree.remove_random_leaf(), None);
    }

    #[test]
    fn remove_random_leaf_one_node_tree_works_as_expected() {
        let mut tree = IntervalTree::default();

        let key1 = (Included(10), Excluded(20));
        tree.insert(key1.clone());

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

        tree.insert(key1.clone());
        tree.insert(key2.clone());
        tree.insert(key3.clone());
        tree.insert(key4.clone());
        tree.insert(key5.clone());
        tree.insert(key6.clone());

        let mut tree_deleted_key5 = IntervalTree::default();

        let key1_deleted5 = (Included(16), Unbounded);
        let key2_deleted5 = (Included(8), Excluded(9));
        let key3_deleted5 = (Included(5), Excluded(8));
        let key4_deleted5 = (Excluded(15), Included(23));
        let key6_deleted5 = (Included(13), Excluded(26));

        tree_deleted_key5.insert(key1_deleted5.clone());
        tree_deleted_key5.insert(key2_deleted5.clone());
        tree_deleted_key5.insert(key3_deleted5.clone());
        tree_deleted_key5.insert(key4_deleted5.clone());
        tree_deleted_key5.insert(key6_deleted5.clone());

        let mut tree_deleted_key6 = IntervalTree::default();

        let key1_deleted6 = (Included(16), Unbounded);
        let key2_deleted6 = (Included(8), Excluded(9));
        let key3_deleted6 = (Included(5), Excluded(8));
        let key4_deleted6 = (Excluded(15), Included(23));
        let key5_deleted6 = (Included(0), Included(3));

        tree_deleted_key6.insert(key1_deleted6.clone());
        tree_deleted_key6.insert(key2_deleted6.clone());
        tree_deleted_key6.insert(key3_deleted6.clone());
        tree_deleted_key6.insert(key4_deleted6.clone());
        tree_deleted_key6.insert(key5_deleted6.clone());


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
            all_deleted.insert(deleted.clone());
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
}
