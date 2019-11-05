use std::cmp::Ordering;
use std::cmp::Ordering::*;
use std::ops::Bound;
use std::ops::Bound::*;

type Range<Q> = (Bound<Q>, Bound<Q>);

#[derive(Clone)]
pub struct IntervalTree<Q: Ord + Clone> {
    root: Option<Box<Node<Q>>>,
}

pub struct IntervalTreeIter<'a, Q: Ord + Clone> {
    to_visit: Vec<&'a Box<Node<Q>>>,
    curr: &'a Option<Box<Node<Q>>>,
}

impl<Q> Default for IntervalTree<Q>
where
    Q: Ord + Clone,
{
    fn default() -> IntervalTree<Q> {
        IntervalTree { root: None }
    }
}

impl<Q> IntervalTree<Q>
where
    Q: Ord + Clone,
{
    pub fn iter<'a>(&'a self) -> IntervalTreeIter<'a, Q> {
        IntervalTreeIter {
            to_visit: vec![],
            curr: &self.root,
        }
    }

    pub fn insert(&mut self, range: Range<Q>) {
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

    pub fn contains_point(&self, q: &Q) -> bool {
        self.contains_interval(&(Included(q.clone()), Included(q.clone())))
    }

    pub fn contains_interval(&self, q: &Range<Q>) -> bool {
        self.get_interval_difference(q).is_empty()
    }

    pub fn get_interval_overlaps(&self, q: &Range<Q>) -> Vec<&Range<Q>> {
        let curr = &self.root;
        let mut acc = Vec::new();

        Self::get_interval_overlaps_rec(curr, q, &mut acc);
        acc
    }

    pub fn get_interval_difference<'a>(&'a self, q: &'a Range<Q>) -> Vec<Range<&'a Q>> {
        let overlaps = self.get_interval_overlaps(q);

        // If there is no overlap, then the difference is the query `q` itself.
        if overlaps.is_empty() {
            let min = match q.0 {
                Included(ref x) => Included(x),
                Excluded(ref x) => Excluded(x),
                Unbounded => Unbounded,
            };
            let max = match q.1 {
                Included(ref x) => Included(x),
                Excluded(ref x) => Excluded(x),
                Unbounded => Unbounded,
            };
            return vec![(min, max)];
        }

        let mut acc = Vec::new();
        let first = &overlaps.first().unwrap();

        // If q.min < first.min, we have a difference to append.
        match (&q.0, &first.0) {
            (Unbounded, Included(first_min)) => acc.push((Unbounded, Excluded(first_min))),
            (Unbounded, Excluded(first_min)) => acc.push((Unbounded, Included(first_min))),
            (Included(q_min), Included(first_min)) if q_min < first_min => {
                acc.push((Included(q_min), Excluded(first_min)))
            }
            (Excluded(q_min), Included(first_min)) if q_min < first_min => {
                acc.push((Excluded(q_min), Excluded(first_min)))
            }
            (Excluded(q_min), Excluded(first_min)) if q_min < first_min => {
                acc.push((Excluded(q_min), Included(first_min)))
            }
            (Included(q_min), Excluded(first_min)) if q_min <= first_min => {
                acc.push((Included(q_min), Included(first_min)))
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
                    acc.push((Excluded(contiguous_max), Excluded(overlap_min)));
                    contiguous = &overlap.1;
                }
                (Included(contiguous_max), Excluded(overlap_min))
                    if contiguous_max < overlap_min =>
                {
                    acc.push((Excluded(contiguous_max), Included(overlap_min)));
                    contiguous = &overlap.1;
                }
                (Excluded(contiguous_max), Included(overlap_min))
                    if contiguous_max < overlap_min =>
                {
                    acc.push((Included(contiguous_max), Excluded(overlap_min)));
                    contiguous = &overlap.1;
                }
                (Excluded(contiguous_max), Excluded(overlap_min))
                    if contiguous_max <= overlap_min =>
                {
                    acc.push((Included(contiguous_max), Included(overlap_min)));
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
                acc.push((Excluded(contiguous_max), Included(q_max)))
            }
            (Included(contiguous_max), Excluded(q_max)) if contiguous_max < q_max => {
                acc.push((Excluded(contiguous_max), Excluded(q_max)))
            }
            (Excluded(contiguous_max), Excluded(q_max)) if contiguous_max < q_max => {
                acc.push((Included(contiguous_max), Excluded(q_max)))
            }
            (Excluded(contiguous_max), Included(q_max)) if contiguous_max <= q_max => {
                acc.push((Included(contiguous_max), Included(q_max)))
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

        if !self.to_visit.is_empty() {
            let visited = self.to_visit.pop();
            self.curr = &visited.as_ref().unwrap().right;
            Some(&visited.unwrap().key)
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct Node<Q: Ord + Clone> {
    key: Range<Q>,
    value: Bound<Q>, // Max end-point.
    left: Option<Box<Node<Q>>>,
    right: Option<Box<Node<Q>>>,
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

    // Both left-bounds are equal, we have to compare the right-bounds as a tie-breaker.
    // Note that we have inversed the 2nd value in the tuple, as the Included/Excluded rules
    // are flipped for the upper bound.
    let r1_max = match &r1.1 {
        Included(x) => Some((x, 2)),
        Excluded(x) => Some((x, 1)),
        Unbounded => None,
    };
    let r2_max = match &r2.1 {
        Included(x) => Some((x, 2)),
        Excluded(x) => Some((x, 1)),
        Unbounded => None,
    };

    match (r1_max, r2_max) {
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

        assert!(tree.get_interval_overlaps(&(Included((2, 0)), Included((2, 30)))).is_empty());
        assert_eq!(tree.get_interval_overlaps(&(Included((1, 3)), Included((1, 5)))), vec![&root_key]);
        assert_eq!(tree.get_interval_difference(&(Excluded((1, 1)), Included((1, 5)))), vec![(Excluded(&(1, 1)), Excluded(&(1, 2))), (Included(&(1, 4)), Included(&(1, 5)))]);
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
            tree.get_interval_difference(&(Excluded(0), Included(100))),
            vec![
                (Excluded(&0), Excluded(&2)),
                (Included(&10), Included(&10)),
                (Included(&20), Excluded(&30)),
                (Excluded(&40), Included(&45))
            ]
        );
        assert_eq!(
            tree.get_interval_difference(&(Included(19), Included(40))),
            vec![(Included(&20), Excluded(&30))]
        );
        assert_eq!(
            tree.get_interval_difference(&(Included(20), Included(40))),
            vec![(Included(&20), Excluded(&30))]
        );
        assert_eq!(
            tree.get_interval_difference(&(Included(20), Included(45))),
            vec![
                (Included(&20), Excluded(&30)),
                (Excluded(&40), Included(&45))
            ]
        );
        assert_eq!(
            tree.get_interval_difference(&(Included(20), Excluded(45))),
            vec![
                (Included(&20), Excluded(&30)),
                (Excluded(&40), Excluded(&45))
            ]
        );
        assert_eq!(
            tree.get_interval_difference(&(Included(2), Included(10))),
            vec![(Included(&10), Included(&10))]
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
            tree.get_interval_difference(&(Included(0), Included(40))),
            vec![
                (Included(&0), Excluded(&10)),
                (Included(&20), Included(&30)),
                (Included(&40), Included(&40))
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

        tree.insert(key1.clone());
        tree.insert(key2.clone());
        tree.insert(key3.clone());

        assert!(tree.contains_interval(&key1));
        assert!(!tree.contains_interval(&(Included(10), Included(20))));
        assert!(!tree.contains_interval(&(Unbounded, Included(0))));
        assert!(tree.contains_interval(&(Included(35), Included(37))));
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
}
