use std::ops::Bound;
use std::ops::Bound::*;
use std::cmp::Ordering;
use std::cmp::Ordering::*;

type Range<Q> = (Bound<Q>, Bound<Q>);

pub struct IntervalTree<Q: Ord + Clone> {
    root: Option<Box<Node<Q>>>,
}

impl<Q> Default for IntervalTree<Q>
where Q: Ord + Clone {
    fn default() -> IntervalTree<Q> {
        IntervalTree {
            root: None,
        }
    }
}

impl<Q> IntervalTree<Q>
where Q: Ord + Clone {
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
                        },
                        Some(ref mut node) => curr = node,
                    };
                },
                Greater => {
                    match curr.left {
                        None => {
                            curr.left = Some(Box::new(Node::new(range)));
                            return;
                        },
                        Some(ref mut node) => curr = node,
                    };
                },
            };
        };
    }

    pub fn get_interval_intersec(&self, q: &Range<Q>) -> Vec<&Range<Q>> {
        let curr = self.root.as_ref();
        let mut acc = Vec::new();

        Self::get_interval_intersec_rec(curr, q, &mut acc);
        acc
    }

    fn get_interval_intersec_rec<'a>(curr: Option<&'a Box<Node<Q>>>, q: &Range<Q>, acc: &mut Vec<&'a Range<Q>>) {
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
            (Some(max_subtree), Some(min_q)) => {
                if max_subtree < min_q {
                    return;
                }
            },
            _ => {},
        };

        // Search left subtree.
        Self::get_interval_intersec_rec(node.left.as_ref(), q, acc);

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
                    (Some(max_node), Some(min_q)) if max_node < min_q => {},
                    _ => acc.push(&node.key),
                };
            },
        };

        // Search right subtree.
        Self::get_interval_intersec_rec(node.right.as_ref(), q, acc);
    }
}

struct Node<Q: Ord + Clone> {
    key: Range<Q>,
    value: Bound<Q>, // Max end-point.
    left: Option<Box<Node<Q>>>,
    right: Option<Box<Node<Q>>>,
}

impl<Q> Node<Q>
where Q: Ord + Clone {
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
            (None, _) => {},
            (_, None) => self.value = Unbounded,
            (Some(self_max_q), Some(inserted_max_q)) => {
                if self_max_q < inserted_max_q {
                    self.value = inserted_max.clone();
                }
            },
        };
    }
}

fn cmp<Q>(r1: &Range<Q>, r2: &Range<Q>) -> Ordering
where Q: Ord {
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
        (None, None) => {}, // Left-bounds are equal, we can't return yet.
        (None, Some(_)) => return Less,
        (Some(_), None) => return Greater,
        (Some(r1), Some(ref r2)) => {
            match r1.cmp(r2) {
                Less => return Less,
                Greater => return Greater,
                Equal => {}, // Left-bounds are equal, we can't return yet.
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
        assert_eq!(tree.root.as_ref().unwrap().left.as_ref().unwrap().value, left_key.1);
    
        tree.insert(left_right_key.clone());
        assert!(tree.root.as_ref().unwrap().left.as_ref().unwrap().right.is_some());
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
        assert_eq!(tree.root.as_ref().unwrap().left.as_ref().unwrap().value, left_key.1);

        tree.insert(left_left_key.clone());
        assert_eq!(tree.root.as_ref().unwrap().value, left_left_key.1);
        assert_eq!(tree.root.as_ref().unwrap().left.as_ref().unwrap().value, left_left_key.1);
        assert!(tree.root.as_ref().unwrap().left.as_ref().unwrap().left.is_some());
        assert_eq!(tree.root.as_ref().unwrap().left.as_ref().unwrap().left.as_ref().unwrap().value, left_left_key.1);

        tree.insert(right_key.clone());
        assert_eq!(tree.root.as_ref().unwrap().value, right_key.1);
        assert!(tree.root.as_ref().unwrap().right.is_some());
        assert_eq!(tree.root.as_ref().unwrap().left.as_ref().unwrap().value, left_left_key.1);
        assert_eq!(tree.root.as_ref().unwrap().right.as_ref().unwrap().value, right_key.1);
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
        let key_str3 : (_, Bound<&str>) = (Included("bbc"), Unbounded);

        assert_eq!(cmp(&key1, &key1), Equal);
        assert_eq!(cmp(&key1, &key2), Less);
        assert_eq!(cmp(&key2, &key3), Less);
        assert_eq!(cmp(&key0, &key1), Less);
        assert_eq!(cmp(&key4, &key5), Less);
        assert_eq!(cmp(&key_str1, &key_str2), Less);
        assert_eq!(cmp(&key_str2, &key_str3), Less);
    }

    #[test]
    fn intersection_works_as_expected() {
        let mut tree = IntervalTree::default();

        let root_key = (Included(2), Included(3));
        let left_key = (Included(0), Included(1));
        let left_left_key = (Included(-5), Excluded(10));
        let right_key = (Excluded(3), Unbounded);

        tree.insert(root_key.clone());
        tree.insert(left_key.clone());
        assert_eq!(tree.get_interval_intersec(&root_key), vec![&root_key]);

        tree.insert(left_left_key.clone());
        assert_eq!(tree.get_interval_intersec(&(Unbounded, Unbounded)), vec![&left_left_key, &left_key, &root_key]);
        assert_eq!(tree.get_interval_intersec(&(Included(100), Unbounded)), Vec::<&Range<i32>>::new());

        tree.insert(right_key);
        assert_eq!(tree.get_interval_intersec(&root_key), vec![&left_left_key, &root_key]);
        assert_eq!(tree.get_interval_intersec(&(Unbounded, Unbounded)), vec![&left_left_key, &left_key, &root_key, &right_key]);
        assert_eq!(tree.get_interval_intersec(&(Included(100), Unbounded)), vec![&right_key]);
        assert_eq!(tree.get_interval_intersec(&(Included(3), Excluded(10))), vec![&left_left_key, &root_key, &right_key]);     
        assert_eq!(tree.get_interval_intersec(&(Excluded(3), Excluded(10))), vec![&left_left_key, &right_key]);   
        assert_eq!(tree.get_interval_intersec(&(Unbounded, Excluded(2))), vec![&left_left_key, &left_key]);
        assert_eq!(tree.get_interval_intersec(&(Unbounded, Included(2))), vec![&left_left_key, &left_key, &root_key]);
        assert_eq!(tree.get_interval_intersec(&(Unbounded, Included(3))), vec![&left_left_key, &left_key, &root_key]);   
    }
}
