# Unbounded Interval Tree

A Rust implementation of an interval tree, based on the one described by Cormen et al., (2009), Introduction to Algorithms (3rd ed., Section 14.3: Interval trees, pp. 348–354). An interval tree is useful to query efficiently a database of intervals. This implementation is generic in that it works with intervals of values implementing `Ord+Clone` traits. The bounds can be inclusive, exclusive, or unbounded. Here are some examples of valid intervals:

* [5, 9] <- inclusive/inclusive integers
* [-2.3, 18.81) <- inclusive/exclusive floats
* ("abc", "hi"] <- exclusive/inclusive strings
* (-inf, November 7 2019] <- unbounded/inclusive dates

## How To Use

I would suggest to look at the examples part of the documentation (as they are tested by the Rust ecosystem), but here's a current example.

```rust
use unbounded_interval_tree::IntervalTree;
use std::ops::Bound::{Included, Excluded, Unbounded};

// Default interval tree.
let mut tree = IntervalTree::default();

// Ranges are defined as a 2-ple of Bounds.
let interval1 = (Included(5), Excluded(9));
let interval2 = (Unbounded, Included(-2));
let interval3 = (Included(30), Included(30));

// Add intervals to the tree.
tree.insert(interval1);
tree.insert(interval2);
tree.insert(interval3);

// Iterate through the intervals inorder.
for (start, end) in tree.iter() {
  println!("Start: {:?}\tEnd: {:?}", start, end);
}

// Get overlapping intervals.
let overlaps = tree.get_interval_overlaps(
  &(Included(0), Excluded(30)));

// Get the difference between the database
// of intervals and the query interval.
let diff = tree.get_interval_difference(
  (Included(0), Excluded(30)));
```

## Roadmap

*What's next...*

* Add another `IntervalTree` constructor (other than the default one).
* Allow to remove intervals from the tree (started in the `delete` branch).
  * I have added a `remove_random_leaf` method to the API. Removing leaves is significantly simpler with this data structure, hence I started by tackling this problem.
* Keep the tree balanced, by rotating during insertions/deletions
* Assert that the start bound of an interval is smaller or equal to the end bound of the same interval.
