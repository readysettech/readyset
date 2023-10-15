//! Utilities for dealing with ['Hash'][std::hash::Hash]ing values.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Calculate the hash of `x` using the [`DefaultHasher`]
///
/// ```rust
/// let x: i32 = 123;
/// assert_eq!(readyset_util::hash::hash(&x), 14370432302296844161);
/// ```
// TODO: We probably want to use u128 here--u64 has 3% chance of collision once we get to 1 Billion
// entries.
pub fn hash<T: Hash>(x: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    x.hash(&mut hasher);
    hasher.finish()
}
