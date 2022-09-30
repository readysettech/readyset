use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use strum_macros::{EnumCount, FromRepr};
use test_strategy::Arbitrary;

/// Description for how string values should be compared against each other for ordering and
/// equality.
///
/// This currently represents a subset of the collations provided [by MySQL][mysql] and
/// [Postgres][postgres], but will be expanded in the future to support more collations
///
/// [mysql]: https://dev.mysql.com/doc/refman/8.0/en/charset-mysql.html
/// [postgres]: https://www.postgresql.org/docs/current/collation.html
#[derive(
    Clone,
    Copy,
    Default,
    Serialize,
    Deserialize,
    Debug,
    PartialEq,
    Eq,
    EnumCount,
    FromRepr,
    Arbitrary,
)]
#[repr(u8)]
pub enum Collation {
    /// The UTF-8 collation.
    ///
    /// This collation, which is the default for PostgreSQL (and the [`Default`] for this type),
    /// corresponds to the *default* behavior of rust's [`String`] type.
    #[default]
    Utf8,
}

impl Collation {
    /// Hash the given string according to this collation
    pub(crate) fn hash_str<H>(self, s: &str, state: &mut H)
    where
        H: Hasher,
    {
        s.hash(state)
    }

    /// Compare the given strings according to this collation
    pub(crate) fn compare_strs(self, s1: &str, s2: &str) -> Ordering {
        s1.cmp(s2)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use strum::EnumCount;
    use test_strategy::proptest;

    use super::*;

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn no_more_than_16_collations() {
        // Because we will be squeezing Collation into a nibble within TinyText, we can never go
        // over 16 variants
        assert!(Collation::COUNT <= 16)
    }

    #[proptest]
    fn hash_matches_eq(collation: Collation, s1: String, s2: String) {
        if collation.compare_strs(&s1, &s2) == Ordering::Equal {
            let [h1, h2] = [&s1, &s2].map(|s| {
                let mut hasher = DefaultHasher::new();
                collation.hash_str(s, &mut hasher);
                hasher.finish()
            });

            assert_eq!(h1, h2);
        }
    }
}
