use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

use readyset_util::iter::cmp_by;
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
    Hash,
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

    /// The case-insensitive text collation.
    ///
    /// This collation corresponds to the behavior of the
    /// [PostgreSQL `CITEXT` type](https://www.postgresql.org/docs/current/citext.html) with the
    /// locale set to `en_US.utf8`.
    Citext,
}

impl Display for Collation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Utf8 => write!(f, "utf-8"),
            Self::Citext => write!(f, "citext"),
        }
    }
}

impl Collation {
    /// Normalize the given string according to this collation.
    ///
    /// It will always be the case that two normalized strings compare in the same way as
    /// [`compare_strs`][]
    ///
    /// [`compare_strs`]: Collation::compare_strs
    pub(crate) fn normalize(self, s: &str) -> Cow<str> {
        match self {
            Collation::Utf8 => s.into(),
            Collation::Citext => s.to_lowercase().into(),
        }
    }

    /// Hash the given string according to this collation
    pub(crate) fn hash_str<H>(self, s: &str, state: &mut H)
    where
        H: Hasher,
    {
        match self {
            Collation::Utf8 => s.hash(state),
            Collation::Citext => s.to_lowercase().hash(state),
        }
    }

    /// Compare the given strings according to this collation
    pub(crate) fn compare_strs(self, s1: &str, s2: &str) -> Ordering {
        match self {
            Collation::Utf8 => s1.cmp(s2),
            Collation::Citext => cmp_by(
                s1.chars().map(|c| c.to_lowercase()),
                s2.chars().map(|c| c.to_lowercase()),
                |c1, c2| c1.cmp(c2),
            ),
        }
    }

    /// Returns `true` if the collation is [`Utf8`].
    ///
    /// [`Utf8`]: Collation::Utf8
    #[must_use]
    pub fn is_utf8(&self) -> bool {
        matches!(self, Self::Utf8)
    }

    /// Create a Readyset collation from a MySQL collation.
    pub fn from_mysql_collation(database_collation: &str) -> Option<Self> {
        match database_collation.ends_with("_ci") {
            true => Some(Self::Citext),
            false => Some(Self::Utf8),
        }
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

    #[proptest]
    fn normalize_matches_cmp(collation: Collation, s1: String, s2: String) {
        assert_eq!(
            collation.compare_strs(&s1, &s2),
            collation.normalize(&s1).cmp(&collation.normalize(&s2))
        )
    }

    #[test]
    fn citext_equal() {
        #[track_caller]
        fn citext_strings_equal(s1: &str, s2: &str) {
            assert_eq!(Collation::Citext.compare_strs(s1, s2), Ordering::Equal)
        }

        #[track_caller]
        fn citext_strings_inequal(s1: &str, s2: &str) {
            assert_ne!(Collation::Citext.compare_strs(s1, s2), Ordering::Equal)
        }

        citext_strings_equal("abcdef", "abcdef");
        citext_strings_equal("ABcDeF", "abCdEf");
        citext_strings_inequal("ABcDeFf", "abCdEf");
        citext_strings_equal("aÄbø", "aäbØ");
        citext_strings_inequal("ß", "ss");

        // LATIN CAPITAL LETTER I WITH OGONEK, which has some interesting casing rules
        citext_strings_equal("Į", "į");
    }

    #[test]
    fn citext_ordering() {
        #[track_caller]
        fn citext_strings_less(s1: &str, s2: &str) {
            assert_eq!(Collation::Citext.compare_strs(s1, s2), Ordering::Less)
        }

        citext_strings_less("a", "b");
        citext_strings_less("A", "b");
        citext_strings_less("a", "B");
    }
}
