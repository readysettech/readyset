use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::Hash;

use icu::collator::{Collator, CollatorOptions, Strength};
use serde::{Deserialize, Serialize};
use strum::{EnumCount, FromRepr};
use test_strategy::Arbitrary;

thread_local! {
    static UTF8: Collator = Collator::try_new(
        &Default::default(),
        collator_options(Some(Strength::Tertiary))
    )
    .expect("cannot create default collator!");
}

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
            Self::Utf8 => write!(f, "utf8"),
            Self::Citext => write!(f, "citext"),
        }
    }
}

impl Collation {
    /// Normalize the given string according to this collation.
    pub(crate) fn normalize(self, s: &str) -> Cow<str> {
        match self {
            Self::Utf8 => s.into(),
            Self::Citext => s.to_lowercase().into(),
        }
    }

    /// Compare the given strings according to this collation
    pub(crate) fn compare<A, B>(&self, a: A, b: B) -> Ordering
    where
        A: AsRef<str>,
        B: AsRef<str>,
    {
        let a = self.normalize(a.as_ref());
        let b = self.normalize(b.as_ref());
        let cmp = |c: &Collator| c.compare(&a, &b);
        match self {
            Self::Utf8 => UTF8.with(cmp),
            Self::Citext => UTF8.with(cmp),
        }
    }

    /// Create a Readyset collation from a MySQL collation.
    pub fn from_mysql_collation(database_collation: &str) -> Option<Self> {
        match database_collation.ends_with("_ci") {
            true => Some(Self::Citext),
            false => Some(Self::Utf8),
        }
    }
}

fn collator_options(strength: Option<Strength>) -> CollatorOptions {
    let mut options = CollatorOptions::new();
    options.strength = strength;
    options
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    use strum::EnumCount;
    use test_strategy::proptest;
    use test_utils::tags;

    use super::*;

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn no_more_than_16_collations() {
        // Because we will be squeezing Collation into a nibble within TinyText, we can never go
        // over 16 variants
        assert!(Collation::COUNT <= 16)
    }

    #[tags(no_retry)]
    #[proptest]
    fn hash_matches_eq(collation: Collation, s1: String, s2: String) {
        if collation.compare(&s1, &s2) == Ordering::Equal {
            let [h1, h2] = [&s1, &s2].map(|s| {
                let mut hasher = DefaultHasher::new();
                collation.normalize(s).hash(&mut hasher);
                hasher.finish()
            });

            assert_eq!(h1, h2);
        }
    }

    #[test]
    fn citext_equal() {
        #[track_caller]
        fn citext_strings_equal(s1: &str, s2: &str) {
            assert_eq!(Collation::Citext.compare(s1, s2), Ordering::Equal)
        }

        #[track_caller]
        fn citext_strings_inequal(s1: &str, s2: &str) {
            assert_ne!(Collation::Citext.compare(s1, s2), Ordering::Equal)
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
            assert_eq!(Collation::Citext.compare(s1, s2), Ordering::Less)
        }

        citext_strings_less("a", "b");
        citext_strings_less("A", "b");
        citext_strings_less("a", "B");
    }
}
