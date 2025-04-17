use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::Hash;

use icu::collator::{Collator, CollatorOptions, Strength};
use icu::normalizer::ComposingNormalizer;
use serde::{Deserialize, Serialize};
use strum::{EnumCount, FromRepr};
use test_strategy::Arbitrary;
use tracing::error;

use crate::{Dialect, SqlEngine};

thread_local! {
    static UTF8: Collator = Collator::try_new(
        &Default::default(),
        collator_options(Some(Strength::Tertiary))
    )
    .expect("cannot create default collator!");
    static UTF8_AI_CI: Collator = Collator::try_new(
        &Default::default(),
        collator_options(Some(Strength::Primary))
    )
    .expect("cannot create default collator!");

    static NORMALIZER: ComposingNormalizer = const { ComposingNormalizer::new_nfc() };
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
    Clone, Copy, Hash, Serialize, Deserialize, Debug, PartialEq, Eq, EnumCount, FromRepr, Arbitrary,
)]
#[repr(u8)]
pub enum Collation {
    /// The UTF-8 collation.
    ///
    /// This collation, which is the default for PostgreSQL (and the [`Default`] for this type),
    /// corresponds to the *default* behavior of rust's [`String`] type.
    Utf8,

    /// The case-insensitive text collation.
    ///
    /// This collation corresponds to the behavior of the
    /// [PostgreSQL `CITEXT` type](https://www.postgresql.org/docs/current/citext.html) with the
    /// locale set to `en_US.utf8`.
    Citext,

    /// UTF-8, accent-insensitive, case-insensitive
    Utf8AiCi,
}

impl Display for Collation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Utf8 => write!(f, "utf8"),
            Self::Citext => write!(f, "citext"),
            Self::Utf8AiCi => write!(f, "utf8_ai_ci"),
        }
    }
}

impl Collation {
    /// Normalize the given string according to this collation.
    pub(crate) fn normalize(self, s: &str) -> Cow<str> {
        match self {
            Self::Utf8 => s.into(),
            Self::Citext => s.to_lowercase().into(),
            Self::Utf8AiCi => s.to_lowercase().into(),
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
            Self::Utf8AiCi => UTF8_AI_CI.with(cmp),
        }
    }

    /// The default collation for a dialect.
    pub fn default_for(dialect: Dialect) -> Self {
        match dialect.engine() {
            SqlEngine::PostgreSQL => Self::Utf8,
            SqlEngine::MySQL => Self::Utf8AiCi,
        }
    }

    /// Map a dialect's collation name to our internal collation if a mapping exists.
    fn try_get(dialect: Dialect, collation: &str) -> Option<Self> {
        match (dialect.engine(), collation) {
            (SqlEngine::MySQL, "utf8mb4_0900_ai_ci") => Some(Collation::Utf8AiCi),
            (SqlEngine::MySQL, "utf8mb4_0900_as_cs") => Some(Collation::Utf8),
            (_, _) => None,
        }
    }

    /// Attempt to map a collation name to our internal collation.  If no mapping exists,
    /// return a reasonable default for the dialect.
    pub fn get_or_default(dialect: Dialect, collation: &str) -> Self {
        Self::try_get(dialect, collation).unwrap_or_else(|| {
            error!("Unknown {} collation {}", dialect.engine(), collation);
            Self::default_for(dialect)
        })
    }

    /// Analogous to [`Option::unwrap_or_default`], but specific to the dialect.
    pub fn unwrap_or_default(collation: Option<Collation>, dialect: Dialect) -> Collation {
        collation.unwrap_or_else(|| Self::default_for(dialect))
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

    #[test]
    fn utf8_ai_ci_ordering() {
        let col = Collation::Utf8AiCi;
        assert_eq!(col.compare("e", "E"), Ordering::Equal);
        assert_eq!(col.compare("e", "é"), Ordering::Equal);
        assert_eq!(col.compare("é", "é"), Ordering::Equal); // second has combining accent
        assert_eq!(col.compare("é", "f"), Ordering::Less);
    }
}
