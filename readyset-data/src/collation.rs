use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::Hash;

use icu::collator::options::{CollatorOptions, Strength};
use icu::collator::{Collator, CollatorBorrowed};
use log_once::{error_once, warn_once};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;
use strum::{EnumCount, EnumIter, FromRepr, IntoEnumIterator};
use test_strategy::Arbitrary;

use crate::{Dialect, SqlEngine};

mod mysql_latin1_swedish_ci;
mod mysql_latin1_swedish_ci_weights;

thread_local! {
    static UTF8: CollatorBorrowed<'static> = Collator::try_new(
        Default::default(),
        collator_options(Some(Strength::Tertiary))
    )
    .expect("cannot create UTF8 collator!");
    static UTF8_CI: CollatorBorrowed<'static> = Collator::try_new(
        Default::default(),
        collator_options(Some(Strength::Secondary))
    )
    .expect("cannot create UTF8_CI collator!");
    static UTF8_AI_CI: CollatorBorrowed<'static> = Collator::try_new(
        Default::default(),
        collator_options(Some(Strength::Primary))
    )
    .expect("cannot create UTF8_AI_CI collator!");
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
    Utf8,

    /// UTF-8 case-insensitive.
    ///
    /// This collation corresponds to the behavior of the
    /// [PostgreSQL `CITEXT` type](https://www.postgresql.org/docs/current/citext.html) with the
    /// locale set to `en_US.utf8`.
    Utf8Ci,

    /// UTF-8, accent-insensitive, case-insensitive.
    Utf8AiCi,

    /// The binary collation, that simply compares bytes.
    Binary,

    /// Collation that matches the old MySQL default of latin1_swedish_ci.
    Latin1SwedishCi,

    /// A binary collation that compares codepoints.
    Utf8Binary,

    /// Like Utf8Ci, but trailing spaces are ignored (for a few legacy MySQL collations).
    Utf8AiCiPad,
}

/// External collation names supported per SQL engine.
#[derive(Clone, Copy, Debug, EnumIter)]
enum ExternalCollation {
    // MySQL collations
    MySqlUtf8mb40900AiCi,
    MySqlUtf8mb40900AsCi,
    MySqlUtf8mb40900AsCs,
    MySqlUtf8mb40900Bin,
    MySqlUtf8mb4Bin,
    MySqlUtf8mb3Bin,
    MySqlUtf8Bin,
    MySqlBinary,
    MySqlLatin1SwedishCi,
    MySqlUtf8mb4GeneralCi,
    MySqlUtf8mb4UnicodeCi,
    MySqlUtf8mb3UnicodeCi,
}

// O(1) lookup for MySQL external collation names to internal Collation
static MYSQL_COLLATION_LOOKUP: LazyLock<HashMap<&'static str, Collation>> = LazyLock::new(|| {
    ExternalCollation::iter()
        .filter(|e| e.engine() == SqlEngine::MySQL)
        .map(|e| (e.name(), e.internal()))
        .collect()
});

impl ExternalCollation {
    /// Lookup an external collation name to our internal collation.
    fn lookup(engine: SqlEngine, name: &str) -> Option<Collation> {
        match engine {
            SqlEngine::MySQL => MYSQL_COLLATION_LOOKUP.get(name).copied(),
            SqlEngine::PostgreSQL => None,
        }
    }

    /// Return the engine of the external collation.
    fn engine(&self) -> SqlEngine {
        match self {
            Self::MySqlUtf8mb40900AiCi
            | Self::MySqlUtf8mb40900AsCi
            | Self::MySqlUtf8mb40900AsCs
            | Self::MySqlUtf8mb40900Bin
            | Self::MySqlUtf8mb4Bin
            | Self::MySqlUtf8mb3Bin
            | Self::MySqlUtf8Bin
            | Self::MySqlBinary
            | Self::MySqlLatin1SwedishCi
            | Self::MySqlUtf8mb4GeneralCi
            | Self::MySqlUtf8mb4UnicodeCi
            | Self::MySqlUtf8mb3UnicodeCi => SqlEngine::MySQL,
        }
    }

    /// Return the name of the external collation.
    fn name(&self) -> &'static str {
        match self {
            Self::MySqlUtf8mb40900AiCi => "utf8mb4_0900_ai_ci",
            Self::MySqlUtf8mb40900AsCi => "utf8mb4_0900_as_ci",
            Self::MySqlUtf8mb40900AsCs => "utf8mb4_0900_as_cs",
            Self::MySqlUtf8mb40900Bin => "utf8mb4_0900_bin",
            Self::MySqlUtf8mb4Bin => "utf8mb4_bin",
            Self::MySqlUtf8mb3Bin => "utf8mb3_bin",
            Self::MySqlUtf8Bin => "utf8_bin",
            Self::MySqlBinary => "binary",
            Self::MySqlLatin1SwedishCi => "latin1_swedish_ci",
            Self::MySqlUtf8mb4GeneralCi => "utf8mb4_general_ci",
            Self::MySqlUtf8mb4UnicodeCi => "utf8mb4_unicode_ci",
            Self::MySqlUtf8mb3UnicodeCi => "utf8mb3_unicode_ci",
        }
    }

    fn internal(&self) -> Collation {
        match self {
            Self::MySqlUtf8mb40900AiCi => Collation::Utf8AiCi,
            Self::MySqlUtf8mb40900AsCi => Collation::Utf8Ci,
            Self::MySqlUtf8mb40900AsCs => Collation::Utf8,
            Self::MySqlUtf8mb40900Bin
            | Self::MySqlUtf8mb4Bin
            | Self::MySqlUtf8mb3Bin
            | Self::MySqlUtf8Bin => Collation::Utf8Binary,
            Self::MySqlBinary => Collation::Binary,
            Self::MySqlLatin1SwedishCi => Collation::Latin1SwedishCi,
            Self::MySqlUtf8mb4GeneralCi
            | Self::MySqlUtf8mb4UnicodeCi
            | Self::MySqlUtf8mb3UnicodeCi => Collation::Utf8AiCiPad,
        }
    }
}

impl Display for Collation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Utf8 => write!(f, "utf8"),
            Self::Utf8Ci => write!(f, "utf8_ci"),
            Self::Utf8AiCi => write!(f, "utf8_ai_ci"),
            Self::Binary => write!(f, "binary"),
            Self::Latin1SwedishCi => write!(f, "latin1_swedish_ci"),
            Self::Utf8Binary => write!(f, "utf8_binary"),
            Self::Utf8AiCiPad => write!(f, "utf8_ai_ci(pad)"),
        }
    }
}

impl Collation {
    /// Compare the given strings according to this collation.
    pub(crate) fn compare<A, B>(&self, a: A, b: B) -> Ordering
    where
        A: AsRef<str>,
        B: AsRef<str>,
    {
        let (a, b) = (a.as_ref(), b.as_ref());
        let cmp = |c: &CollatorBorrowed| c.compare(a, b);
        match self {
            Self::Utf8 => UTF8.with(cmp),
            Self::Utf8Ci => UTF8_CI.with(cmp),
            Self::Utf8AiCi => UTF8_AI_CI.with(cmp),
            Self::Binary => a.cmp(b),
            Self::Latin1SwedishCi => mysql_latin1_swedish_ci::compare(a, b),
            Self::Utf8Binary => a.chars().cmp(b.chars()),
            Self::Utf8AiCiPad => Self::compare_ai_ci_pad(a, b),
        }
    }

    fn compare_ai_ci_pad(a: &str, b: &str) -> Ordering {
        let (ac, bc) = (a.chars().count(), b.chars().count());
        let (a, b, ac, bc, flip) = if ac > bc {
            (a, b, ac, bc, false)
        } else {
            (b, a, bc, ac, true)
        };

        let pad = ac - bc;
        let mut b = String::from(b);
        for _ in 0..pad {
            b.push(' ');
        }

        let b = b.as_str();
        let ord = UTF8_AI_CI.with(|c| c.compare(a, b));
        if flip {
            ord.reverse()
        } else {
            ord
        }
    }

    /// Compute a collation key for a string.  This key may be compared bytewise with another
    /// key or hashed.
    pub(crate) fn key<S>(&self, s: S) -> Vec<u8>
    where
        S: AsRef<str>,
    {
        let mut s = s.as_ref();
        let len = match self {
            Self::Utf8 | Self::Utf8Binary => s.len() * 4,
            Self::Utf8Ci | Self::Utf8AiCiPad => s.len() * 2,
            Self::Utf8AiCi | Self::Binary | Self::Latin1SwedishCi => s.len(),
        };

        if *self == Self::Utf8AiCiPad {
            s = s.trim_end_matches(' ');
        }

        let mut out = Vec::with_capacity(len); // just a close guess
        let make = |c: &CollatorBorrowed| c.write_sort_key_to(s.as_ref(), &mut out);
        let Ok(()) = match self {
            Self::Utf8 => UTF8.with(make),
            Self::Utf8Ci | Self::Utf8AiCiPad => UTF8_CI.with(make),
            Self::Utf8AiCi => UTF8_AI_CI.with(make),
            Self::Binary => {
                out.extend_from_slice(s.as_bytes());
                Ok(())
            }
            Self::Latin1SwedishCi => {
                mysql_latin1_swedish_ci::key(s, &mut out);
                Ok(())
            }
            Self::Utf8Binary => {
                for c in s.chars() {
                    // big endian because sort keys are compared byte by byte
                    out.extend_from_slice(&(c as u32).to_be_bytes());
                }
                Ok(())
            }
        };

        out
    }

    /// The default collation for a dialect.
    pub fn default_for(dialect: Dialect) -> Self {
        match dialect.engine() {
            SqlEngine::PostgreSQL => Self::Utf8,
            SqlEngine::MySQL => Self::Utf8AiCi,
        }
    }

    /// Attempt to map a collation name to our internal collation.  If no mapping exists,
    /// return a reasonable default for the dialect.
    pub fn get_or_default(dialect: Dialect, name: &str) -> Self {
        if let Some(coll) = ExternalCollation::lookup(dialect.engine(), name) {
            return coll;
        }

        let coll = if dialect.engine() == SqlEngine::MySQL {
            if name.ends_with("_ai_ci") {
                Some(Self::Utf8AiCi)
            } else if name.ends_with("_ci") {
                Some(Self::Utf8Ci)
            } else if name.ends_with("_cs") {
                Some(Self::Utf8)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(coll) = coll {
            warn_once!(
                "Unknown {} collation {name}, mapping to {coll}",
                dialect.engine()
            );
            return coll;
        }

        let coll = Self::default_for(dialect);
        error_once!(
            "Unknown {} collation {name}, mapping to {coll}",
            dialect.engine()
        );
        coll
    }

    /// Analogous to [`Option::unwrap_or_default`], but specific to the dialect.
    pub fn unwrap_or_default(collation: Option<Collation>, dialect: Dialect) -> Collation {
        collation.unwrap_or_else(|| Self::default_for(dialect))
    }
}

fn collator_options(strength: Option<Strength>) -> CollatorOptions {
    let mut options = CollatorOptions::default();
    options.strength = strength;
    options
}

#[cfg(test)]
mod tests {
    use strum::EnumCount;

    use super::*;

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn no_more_than_16_collations() {
        // Because we will be squeezing Collation into a nibble within TinyText, we can never go
        // over 16 variants
        assert!(Collation::COUNT <= 16)
    }

    #[test]
    fn citext_equal() {
        #[track_caller]
        fn citext_strings_equal(s1: &str, s2: &str) {
            assert_eq!(Collation::Utf8Ci.compare(s1, s2), Ordering::Equal)
        }

        #[track_caller]
        fn citext_strings_inequal(s1: &str, s2: &str) {
            assert_ne!(Collation::Utf8Ci.compare(s1, s2), Ordering::Equal)
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
            assert_eq!(Collation::Utf8Ci.compare(s1, s2), Ordering::Less)
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

    #[test]
    fn latin1_pad_space() {
        let col = Collation::Latin1SwedishCi;
        assert_eq!(col.compare("a", "a "), Ordering::Equal);
        assert_eq!(col.key("a"), col.key("a "));
        assert_eq!(col.compare("a ", "a"), Ordering::Equal);
        assert_eq!(col.key("a "), col.key("a"));
        assert_eq!(col.compare("a", "b"), Ordering::Less);
        assert!(col.key("a").lt(&col.key("b")));
    }

    #[test]
    fn utf8_pad_space() {
        let col = Collation::Utf8AiCiPad;
        assert_eq!(col.compare("A", "a "), Ordering::Equal);
        assert_eq!(col.key("A"), col.key("a "));
        assert_eq!(col.compare("A ", "a"), Ordering::Equal);
        assert_eq!(col.key("A "), col.key("a"));
        assert_eq!(col.compare("a", "b "), Ordering::Less);
        assert_eq!(col.compare("b", "a "), Ordering::Greater);
    }

    #[test]
    fn external_collation_file_is_up_to_date() {
        // collation.txt file is used in our external documentation to inform the list of supported collations.
        let file = std::fs::read_to_string("tests/collation.txt").unwrap();
        // check that all the external collations are present in the file
        for e in ExternalCollation::iter() {
            assert!(file.contains(e.name()), "{} not found in file", e.name());
        }
    }
}
