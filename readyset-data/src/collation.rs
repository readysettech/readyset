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
    #[default]
    Utf8,
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
}
