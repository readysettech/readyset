use serde::{Deserialize, Serialize};

/// Description for how string values should be compared against each other for ordering and
/// equality.
///
/// This currently represents a subset of the collations provided [by MySQL][mysql] and
/// [Postgres][postgres], but will be expanded in the future to support more collations
#[derive(Clone, Copy, Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(u8)] // NOTE: Because we will be squeezing this type into a nibble within TinyText, we can
            // never go over 16 variants!
pub enum Collation {
    #[default]
    Utf8,
}
