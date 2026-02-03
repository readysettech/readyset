use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// A monotonically increasing counter tracking schema changes.
///
/// Valid generations are 1 to u64::MAX. Use `Option<SchemaGeneration>` where
/// "unset" semantics are needed - `None` indicates no valid generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct SchemaGeneration(NonZeroU64);

impl SchemaGeneration {
    /// The initial generation for a new deployment.
    pub const INITIAL: Self = Self(NonZeroU64::new(1).unwrap());

    /// Increment to the next generation.
    /// Wraps from u64::MAX back to 1 (never 0).
    pub fn next(self) -> Self {
        match self.0.get().checked_add(1) {
            Some(n) => Self(NonZeroU64::new(n).expect("checked_add result > 0")),
            None => Self::INITIAL,
        }
    }

    /// Get the underlying u64 value.
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Check if this generation is immediately followed by `other`, allowing for wraparound.
    pub fn precedes(self, other: Self) -> bool {
        self.next() == other
    }

    /// Create a new generation from a non-zero u64.
    ///
    /// Returns `None` if the value is 0.
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }
}

impl From<SchemaGeneration> for u64 {
    fn from(value: SchemaGeneration) -> Self {
        value.get()
    }
}

impl std::fmt::Display for SchemaGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for SchemaGeneration {
    fn default() -> Self {
        Self::INITIAL
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_size_optimization() {
        assert_eq!(std::mem::size_of::<SchemaGeneration>(), 8);
        assert_eq!(std::mem::size_of::<Option<SchemaGeneration>>(), 8);
    }

    #[test]
    fn initial_is_one() {
        assert_eq!(SchemaGeneration::INITIAL.get(), 1);
    }

    #[test]
    fn next_increments() {
        let g = SchemaGeneration::INITIAL;
        assert_eq!(g.next().get(), 2);
        assert_eq!(g.next().next().get(), 3);
    }

    #[test]
    fn precedes_works() {
        let g1 = SchemaGeneration::INITIAL;
        let g2 = g1.next();
        assert!(g1.precedes(g2));
        assert!(!g2.precedes(g1));
        assert!(!g1.precedes(g1));
    }

    #[test]
    fn next_wraps_at_max() {
        let max = SchemaGeneration::new(u64::MAX).unwrap();
        assert_eq!(max.next(), SchemaGeneration::INITIAL);
    }

    #[test]
    fn precedes_across_wraparound() {
        let max = SchemaGeneration::new(u64::MAX).unwrap();
        assert!(max.precedes(SchemaGeneration::INITIAL));
        assert!(!SchemaGeneration::INITIAL.precedes(max));
    }

    #[test]
    fn new_zero_returns_none() {
        assert!(SchemaGeneration::new(0).is_none());
    }
}
