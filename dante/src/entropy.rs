//! Entropy wrapper for controlled randomness.
//!
//! Every random choice in the generator goes through [`Entropy`] so that
//! Antithesis can control the random source during fault-injection testing.

use std::fmt;

use rand::distr::StandardUniform;
use rand::prelude::*;

/// Wrapper around a `dyn rand::Rng` providing convenience methods for generation.
///
/// This type is not generic -- it uses dynamic dispatch for the inner RNG. This
/// enables trait objects to remain dyn-compatible. The cost of dynamic dispatch
/// is negligible compared to database operations.
pub struct Entropy<'a> {
    rng: &'a mut dyn rand::Rng,
}

impl fmt::Debug for Entropy<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entropy").finish_non_exhaustive()
    }
}

impl<'a> Entropy<'a> {
    /// Wrap a mutable reference to any `rand::Rng` implementation.
    pub fn new(rng: &'a mut dyn rand::Rng) -> Self {
        Self { rng }
    }

    /// Reborrow as a new `Entropy` with a shorter lifetime.
    pub fn reborrow(&mut self) -> Entropy<'_> {
        Entropy { rng: self.rng }
    }

    /// Pick a random element from a non-empty slice.
    ///
    /// # Panics
    ///
    /// Panics if the slice is empty.
    pub fn choose<'b, T>(&mut self, items: &'b [T]) -> &'b T {
        assert!(
            !items.is_empty(),
            "Entropy::choose called with empty slice: caller precondition violated"
        );
        let idx = self.range(0..items.len());
        &items[idx]
    }

    /// Pick a random element from a non-empty slice, returning a clone.
    ///
    /// # Panics
    ///
    /// Panics if the slice is empty.
    pub fn choose_cloned<T: Clone>(&mut self, items: &[T]) -> T {
        self.choose(items).clone()
    }

    /// Weighted random selection. Each item is paired with a non-negative weight.
    ///
    /// # Panics
    ///
    /// Panics if `items` is empty or all weights are zero.
    pub fn choose_weighted<'b, T>(&mut self, items: &'b [(T, u32)]) -> &'b T {
        assert!(
            !items.is_empty(),
            "Entropy::choose_weighted called with empty items: caller precondition violated"
        );
        let total: u32 = items.iter().map(|(_, w)| *w).sum();
        assert!(
            total > 0,
            "Entropy::choose_weighted: total weight must be positive"
        );
        let mut pick = self.range(0..total);
        for (item, weight) in items {
            if pick < *weight {
                return item;
            }
            pick -= *weight;
        }
        &items.last().expect("non-empty items").0
    }

    /// Generate a random value in the given range.
    pub fn range<T>(&mut self, range: std::ops::Range<T>) -> T
    where
        T: rand::distr::uniform::SampleUniform + PartialOrd,
    {
        self.rng.random_range(range)
    }

    /// Returns `true` with the given probability (0.0 = never, 1.0 = always).
    pub fn probability(&mut self, p: f64) -> bool {
        debug_assert!(
            (0.0..=1.0).contains(&p),
            "probability must be in [0.0, 1.0]"
        );
        let val: f64 = self.rng.sample(StandardUniform);
        val < p
    }

    /// Generate a random `bool` (50/50).
    pub fn coin_flip(&mut self) -> bool {
        self.rng.random()
    }

    /// Borrow the inner RNG mutably.
    pub fn rng_mut(&mut self) -> &mut dyn rand::Rng {
        self.rng
    }
}

impl rand::rand_core::TryRng for Entropy<'_> {
    type Error = std::convert::Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        Ok(self.rng.next_u32())
    }

    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        Ok(self.rng.next_u64())
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Self::Error> {
        self.rng.fill_bytes(dest);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::SmallRng;

    use super::*;

    #[test]
    fn seeded_entropy_is_deterministic() {
        let mut rng1 = SmallRng::seed_from_u64(42);
        let mut rng2 = SmallRng::seed_from_u64(42);
        let mut e1 = Entropy::new(&mut rng1);
        let mut e2 = Entropy::new(&mut rng2);
        let vals1: Vec<u32> = (0..10).map(|_| e1.range(0..1000)).collect();
        let vals2: Vec<u32> = (0..10).map(|_| e2.range(0..1000)).collect();
        assert_eq!(vals1, vals2);
    }

    #[test]
    fn choose_returns_valid_element() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        let items = [10, 20, 30, 40, 50];
        for _ in 0..100 {
            let picked = e.choose(&items);
            assert!(items.contains(picked));
        }
    }

    #[test]
    fn choose_weighted_respects_weights() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        let items = [("always", 1000u32), ("never", 0u32)];
        for _ in 0..100 {
            let picked = e.choose_weighted(&items);
            assert_eq!(*picked, "always");
        }
    }

    #[test]
    fn probability_extremes() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        for _ in 0..100 {
            assert!(!e.probability(0.0));
        }
        for _ in 0..100 {
            assert!(e.probability(1.0));
        }
    }

    #[test]
    fn reborrow_continues_sequence() {
        // Reborrowing must consume bytes from the parent RNG, so the
        // sequence observed through a reborrow is a contiguous slice of
        // the parent's stream. Compare two parallel runs that draw the
        // same number of values with and without a reborrow in the middle:
        // the values must match exactly.
        let mut rng_a = SmallRng::seed_from_u64(99);
        let mut e_a = Entropy::new(&mut rng_a);
        let with_reborrow: Vec<u32> = {
            let v1 = e_a.range(0..1000u32);
            let v2 = {
                let mut e2 = e_a.reborrow();
                e2.range(0..1000u32)
            };
            let v3 = e_a.range(0..1000u32);
            vec![v1, v2, v3]
        };

        let mut rng_b = SmallRng::seed_from_u64(99);
        let mut e_b = Entropy::new(&mut rng_b);
        let no_reborrow: Vec<u32> = (0..3).map(|_| e_b.range(0..1000u32)).collect();

        assert_eq!(
            with_reborrow, no_reborrow,
            "reborrow should consume bits from the parent stream"
        );
    }

    #[test]
    fn coin_flip_returns_both_values() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        let results: Vec<bool> = (0..100).map(|_| e.coin_flip()).collect();
        assert!(results.contains(&true));
        assert!(results.contains(&false));
    }
}
