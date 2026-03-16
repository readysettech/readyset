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
///
/// Supports checkpoint / restore so that constraint resolvers can backtrack
/// over speculative branches without leaking RNG state across the rollback
/// boundary. While a checkpoint is outstanding, every `next_u64` is recorded on
/// a tape; on restore the read head is rewound and subsequent reads replay
/// from the tape until exhausted, then resume drawing from the inner RNG.
pub struct Entropy<'a> {
    rng: &'a mut dyn rand::Rng,
    /// u64s consumed since the last [`Self::release`]; replayed on restore.
    tape: Vec<u64>,
    /// Index into `tape`. Reads with `head < tape.len()` replay from the tape;
    /// reads with `head == tape.len()` draw from `rng` and append.
    head: usize,
}

/// Opaque snapshot of an [`Entropy`] read position; pass back to
/// [`Entropy::restore`] to rewind the RNG to that point.
#[derive(Debug, Clone, Copy)]
pub struct EntropyCheckpoint {
    head: usize,
}

impl fmt::Debug for Entropy<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entropy")
            .field("tape_len", &self.tape.len())
            .field("head", &self.head)
            .finish_non_exhaustive()
    }
}

impl<'a> Entropy<'a> {
    /// Wrap a mutable reference to any `rand::Rng` implementation.
    pub fn new(rng: &'a mut dyn rand::Rng) -> Self {
        Self {
            rng,
            tape: Vec::new(),
            head: 0,
        }
    }

    /// Capture the current RNG read position so callers can rewind to it on
    /// rollback. Cheap; does not allocate.
    pub fn checkpoint(&self) -> EntropyCheckpoint {
        EntropyCheckpoint { head: self.head }
    }

    /// Rewind the RNG read head to a previous checkpoint. Subsequent reads
    /// replay tape entries until the head catches up to the recorded length,
    /// then resume drawing from the inner RNG.
    pub fn restore(&mut self, cp: EntropyCheckpoint) {
        debug_assert!(
            cp.head <= self.tape.len(),
            "checkpoint head {} exceeds tape length {}",
            cp.head,
            self.tape.len()
        );
        self.head = cp.head;
    }

    /// Discard the recorded tape if all bytes have been consumed. Call once
    /// the resolver has committed to a branch and no further restore can
    /// happen against earlier checkpoints. No-op if a restore is still
    /// pending (head < tape.len()).
    pub fn release(&mut self) {
        if self.head == self.tape.len() {
            self.tape.clear();
            self.head = 0;
        }
    }

    /// Pick a random element from a slice.
    ///
    /// Returns `None` if the slice is empty.
    pub fn choose<'b, T>(&mut self, items: &'b [T]) -> Option<&'b T> {
        if items.is_empty() {
            return None;
        }
        let idx = self.range(0..items.len());
        Some(&items[idx])
    }

    /// Pick a random element from a slice, returning a clone.
    ///
    /// Returns `None` if the slice is empty.
    pub fn choose_cloned<T: Clone>(&mut self, items: &[T]) -> Option<T> {
        self.choose(items).cloned()
    }

    /// Weighted random selection. Each item is paired with a non-negative weight.
    ///
    /// Returns `None` if `items` is empty or all weights are zero.
    pub fn choose_weighted<'b, T>(&mut self, items: &'b [(T, u32)]) -> Option<&'b T> {
        if items.is_empty() {
            return None;
        }
        let total: u32 = items.iter().map(|(_, w)| *w).sum();
        if total == 0 {
            return None;
        }
        let mut pick = self.range(0..total);
        for (item, weight) in items {
            if pick < *weight {
                return Some(item);
            }
            pick -= *weight;
        }
        // Unreachable if total > 0, but safe fallback
        Some(&items.last()?.0)
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

    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        let v = if self.head < self.tape.len() {
            let v = self.tape[self.head];
            self.head += 1;
            v
        } else {
            let v = self.rng.next_u64();
            self.tape.push(v);
            self.head += 1;
            v
        };
        Ok(v)
    }

    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        // Truncate the recorded u64 so that backtracking over a u32 read
        // replays a deterministic value. Rate-limited callers (range, etc.)
        // saturate well below the u32 ceiling so the truncation is harmless.
        Ok(self.try_next_u64()? as u32)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Self::Error> {
        for chunk in dest.chunks_mut(8) {
            let bytes = self.try_next_u64()?.to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
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
            let picked = e.choose(&items).unwrap();
            assert!(items.contains(picked));
        }
    }

    #[test]
    fn choose_weighted_respects_weights() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        let items = [("always", 1000u32), ("never", 0u32)];
        for _ in 0..100 {
            let picked = e.choose_weighted(&items).unwrap();
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
    fn coin_flip_returns_both_values() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        let results: Vec<bool> = (0..100).map(|_| e.coin_flip()).collect();
        assert!(results.contains(&true));
        assert!(results.contains(&false));
    }

    #[test]
    fn choose_empty_returns_none() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        let items: &[i32] = &[];
        assert!(e.choose(items).is_none());
    }

    #[test]
    fn choose_weighted_empty_returns_none() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        let items: &[(i32, u32)] = &[];
        assert!(e.choose_weighted(items).is_none());
    }

    #[test]
    fn choose_weighted_zero_total_returns_none() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        let items = [(1, 0u32), (2, 0u32)];
        assert!(e.choose_weighted(&items).is_none());
    }

    #[test]
    fn checkpoint_restore_replays_recorded_values() {
        // Reads after a restore must replay the values consumed since the
        // checkpoint. This is the foundational property for Or-branch
        // backtracking: branch_b sees the RNG in the same state branch_a
        // saw it in.
        let mut rng = SmallRng::seed_from_u64(42);
        let mut e = Entropy::new(&mut rng);
        let cp = e.checkpoint();
        let a1 = e.next_u64();
        let a2 = e.next_u64();
        e.restore(cp);
        let b1 = e.next_u64();
        let b2 = e.next_u64();
        assert_eq!(a1, b1);
        assert_eq!(a2, b2);
    }

    #[test]
    fn checkpoint_restore_branch_independence() {
        // Reads after a checkpoint+consume+restore are equivalent to fresh
        // reads from the same seed (i.e., the speculative reads do not
        // leak into the post-restore stream).
        let mut rng_a = SmallRng::seed_from_u64(7);
        let mut e_a = Entropy::new(&mut rng_a);
        let cp = e_a.checkpoint();
        let _ = e_a.next_u64();
        let _ = e_a.next_u64();
        e_a.restore(cp);
        let v_after_restore = e_a.next_u64();

        let mut rng_b = SmallRng::seed_from_u64(7);
        let mut e_b = Entropy::new(&mut rng_b);
        let v_fresh = e_b.next_u64();

        assert_eq!(v_after_restore, v_fresh);
    }

    #[test]
    fn release_clears_tape_when_fully_consumed() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut e = Entropy::new(&mut rng);
        let cp = e.checkpoint();
        let _ = e.next_u64();
        let _ = e.next_u64();
        e.release();
        // After release with head == tape.len(), tape should reset so that
        // the next checkpoint starts fresh.
        let cp2 = e.checkpoint();
        assert_eq!(cp2.head, 0);
        // Underlying stream continues — no replay of the prior reads.
        let _ = cp; // silences unused warning across the older checkpoint
    }
}
