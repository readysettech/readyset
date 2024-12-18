//! Conveniences for the indexmap crate.

/// Shrink the indexmap if its capacity is more than 4x greater than it needs to be.
/// (It grows by 2x, so we shrink at 4x to avoid pingponging if near a power of two.)
#[macro_export]
macro_rules! maybe_shrink {
    ($m: expr) => {{
        let limit = $m.len() * 4;
        if $m.capacity() > limit {
            $m.shrink_to_fit();
        }
    }};
}
