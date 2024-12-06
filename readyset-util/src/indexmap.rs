//! Conveniences for the indexmap crate.

/// Shrink the indexmap if its capacity is more than 10% greater than it needs to be.
#[macro_export]
macro_rules! maybe_shrink {
    ($m: expr) => {{
        let limit = ($m.len() as f64 * 1.1) as usize;
        if $m.capacity() > limit {
            $m.shrink_to_fit();
        }
    }};
}
