//! Inspired by the crate of the same name in rust-analyzer.
//! https://github.com/rust-analyzer/rust-analyzer/blob/master/crates/test_utils/src/lib.rs

/// Returns `false` if slow tests should not run, otherwise returns `true`.
pub fn skip_slow_tests() -> bool {
    let should_skip = std::env::var("RUN_SLOW_TESTS").is_err();
    if should_skip {
        eprintln!("ignoring slow test");
    }
    should_skip
}
