// See test-discovery crate documentation for why test files are consolidated
// into a single binary via include!. proptest.rs is renamed to avoid collision
// with the proptest crate dependency.
#[path = "tests/proptest.rs"]
mod proptest_tests;

include!(concat!(env!("OUT_DIR"), "/tests.rs"));
