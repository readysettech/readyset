// See test-discovery crate documentation for why test files are consolidated
// into a single binary via include!. Shared modules must be declared here at
// the crate root and accessed via `crate::` from test modules.
#[path = "tests/common/mod.rs"]
mod common;
include!(concat!(env!("OUT_DIR"), "/tests.rs"));
