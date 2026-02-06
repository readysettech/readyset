// See test-discovery crate documentation for why test files are consolidated
// into a single binary via include!. Shared modules must be declared here at
// the crate root and accessed via `crate::` from test modules.
//
// Load utils first with #[macro_use] so its macros are available to all test modules.
#[macro_use]
#[path = "tests/utils.rs"]
mod utils;

include!(concat!(env!("OUT_DIR"), "/tests.rs"));
