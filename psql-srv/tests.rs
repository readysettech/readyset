// See test-discovery crate documentation for why test files are consolidated
// into a single binary via include!.
include!(concat!(env!("OUT_DIR"), "/tests.rs"));
