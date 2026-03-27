#![allow(unused_imports)]

use test_utils::upstream;

#[upstream(sqlite)]
#[test]
fn bad_family() {}

fn main() {}
