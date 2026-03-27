#![allow(unused_imports)]

use test_utils::upstream;

#[upstream(postgres, 14)]
#[test]
fn bad_version() {}

fn main() {}
