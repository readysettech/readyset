#![allow(unused_imports)]

use test_utils::upstream;

// Flag in the version-set position should produce a helpful hint.
#[upstream(mysql, gtid)]
#[test]
fn flag_in_wrong_position() {}

fn main() {}
