#![allow(unused_imports)]

use test_utils::upstream;

// Flag in the version-set position for postgres should explain that flags are mysql-only.
#[upstream(postgres, gtid)]
#[test]
fn flag_in_wrong_position() {}

fn main() {}
