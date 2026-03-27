#![allow(unused_imports)]

use test_utils::upstream;

#[upstream(postgres, all, gtid)]
#[test]
fn postgres_gtid() {}

fn main() {}
