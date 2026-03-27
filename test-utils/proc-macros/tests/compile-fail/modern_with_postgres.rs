#![allow(unused_imports)]

use test_utils::upstream;

#[upstream(postgres, modern)]
#[test]
fn postgres_modern() {}

fn main() {}
