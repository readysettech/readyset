#![allow(unused_imports)]

use test_utils::upstream;

#[upstream(mysql, 57, gtid)]
#[test]
fn old_mysql_gtid() {}

fn main() {}
