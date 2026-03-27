#![allow(unused_imports)]

use test_utils::upstream;

// mysql57 doesn't support flags, so mysql + all + gtid should fail
#[upstream(mysql, all, gtid)]
#[test]
fn all_mysql_gtid() {}

fn main() {}
