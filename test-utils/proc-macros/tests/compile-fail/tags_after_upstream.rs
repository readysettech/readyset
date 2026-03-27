#![allow(unused_imports)]

use test_utils::{tags, upstream};

// Wrong ordering: #[upstream] before #[tags] should fail.
#[upstream(mysql, modern)]
#[tags(serial)]
#[test]
fn tags_after_upstream() {
    assert_eq!(1 + 1, 2);
}

fn main() {}
