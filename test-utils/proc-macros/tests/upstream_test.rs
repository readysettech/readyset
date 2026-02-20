use test_strategy::proptest;
use test_utils::{tags, upstream};

#[test]
fn tags_after_upstream_fails_to_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile-fail/tags_after_upstream.rs");
}

// Parameterless + upstream only
#[upstream(mysql80, mysql84)]
#[test]
fn parameterless_sync() {
    assert_eq!(1 + 1, 2);
}

// Parameterless + async
#[upstream(mysql80, postgres15)]
#[tokio::test]
async fn parameterless_async() {
    assert_eq!(2 + 2, 4);
}

// Parameterless + tags + upstream
#[tags(serial)]
#[upstream(mysql80, mysql84)]
#[test]
fn parameterless_with_tags() {
    assert_eq!(3 + 3, 6);
}

// Parameterized proptest + upstream
#[upstream(mysql80, mysql84)]
#[proptest]
fn proptest_with_params(#[strategy(1..100i32)] x: i32) {
    assert!(x > 0);
}

// Parameterized proptest + tags + upstream
#[tags(serial, no_retry)]
#[upstream(postgres13, postgres15)]
#[proptest]
fn proptest_with_tags(#[strategy(1..100i32)] x: i32) {
    assert!(x > 0);
}
