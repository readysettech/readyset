use test_strategy::proptest;
use test_utils::{tags, upstream};

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
