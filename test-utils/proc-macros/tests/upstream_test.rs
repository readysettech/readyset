use test_strategy::proptest;
use test_utils::{tags, upstream};

#[test]
fn compile_fail_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile-fail/tags_after_upstream.rs");
    t.compile_fail("tests/compile-fail/unknown_family.rs");
    t.compile_fail("tests/compile-fail/postgres_with_flag.rs");
    t.compile_fail("tests/compile-fail/modern_with_postgres.rs");
    t.compile_fail("tests/compile-fail/flag_on_all_mysql.rs");
    t.compile_fail("tests/compile-fail/invalid_version.rs");
    t.compile_fail("tests/compile-fail/empty_args.rs");
    t.compile_fail("tests/compile-fail/flag_on_single_old_mysql.rs");
    t.compile_fail("tests/compile-fail/flag_as_version_mysql.rs");
    t.compile_fail("tests/compile-fail/flag_as_version_postgres.rs");
}

// All MySQL versions
#[upstream(mysql)]
#[test]
fn mysql_all() {
    assert_eq!(1 + 1, 2);
}

// Modern MySQL (8.0+)
#[upstream(mysql, modern)]
#[test]
fn mysql_modern() {
    assert_eq!(2 + 2, 4);
}

// Tags + upstream
#[tags(serial)]
#[upstream(mysql, modern)]
#[test]
fn mysql_modern_with_tags() {
    assert_eq!(3 + 3, 6);
}

// Proptest + upstream
#[upstream(mysql, modern)]
#[proptest]
fn proptest_with_upstream(#[strategy(1..100i32)] x: i32) {
    assert!(x > 0);
}

// Proptest + tags + upstream
#[tags(serial, no_retry)]
#[upstream(postgres)]
#[proptest]
fn proptest_with_tags(#[strategy(1..100i32)] x: i32) {
    assert!(x > 0);
}

// Modern MySQL with gtid flag
#[upstream(mysql, modern, gtid)]
#[test]
fn mysql_modern_gtid() {
    assert_eq!(4 + 4, 8);
}

// Single MySQL version with flag
#[upstream(mysql, 84, gtid)]
#[test]
fn mysql_single_gtid() {
    assert_eq!(5 + 5, 10);
}

// Modern MySQL with nogtid flag
#[upstream(mysql, modern, nogtid)]
#[test]
fn mysql_modern_nogtid() {
    assert_eq!(6 + 6, 12);
}

// Modern MySQL with mrbr flag
#[upstream(mysql, modern, mrbr)]
#[test]
fn mysql_modern_mrbr() {
    assert_eq!(10 + 10, 20);
}

// Single MySQL version with mrbr_gtid flag (fully specified, no __xp_ expansion)
#[upstream(mysql, 84, mrbr_gtid)]
#[test]
fn mysql_single_mrbr_gtid() {
    assert_eq!(11 + 11, 22);
}

// All PostgreSQL versions
#[upstream(postgres)]
#[test]
fn postgres_all() {
    assert_eq!(7 + 7, 14);
}

// Single PostgreSQL version
#[upstream(postgres, 15)]
#[test]
fn postgres_single() {
    assert_eq!(8 + 8, 16);
}

// Async test
#[upstream(mysql, modern)]
#[tokio::test]
async fn mysql_modern_async() {
    assert_eq!(9 + 9, 18);
}

// Compile-time verification that the macro expands to the correct modules.
// This catches regressions in both explicit variant generation and __xp_ auto-expansion.
#[allow(unused_imports)]
mod _verify_expansion {
    // ── Explicit variants ──
    use super::mysql_all::{mysql57, mysql80, mysql84};
    use super::mysql_modern::{mysql80 as _m80, mysql84 as _m84};
    use super::mysql_modern_gtid::{mysql80_gtid, mysql84_gtid};
    use super::mysql_modern_mrbr::{mysql80_mrbr, mysql84_mrbr};
    use super::mysql_modern_nogtid::{mysql80_nogtid, mysql84_nogtid};
    use super::mysql_single_gtid::mysql84_gtid as _s84g;
    use super::mysql_single_mrbr_gtid::mysql84_mrbr_gtid as _s84mg;
    use super::postgres_all::{postgres13, postgres15};
    use super::postgres_single::postgres15 as _p15;

    // ── __xp_ auto-expansion for unflagged modern MySQL ──
    // mysql80 (from mysql_all) → all four __xp_ variants
    use super::mysql_all::mysql80__xp_gtid;
    use super::mysql_all::mysql80__xp_mrbr;
    use super::mysql_all::mysql80__xp_mrbr_gtid;
    use super::mysql_all::mysql80__xp_nogtid;
    // mysql84 (from mysql_all) → same four
    use super::mysql_all::mysql84__xp_gtid as _;
    use super::mysql_all::mysql84__xp_mrbr as _;
    use super::mysql_all::mysql84__xp_mrbr_gtid as _;
    use super::mysql_all::mysql84__xp_nogtid as _;
    // mysql80 (from mysql_modern) → same four __xp_ variants
    use super::mysql_modern::mysql80__xp_gtid as _mm80_gtid;
    use super::mysql_modern::mysql80__xp_mrbr as _mm80_mrbr;
    use super::mysql_modern::mysql80__xp_mrbr_gtid as _mm80_mrbr_gtid;
    use super::mysql_modern::mysql80__xp_nogtid as _mm80_nogtid;
    // mysql84 (from mysql_modern) → same four
    use super::mysql_modern::mysql84__xp_gtid as _mm84_gtid;
    use super::mysql_modern::mysql84__xp_mrbr as _mm84_mrbr;
    use super::mysql_modern::mysql84__xp_mrbr_gtid as _mm84_mrbr_gtid;
    use super::mysql_modern::mysql84__xp_nogtid as _mm84_nogtid;

    // ── __xp_ auto-expansion for flagged modern MySQL ──
    // mysql80_gtid → + __xp_mrbr_gtid (base is "mysql80", not "mysql80_gtid")
    use super::mysql_modern_gtid::mysql80__xp_mrbr_gtid as _g80xp;
    use super::mysql_modern_gtid::mysql84__xp_mrbr_gtid as _g84xp;
    // mysql80_mrbr → + __xp_mrbr_gtid
    use super::mysql_modern_mrbr::mysql80__xp_mrbr_gtid as _mr80xp;
    use super::mysql_modern_mrbr::mysql84__xp_mrbr_gtid as _mr84xp;
    // mysql84_gtid (single) → + __xp_mrbr_gtid
    use super::mysql_single_gtid::mysql84__xp_mrbr_gtid as _sg84xp;

    // ── Verify NO __xp_ expansion for nogtid (nogtid is terminal) ──
    // mysql_modern_nogtid should only contain mysql80_nogtid and mysql84_nogtid.
    // (Cannot verify absence at compile time, but the explicit imports above
    // confirm that the expansion logic doesn't bleed across modules.)

    // ── Verify NO __xp_ expansion for mrbr_gtid (fully specified) ──
    // mysql_single_mrbr_gtid should only contain mysql84_mrbr_gtid.
}
