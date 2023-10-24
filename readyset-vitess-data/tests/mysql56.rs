use std::collections::HashMap;
use std::str::FromStr;

use readyset_vitess_data::mysql56::{Mysql56Gtid, Mysql56GtidSet};
use uuid::Uuid;

#[test]
fn parse_mysql56_gtid() {
    let input = "00010203-0405-0607-0809-0A0B0C0D0E0F:56789";
    let want = Mysql56Gtid {
        server: "00010203-0405-0607-0809-0A0B0C0D0E0F".try_into().unwrap(),
        sequence: 56789,
    };
    let parsed = Mysql56Gtid::try_from(input);
    assert!(parsed.is_ok());
    assert_eq!(parsed.unwrap(), want);
}

#[test]
fn parse_invalid_mysql56_gtid() {
    let broken = vec![
        "",
        "00010203-0405-0607-0809-0A0B0C0D0E0F",
        "00010203-0405-0607-0809-0A0B0C0D0E0F:1-5",
        "00010203-0405-0607-0809-0A0B0C0D0E0F:1:2",
        "00010203-0405-0607-0809-0A0B0C0D0E0X:1",
    ];

    for input in broken {
        let parsed = Mysql56Gtid::try_from(input);
        assert!(parsed.is_err());
    }
}

#[test]
fn format_mysql56_gtid() {
    let input = Mysql56Gtid {
        server: Uuid::from_str("00010203-0405-0607-0809-0A0B0C0D0E0F").unwrap(),
        sequence: 56789,
    };
    let want = "00010203-0405-0607-0809-0a0b0c0d0e0f:56789";
    let formatted = input.to_string();
    assert_eq!(formatted, want);
}

// Creates a HashMap of Uuid -> Vec<Interval>
macro_rules! intervals {
    () => {
        HashMap::new()
    };
    ($( $key:expr => [$($range:expr),*] ),*) => {{
        let mut map = HashMap::new();
        $(
            let vec: Vec<_> = vec![$($range),*].into_iter().map(|r| r.into()).collect();
            map.insert(Uuid::from_str($key).unwrap(), vec);
        )*
        map
    }};
}

#[test]
fn parse_mysql56_gtid_set() {
    let test_cases = vec![
        // Empty
        ("", intervals!()),
        // Simple case
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5]
            ),
        ),
        // Capital hex chars
        (
            "00010203-0405-0607-0809-0A0B0C0D0E0F:1-5",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5]
            ),
        ),
        // Interval with same start and end
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:12",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [12..12]
            ),
        ),
        // Multiple intervals
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5, 10..20]
            ),
        ),
        // Multiple intervals, out of order
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:10-20:1-5",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5, 10..20]
            ),
        ),
        // Intervals with end < start are discarded by MySQL 5.6
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:8-7:10-20",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5, 10..20]
            ),
        ),
        // Same repeating SIDs
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5,00010203-0405-0607-0809-0a0b0c0d0e0f:10-20",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5, 10..20]
            ),
        ),
        // Same repeating SIDs, backwards order
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:10-20,00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5, 10..20]
            ),
        ),
        // Multiple SIDs
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5, 10..20],
                "00010203-0405-0607-0809-0a0b0c0d0eff" => [1..5, 50..50]
            ),
        ),
        // Multiple SIDs with space around the comma
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20, 00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5, 10..20],
                "00010203-0405-0607-0809-0a0b0c0d0eff" => [1..5, 50..50]
            ),
        ),
    ];

    for (input, want) in test_cases {
        let parsed = Mysql56GtidSet::try_from(input);
        if let Ok(parsed) = parsed {
            assert_eq!(parsed.intervals, want);
        } else {
            panic!(
                "Failed to parse: {}. Error: {}",
                input,
                parsed.err().unwrap()
            );
        }
    }
}

#[test]
fn parse_invalid_mysql56_gtid_set() {
    let broken = vec![
        // No intervals
        "00010203-0405-0607-0809-0a0b0c0d0e0f",
        // Invalid SID
        "00010203-0405-060X-0809-0a0b0c0d0e0f:1-5",
        // Invalid intervals
        "00010203-0405-0607-0809-0a0b0c0d0e0f:0-5",
        "00010203-0405-0607-0809-0a0b0c0d0e0f:-5-10",
        "00010203-0405-0607-0809-0a0b0c0d0e0f:-5",
        "00010203-0405-0607-0809-0a0b0c0d0e0f:1-2-3",
        "00010203-0405-0607-0809-0a0b0c0d0e0f:1-",
    ];

    for input in broken {
        let parsed = Mysql56GtidSet::try_from(input);
        if parsed.is_ok() {
            panic!("Parsed invalid input: {}", input);
        }
        assert!(parsed.is_err());
    }
}

#[test]
fn format_mysql56_gtid_set() {
    let test_cases = vec![
        // Empty
        ("", HashMap::new()),
        // Simple case
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5]
            ),
        ),
        // Multiple intervals
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5, 10..20]
            ),
        ),
        // Multiple SIDs
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50",
            intervals!(
                "00010203-0405-0607-0809-0a0b0c0d0e0f" => [1..5, 10..20],
                "00010203-0405-0607-0809-0a0b0c0d0eff" => [1..5, 50..50]
            ),
        ),
    ];

    for (want, intervals) in test_cases {
        let gtid_set = Mysql56GtidSet { intervals };
        let formatted = gtid_set.to_string();
        assert_eq!(formatted, want);
    }
}

#[test]
fn contains_gtid() {
    let test_cases = vec![
        // Empty
        ("", "00010203-0405-0607-0809-0a0b0c0d0e0f:1", false),
        // Simple case
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1",
            true,
        ),
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:2",
            true,
        ),
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:5",
            true,
        ),
        // Different SID
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            "00010203-0405-0607-0809-0a0b0c0d0eff:1",
            false,
        ),
        // Out of range
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:10",
            false,
        ),
        // Different SID + out of range
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            "00010203-0405-0607-0809-0a0b0c0d0eff:10",
            false,
        ),
        // Multiple intervals
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1",
            true,
        ),
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:11",
            true,
        ),
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:6",
            false,
        ),
    ];

    for (gtid_set, gtid, want) in test_cases {
        let gtid_set = Mysql56GtidSet::try_from(gtid_set).unwrap();
        let gtid = Mysql56Gtid::try_from(gtid).unwrap();
        let got = gtid_set.contains_gtid(&gtid);
        if got != want {
            panic!(
                "contains_gtid({}, {}) = {}, want {}",
                gtid_set, gtid, got, want
            );
        }
    }
}

#[test]
fn contains_gtid_set_positive() {
    let sid1 = "00010203-0405-0607-0809-0a0b0c0d0e0f";
    let sid2 = "00010203-0405-0607-0809-0a0b0c0d0eff";

    // The set to test against.
    let set = Mysql56GtidSet {
        intervals: intervals!(
            sid1 => [20..30, 35..40],
            sid2 => [1..5, 50..50, 60..70]
        ),
    };

    // Test cases that should return contains=true
    let test_cases = vec![
        // The set should contain itself
        set.clone(),
        // Every set contains the empty set
        Mysql56GtidSet {
            intervals: intervals!(),
        },
        // Simple case
        Mysql56GtidSet {
            intervals: intervals!(sid1 => [25..30]),
        },
        // Multiple intervals
        Mysql56GtidSet {
            intervals: intervals!(sid2 => [1..2, 4..5, 60..70]),
        },
        // Multiple SIDs
        Mysql56GtidSet {
            intervals: intervals!(
                sid1 => [25..30, 35..37],
                sid2 => [1..5]
            ),
        },
    ];

    for test_case in test_cases {
        let got = set.contains(&test_case);
        if !got {
            panic!(
                "
                 contains_gtid_set(
                    {},
                    {}
                 ) = {}, want true",
                set, test_case, got
            );
        }
    }
}

#[test]
fn contains_gtid_set_negative() {
    let sid1 = "00010203-0405-0607-0809-0a0b0c0d0e0f";
    let sid2 = "00010203-0405-0607-0809-0a0b0c0d0eff";

    // The set to test against.
    let set = Mysql56GtidSet {
        intervals: intervals!(
            sid1 => [20..30, 35..40],
            sid2 => [1..5, 50..50, 60..70]
        ),
    };

    // Test cases that should return contains=false
    let test_cases = vec![
        // Simple cases
        Mysql56GtidSet {
            intervals: intervals!(sid1 => [1..5]),
        },
        Mysql56GtidSet {
            intervals: intervals!(sid1 => [10..19]),
        },
        // Overlapping intervals
        Mysql56GtidSet {
            intervals: intervals!(sid1 => [10..20]),
        },
        Mysql56GtidSet {
            intervals: intervals!(sid1 => [10..25]),
        },
        Mysql56GtidSet {
            intervals: intervals!(sid1 => [25..31]),
        },
        Mysql56GtidSet {
            intervals: intervals!(sid1 => [30..31]),
        },
        // Multiple intervals
        Mysql56GtidSet {
            intervals: intervals!(sid1 => [20..30, 34..34]),
        },
        // Multiple SIDs
        Mysql56GtidSet {
            intervals: intervals!(
                sid1 => [20..30, 36..36],
                sid2 => [3..5, 55..60]
            ),
        },
    ];

    for test_case in test_cases {
        let got = set.contains(&test_case);
        if got {
            panic!(
                "
                 contains_gtid_set(
                    {},
                    {}
                 ) = {}, want false",
                set, test_case, got
            );
        }
    }
}

#[test]
fn mysql56_gtid_set_equal() {
    let test_cases = vec![
        // Empty
        ("", "", true),
        // Simple case
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
            true,
        ),
        // Multiple intervals
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
            true,
        ),
        // Multiple SIDs
        (
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50",
            true,
        ),
        // Different order
        (
            "00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50,00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
            "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50",
            true,
        ),
        // Different SIDs
        (
            "00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50,00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
            "00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:10-20",
            false,
        ),
    ];

    for (gtid_set1, gtid_set2, want) in test_cases {
        let gtid_set1 = Mysql56GtidSet::try_from(gtid_set1).unwrap();
        let gtid_set2 = Mysql56GtidSet::try_from(gtid_set2).unwrap();
        let got = gtid_set1 == gtid_set2;
        if got != want {
            panic!(
                "
                 mysql56_gtid_set_equal(
                    {},
                    {}
                 ) = {}, want {}",
                gtid_set1, gtid_set2, got, want
            );
        }
    }
}
