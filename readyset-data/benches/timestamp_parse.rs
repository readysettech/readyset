use std::str::FromStr;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};

enum ChronoThingy {
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<FixedOffset>),
}

fn reparse(input: &str) -> ChronoThingy {
    if let Ok(dt) = DateTime::<FixedOffset>::parse_from_str(input, "%Y-%m-%d %H:%M:%S%.f%#z") {
        ChronoThingy::TimestampTz(dt)
    } else if let Ok(dt) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S%.f") {
        ChronoThingy::Timestamp(dt)
    } else {
        ChronoThingy::Date(NaiveDate::parse_from_str(input, "%Y-%m-%d").unwrap())
    }
}

fn timestamp_reparse(c: &mut Criterion) {
    let run_benchmark = |b: &mut Bencher, ts: &str| b.iter(|| black_box(reparse(ts)));

    c.benchmark_group("timestamp_reparse")
        .bench_with_input("date", "2024-02-13", run_benchmark)
        .bench_with_input("timestamp", "2024-02-13 18:35:23.176", run_benchmark)
        .bench_with_input("timestamptz", "2024-02-13 18:35:23.176-0800", run_benchmark);
}

fn parse(input: &str) -> ChronoThingy {
    let (date, remainder) = NaiveDate::parse_and_remainder(input, "%Y-%m-%d").unwrap();
    if remainder.is_empty() {
        return ChronoThingy::Date(date);
    }

    let (time, remainder) = NaiveTime::parse_and_remainder(remainder, "%H:%M:%S%.f").unwrap();
    let timestamp = date.and_time(time);
    if remainder.is_empty() {
        return ChronoThingy::Timestamp(timestamp);
    }

    let remainder = remainder.trim_start();
    let offset = FixedOffset::from_str(remainder).unwrap();
    ChronoThingy::TimestampTz(offset.from_local_datetime(&timestamp).single().unwrap())
}

// TODO: add comement
fn timestamp_incremental_parse(c: &mut Criterion) {
    let run_benchmark = |b: &mut Bencher, ts: &str| b.iter(|| black_box(parse(ts)));

    c.benchmark_group("timestamp_incremental_parse")
        .bench_with_input("date", "2024-02-13", run_benchmark)
        .bench_with_input("timestamp", "2024-02-13 18:35:23.176", run_benchmark)
        .bench_with_input("timestamptz", "2024-02-13 18:35:23.176-0800", run_benchmark);
}

criterion_group!(
    benches,
    // timestamp_text_parse_offsets,
    // timestamp_text_parse_reparse,
    timestamp_incremental_parse,
    timestamp_reparse,
);
criterion_main!(benches);
