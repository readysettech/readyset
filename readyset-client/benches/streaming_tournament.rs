use chrono::{FixedOffset, TimeDelta, TimeZone};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
// use dataflow_expression::{
//     PostLookup, PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates,
// };
use nom_sql::OrderType;
use pprof::criterion::{Output, PProfProfiler};
use rand::seq::SliceRandom;
use rand::Rng;
use readyset_client::results::RowComparator;
use readyset_data::DfValue;
use tournament_kway::Comparator;

const NUM_ROWS: usize = 128159;

// fn streaming_tournament(c: &mut Criterion) {
//     let mut group = c.benchmark_group("streaming tournament");
//     let mut rng = rand::thread_rng();
//     let base_time = FixedOffset::west_opt(0)
//         .unwrap()
//         .with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
//         .single()
//         .unwrap();
//     let num_rows = SmallVec::from_vec(
//         (1..=NUM_ROWS)
//             .map(|i| {
//                 let ts_hour = DfValue::TimestampTz(
//                     base_time
//                         .checked_add_signed(TimeDelta::minutes(i as i64))
//                         .unwrap()
//                         .into(),
//                 );
//                 let service: DfValue = format!("service_{}", (i / 1440) % 10 + 1).into();
//                 let metric_name: DfValue = format!("metricName_{}", (i / 720) % 20 + 1).into();
//                 let tid: DfValue = "tid".into();
//                 let phase_type: DfValue = "PreSampling".into();
//                 let hll_estimate: DfValue = rng.gen_range(1..=1000).into();
//                 vec![
//                     black_box(ts_hour),
//                     service,
//                     metric_name,
//                     tid,
//                     phase_type,
//                     hll_estimate,
//                 ]
//                 .into_boxed_slice()
//             })
//             .collect::<Vec<Box<[DfValue]>>>(),
//     );
//     let result_sets = SmallVec::from_vec(
//         (0..NUM_ITERS)
//             .map(|_| triomphe::Arc::new(num_rows.clone()))
//             .collect::<Vec<_>>(),
//     );
//     let post_lookup = PostLookup {
//         default_row: None,
//         limit: None,
//         order_by: None,
//         // order_by: Some(vec![(0, OrderType::OrderAscending)]),
//         returned_cols: Some(vec![0]),
//         aggregates: Some(PostLookupAggregates {
//             group_by: vec![0],
//             aggregates: vec![PostLookupAggregate {
//                 column: 0,
//                 function: PostLookupAggregateFunction::Max,
//             }],
//         }),
//     };
//
//     let mut iters = black_box(ResultIterator::new(
//         black_box(result_sets),
//         &post_lookup,
//         None,
//         None,
//         None,
//     ));
//
//     group.bench_function("advance with timestamp comparisons", |b| {
//         b.iter(|| {
//             iters.advance_filtered();
//         })
//     });
// }
//
fn cmp(c: &mut Criterion) {
    let mut group = c.benchmark_group("dfvalue cmp");
    let mut rng = rand::thread_rng();
    let base_time = FixedOffset::west_opt(0)
        .unwrap()
        .with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
        .single()
        .unwrap();
    let mut num_rows = (1..=10000000)
        .map(|i| {
            let ts_hour = DfValue::TimestampTz(
                base_time
                    .checked_add_signed(TimeDelta::minutes(i as i64))
                    .unwrap()
                    .into(),
            );
            let service: DfValue = format!("service_{}", (i / 1440) % 10 + 1).into();
            let metric_name: DfValue = format!("metricName_{}", (i / 720) % 20 + 1).into();
            let tid: DfValue = "tid".into();
            let phase_type: DfValue = "PreSampling".into();
            let hll_estimate: DfValue = rng.gen_range(1..=1000).into();
            vec![
                black_box(ts_hour),
                service,
                metric_name,
                tid,
                phase_type,
                hll_estimate,
            ]
            .into_boxed_slice()
        })
        .collect::<Vec<Box<[DfValue]>>>();

    num_rows.shuffle(&mut rng);

    let comparator = RowComparator {
        order_by: std::sync::Arc::new([(0, OrderType::OrderAscending)]),
    };

    group.bench_function("goooo", |b| {
        let mut i = 0;
        let mut j = 0;

        b.iter(|| {
            let _a = black_box(comparator.cmp(
                black_box(&num_rows[i % NUM_ROWS]),
                black_box(&num_rows[j % NUM_ROWS]),
            ));
            i += 1;
            j += 2;
        })
    });
}

fn flamegraphs_profiler() -> Criterion {
    return Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
}

criterion_group!(
    name = benches;
    config = flamegraphs_profiler();
    targets = cmp
);
criterion_main!(benches);
