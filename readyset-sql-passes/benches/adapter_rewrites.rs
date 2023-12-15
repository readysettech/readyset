use criterion::{black_box, criterion_group, criterion_main, BatchSize, Bencher, Criterion};
use nom_sql::{parse_select_statement, Dialect};
use readyset_sql_passes::adapter_rewrites;

fn auto_parametrize_query(c: &mut Criterion) {
    let run_benchmark = |b: &mut Bencher, src: &str| {
        let q = parse_select_statement(Dialect::MySQL, src).unwrap();
        b.iter_batched(
            || q.clone(),
            |mut q| {
                adapter_rewrites::auto_parametrize_query(&mut q);
                black_box(q)
            },
            BatchSize::SmallInput,
        )
    };

    c.benchmark_group("auto_parametrize_query")
        .bench_with_input("trivial", "SELECT * FROM t", run_benchmark)
        .bench_with_input(
            "simple",
            "SELECT customer_id, amount, account_name FROM payment WHERE customer_id = 1",
            run_benchmark,
        )
        .bench_with_input(
            "moderate",
            "SELECT * FROM t \
                 WHERE x = 1 \
                 AND ( \
                    y = 4 \
                    AND ( \
                      z = 5 \
                      AND q = 7 \
                      AND (\
                        x = 80 \
                        AND w = ? \
                        AND xx = 43 \
                        AND yz IN (x + 4 - 8))))",
            run_benchmark,
        );
}

criterion_group!(benches, auto_parametrize_query);
criterion_main!(benches);
