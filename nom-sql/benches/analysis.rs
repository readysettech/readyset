use std::iter;

use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use nom_sql::{parse_expr, Dialect, Expr};
use pprof::criterion::{Output, PProfProfiler};

fn recursive_subexpressions(c: &mut Criterion) {
    let run_benchmark = |b: &mut Bencher, src: &str| {
        let expr = parse_expr(Dialect::MySQL, src).unwrap();
        b.iter(|| {
            black_box(
                iter::once(&expr)
                    .chain(expr.recursive_subexpressions())
                    .any(|subexpr| matches!(subexpr, Expr::BinaryOp { .. })),
            )
        })
    };

    c.benchmark_group("recursive_subexpressions")
        .bench_with_input("trivial", "1", run_benchmark)
        .bench_with_input("simple", "x = 4", run_benchmark)
        .bench_with_input(
            "moderate",
            "x = 1 \
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

criterion_group!(benches, recursive_subexpressions);
criterion_main!(benches);
