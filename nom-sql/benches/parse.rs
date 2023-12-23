use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use nom_sql::Dialect;

fn selects(c: &mut Criterion) {
    let run_benchmark = |b: &mut Bencher, query: &str| {
        b.iter(|| black_box(nom_sql::parse_query(Dialect::MySQL, query).unwrap()));
    };

    c.benchmark_group("single_select_statements")
        .bench_with_input("plain", "select * from t where id = ?", run_benchmark)
        .bench_with_input(
            "namespaced",
            "select t.* from t where t.id = ?",
            run_benchmark,
        )
        .bench_with_input(
            "compound_union",
            "SELECT employee_id, first_name FROM employees WHERE department_id = ?
            UNION
            SELECT employee_id, first_name FROM employees WHERE department_id = ?",
            run_benchmark,
        );
}

fn transactions(c: &mut Criterion) {
    let run_benchmark = |b: &mut Bencher, query: &str| {
        b.iter(|| nom_sql::parse_query(Dialect::MySQL, query).unwrap());
    };

    c.benchmark_group("transactions")
        .bench_with_input("start", "start transaction", run_benchmark)
        .bench_with_input("begin", "begin work", run_benchmark)
        .bench_with_input("commit", "commit", run_benchmark)
        .bench_with_input("rollback", "rollback", run_benchmark);
}

criterion_group!(benches, selects, transactions);
criterion_main!(benches);
