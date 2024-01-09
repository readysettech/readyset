use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use nom_sql::Dialect;
use pprof::criterion::{Output, PProfProfiler};

fn selects_nom_sql_by_db(c: &mut Criterion, dialect: Dialect) {
    let run_benchmark = |b: &mut Bencher, query: &str| {
        b.iter(|| black_box(nom_sql::parse_query(dialect, query).unwrap()));
    };

    c.benchmark_group(format!("single_select_statements-nom_sql-{:?}", dialect))
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

fn selects_nom_sql(c: &mut Criterion) {
    Dialect::ALL
        .iter()
        .for_each(|d| selects_nom_sql_by_db(c, *d));
}

fn selects_sqlparser_by_db<D>(c: &mut Criterion, dialect: D)
where
    D: sqlparser::dialect::Dialect,
{
    let run_benchmark = |b: &mut Bencher, query: &str| {
        b.iter(|| black_box(sqlparser::parser::Parser::parse_sql(&dialect, query).unwrap()));
    };

    c.benchmark_group(format!("single_select_statements-sqlparser-{:?}", &dialect))
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

fn selects_sqlparser(c: &mut Criterion) {
    selects_sqlparser_by_db(c, sqlparser::dialect::MySqlDialect {});
    selects_sqlparser_by_db(c, sqlparser::dialect::PostgreSqlDialect {});
}

fn transactions_nom_sql_by_db(c: &mut Criterion, dialect: Dialect) {
    let run_benchmark = |b: &mut Bencher, query: &str| {
        b.iter(|| black_box(nom_sql::parse_query(dialect, query).unwrap()));
    };

    c.benchmark_group(format!("transactions-nom_sql-{:?}", dialect))
        .bench_with_input("start", "start transaction", run_benchmark)
        .bench_with_input("begin", "begin work", run_benchmark)
        .bench_with_input("commit", "commit", run_benchmark)
        .bench_with_input("rollback", "rollback", run_benchmark);
}

fn transactions_nom_sql(c: &mut Criterion) {
    Dialect::ALL
        .iter()
        .for_each(|d| transactions_nom_sql_by_db(c, *d));
}

fn transactions_sqlparser_by_db<D>(c: &mut Criterion, dialect: D)
where
    D: sqlparser::dialect::Dialect,
{
    let run_benchmark = |b: &mut Bencher, query: &str| {
        b.iter(|| black_box(sqlparser::parser::Parser::parse_sql(&dialect, query).unwrap()));
    };

    c.benchmark_group(format!("transactions-sqlparser-{:?}", dialect))
        .bench_with_input("start", "start transaction", run_benchmark)
        .bench_with_input("begin", "begin work", run_benchmark)
        .bench_with_input("commit", "commit", run_benchmark)
        .bench_with_input("rollback", "rollback", run_benchmark);
}

fn transactions_sqlparser(c: &mut Criterion) {
    transactions_sqlparser_by_db(c, sqlparser::dialect::MySqlDialect {});
    transactions_sqlparser_by_db(c, sqlparser::dialect::PostgreSqlDialect {});
}

// Instead of running with the Standard Criterion timer profile, plug-in the
// flamegraph output of the pprof-rs profiler.[0]
//
// [0] https://bheisler.github.io/criterion.rs/book/user_guide/profiling.html
fn flamegraphs_profiler() -> Criterion {
    return Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
}

criterion_group!(
    name = benches;
    config = flamegraphs_profiler();
    targets = selects_nom_sql,
    selects_sqlparser,
    transactions_nom_sql,
    transactions_sqlparser,
);
criterion_main!(benches);
