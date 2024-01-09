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

#[derive(Debug)]
enum PgQueryAction {
    Parse,
    Fingerprint,
}

fn selects_pg_query_by_fn(c: &mut Criterion, action: PgQueryAction) {
    let run_benchmark = match action {
        PgQueryAction::Parse => |b: &mut Bencher, query: &str| {
            b.iter(|| black_box(pg_query::parse(query).unwrap()));
        },
        PgQueryAction::Fingerprint => |b: &mut Bencher, query: &str| {
            b.iter(|| black_box(pg_query::fingerprint(query).unwrap()));
        },
    };

    c.benchmark_group(format!("single_select_statements-pg_query-{:?}", action))
        .bench_with_input("plain", "select * from t where id = 1", run_benchmark)
        .bench_with_input(
            "namespaced",
            "select t.* from t where t.id = 1",
            run_benchmark,
        )
        .bench_with_input(
            "compound_union",
            "SELECT employee_id, first_name FROM employees WHERE department_id = 42
            UNION
            SELECT employee_id, first_name FROM employees WHERE department_id = 42",
            run_benchmark,
        );
}

fn selects_pg_query(c: &mut Criterion) {
    selects_pg_query_by_fn(c, PgQueryAction::Parse);
    selects_pg_query_by_fn(c, PgQueryAction::Fingerprint);
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

fn transactions_pg_query_by_fn(c: &mut Criterion, action: PgQueryAction) {
    let run_benchmark = match action {
        PgQueryAction::Parse => |b: &mut Bencher, query: &str| {
            b.iter(|| black_box(pg_query::parse(query).unwrap()));
        },
        PgQueryAction::Fingerprint => |b: &mut Bencher, query: &str| {
            b.iter(|| black_box(pg_query::fingerprint(query).unwrap()));
        },
    };

    c.benchmark_group(format!("transactions-pq_query-{:?}", action))
        .bench_with_input("start", "start transaction", run_benchmark)
        .bench_with_input("begin", "begin work", run_benchmark)
        .bench_with_input("commit", "commit", run_benchmark)
        .bench_with_input("rollback", "rollback", run_benchmark);
}

fn transactions_pg_query(c: &mut Criterion) {
    transactions_pg_query_by_fn(c, PgQueryAction::Parse);
    transactions_pg_query_by_fn(c, PgQueryAction::Fingerprint);
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
    selects_pg_query,
    transactions_nom_sql,
    transactions_sqlparser,
    transactions_pg_query,
);
criterion_main!(benches);
