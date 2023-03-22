use std::env;

use readyset_data::DfValue;
use tokio_postgres::{Client, NoTls};

use self::common::parse_lower_eval;

mod common;

fn config() -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::new();
    config
        .host(env::var("PGHOST").as_deref().unwrap_or("localhost"))
        .port(
            env::var("PGPORT")
                .unwrap_or_else(|_| "5432".into())
                .parse()
                .unwrap(),
        )
        .user(env::var("PGUSER").as_deref().unwrap_or("postgres"))
        .password(env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into()));
    config
}
async fn postgres_eval(expr: &str, client: &Client) -> DfValue {
    client
        .query_one(&format!("SELECT {expr};"), &[])
        .await
        .unwrap()
        .get(0)
}

#[track_caller]
async fn compare_eval(expr: &str, client: &Client) {
    let our_result = parse_lower_eval(
        expr,
        nom_sql::Dialect::PostgreSQL,
        dataflow_expression::Dialect::DEFAULT_POSTGRESQL,
    );
    let pg_result = postgres_eval(expr, client).await;
    assert_eq!(
        our_result, pg_result,
        "mismatched results for {expr} (left: us, right: postgres)"
    );
}

#[tokio::test]
async fn example_exprs_eval_same_as_postgres() {
    let (client, conn) = config().connect(NoTls).await.unwrap();
    tokio::spawn(conn);

    client
        .simple_query("DROP TYPE IF EXISTS abc;")
        .await
        .unwrap();
    client
        .simple_query("CREATE TYPE abc AS ENUM ('a', 'b', 'c')")
        .await
        .unwrap();

    client
        .simple_query("DROP TYPE IF EXISTS cba;")
        .await
        .unwrap();
    client
        .simple_query("CREATE TYPE cba AS ENUM ('c', 'b', 'a')")
        .await
        .unwrap();

    for expr in [
        "1 != 2",
        "1 != 1",
        "4 + 5",
        "5 > 4",
        "'a' like 'A'",
        "'a' ilike 'A'",
        "'a' not like 'a'",
        "'a' not ilike 'b'",
        "'a' ilike all ('{a,A}')",
        "'a' ilike all ('{a,A,b}')",
        "'a'::abc = 'a'",
        "'a'::abc < all('{b,c}')",
        "'c'::cba < all('{{a,b,a},{b,a,b}}')",
        "1 != all('{2,3}')",
        "1 != all('{1,2,3}')",
        "1 != any('{2,3}')",
        "1 != any('{1,1,1}')",
        "null = null",
        "null != null",
        "1 = null",
        "null != 1",
        "substring('abcdef' for 3)",
    ] {
        compare_eval(expr, &client).await;
    }
}
