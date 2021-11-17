use mysql::prelude::{AsStatement, FromRow, Queryable};
use mysql::{Conn, Params};
use std::time::{Duration, Instant};
use tokio::time::sleep;

fn equal_rows<T>(a: &[T], b: &[T]) -> bool
where
    T: FromRow + std::cmp::PartialEq,
{
    let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
    matching == a.len() && matching == b.len()
}

pub async fn query_until_expected<S, T, P>(
    conn: &mut Conn,
    query: S,
    params: P,
    expected: &[T],
    timeout: Duration,
) -> bool
where
    S: AsStatement + Clone,
    P: Into<Params> + Clone,
    T: FromRow + std::cmp::PartialEq,
{
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        let result: Vec<T> = conn.exec(query.clone(), params.clone()).unwrap();
        if equal_rows(&result, expected) {
            return true;
        }

        sleep(Duration::from_millis(10)).await;
    }
}
