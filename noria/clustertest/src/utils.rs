use mysql_async::prelude::{FromRow, Queryable, StatementLike};
use mysql_async::{Conn, Params};
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
    S: StatementLike + Clone,
    P: Into<Params> + Clone + std::marker::Send,
    T: FromRow + std::cmp::PartialEq + std::marker::Send + 'static,
{
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        let remaining = timeout - start.elapsed();
        let result =
            tokio::time::timeout(remaining, conn.exec(query.clone(), params.clone())).await;

        if let Ok(Ok(r)) = result {
            if equal_rows(&r, expected) {
                return true;
            }
        }

        sleep(Duration::from_millis(10)).await;
    }
}
