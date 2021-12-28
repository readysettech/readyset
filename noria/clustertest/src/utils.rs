use mysql_async::prelude::{FromRow, Queryable, StatementLike};
use mysql_async::{Conn, Params};
use noria::get_metric;
use noria::metrics::client::MetricsClient;
use noria::metrics::{recorded, DumpedMetricValue};
use std::time::{Duration, Instant};
use tokio::time::sleep;

fn equal_rows<T>(a: &[T], b: &[T]) -> bool
where
    T: FromRow + std::cmp::PartialEq,
{
    let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
    matching == a.len() && matching == b.len()
}

/// Tracks the set of allowed values after a set of writes with eventual
/// consistency. This is intended to be used with [`query_until_expected`]
/// and [`query_until_expected_from_noria`] via [`as_expected_results`].
///
/// Writes indicated via the [`write`] function, will populate the
/// intermediate and expected results to meet eventual consistency
/// correctness accordingly.
pub struct EventuallyConsistentResults<T>
where
    T: Clone,
{
    intermediate: Vec<Vec<T>>,
    expected: Vec<T>,
}

#[allow(dead_code)]
impl<T> EventuallyConsistentResults<T>
where
    T: Clone,
{
    /// Create an empty set of results. This includes the empty result
    /// set as the initial expected value.
    pub fn new() -> Self {
        Self {
            intermediate: vec![vec![]],
            expected: vec![],
        }
    }

    /// The results to a query should either be empty or the expected
    /// value. All other values should be rejected.
    pub fn empty_or(expected: &[T]) -> Self {
        Self {
            intermediate: Vec::new(),
            expected: expected.to_vec(),
        }
    }

    /// Perform an eventually consistent write. This includes the write in the
    /// set of writes we expected to see in the intermediate set. If it is the
    /// last write in the system, it adds it as the expected write, the write
    /// the system should converge to.
    pub fn write(&mut self, res: &[T]) {
        self.intermediate.push(res.to_vec());
        self.expected = res.to_vec();
    }
}

/// Returns true when a prepare and execute returns the expected results,
/// [`EventuallyConsistentResults::expected`]. This function returns false if:
///   - The `timeout` is reached without the expected result being yielded.
///   - The query returns a value that is neither [`EventuallyConsistentResults::expected`]
///     nor in the [`EventuallyConsistentResults::intermediate`] list.
///   - A query to the connection returns an error.
///
/// This function should be used in place of sleeping and executing a query after
/// the write propagation delay. It can also be used to assert that ReadySet
/// returns eventually consistent results, while waiting for an expected result.
pub async fn query_until_expected<S, T, P>(
    conn: &mut Conn,
    query: S,
    params: P,
    results: &EventuallyConsistentResults<T>,
    timeout: Duration,
) -> bool
where
    S: StatementLike + Clone,
    P: Into<Params> + Clone + std::marker::Send,
    T: FromRow + std::cmp::PartialEq + std::marker::Send + std::fmt::Debug + Clone + 'static,
{
    let mut last: Option<Vec<T>> = None;
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            println!("query_until_expected timed out, last: {:?}", last);
            return false;
        }
        let remaining = std::cmp::min(Duration::from_secs(5), timeout - start.elapsed());
        let result =
            tokio::time::timeout(remaining, conn.exec(query.clone(), params.clone())).await;
        match result {
            Ok(Ok(r)) => {
                if equal_rows(&r, &results.expected) {
                    return true;
                }
                if !results
                    .intermediate
                    .iter()
                    .any(|intermediate| equal_rows(&r, intermediate))
                {
                    println!("Query results did not match accepted intermediate results. Results: {:?}, Accepted: {:?}", r, results.intermediate);
                    return false;
                }
                last = Some(r.clone());
            }
            Ok(Err(e)) => {
                println!("Returned an error when querying for results, {:?}", e);
                return false;
            }
            Err(_) => {
                println!("Timed out when querying conn.");
            }
        }

        sleep(Duration::from_millis(10)).await;
    }
}

async fn get_num_view_queries(metrics: &mut MetricsClient) -> u32 {
    let metrics_dumps = metrics.get_metrics().await.unwrap();
    metrics_dumps
        .iter()
        .map(
            |d| match get_metric!(d.metrics, recorded::SERVER_VIEW_QUERY_RESULT) {
                Some(DumpedMetricValue::Counter(n)) => n as u32,
                _ => 0,
            },
        )
        .sum()
}

/// Like [`query_until_expected`], except requires that the expected result was
/// queried from Noria. The intermediate results, and the expected result may be
/// returned via fallback without failing.
pub async fn query_until_expected_from_noria<S, T, P>(
    conn: &mut Conn,
    metrics: &mut MetricsClient,
    query: S,
    params: P,
    results: &EventuallyConsistentResults<T>,
    timeout: Duration,
) -> bool
where
    S: StatementLike + Clone,
    P: Into<Params> + Clone + std::marker::Send,
    T: FromRow + std::cmp::PartialEq + std::marker::Send + std::fmt::Debug + Clone + 'static,
{
    let mut last: Option<Vec<T>> = None;
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            println!("query_until_expected timed out, last: {:?}", last);
            return false;
        }
        let remaining = std::cmp::min(Duration::from_secs(5), timeout - start.elapsed());

        let num_view_queries_before = get_num_view_queries(metrics).await;
        let result =
            tokio::time::timeout(remaining, conn.exec(query.clone(), params.clone())).await;
        match result {
            Ok(Ok(r)) => {
                let noria_queried = get_num_view_queries(metrics).await > num_view_queries_before;
                let has_expected_result = equal_rows(&r, &results.expected);
                if has_expected_result && noria_queried {
                    return true;
                }
                if !has_expected_result
                    && !results
                        .intermediate
                        .iter()
                        .any(|intermediate| equal_rows(&r, intermediate))
                {
                    println!("Query results did not match accepted intermediate results or the results did not come from Noria.");
                    println!(
                        "Results: {:?}, Accepted: {:?}, Expected: {:?}",
                        r, results.intermediate, results.expected
                    );
                    return false;
                }
                last = Some(r.clone());
            }
            Ok(Err(e)) => {
                println!("Returned an error when querying for results, {:?}", e);
                return false;
            }
            Err(_) => {
                println!("Timed out when querying conn.");
            }
        }

        sleep(Duration::from_millis(10)).await;
    }
}
