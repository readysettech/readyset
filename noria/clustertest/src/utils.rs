use std::time::{Duration, Instant};

use mysql_async::prelude::{FromRow, Queryable, StatementLike};
use mysql_async::{Conn, Params};
use noria::get_metric;
use noria::metrics::client::MetricsClient;
use noria::metrics::{recorded, DumpedMetricValue};
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
/// and [`query_until_expected_from_noria`].
///
/// Writes indicated via the [`write`] function, will populate the
/// intermediate and expected results to meet eventual consistency
/// correctness accordingly. This is done by tracking the set of intermediate
/// values that a read *may* return, and the final value that a read *must*
/// return. If a read returns a value not in the intermediate set or match
/// the final value, we have violated correctness and `query_until_expected`
/// will fail.
///
/// Example: Imagine that we want to check that executing the query
/// `SELECT * FROM t1 where c1 = ?`returns eventually consistent results under
/// some conditions, while we are performing writes.
///
/// Say we update the database twice:
/// `INSERT INTO t1 VALUES (4,5);`
/// `UPDATE t1 SET c2 = 4 where c1 = 4`
/// `UPDATE t1 SET c2 = 3 where c1 = 4`
///
/// For this set of writes we would expect an eventually consistent database
/// to return the following for `SELECT * FROM t1 where c1 = ?`:
/// 1. (), an empty set before any value with c1 = 4 is inserted.
/// 2. (4,5)
/// 3. (4,4)
/// 4. (4,3)
///
/// From a reader's perspective we may only see a subset of these values, however,
/// we can *never* see a value that is not one of these four. This can be guaranteed
/// by using [`EventuallyConsistentResults`] by mirroring the set of writes to the
/// result set with `write`.
pub struct EventuallyConsistentResults<T>
where
    T: Clone,
{
    intermediate: Vec<Vec<T>>,
    expected: Vec<T>,
}

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

/// How to execute a query against a MySQL backend.
pub enum QueryExecution<S, P>
where
    S: StatementLike + Clone,
    P: Into<Params> + Clone + std::marker::Send,
{
    /// Use preparing and executing a query.
    PrepareExecute(S, P),
    /// Query via the query text.
    Query(&'static str),
}

/// Where the expected result should come from in `query_until_expected_inner`.
pub enum ResultSource<'a> {
    /// Results must be retrieved from Noria, verified via the use of the MetricsClient.
    FromNoria(&'a mut MetricsClient),
    /// Results can come from either Noria or MySQL.
    FromAnywhere,
}

pub async fn query_until_expected<S, T, P>(
    conn: &mut Conn,
    query: QueryExecution<S, P>,
    results: &EventuallyConsistentResults<T>,
    timeout: Duration,
) -> bool
where
    S: StatementLike + Clone,
    P: Into<Params> + Clone + std::marker::Send,
    T: FromRow + std::cmp::PartialEq + std::marker::Send + std::fmt::Debug + Clone + 'static,
{
    query_until_expected_inner(conn, query, ResultSource::FromAnywhere, results, timeout).await
}

/// Like [`query_until_expected`], except requires that the expected result was
/// queried from Noria. The intermediate results, and the expected result may be
/// returned via fallback without failing.
pub async fn query_until_expected_from_noria<S, T, P>(
    conn: &mut Conn,
    metrics: &mut MetricsClient,
    query: QueryExecution<S, P>,
    results: &EventuallyConsistentResults<T>,
    timeout: Duration,
) -> bool
where
    S: StatementLike + Clone,
    P: Into<Params> + Clone + std::marker::Send,
    T: FromRow + std::cmp::PartialEq + std::marker::Send + std::fmt::Debug + Clone + 'static,
{
    query_until_expected_inner(
        conn,
        query,
        ResultSource::FromNoria(metrics),
        results,
        timeout,
    )
    .await
}

pub async fn query_until_expected_inner<S, T, P>(
    conn: &mut Conn,
    query: QueryExecution<S, P>,
    mut source: ResultSource<'_>,
    results: &EventuallyConsistentResults<T>,
    timeout: Duration,
) -> bool
where
    S: StatementLike + Clone,
    P: Into<Params> + Clone + std::marker::Send,
    T: FromRow + std::cmp::PartialEq + std::marker::Send + std::fmt::Debug + Clone + 'static,
{
    let mut last: Option<(Vec<T>, bool)> = None;
    let start = Instant::now();
    loop {
        // Check if we have timed out, if we have not execute the query against the connection
        // with up to as long as we have left in the timeout. We timeout any query after 5
        // seconds to quickly retry due to failures.
        if start.elapsed() > timeout {
            println!("query_until_expected timed out, last: {:?}", last);
            return false;
        }
        let remaining = std::cmp::min(Duration::from_secs(5), timeout - start.elapsed());
        let num_noria_queries_before = if let ResultSource::FromNoria(metrics) = &mut source {
            get_num_view_queries(metrics).await
        } else {
            0
        };

        let result = match &query {
            QueryExecution::PrepareExecute(query, params) => {
                tokio::time::timeout(remaining, conn.exec(query.clone(), params.clone())).await
            }
            QueryExecution::Query(q) => tokio::time::timeout(remaining, conn.query(q)).await,
        };

        //
        match result {
            Ok(Ok(r)) => {
                let correct_source = match &mut source {
                    ResultSource::FromAnywhere => true,
                    ResultSource::FromNoria(metrics) => {
                        get_num_view_queries(metrics).await > num_noria_queries_before
                    }
                };
                let has_expected_result = equal_rows(&r, &results.expected);
                if has_expected_result && correct_source {
                    return true;
                }
                if !has_expected_result
                    && !results
                        .intermediate
                        .iter()
                        .any(|intermediate| equal_rows(&r, intermediate))
                {
                    println!("Query results did not match accepted intermediate results. Results: {:?}, Accepted: {:?}", r, results.intermediate);
                    return false;
                }
                last = Some((r.clone(), correct_source));
            }
            Ok(Err(e)) => {
                println!("Returned an error when querying for results, {:?}", e);
                return false;
            }
            Err(_) => {
                println!("Timed out when querying conn.");
            }
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn get_num_view_queries(metrics: &mut MetricsClient) -> u32 {
    match metrics.get_metrics().await {
        Ok(metrics) => metrics
            .iter()
            .map(
                |d| match get_metric!(d.metrics, recorded::SERVER_VIEW_QUERY_HIT) {
                    Some(DumpedMetricValue::Counter(n)) => n as u32,
                    _ => 0,
                } + match get_metric!(d.metrics, recorded::SERVER_VIEW_QUERY_MISS) {
                    Some(DumpedMetricValue::Counter(n)) => n as u32,
                    _ => 0,
                },
            )
            .sum(),
        // If we cannot reach the metrics client we return 0 view queries, this will always fail a
        // check that we executed the query against Noria.
        Err(_) => 0,
    }
}
