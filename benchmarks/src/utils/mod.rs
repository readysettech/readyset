use std::future::Future;
use std::num::ParseIntError;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use database_utils::{DatabaseURL, QueryableConnection};
use nom_sql::DialectDisplay;
use readyset_data::DfValue;
use tracing::info;

pub mod backend;
pub mod generate;
pub mod multi_thread;
pub mod path;
pub mod prometheus;
pub mod query;
pub mod spec;

pub fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1000.
}

pub fn seconds_as_str_to_duration(input: &str) -> std::result::Result<Duration, ParseIntError> {
    Ok(Duration::from_secs(u64::from_str(input)?))
}

pub async fn run_for(
    func: impl Future<Output = Result<()>>,
    duration: Option<Duration>,
) -> Result<()> {
    if let Some(duration) = duration {
        match tokio::time::timeout(duration, func).await {
            Ok(r) => r,
            Err(_) => Ok(()), //Timeout was hit without failing prior.
        }
    } else {
        func.await
    }
}

#[macro_export]
macro_rules! make_key {
    ($name: expr, $unit: ident) => {
        ::metrics::Key::from_name(format!(
            "benchmark.{}_{}",
            $name,
            ::metrics::Unit::$unit.as_str(),
        ))
    };
    ($name: expr, $unit: ident $(, $label_key: expr => $label_value: expr)*) => {{
        let labels = vec![$(($label_key, $label_value),)*]
            .iter()
            .map(::metrics::Label::from)
            .collect::<Vec<_>>();
        ::metrics::Key::from_parts(
            format!(
                "benchmark.{}_{}",
                $name,
                ::metrics::Unit::$unit.as_str(),
            ),
            labels
        )
    }};
}

/// Wait for replication to finish by lazy looping over "SHOW READYSET STATUS"
pub async fn readyset_ready(target: &str) -> anyhow::Result<()> {
    info!("Waiting for the target database to be ready...");
    // First attempt to connect to the readyset adapter at all
    let mut conn = loop {
        match DatabaseURL::from_str(target)?.connect(None).await {
            Ok(conn) => break conn,
            _ => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    };

    // Then query status until snapshot is completed
    let q = nom_sql::ShowStatement::ReadySetStatus;
    loop {
        // We have to use simple query here because ReadySet does not support preparing `SHOW`
        // queries
        let res = conn
            .simple_query(q.display(readyset_sql::Dialect::MySQL).to_string())
            .await;

        if let Ok(data) = res {
            let snapshot_status = Vec::<Vec<DfValue>>::try_from(data)
                .unwrap()
                .into_iter()
                .find(|r| r[0] == "Snapshot Status".into());
            let snapshot_status: String = if let Some(s) = snapshot_status {
                s[1].clone().try_into().unwrap()
            } else {
                continue;
            };

            if snapshot_status == "Completed" {
                info!("Database ready!");
                return Ok(());
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[macro_export]
macro_rules! benchmark_gauge {
    ($name: expr, $unit: ident, $description: expr, $value: expr $(, $label_key: expr => $label_value: expr)*) => {
        metrics::with_recorder(|recorder| {
            let key = $crate::make_key!($name, $unit $(, $label_key => $label_value)*);
            let meta = metrics::Metadata::new("", metrics::Level::INFO, None);
            let g = recorder.register_gauge(&key, &meta);
            recorder.describe_gauge(key.into_parts().0, Some(::metrics::Unit::$unit), $description);
            g.set($value);
        })
    };
}

#[macro_export]
macro_rules! benchmark_counter {
    ($name: expr, $unit: ident, $description: expr, $value: expr $(, $label_key: expr => $label_value: expr)*) => {
        metrics::with_recorder(|recorder| {
            let key = $crate::make_key!($name, $unit $(, $label_key => $label_value)*);
            let meta = metrics::Metadata::new("", metrics::Level::INFO, None);
            let c = recorder.register_counter(&key, &meta);
            recorder.describe_counter(key.into_parts().0, Some(::metrics::Unit::$unit), $description);
            c.increment($value);
        })
    };
    ($name: expr, $unit: ident, $description: expr) => {
        benchmark_counter!($name, $unit, $description, 0)
    };
}

#[macro_export]
macro_rules! benchmark_increment_counter {
    ($name: expr, $unit: ident, $value: expr $(, $label_key: expr => $label_value: expr)*) => {
        metrics::with_recorder(|recorder| {
            let key = $crate::make_key!($name, $unit $(, $label_key => $label_value)*);
            let meta = metrics::Metadata::new("", metrics::Level::INFO, None);
            let c = recorder.register_counter(&key, &meta);
            c.increment($value);
        })
    };
    ($name: expr, $unit: ident) => {
        benchmark_increment_counter!($name, $unit, 1)
    };
}

#[macro_export]
macro_rules! benchmark_histogram {
    ($name: expr, $unit: ident, $description: expr, $value: expr $(, $label_key: expr => $label_value: expr)*) => {
        metrics::with_recorder(|recorder| {
            let key = $crate::make_key!($name, $unit $(, $label_key => $label_value)*);
            let meta = metrics::Metadata::new("", metrics::Level::INFO, None);
            let h = recorder.register_histogram(&key, &meta);
            recorder.describe_histogram(key.into_parts().0, Some(::metrics::Unit::$unit), $description);
            h.record($value);
        })
    };
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use metrics_exporter_prometheus::*;

    macro_rules! assert_delta {
        ($x:expr, $y:expr, $d:expr) => {
            if !($x - $y < $d || $y - $x < $d) {
                panic!("{} and {} differ by more than {}", $x, $y, $d);
            }
        };
    }

    fn setup() -> PrometheusHandle {
        let builder =
            PrometheusBuilder::new().idle_timeout(metrics_util::MetricKindMask::ALL, None);
        let recorder = builder.build_recorder();
        let handle = recorder.handle();
        metrics::set_global_recorder(recorder).expect("recorder already set");
        handle
    }

    #[test]
    fn test_metrics_macros() {
        let handle = setup();

        benchmark_gauge!("test", Count, "desc".into(), 1.0);
        benchmark_gauge!("test", Count, "desc".into(), 2.0);
        benchmark_gauge!("test", Count, "desc".into(), 3.0, "a" => "b");
        benchmark_gauge!("test", Count, "desc".into(), 4.0, "c" => "d");
        benchmark_gauge!("test", Count, "desc".into(), 5.0, "e" => "f", "g" => "h");

        benchmark_counter!("one", Seconds, "desc".into(), 0);
        benchmark_counter!("two", Seconds, "desc".into(), 39);
        benchmark_increment_counter!("two", Seconds, 2);
        benchmark_increment_counter!("two", Seconds);
        benchmark_increment_counter!("three", Seconds);

        for i in 1..=100 {
            benchmark_histogram!("percentile", Bytes, "desc".into(), i as f64);
        }

        // TODO:  If https://github.com/metrics-rs/metrics/pull/236 lands, use that instead of checking rendered text
        let output = handle.render();
        println!("{}", output);
        assert!(output.contains(indoc! {"
            # TYPE benchmark_test_count gauge
            benchmark_test_count"
        }));
        assert!(output.contains("\nbenchmark_test_count{a=\"b\"} 3\n"));
        assert!(output.contains("\nbenchmark_test_count{c=\"d\"} 4\n"));
        assert!(
            output.contains("\nbenchmark_test_count{e=\"f\",g=\"h\"} 5\n")
                || output.contains("\nbenchmark_test_count{g=\"h\",e=\"f\"} 5\n")
        );
        assert!(output.contains(indoc! {"
            # TYPE benchmark_one_seconds counter
            benchmark_one_seconds 0
        "}));
        assert!(output.contains(indoc! {"
            # TYPE benchmark_two_seconds counter
            benchmark_two_seconds 42
        "}));
        assert!(output.contains(indoc! {"
            # TYPE benchmark_three_seconds counter
            benchmark_three_seconds 1
        "}));

        const EXPECT: &[(&str, f64)] = &[
            (r#"benchmark_percentile_bytes{quantile="0"}"#, 1.0),
            (
                r#"benchmark_percentile_bytes{quantile="0.5"}"#,
                50.00385027884824,
            ),
            (
                r#"benchmark_percentile_bytes{quantile="0.9"}"#,
                90.00813093751373,
            ),
            (
                r#"benchmark_percentile_bytes{quantile="0.95"}"#,
                95.00219629040446,
            ),
            (
                r#"benchmark_percentile_bytes{quantile="0.99"}"#,
                98.99803587754256,
            ),
            (
                r#"benchmark_percentile_bytes{quantile="0.999"}"#,
                98.99803587754256,
            ),
            (r#"benchmark_percentile_bytes{quantile="1"}"#, 100.0),
            (r#"benchmark_percentile_bytes_sum"#, 5050.0),
            (r#"benchmark_percentile_bytes_count"#, 100.0),
        ];
        for (k, v) in EXPECT {
            let mut found = false;
            for ln in output.lines() {
                if ln.starts_with(k) {
                    let actual = ln.split(' ').nth(1).unwrap().parse::<f64>().unwrap();
                    assert_delta!(*v, actual, 0.0000001);
                    found = true;
                }
            }
            assert!(found, "{} not found!", k);
        }
    }
}
