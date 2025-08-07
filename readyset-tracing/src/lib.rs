//! This crate configures tracing; because we perform logging through the tracing subsystem,
//! logging is configured here as well.
//!
//! For most purposes, you can use the normal set of primitives from the [tracing] family of
//! crates, such as the [`#[instrument]`](tracing-attributes::instrument) macro, and simply allow
//! this crate to deal with configuration.
//!
//! Other than configuration, the functionality provided by this crate is primarily useful for
//! Performance-critical codepaths, such as the hotpath for queries in ReadySet and the adapter.
//!
//! # Performance-critical codepaths
//! For performance-critical pieces, the story is a bit more complex.  Because there is a
//! substantial performance cost involved with creating a [Span](tracing::Span) to begin with,
//! compared to a call to [Span::none()](tracing::Span::none), there are savings to be had by
//! [presampling](presampled) - sampling spans at creation time rather than when a subscriber would
//! send them to a collector.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Args, ValueEnum};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{Sampler, Tracer};
use opentelemetry_sdk::Resource;
use tracing::Subscriber;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, fmt, EnvFilter, Layer};

mod error;
pub use error::Error;
mod logformat;
use logformat::LogFormat;
mod percent;
use percent::Percent;
pub mod presampled;
pub mod propagation;

#[derive(Debug, Args)]
#[group(id = "logging")]
pub struct Options {
    /// Optional path to a directory where log files will be placed. Logs will be written to
    /// `readyset.log` within this directory. If set, logs will rollover based on the chosen
    /// `log_rotation` policy, which defaults to daily. Readyset must have write permissions.
    #[arg(long, env = "LOG_PATH")]
    pub log_path: Option<PathBuf>,

    /// Log [`RotationCadence`] to use if a log file is set. Defaults to daily. Does nothing if no
    /// log file is set. Possible Values: [daily, hourly, minutely, never]
    #[arg(long, env = "LOG_ROTATION", default_value = "daily", value_enum)]
    pub log_rotation: RotationCadence,

    /// Format to use when emitting log events.
    #[arg(long, env = "LOG_FORMAT", default_value = "full", value_enum)]
    pub log_format: LogFormat,

    /// Disable colors in all log output
    #[arg(long, env = "NO_COLOR")]
    pub no_color: bool,

    /// Log level filter for spans and events. The log level filter string is a comma separated
    /// list of directives.
    /// See [`tracing_subscriber::EnvFilter`] for full documentation on the directive syntax.
    ///
    /// Examples:
    ///
    /// Log at INFO level for all crates and dependencies.
    /// ```bash
    /// LOG_LEVEL=info
    /// ```
    ///
    /// Log at TRACE level for all crates and dependencies except
    /// tower which should be logged at ERROR level.
    /// ```bash
    /// LOG_LEVEL=trace,tower=error
    /// ```
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Host and port to send OTLP traces/spans data, via GRPC OLTP
    #[arg(long, env = "TRACING_HOST", hide = true)]
    pub tracing_host: Option<String>,

    /// Portion of traces that will be sent to the tracing endpoint; [0.0~1.0]
    #[arg(long, env = "TRACING_SAMPLE_PERCENT", default_value_t = Percent(0.01), hide = true)]
    pub tracing_sample_percent: Percent,

    /// Whether to log all statements received by ReadySet via the client or replicators
    #[arg(long, env = "STATEMENT_LOGGING", hide = true)]
    pub statement_logging: bool,

    /// Optional filename for storing the statement log. Defaults to
    /// <deployment-name>_statements.log.
    #[arg(
        long,
        env = "STATEMENT_LOG_PATH",
        requires = "statement_logging",
        hide = true
    )]
    pub statement_log_path: Option<String>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            log_path: None,
            log_rotation: "daily".try_into().expect("daily is a valid log rotation"),
            log_format: LogFormat::Full,
            no_color: false,
            log_level: "info".to_owned(),
            tracing_host: None,
            tracing_sample_percent: Percent(0.01),
            statement_logging: false,
            statement_log_path: None,
        }
    }
}

/// The rotation policy for log files
// This wrapper allows us to parse from a str since Rotation itself doesn't support that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum RotationCadence {
    /// Rotate logs daily
    Daily,
    /// Rotate logs hourly
    Hourly,
    /// Rotate logs minutely
    Minutely,
    /// Never rotate logs
    Never,
}

impl TryFrom<&str> for RotationCadence {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "daily" => Ok(Self::Daily),
            "hourly" => Ok(Self::Hourly),
            "minutely" => Ok(Self::Minutely),
            "never" => Ok(Self::Never),
            _ => Err(Error::InvalidRotationCadence(s.to_owned())),
        }
    }
}

impl From<RotationCadence> for Rotation {
    fn from(value: RotationCadence) -> Self {
        match value {
            RotationCadence::Daily => Rotation::DAILY,
            RotationCadence::Hourly => Rotation::HOURLY,
            RotationCadence::Minutely => Rotation::MINUTELY,
            RotationCadence::Never => Rotation::NEVER,
        }
    }
}

/// Whether the target matches the target set for statement logs
fn is_statement_log(target: &str) -> bool {
    target == "client_statement" || target == "replicator_statement"
}

impl Options {
    fn tracing_layer<S>(
        &self,
        service_name: &str,
        deployment: &str,
    ) -> OpenTelemetryLayer<S, Tracer>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        let resources = vec![
            KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name.to_owned(),
            ),
            KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAMESPACE,
                deployment.to_owned(),
            ),
        ];

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(self.tracing_host.as_ref().unwrap()),
            )
            .with_trace_config(
                opentelemetry_sdk::trace::config()
                    .with_sampler(Sampler::TraceIdRatioBased(self.tracing_sample_percent.0))
                    .with_max_events_per_span(64)
                    .with_max_attributes_per_span(16)
                    .with_resource(Resource::new(resources)),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)
            .unwrap();

        tracing_opentelemetry::layer().with_tracer(tracer)
    }

    #[allow(clippy::type_complexity)]
    fn statement_logging_layer<S>(&self, file_name: &str) -> Box<dyn Layer<S> + Send + Sync>
    where
        S: Subscriber + Send + Sync + for<'span> LookupSpan<'span>,
    {
        match File::create(file_name) {
            Ok(f) => Box::new(fmt::layer().with_writer(Arc::new(f)).with_filter(
                filter::filter_fn(|metadata| is_statement_log(metadata.target())),
            )),
            // If we can't create the file, include statements with other logs
            _ => Box::new(fmt::layer().with_filter(filter::filter_fn(|metadata| {
                is_statement_log(metadata.target())
            }))),
        }
    }

    /// This is the primary entrypoint to the combined logging/tracing subsystem.  If
    /// tracing, statement logging, and the log path are all not configured, it will initialize
    /// logging with static dispatch for the format, saving some performance cost.
    ///
    /// When the `Options` struct itself goes out of scope, it will take care of the call to
    /// [opentelemetry::global::shutdown_tracer_provider] so that, when developing calling code,
    /// you don't need to remember.
    ///
    /// If statement logging is enabled, statement logs will be sent to a separate statement logging
    /// file, which is <deployment_name>_statments.log by default, or configurable with the
    /// `statement_log_path` option.
    ///
    /// If a log file is configured, it will be rolled over based on the configured rotation, and
    /// when the WorkerGuard is dropped, the logs will be flushed.
    ///
    /// # Panics
    /// This will panic if called with tracing enabled outside the context of a tokio runtime.
    ///
    /// Example:
    /// ```
    /// use clap::Parser;
    ///
    /// #[derive(Debug, Parser)]
    /// struct Options {
    ///     #[command(flatten)]
    ///     tracing: readyset_tracing::Options,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let options = Options::parse();
    ///     let _guard = options
    ///         .tracing
    ///         .init("tracing-example", "example-deployment")
    ///         .unwrap();
    ///
    ///     // Perform work!
    /// }
    /// ```
    pub fn init(&self, service_name: &str, deployment: &str) -> Result<WorkerGuard, Error> {
        let (non_blocking, worker_guard) = if let Some(log_path) = &self.log_path {
            tracing_appender::non_blocking(RollingFileAppender::new(
                self.log_rotation.into(),
                log_path,
                "readyset.log",
            ))
        } else {
            tracing_appender::non_blocking(std::io::stdout())
        };
        let fmt_layer = fmt::layer()
            .with_ansi(!self.no_color)
            .with_writer(non_blocking);
        let tracing_layer = if self.tracing_host.is_some() {
            Some(self.tracing_layer(service_name, deployment))
        } else {
            None
        };
        let statement_layer = if self.statement_logging {
            Some(self.statement_logging_layer(&self.statement_log_path_or_default(deployment)))
        } else {
            None
        };

        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        let fmt_filter = filter::filter_fn(|metadata| !is_statement_log(metadata.target()));

        let s = tracing_subscriber::registry()
            .with(statement_layer)
            .with(env_filter)
            .with(tracing_layer);

        match self.log_format {
            LogFormat::Compact => s.with(fmt_layer.compact().with_filter(fmt_filter)).init(),
            LogFormat::Full => s.with(fmt_layer.with_filter(fmt_filter)).init(),
            LogFormat::Pretty => s.with(fmt_layer.pretty().with_filter(fmt_filter)).init(),
            LogFormat::Json => s
                .with(
                    fmt_layer
                        .json()
                        .with_current_span(true)
                        .with_filter(fmt_filter),
                )
                .init(),
        }

        #[cfg(debug_assertions)]
        tracing::warn!("Running a debug build");

        Ok(worker_guard)
    }

    // Returns the provided `statement_log_path` or a default filename.
    fn statement_log_path_or_default(&self, deployment: &str) -> String {
        match self.statement_log_path {
            Some(ref p) => p.clone(),
            None => format!("{deployment}_statements.log"),
        }
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        if self.tracing_host.is_some() {
            opentelemetry::global::shutdown_tracer_provider()
        }
    }
}

/// Configure the global tracing subscriber for logging inside of tests. If this is running under
/// Antithesis, we'll output JSONL to the special sink directory. See [docs].
///
/// [docs]: https://antithesis.com/docs/environment/the_antithesis_environment/#generating-structured-events
pub fn init_test_logging() {
    // This errors out if it's already been called within the scope of a process, which we don't
    // care about, so we just discard the result
    if let Ok(output_dir) = std::env::var("ANTITHESIS_OUTPUT_DIR") {
        let file = std::fs::File::create(format!("{output_dir}/readyset-test.jsonl")).unwrap();
        let _ = tracing_subscriber::fmt()
            .json()
            .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
            .with_writer(file)
            .try_init();
    } else {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
            .with_test_writer()
            .try_init();
    }
}
