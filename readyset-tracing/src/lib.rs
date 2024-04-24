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

#![feature(core_intrinsics)]
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Args, ValueEnum};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{Sampler, Tracer};
use opentelemetry_sdk::Resource;
use tracing::Subscriber;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
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

pub fn warn_if_debug_build() {
    if cfg!(debug) {
        tracing::warn!("Running a debug build")
    }
}

#[derive(Debug, Args)]
#[group(id = "logging")]
pub struct Options {
    /// Optional path to write logs to. If set, logs will rollover based on the chosen
    /// `log_rotation` policy, which defaults to daily. Readyset must have write permissions to
    /// the provided path.
    /// Logs will be written to `readyset.log` within this path.
    #[arg(long, env = "LOG_PATH")]
    pub log_path: Option<PathBuf>,

    /// Log [`Rotation`](https://docs.rs/tracing-appender/latest/tracing_appender/rolling/struct.Rotation.html)
    /// to use if a log file is set. Defaults to daily.
    /// Does nothing if no log file is set.
    /// Possible Values: [daily, hourly, minutely, never]
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

/// Initializes a SubscriberBuilder based on self.log_format.
// This is a macro rather than a fn because SubscriberBuilder embeds its layering types into the
// type itself, and to make it variadic
macro_rules! log_format_init {
    ($self:expr, $subscriber_builder:expr, $fmt_layer:expr, $tracing_layer:expr) => {
        match $self.log_format {
            LogFormat::Compact => $subscriber_builder
                .with($tracing_layer)
                .with(
                    $fmt_layer
                        .compact()
                        .with_filter(filter::filter_fn(|metadata| {
                            !is_statement_log(metadata.target())
                        })),
                )
                .init(),
            LogFormat::Full => $subscriber_builder
                .with($tracing_layer)
                .with($fmt_layer.with_filter(filter::filter_fn(|metadata| {
                    !is_statement_log(metadata.target())
                })))
                .init(),
            LogFormat::Pretty => $subscriber_builder
                .with($tracing_layer)
                .with(
                    $fmt_layer
                        .pretty()
                        .with_filter(filter::filter_fn(|metadata| {
                            !is_statement_log(metadata.target())
                        })),
                )
                .init(),
            LogFormat::Json => $subscriber_builder
                .with($tracing_layer)
                .with(
                    $fmt_layer
                        .json()
                        .with_current_span(true)
                        .with_filter(filter::filter_fn(|metadata| {
                            !is_statement_log(metadata.target())
                        })),
                )
                .init(),
        };
    };
    ($self:expr, $subscriber_builder:expr, $fmt_layer:expr) => {
        match $self.log_format {
            LogFormat::Compact => $subscriber_builder
                .with(
                    $fmt_layer
                        .compact()
                        .with_filter(filter::filter_fn(|metadata| {
                            !is_statement_log(metadata.target())
                        })),
                )
                .init(),
            LogFormat::Full => $subscriber_builder
                .with($fmt_layer.with_filter(filter::filter_fn(|metadata| {
                    !is_statement_log(metadata.target())
                })))
                .init(),
            LogFormat::Pretty => $subscriber_builder
                .with(
                    $fmt_layer
                        .pretty()
                        .with_filter(filter::filter_fn(|metadata| {
                            !is_statement_log(metadata.target())
                        })),
                )
                .init(),
            LogFormat::Json => $subscriber_builder
                .with(
                    $fmt_layer
                        .json()
                        .with_current_span(true)
                        .with_filter(filter::filter_fn(|metadata| {
                            !is_statement_log(metadata.target())
                        })),
                )
                .init(),
        };
    };
    ($self:expr, $subscriber_builder:expr) => {
        match &$self.log_format {
            LogFormat::Compact => $subscriber_builder.compact().init(),
            LogFormat::Full => $subscriber_builder.init(),
            LogFormat::Pretty => $subscriber_builder.pretty().init(),
            LogFormat::Json => $subscriber_builder.json().with_current_span(true).init(),
        }
    };
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

    /// Sets up a non-blocking filing appender with the configured log rotation policy.
    /// Note that the returned WorkerGuard should be kept in scope for the duration of the
    /// process--it's Drop is what flushes the logs before a shutdown or crash.
    fn setup_file_appender(&self, log_path: &Path) -> (NonBlocking, WorkerGuard) {
        tracing_appender::non_blocking(RollingFileAppender::new(
            self.log_rotation.into(),
            log_path,
            "readyset.log",
        ))
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
    pub fn init(&self, service_name: &str, deployment: &str) -> Result<Option<WorkerGuard>, Error> {
        // Note: There isn't a great way to make partial builders here and avoid this match, because
        // the subscriber builder type embeds the layering types.
        let res = Ok(
            match (
                self.tracing_host.is_some(),
                self.statement_logging,
                self.log_path.is_some(),
            ) {
                (true, true, true) => {
                    let log_path = self.log_path.as_ref().expect("is some").as_path();
                    self.setup_tracing_statement_logging_and_log_file(
                        log_path,
                        service_name,
                        deployment,
                    )
                }
                (true, true, false) => {
                    self.setup_tracing_and_statement_logging(service_name, deployment)
                }
                (true, false, true) => {
                    let log_path = self.log_path.as_ref().expect("is some").as_path();
                    self.setup_tracing_and_log_file(log_path, service_name, deployment)
                }
                (true, false, false) => self.setup_tracing(service_name, deployment),
                (false, true, true) => {
                    let log_path = self.log_path.as_ref().expect("is some").as_path();
                    self.setup_statement_logging_and_log_file(log_path, deployment)
                }
                (false, true, false) => self.setup_statement_logging(deployment),
                (false, false, true) => {
                    let log_path = self.log_path.as_ref().expect("is some").as_path();
                    self.setup_log_file(log_path)
                }
                (false, false, false) => self.setup_basic(),
            },
        );

        #[cfg(debug)]
        warn_if_debug_build();

        res
    }

    // Returns the provided `statement_log_path` or a default filename.
    fn statement_log_path_or_default(&self, deployment: &str) -> String {
        match self.statement_log_path {
            Some(ref p) => p.clone(),
            None => format!("{}_statements.log", deployment),
        }
    }

    fn setup_tracing_statement_logging_and_log_file(
        &self,
        log_path: &Path,
        service_name: &str,
        deployment: &str,
    ) -> Option<WorkerGuard> {
        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        let s = tracing_subscriber::registry()
            .with(self.statement_logging_layer(&self.statement_log_path_or_default(deployment)))
            .with(env_filter);

        let (non_blocking, worker_guard) = self.setup_file_appender(log_path);
        let fmt_layer = fmt::layer()
            .with_ansi(!self.no_color)
            .with_writer(non_blocking);
        let tracing_layer = self.tracing_layer(service_name, deployment);

        log_format_init!(self, s, fmt_layer, tracing_layer);

        Some(worker_guard)
    }

    fn setup_tracing_and_statement_logging(
        &self,
        service_name: &str,
        deployment: &str,
    ) -> Option<WorkerGuard> {
        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        let s = tracing_subscriber::registry()
            .with(self.statement_logging_layer(&self.statement_log_path_or_default(deployment)))
            .with(env_filter);
        let fmt_layer = fmt::layer().with_ansi(!self.no_color);
        let tracing_layer = self.tracing_layer(service_name, deployment);

        log_format_init!(self, s, fmt_layer, tracing_layer);

        None
    }

    fn setup_tracing_and_log_file(
        &self,
        log_path: &Path,
        service_name: &str,
        deployment: &str,
    ) -> Option<WorkerGuard> {
        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        let s = tracing_subscriber::registry().with(env_filter);

        let (non_blocking, worker_guard) = self.setup_file_appender(log_path);
        let fmt_layer = fmt::layer()
            .with_ansi(!self.no_color)
            .with_writer(non_blocking);
        let tracing_layer = self.tracing_layer(service_name, deployment);

        log_format_init!(self, s, fmt_layer, tracing_layer);

        Some(worker_guard)
    }

    fn setup_tracing(&self, service_name: &str, deployment: &str) -> Option<WorkerGuard> {
        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        let s = tracing_subscriber::registry().with(env_filter);
        let fmt_layer = fmt::layer().with_ansi(!self.no_color);
        let tracing_layer = self.tracing_layer(service_name, deployment);

        log_format_init!(self, s, fmt_layer, tracing_layer);

        None
    }

    /// Sets up a subscriber with no tracing, but statement logging and a log file configured
    fn setup_statement_logging_and_log_file(
        &self,
        log_path: &Path,
        deployment: &str,
    ) -> Option<WorkerGuard> {
        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        let s = tracing_subscriber::registry()
            .with(self.statement_logging_layer(&self.statement_log_path_or_default(deployment)))
            .with(env_filter);

        let (non_blocking, worker_guard) = self.setup_file_appender(log_path);
        let fmt_layer = fmt::layer()
            .with_ansi(!self.no_color)
            .with_writer(non_blocking);

        log_format_init!(self, s, fmt_layer);

        Some(worker_guard)
    }

    /// Sets up a subscriber with no tracing, statement logging and no log file configured
    fn setup_statement_logging(&self, deployment: &str) -> Option<WorkerGuard> {
        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        let s = tracing_subscriber::registry()
            .with(self.statement_logging_layer(&self.statement_log_path_or_default(deployment)))
            .with(env_filter);

        let fmt_layer = fmt::layer().with_ansi(!self.no_color);

        log_format_init!(self, s, fmt_layer);

        None
    }

    /// Sets up a subscriber with no tracing, no statement logging, but a log file configured
    fn setup_log_file(&self, log_path: &Path) -> Option<WorkerGuard> {
        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        let (non_blocking, worker_guard) = self.setup_file_appender(log_path);
        let s = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_ansi(!self.no_color)
            .with_writer(non_blocking);

        log_format_init!(self, s);

        Some(worker_guard)
    }

    /// Sets up a subscriber with no tracing, no statement logging, and no log file configured
    // In this case we can avoid dynamic dispatch/using the registry
    fn setup_basic(&self) -> Option<WorkerGuard> {
        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        let s = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_ansi(!self.no_color);

        log_format_init!(self, s);

        None
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        if self.tracing_host.is_some() {
            opentelemetry::global::shutdown_tracer_provider()
        }
    }
}

/// Configure the global tracing subscriber for logging inside of tests
pub fn init_test_logging() {
    // This errors out if it's already been called within the scope of a process, which we don't
    // care about, so we just discard the result
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .with_test_writer()
        .try_init();
}
