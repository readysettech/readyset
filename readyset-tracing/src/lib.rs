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
use std::sync::Arc;

use clap::Args;
use opentelemetry::sdk::trace::{Sampler, Tracer};
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use tracing::Subscriber;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::filter::{self, ParseError};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{fmt, EnvFilter, Layer};

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
    /// Format to use when emitting log events.
    #[clap(long, env = "LOG_FORMAT", default_value = "full", value_enum)]
    log_format: LogFormat,

    /// Disable colors in all log output
    #[clap(long, env = "NO_COLOR", hide = true)]
    no_color: bool,

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
    #[clap(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Host and port to send OTLP traces/spans data, via GRPC OLTP
    #[clap(long, env = "TRACING_HOST", hide = true)]
    tracing_host: Option<String>,

    /// Portion of traces that will be sent to the tracing endpoint; [0.0~1.0]
    #[clap(long, env = "TRACING_SAMPLE_PERCENT", default_value_t = Percent(0.01), hide = true)]
    tracing_sample_percent: Percent,

    /// Whether to log all statements received by ReadySet via the client or replicators
    #[clap(long, env = "STATEMENT_LOGGING", hide = true)]
    pub statement_logging: bool,

    /// Optional filename for storing the statement log. Defaults to
    /// <deployment-name>_statements.log.
    #[clap(
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

/// Whether the target matches the target set for statement logs
fn is_statement_log(target: &str) -> bool {
    target == "client_statement" || target == "replicator_statement"
}

impl Options {
    fn tracing_layer<S>(
        &self,
        service_name: &str,
        deployment: &str,
    ) -> Result<OpenTelemetryLayer<S, Tracer>, Error>
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
                opentelemetry::sdk::trace::config()
                    .with_sampler(Sampler::TraceIdRatioBased(self.tracing_sample_percent.0))
                    .with_max_events_per_span(64)
                    .with_max_attributes_per_span(16)
                    .with_resource(Resource::new(resources)),
            )
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();

        Ok(tracing_opentelemetry::layer().with_tracer(tracer))
    }

    // Anything we do with a templated type alias or a custom trait is just going to make this more
    // difficult to read/follow
    #[allow(clippy::type_complexity)]
    fn logging_layer<S>(&self) -> Result<Box<dyn Layer<S> + Send + Sync>, ParseError>
    where
        S: Subscriber + Send + Sync + for<'span> LookupSpan<'span>,
    {
        let layer: Box<dyn Layer<S> + Send + Sync> = match self.log_format {
            LogFormat::Compact => Box::new(fmt::layer().compact().with_ansi(!self.no_color)),
            LogFormat::Full => Box::new(fmt::layer().with_ansi(!self.no_color)),
            LogFormat::Pretty => Box::new(fmt::layer().pretty().with_ansi(!self.no_color)),
            LogFormat::Json => Box::new(
                fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_ansi(!self.no_color),
            ),
        };
        Ok(layer)
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

    fn init_logging_and_tracing(&self, service_name: &str, deployment: &str) -> Result<(), Error> {
        use tracing_subscriber::prelude::*;
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&self.log_level))
            .with(self.tracing_layer(service_name, deployment)?)
            .with(
                self.logging_layer()?
                    .with_filter(filter::filter_fn(|metadata| {
                        !is_statement_log(metadata.target())
                    })),
            )
            .with(self.statement_logging_layer(&self.statement_log_path_or_default(deployment)))
            .init();
        Ok(())
    }

    // Initializes logging, and conditionally, statement logging
    fn init_logging_only(&self, deployment: &str) -> Result<(), ParseError> {
        let env_filter = tracing_subscriber::EnvFilter::new(&self.log_level);
        // Avoid using the registry if we are only using one layer
        if self.statement_logging {
            use tracing_subscriber::prelude::*;
            tracing_subscriber::registry()
                .with(
                    self.logging_layer()?
                        .with_filter(filter::filter_fn(|metadata| {
                            !is_statement_log(metadata.target())
                        })),
                )
                .with(self.statement_logging_layer(&self.statement_log_path_or_default(deployment)))
                .with(env_filter)
                .init();
        } else {
            let s = tracing_subscriber::fmt().with_env_filter(env_filter);
            match self.log_format {
                LogFormat::Compact => s.compact().init(),
                LogFormat::Full => s.init(),
                LogFormat::Pretty => s.pretty().init(),
                LogFormat::Json => s.json().with_current_span(true).init(),
            }
        }

        #[cfg(debug)]
        warn_if_debug_build();

        Ok(())
    }

    /// This is the primary entrypoint to the combined logging/tracing subsystem.  If tracing is
    /// not configured, it will initialize logging with static dispatch for the format, saving the
    /// performance cost.
    ///
    /// When the `Options` struct itself goes out of scope, it will take care of the call to
    /// [opentelemetry::global::shutdown_tracer_provider] so that, when developing calling code,
    /// you don't need to remember.
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
    ///     #[clap(flatten)]
    ///     tracing: readyset_tracing::Options,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let options = Options::parse();
    ///     options
    ///         .tracing
    ///         .init("tracing-example", "example-deployment")
    ///         .unwrap();
    ///
    ///     // Perform work!
    /// }
    /// ```
    pub fn init(&self, service_name: &str, deployment: &str) -> Result<(), Error> {
        if self.tracing_host.is_some() {
            self.init_logging_and_tracing(service_name, deployment)
        } else {
            self.init_logging_only(deployment).map_err(|e| e.into())
        }
    }

    // Returns the provided `statement_log_path` or a default filename.
    fn statement_log_path_or_default(&self, deployment: &str) -> String {
        match self.statement_log_path {
            Some(ref p) => p.clone(),
            None => format!("{}_statements.log", deployment),
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

/// Configure the global tracing subscriber for logging inside of tests
pub fn init_test_logging() {
    // This errors out if it's already been called within the scope of a process, which we don't
    // care about, so we just discard the result
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .with_test_writer()
        .try_init();
}
