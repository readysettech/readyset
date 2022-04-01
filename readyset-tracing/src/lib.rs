//! This crate configures tracing; because we perform logging through the tracing subsystem,
//! logging is configured here as well.
//!
//! For most purposes, you can use the normal set of primitives from the [tracing] family of
//! crates, such as the [`#[instrument]`](tracing-attributes::instrument) macro, and simply allow
//! this crate to deal with configuration.
//!
//! Other than configuration, the functionality provided by this crate is primarily useful for two
//! specific cases:
//! * Traces that need to cross a network or thread boundary, such as the adapter making a query to
//! Noria
//! * Performance-critical codepaths, such as the hotpath for queries in Noria and the adapter
//!
//! # Context propagation
//! For traces that cross a network/thread boundary, [propagation] provides a set of primitives to
//! easily serialize/deserialize tracing context, attach it to a request, and unpack it on the
//! other side.
//!
//! In the following example, imagine that `send_request()` lives in Process A, while
//! `remote_function()` and `process_request()` live in Process B, and under the hood, the
//! `remote_function` call is an RPC made using `tower`, `reqwest`, or your other favorite crate.
//! ```
//! use readyset_tracing::propagation::Instrumented;
//! use readyset_tracing::remote_span;
//! use tracing::instrument;
//!
//! #[instrument]
//! fn send_request(id: u32) {
//!     let request = Instrumented::from(id);
//!     remote_function(request)
//! }
//!
//! fn remote_function(request: Instrumented<u32>) {
//!     // remote_span!() will unpack the Instrumented request, returning both the span and the
//!     // inner request.  Presampling (see below) is respected - if the span with which the
//!     // request was instrumented is disabled, the returned span will also be disabled.
//!     let (span, id) = remote_span!(request, INFO, "handle_request");
//!     span.in_scope(|| process_request(id))
//! }
//!
//! #[instrument]
//! fn process_request(id: u32) {
//!     // do something!
//! }
//! ```
//!
//! # Performance-critical codepaths
//! For performance-critical pieces, the story is a bit more complex.  Because there is a
//! substantial performance cost involved with creating a [Span](tracing::Span) to begin with,
//! compared to a call to [Span::none()](tracing::Span::none), there are savings to be had by
//! [presampling](presampled) - sampling spans at creation time rather than when a subscriber would
//! send them to a collector.
//!
//! In addition to choosing whether or not to create a detailed span at all, only entering spans
//! and instrumenting futures when we _have_ an enabled span speeds things up.
//!
//! The following example highlights both:
//! ```
//! use clap::Parser;
//! use readyset_tracing::presampled::instrument_if_enabled;
//! use readyset_tracing::{child_span, root_span};
//! use tracing::{Instrument, Span};
//!
//! #[derive(Parser, Debug)]
//! struct Options {
//!     #[clap(flatten)]
//!     tracing: readyset_tracing::Options,
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let options = Options::parse();
//!     options.tracing.init("propagation-example").unwrap();
//!
//!     for id in 0..100 {
//!         process_id(id).await;
//!     }
//! }
//!
//! async fn process_id(id: u32) {
//!     // root_span!() performs sampling; if the request is not chosen by the sampler, the
//!     // returned span will be disabled
//!     let span = root_span!(INFO, "process_id", id);
//!     instrument_if_enabled(do_a_thing(id), span).await
//! }
//!
//! async fn do_a_thing(id: u32) {
//!     // child_span!() checks whether or not the current execution context is instrumented with
//!     // an enabled span; if so, it returns an enabled span, otherwise is returns the existing
//!     // disabled span.
//!     let span = child_span!(INFO, "do_a_thing", id);
//!     if span.is_disabled() {
//!         smaller_slice_of_work();
//!     } else {
//!         // For synchronous calls, Span::in_scope() can be used
//!         span.in_scope(|| smaller_slice_of_work());
//!     }
//! }
//!
//! fn smaller_slice_of_work() {
//!     let span = child_span!(INFO, "smaller_slice_of_work");
//!     // _Inside_ synchronous functions, we can simplify to Span::enter()
//!     let _guard = span.enter();
//!     // do something!
//! }
//! ```
//!
//! # Potential footgun
//! If you find that you're getting panics from `presampled.rs` in your tests, such as this:
//! ```text
//! thread 'tokio-runtime-worker' panicked at 'called `Option::unwrap()` on a `None` value', readyset-tracing/src/presampled.rs:47:64
//! ```
//! The most likely cause is that you're exercising code that makes use of presampling, but the
//! presampler has not been initialized.  [init_test_logging()](init_test_logging) does take care
//! of this, but if that isn't being called - for example, because you don't want your test to
//! perform any logging - you can call [init_test_presampler()](init_test_presampler) instead.
//!
//! We do have another option - default to either a 100% or 0% presampler and avoid these panics.
//! However, having an easily-discoverable panic seems to be a less painful alternative to having
//! presampling be silently misconfigured.

#![feature(core_intrinsics)]
use std::str::FromStr;

use clap::Parser;
use opentelemetry::runtime;
use opentelemetry::sdk::trace::Tracer;
pub use readyset_tracing_proc_macros::{instrument_child, instrument_remote, instrument_root};
use tracing::{warn, Subscriber};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::filter::{Filtered, ParseError, Targets};
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
        warn!("Running a debug build")
    }
}

#[derive(Debug, Parser)]
pub struct Options {
    /// Format to use when emitting log events.
    #[clap(
        long,
        env = "LOG_FORMAT",
        parse(try_from_str),
        default_value = "full",
        possible_values = &["compact", "full", "pretty", "json"]
    )]
    log_format: LogFormat,

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

    /// Host and port to send traces/spans to
    #[clap(long, env = "TRACING_HOST")]
    tracing_host: Option<String>,

    /// Portion of traces that will be sent to the tracing endpoint; [0.0~100.0]
    #[clap(long, env = "TRACING_SAMPLE_PERCENT", default_value_t = Percent(0.01))]
    tracing_sample_percent: Percent,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Full,
            log_level: "info".to_owned(),
            tracing_host: None,
            tracing_sample_percent: Percent(0.01),
        }
    }
}

impl Options {
    fn tracing_layer<S>(&self, service_name: &str) -> Result<OpenTelemetryLayer<S, Tracer>, Error>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(self.tracing_host.as_ref().unwrap())
            .with_service_name(service_name)
            .with_auto_split_batch(true)
            .install_batch(runtime::Tokio)?;

        Ok(tracing_opentelemetry::layer().with_tracer(tracer))
    }

    // Anything we do with a templated type alias or a custom trait is just going to make this more
    // difficult to read/follow
    #[allow(clippy::type_complexity)]
    fn logging_layer<S>(
        &self,
    ) -> Result<Filtered<Box<dyn Layer<S> + Send + Sync>, Targets, S>, ParseError>
    where
        S: Subscriber + Send + Sync + for<'span> LookupSpan<'span>,
    {
        let filter = Targets::from_str(&self.log_level)?;
        let layer: Box<dyn Layer<S> + Send + Sync> = match self.log_format {
            LogFormat::Compact => Box::new(fmt::layer().compact()),
            LogFormat::Full => Box::new(fmt::layer()),
            LogFormat::Pretty => Box::new(fmt::layer().pretty()),
            LogFormat::Json => Box::new(fmt::layer().json().with_current_span(true)),
        };
        Ok(layer.with_filter(filter))
    }

    fn init_logging_and_tracing(&self, service_name: &str) -> Result<(), Error> {
        use tracing_subscriber::prelude::*;
        presampled::install_sample_generator(self.tracing_sample_percent.clone());
        tracing_subscriber::registry()
            .with(self.tracing_layer(service_name)?)
            .with(self.logging_layer()?)
            .init();
        Ok(())
    }

    fn init_logging_only(&self) -> Result<(), ParseError> {
        presampled::install_sample_generator(Percent(0.0));
        let filter = EnvFilter::try_new(&self.log_level)?;
        let s = tracing_subscriber::fmt().with_env_filter(filter);

        match self.log_format {
            LogFormat::Compact => s.compact().init(),
            LogFormat::Full => s.init(),
            LogFormat::Pretty => s.pretty().init(),
            LogFormat::Json => s.json().with_current_span(true).init(),
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
    ///     options.tracing.init("tracing-example").unwrap();
    ///
    ///     // Perform work!
    /// }
    /// ```
    pub fn init(&self, service_name: &str) -> Result<(), Error> {
        if self.tracing_host.is_some() {
            self.init_logging_and_tracing(service_name)
        } else {
            self.init_logging_only().map_err(|e| e.into())
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
    init_test_presampler();
    // This errors out if it's already been called within the scope of a process, which we don't
    // care about, so we just discard the result
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .with_test_writer()
        .try_init();
}

/// Configure the presampler only
pub fn init_test_presampler() {
    presampled::install_sample_generator(Percent(0.0));
}
