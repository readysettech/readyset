use std::str::FromStr;

use clap::Parser;
use thiserror::Error;
use tracing::warn;
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
enum LogFormat {
    /// Corresponds to [`tracing_subscriber::fmt::format::Compact`]
    Compact,

    /// Corresponds to [`tracing_subscriber::fmt::format::Full`]
    Full,

    /// Corresponds to [`tracing_subscriber::fmt::format::Pretty`]
    Pretty,

    /// Corresponds to [`tracing_subscriber::fmt::format::Json`]
    Json,
}

/// Error type for the [`FromStr`] implementation for [`LogFormat`]
#[derive(Debug, Error)]
#[error("Invalid log format '{0}', expected one of 'compact', 'full', 'pretty', or 'json'")]
pub struct InvalidLogFormat(String);

impl FromStr for LogFormat {
    type Err = InvalidLogFormat;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "compact" => Ok(Self::Compact),
            "full" => Ok(Self::Full),
            "pretty" => Ok(Self::Pretty),
            "json" => Ok(Self::Json),
            _ => Err(InvalidLogFormat(s.to_owned())),
        }
    }
}

pub fn warn_if_debug_build() {
    if cfg!(debug) {
        warn!("Running a debug build")
    }
}

#[derive(Parser, Debug)]
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
}

impl Default for Options {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Full,
            log_level: "info".to_owned(),
        }
    }
}

impl Options {
    pub fn init(&self) -> anyhow::Result<()> {
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
