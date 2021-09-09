use std::str::FromStr;

use clap::Clap;
use thiserror::Error;
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

#[derive(Clap)]
pub struct Options {
    /// Format to use when emitting log events
    #[clap(
        long,
        env = "LOG_FORMAT",
        parse(try_from_str),
        default_value = "full",
        possible_values = &["compact", "full", "pretty", "json"]
    )]
    log_format: LogFormat,

    /// Level to log at
    #[clap(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,
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

        Ok(())
    }
}

/// Configure the global tracing subscriber for logging inside of tests
pub fn init_test_logging() {
    // This errors out if it's already been called within the scope of a process, which we don't
    // care about, so we just discard the result
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
}
