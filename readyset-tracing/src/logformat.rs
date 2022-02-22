use std::str::FromStr;

#[derive(Debug)]
pub enum LogFormat {
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
#[derive(Debug, thiserror::Error)]
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
