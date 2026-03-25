use chrono::FixedOffset;
use readyset_data::TimestampTz;
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{SetStatement, SqlIdentifier, SqlQuery};

use crate::backend::noria_connector;

/// Represents the session timezone configuration.
///
/// MySQL's `time_zone` session variable can be set to:
/// - `"SYSTEM"` — use the server's local timezone
/// - A fixed offset like `"+05:00"` or `"-08:00"`
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SessionTimezone {
    /// Use the server's local timezone (corresponds to `SET time_zone = 'SYSTEM'`).
    #[default]
    System,
    /// A fixed UTC offset (e.g., `SET time_zone = '+05:00'`).
    FixedOffset(FixedOffset),
}

impl SessionTimezone {
    /// Convert a [`TimestampTz`] to this session timezone.
    #[inline]
    pub fn convert(&self, ts: &TimestampTz) -> TimestampTz {
        match self {
            Self::System => ts.to_local(),
            Self::FixedOffset(tz) => ts.to_fixed_offset(tz),
        }
    }
}

/// How we should be handling a SQL `SET` statement.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetBehavior {
    /// This `SET` statement is unsupported and should error.
    pub unsupported: bool,
    /// This `SET` statement should be proxied upstream verbatim.
    pub proxy: bool,
    /// This `SET` statement turns `autocommit` flag being set either on or off.
    pub set_autocommit: Option<bool>,
    /// This `SET` statement changes the current schema search path.
    pub set_search_path: Option<Vec<SqlIdentifier>>,
    /// This `SET` statement changes the encoding to be used for results. Corresponds to `SET
    /// @@character_set_results` in MySQL or `SET NAMES` in Postgres or MySQL.
    pub set_results_encoding: Option<readyset_data::encoding::Encoding>,
    /// This `SET` statement changes the session timezone for TIMESTAMP conversions.
    /// `Some(tz)` means change to the given timezone.
    /// `None` means no change (this SET didn't touch time_zone).
    pub set_timezone: Option<SessionTimezone>,
}

impl SetBehavior {
    pub fn unsupported(mut self, unsupported: bool) -> Self {
        self.unsupported = self.unsupported || unsupported;
        self
    }

    pub fn set_autocommit(mut self, autocommit: bool) -> Self {
        self.set_autocommit = Some(autocommit);
        self
    }

    pub fn set_search_path(mut self, search_path: Vec<SqlIdentifier>) -> Self {
        self.set_search_path = Some(search_path);
        self
    }

    pub fn set_results_encoding(
        mut self,
        encoding: Option<readyset_data::encoding::Encoding>,
    ) -> Self {
        if let Some(encoding) = encoding {
            self.set_results_encoding = Some(encoding);
        } else {
            self.unsupported = true;
        }
        self
    }

    pub fn set_timezone(mut self, tz: SessionTimezone) -> Self {
        self.set_timezone = Some(tz);
        self
    }
}

impl Default for SetBehavior {
    fn default() -> Self {
        Self {
            unsupported: false,
            proxy: true,
            set_autocommit: None,
            set_search_path: None,
            set_results_encoding: None,
            set_timezone: None,
        }
    }
}

/// A trait describing the behavior of how specific queries should be handled by a noria-client
/// [`Backend`].
pub trait QueryHandler: Sized + Send {
    /// Whether or not a given query requires fallback.
    fn requires_fallback(query: &SqlQuery) -> bool;

    /// Provides a default response for the given query.
    /// This should only be used in cases where the query can't be executed by ReadySet
    /// and there is no fallback mechanism enabled or we deliberately want to return a default
    /// response based on the rules from [`return_default_response`].
    fn default_response(query: &SqlQuery) -> ReadySetResult<noria_connector::QueryResult<'static>>;

    /// Whether or not a given query should return a default response.
    fn return_default_response(query: &SqlQuery) -> bool;

    /// Classify the given SET statement based on how we should handle it
    ///
    /// See the documentation of [`SetStatement`] for more information.
    fn handle_set_statement(stmt: &SetStatement) -> SetBehavior;
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::NaiveDate;
    use readyset_data::TimestampTz;

    use super::*;

    #[test]
    fn convert_fixed_offset() {
        // 2024-06-15 12:00 UTC → +05:00 → 17:00
        let ts = TimestampTz::from(
            NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
        );
        let tz = SessionTimezone::FixedOffset(FixedOffset::east_opt(5 * 3600).unwrap());
        let result = tz.convert(&ts);
        assert_eq!(
            result.to_chrono().naive_local(),
            NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_opt(17, 0, 0)
                .unwrap()
        );
        assert!(!result.has_timezone());
    }

    #[test]
    fn convert_fixed_offset_preserves_subsecond_digits() {
        let ts = TimestampTz::from_str("2024-06-15 12:00:00.123").unwrap();
        assert_eq!(ts.subsecond_digits(), 3);
        let tz = SessionTimezone::FixedOffset(FixedOffset::east_opt(-5 * 3600).unwrap());
        let result = tz.convert(&ts);
        assert_eq!(result.subsecond_digits(), 3);
    }

    #[test]
    fn convert_system_clears_offset_metadata() {
        let ts = TimestampTz::from_str("2024-06-15 12:00:00+05:00").unwrap();
        assert!(ts.has_timezone());
        let result = SessionTimezone::System.convert(&ts);
        assert!(!result.has_timezone());
    }
}
