use chrono::FixedOffset;
use readyset_data::TimestampTz;
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{SetStatement, SqlIdentifier, SqlQuery};

use crate::backend::noria_connector;

/// Maximum offset east of UTC accepted by [`parse_fixed_offset`]: `+14:00`.
/// Matches MySQL's `@@time_zone` upper bound; conservative for PostgreSQL,
/// which accepts a wider range. TODO(psql): widen or take a `Dialect`/bounds
/// parameter once eval-side support lands.
const MAX_TZ_OFFSET_EAST_SECS: i32 = 14 * 3600;
/// Minimum offset east of UTC accepted by [`parse_fixed_offset`]: `-13:59`.
/// See [`MAX_TZ_OFFSET_EAST_SECS`] for the dialect rationale.
const MIN_TZ_OFFSET_EAST_SECS: i32 = -(13 * 3600 + 59 * 60);

/// Represents the session timezone configuration.
///
/// MySQL's `time_zone` session variable can be set to:
/// - `"SYSTEM"` — use the server's local timezone
/// - A fixed offset like `"+05:00"` or `"-08:00"`
/// - An IANA timezone name like `"US/Eastern"` or `"America/New_York"`
///
/// IANA timezones have DST rules, so the UTC offset depends on the specific timestamp
/// being converted — it cannot be resolved to a fixed offset at `SET` time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SessionTimezone {
    /// Use the server's local timezone (corresponds to `SET time_zone = 'SYSTEM'`).
    #[default]
    System,
    /// A fixed UTC offset (e.g., `SET time_zone = '+05:00'`).
    FixedOffset(FixedOffset),
    /// An IANA named timezone (e.g., `SET time_zone = 'US/Eastern'`).
    Named(chrono_tz::Tz),
}

impl SessionTimezone {
    /// Convert a [`TimestampTz`] to this session timezone.
    #[inline]
    pub fn convert(&self, ts: &TimestampTz) -> TimestampTz {
        match self {
            Self::System => ts.to_local(),
            Self::FixedOffset(tz) => ts.to_fixed_offset(tz),
            Self::Named(tz) => ts.to_tz(tz),
        }
    }

    /// Whether this timezone is permanently equivalent to UTC.
    ///
    /// Used as a safety gate while dataflow-expression eval lacks a
    /// `SessionTimezone` parameter: only UTC sessions match the wire
    /// writer's UTC-wallclock passthrough, so non-UTC sessions must be
    /// rejected (or proxied) rather than silently served from cache.
    /// `System` is treated as non-UTC because we don't know the host TZ.
    ///
    /// Named zones are checked against an explicit allowlist of IANA
    /// names that are *permanently* UTC offset 0 with no DST, rather
    /// than checking the offset at the current instant — a zone like
    /// `Europe/London` is UTC in winter and UTC+1 in summer, and a
    /// runtime offset check would silently flip the gate twice a year.
    pub fn is_utc(&self) -> bool {
        use chrono_tz::Tz;
        match self {
            // `FixedOffset` is a static, DST-free offset; its
            // `local_minus_utc` is immutable for the lifetime of the
            // value, so this is safe.
            Self::FixedOffset(tz) => tz.local_minus_utc() == 0,
            Self::Named(tz) => matches!(
                tz,
                Tz::UTC
                    | Tz::UCT
                    | Tz::Universal
                    | Tz::Zulu
                    | Tz::GMT
                    | Tz::GMT0
                    | Tz::GMTPlus0
                    | Tz::GMTMinus0
                    | Tz::Greenwich
                    | Tz::Etc__UTC
                    | Tz::Etc__UCT
                    | Tz::Etc__Universal
                    | Tz::Etc__Zulu
                    | Tz::Etc__GMT
                    | Tz::Etc__GMT0
                    | Tz::Etc__GMTPlus0
                    | Tz::Etc__GMTMinus0
                    | Tz::Etc__Greenwich
            ),
            Self::System => false,
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

/// Parse a session-timezone string into a [`SessionTimezone`].
///
/// Accepts:
/// - The literal `"SYSTEM"` (case-insensitive) → [`SessionTimezone::System`].
/// - Fixed offsets in `±HH:MM` form, range
///   [`MIN_TZ_OFFSET_EAST_SECS`]`..=`[`MAX_TZ_OFFSET_EAST_SECS`].
/// - IANA names matched case-insensitively (e.g. `"US/Eastern"`,
///   `"etc/utc"`) → [`SessionTimezone::Named`].
///
/// Returns `None` for unparseable strings.
pub fn parse_timezone(s: &str) -> Option<SessionTimezone> {
    if s.eq_ignore_ascii_case("SYSTEM") {
        return Some(SessionTimezone::System);
    }

    if let Some(offset) = parse_fixed_offset(s) {
        return Some(SessionTimezone::FixedOffset(offset));
    }

    s.parse::<chrono_tz::Tz>()
        .ok()
        .or_else(|| {
            chrono_tz::TZ_VARIANTS
                .iter()
                .find(|tz| tz.name().eq_ignore_ascii_case(s))
                .copied()
        })
        .map(SessionTimezone::Named)
}

/// Parse a fixed-offset timezone string like `"+05:00"` into a [`FixedOffset`].
fn parse_fixed_offset(s: &str) -> Option<FixedOffset> {
    let (sign, rest) = match s.as_bytes().first() {
        Some(b'+') => (1i32, &s[1..]),
        Some(b'-') => (-1i32, &s[1..]),
        _ => return None,
    };

    let (hours_str, minutes_str) = rest.split_once(':')?;
    // `u32::from_str` accepts a leading `+`, so without this guard
    // `"++05:00"` and `"+-14:00"` would parse as `+05:00` / `+14:00`,
    // bypassing the sign-byte discrimination above. Reject any non-digit
    // byte in either field.
    if hours_str.is_empty()
        || minutes_str.is_empty()
        || !hours_str.bytes().all(|b| b.is_ascii_digit())
        || !minutes_str.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let hours: u32 = hours_str.parse().ok()?;
    let minutes: u32 = minutes_str.parse().ok()?;

    if minutes > 59 {
        return None;
    }

    let magnitude = i32::try_from(hours)
        .ok()?
        .checked_mul(3600)?
        .checked_add(minutes as i32 * 60)?;
    let east_secs = sign.checked_mul(magnitude)?;
    if !(MIN_TZ_OFFSET_EAST_SECS..=MAX_TZ_OFFSET_EAST_SECS).contains(&east_secs) {
        return None;
    }

    FixedOffset::east_opt(east_secs)
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

    #[test]
    fn convert_named_dst() {
        let tz = SessionTimezone::Named(chrono_tz::US::Eastern);

        // 2024-06-15 12:00 UTC → US/Eastern (EDT, UTC-4) → 08:00
        let summer = TimestampTz::from(
            NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
        );
        let result = tz.convert(&summer);
        assert_eq!(
            result.to_chrono().naive_local(),
            NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap()
        );
        assert!(!result.has_timezone());

        // 2024-01-15 12:00 UTC → US/Eastern (EST, UTC-5) → 07:00
        let winter = TimestampTz::from(
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
        );
        let result = tz.convert(&winter);
        assert_eq!(
            result.to_chrono().naive_local(),
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(7, 0, 0)
                .unwrap()
        );
        assert!(!result.has_timezone());
    }

    #[test]
    fn convert_named_preserves_subsecond_digits() {
        let ts = TimestampTz::from_str("2024-06-15 12:00:00.123").unwrap();
        assert_eq!(ts.subsecond_digits(), 3);
        let tz = SessionTimezone::Named(chrono_tz::US::Eastern);
        let result = tz.convert(&ts);
        assert_eq!(result.subsecond_digits(), 3);
    }

    #[test]
    fn is_utc_allowlist() {
        use chrono_tz::Tz;

        // Every chrono_tz variant that is permanently UTC offset 0 with no DST.
        let utc_named = [
            Tz::UTC,
            Tz::UCT,
            Tz::Universal,
            Tz::Zulu,
            Tz::GMT,
            Tz::GMT0,
            Tz::GMTPlus0,
            Tz::GMTMinus0,
            Tz::Greenwich,
            Tz::Etc__UTC,
            Tz::Etc__UCT,
            Tz::Etc__Universal,
            Tz::Etc__Zulu,
            Tz::Etc__GMT,
            Tz::Etc__GMT0,
            Tz::Etc__GMTPlus0,
            Tz::Etc__GMTMinus0,
            Tz::Etc__Greenwich,
        ];
        for tz in utc_named {
            assert!(SessionTimezone::Named(tz).is_utc(), "{tz:?} should be UTC");
        }

        // Spot check zones that are NOT permanently UTC. `Europe::London`
        // is UTC in winter but UTC+1 in summer — a runtime offset check
        // would flip the gate twice a year. `Etc::GMTPlus1` is UTC-1 (the
        // IANA reverse-sign convention).
        for tz in [
            Tz::US__Eastern,
            Tz::Europe__London,
            Tz::Asia__Tokyo,
            Tz::Etc__GMTPlus1,
            Tz::Etc__GMTMinus1,
        ] {
            assert!(
                !SessionTimezone::Named(tz).is_utc(),
                "{tz:?} should not be UTC"
            );
        }

        assert!(SessionTimezone::FixedOffset(FixedOffset::east_opt(0).unwrap()).is_utc());
        assert!(!SessionTimezone::FixedOffset(FixedOffset::east_opt(3600).unwrap()).is_utc());
        assert!(!SessionTimezone::System.is_utc());
    }

    fn fixed(secs: i32) -> SessionTimezone {
        SessionTimezone::FixedOffset(FixedOffset::east_opt(secs).unwrap())
    }

    #[test]
    fn parse_timezone_fixed_offsets() {
        assert_eq!(parse_timezone("+00:00"), Some(fixed(0)));
        assert_eq!(parse_timezone("+05:00"), Some(fixed(18000)));
        assert_eq!(parse_timezone("-05:00"), Some(fixed(-18000)));
        assert_eq!(parse_timezone("+05:30"), Some(fixed(19800)));
        assert_eq!(parse_timezone("-05:30"), Some(fixed(-19800)));
        assert_eq!(parse_timezone("+13:00"), Some(fixed(46800)));
    }

    #[test]
    fn parse_timezone_system() {
        assert_eq!(parse_timezone("SYSTEM"), Some(SessionTimezone::System));
        assert_eq!(parse_timezone("system"), Some(SessionTimezone::System));
    }

    #[test]
    fn parse_timezone_named() {
        use SessionTimezone::Named;
        assert_eq!(
            parse_timezone("US/Eastern"),
            Some(Named(chrono_tz::US::Eastern))
        );
        assert_eq!(
            parse_timezone("America/New_York"),
            Some(Named(chrono_tz::America::New_York))
        );
        assert_eq!(parse_timezone("UTC"), Some(Named(chrono_tz::UTC)));
    }

    /// MySQL's IANA timezone name lookup is case-insensitive; ours must match.
    #[test]
    fn parse_timezone_named_case_insensitive() {
        use SessionTimezone::Named;
        assert_eq!(
            parse_timezone("us/eastern"),
            Some(Named(chrono_tz::US::Eastern))
        );
        assert_eq!(
            parse_timezone("AMERICA/new_york"),
            Some(Named(chrono_tz::America::New_York))
        );
        assert_eq!(
            parse_timezone("Europe/LONDON"),
            Some(Named(chrono_tz::Europe::London))
        );
        assert_eq!(parse_timezone("utc"), Some(Named(chrono_tz::UTC)));
        assert_eq!(
            parse_timezone("etc/gmt-5"),
            Some(Named(chrono_tz::Etc::GMTMinus5))
        );
        assert_eq!(
            parse_timezone("america/port_of_spain"),
            Some(Named(chrono_tz::America::Port_of_Spain))
        );
    }

    #[test]
    fn parse_timezone_invalid() {
        assert_eq!(parse_timezone(""), None);
        assert_eq!(parse_timezone("abc"), None);
        assert_eq!(parse_timezone("+14:00"), Some(fixed(50400)));
        assert_eq!(parse_timezone("+14:01"), None);
        assert_eq!(parse_timezone("+00:60"), None);
        // Range is -13:59 to +14:00 (matches MySQL; conservative for PostgreSQL)
        assert_eq!(parse_timezone("-14:00"), None);
        assert_eq!(parse_timezone("-13:59"), Some(fixed(-50340)));
        assert_eq!(parse_timezone("narnia/cair_paravel"), None);
        // No whitespace trimming
        assert_eq!(parse_timezone("us/eastern "), None);
        assert_eq!(parse_timezone(" us/eastern"), None);
        // Embedded NUL must not match a valid zone
        assert_eq!(parse_timezone("utc\0"), None);
    }

    /// Hour fields large enough to overflow `i32 * 3600` must be rejected, not
    /// wrap silently in release nor panic in debug.
    #[test]
    fn parse_timezone_offset_hour_overflow() {
        assert_eq!(parse_timezone("+99999999:00"), None);
        assert_eq!(parse_timezone("-99999999:00"), None);
        assert_eq!(parse_timezone("+4294967295:00"), None);
        // Hours value where `checked_mul(3600)` succeeds but adding any
        // minutes overflows `i32`: i32::MAX / 3600 == 596523.
        assert_eq!(parse_timezone("+596523:00"), None);
        assert_eq!(parse_timezone("+596523:59"), None);
    }

    /// `u32::from_str` itself accepts a leading `+`, so without an explicit
    /// guard the hour parse would silently treat `"++05:00"` as `"+05:00"`
    /// — bypassing the sign-byte discrimination and accepting nonsense like
    /// `"+-14:00"`. Reject any non-digit byte in the hours/minutes fields.
    #[test]
    fn parse_timezone_rejects_extra_sign_or_whitespace_in_fields() {
        assert_eq!(parse_timezone("++05:00"), None);
        assert_eq!(parse_timezone("+-14:00"), None);
        assert_eq!(parse_timezone("--05:00"), None);
        assert_eq!(parse_timezone("-+05:00"), None);
        assert_eq!(parse_timezone("+ 05:00"), None);
        assert_eq!(parse_timezone("+\t05:00"), None);
        assert_eq!(parse_timezone("+05: 00"), None);
        assert_eq!(parse_timezone("+05:+0"), None);
    }

    /// Property tests for `parse_timezone`. These live with the function so
    /// any caller (MySQL or PostgreSQL) gets the same coverage; per-dialect
    /// `handle_set_*` properties stay in the dialect crates because they
    /// bind that crate's `QueryHandler` impl.
    mod proptest_parse_timezone {
        use proptest::prelude::*;

        use super::*;

        /// Fixed-offset strings that the parser must accept, with their
        /// expected seconds-east-of-UTC. Range matches MySQL (`-13:59..=+14:00`);
        /// conservative for PostgreSQL.
        fn valid_offset_strategy() -> impl Strategy<Value = (String, i32)> {
            prop_oneof![
                (any::<bool>(), 0u32..=13, 0u32..=59).prop_map(|(positive, h, m)| {
                    let sign_char = if positive { '+' } else { '-' };
                    let sign_val: i32 = if positive { 1 } else { -1 };
                    (
                        format!("{sign_char}{h:02}:{m:02}"),
                        sign_val * (h as i32 * 3600 + m as i32 * 60),
                    )
                }),
                Just(("+14:00".to_string(), 14 * 3600)),
            ]
        }

        fn named_timezone_strategy() -> impl Strategy<Value = String> {
            prop_oneof![
                Just("US/Eastern".to_string()),
                Just("US/Central".to_string()),
                Just("US/Pacific".to_string()),
                Just("America/New_York".to_string()),
                Just("America/Chicago".to_string()),
                Just("America/Los_Angeles".to_string()),
                Just("Europe/London".to_string()),
                Just("Europe/Berlin".to_string()),
                Just("Europe/Paris".to_string()),
                Just("Asia/Tokyo".to_string()),
                Just("Asia/Kolkata".to_string()),
                Just("Asia/Shanghai".to_string()),
                Just("Australia/Sydney".to_string()),
                Just("Pacific/Auckland".to_string()),
                Just("Africa/Cairo".to_string()),
                Just("Canada/Eastern".to_string()),
                Just("Etc/GMT".to_string()),
                Just("Etc/GMT+5".to_string()),
                Just("Etc/UTC".to_string()),
                Just("UTC".to_string()),
            ]
        }

        /// Out-of-range or malformed offsets that the parser must reject.
        fn invalid_offset_strategy() -> impl Strategy<Value = String> {
            prop_oneof![
                (any::<bool>(), 15u32..=23, 0u32..=59).prop_map(|(pos, h, m)| {
                    format!("{}{h:02}:{m:02}", if pos { '+' } else { '-' })
                }),
                // ±14:MM where MM > 0 (both exceed MySQL's valid range)
                (any::<bool>(), 1u32..=59).prop_map(|(pos, m)| {
                    format!("{}{:02}:{m:02}", if pos { '+' } else { '-' }, 14)
                }),
                (any::<bool>(), 0u32..=13, 60u32..=99).prop_map(|(pos, h, m)| {
                    format!("{}{h:02}:{m:02}", if pos { '+' } else { '-' })
                }),
            ]
        }

        proptest! {
            #[test]
            fn parse_valid_offset_returns_correct_seconds((s, expected_secs) in valid_offset_strategy()) {
                let result = parse_timezone(&s);
                let expected_offset = FixedOffset::east_opt(expected_secs).unwrap();
                prop_assert_eq!(result, Some(SessionTimezone::FixedOffset(expected_offset)));
            }

            #[test]
            fn parse_valid_offset_result_is_fixed_offset((s, _) in valid_offset_strategy()) {
                match parse_timezone(&s) {
                    Some(SessionTimezone::FixedOffset(_)) => {}
                    other => panic!("expected FixedOffset, got {other:?}"),
                }
            }

            #[test]
            fn parse_invalid_offset_returns_none(s in invalid_offset_strategy()) {
                prop_assert_eq!(parse_timezone(&s), None);
            }

            #[test]
            fn parse_named_timezone_returns_named(s in named_timezone_strategy()) {
                match parse_timezone(&s) {
                    Some(SessionTimezone::Named(_)) => {}
                    other => panic!("expected Named, got {other:?} for {s:?}"),
                }
            }

            /// IANA name lookup must be case-insensitive: any casing variant
            /// of a canonical name resolves to the same `SessionTimezone`.
            #[test]
            fn parse_named_timezone_case_insensitive(
                canonical in named_timezone_strategy(),
                flips in any::<u64>(),
            ) {
                let variant: String = canonical
                    .bytes()
                    .enumerate()
                    .map(|(i, b)| {
                        if b.is_ascii_alphabetic() && (flips >> (i % 64)) & 1 == 1 {
                            (b ^ 0x20) as char
                        } else {
                            b as char
                        }
                    })
                    .collect();
                prop_assert_eq!(parse_timezone(&canonical), parse_timezone(&variant));
            }
        }

        #[test]
        fn parse_system_case_insensitive() {
            for s in ["SYSTEM", "system", "System", "sYsTeM"] {
                assert_eq!(
                    parse_timezone(s),
                    Some(SessionTimezone::System),
                    "{s:?} should parse as SYSTEM"
                );
            }
        }
    }
}
