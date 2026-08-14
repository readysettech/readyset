use std::fmt::{self, Debug, Display, Result};

use chrono_tz::Tz;
use once_cell::sync::{Lazy, OnceCell};
use tracing::warn;

use readyset_sql::ast::SqlIdentifier;

/// MySQL's default value for `group_concat_max_len`.
pub const DEFAULT_GROUP_CONCAT_MAX_LEN: usize = 1024;

pub const DEFAULT_TIMEZONE_NAME: &str = "Etc/UTC";

/// A database server's default collation and its associated character set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpstreamCollation {
    pub id: u16,
    pub character_set_name: String,
    pub collation_name: String,
}

/// Properties read once from the upstream database at startup and shared process-wide via
/// [`system_props`].  Per-session changes on the upstream are not reflected here.
#[derive(Clone, Debug)]
pub struct UpstreamSystemProperties {
    pub search_path: Vec<SqlIdentifier>,
    pub timezone: Tz,
    pub lower_case_database_names: bool,
    pub lower_case_table_names: bool,
    pub db_version: String,
    pub group_concat_max_len: usize,
    pub server_default_collation: Option<UpstreamCollation>,
}

impl Default for UpstreamSystemProperties {
    fn default() -> Self {
        Self {
            search_path: Vec::new(),
            timezone: Tz::UTC,
            lower_case_database_names: false,
            lower_case_table_names: false,
            db_version: String::new(),
            group_concat_max_len: DEFAULT_GROUP_CONCAT_MAX_LEN,
            server_default_collation: None,
        }
    }
}

impl Display for UpstreamSystemProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result {
        Debug::fmt(self, f)
    }
}

/// Parses an upstream timezone name, falling back to UTC (with a warning) if it can't be parsed.
/// The upstream value is virtually always a valid IANA name; the fallback keeps a bad value from
/// blocking every other property.
pub fn parse_upstream_timezone(name: &str) -> Tz {
    name.parse::<Tz>().unwrap_or_else(|e| {
        warn!(timezone = %name, error = %e, "Could not parse upstream timezone; using UTC");
        Tz::UTC
    })
}

static SYSTEM_PROPS: OnceCell<UpstreamSystemProperties> = OnceCell::new();

pub fn init_system_props(props: &UpstreamSystemProperties) {
    let _ = SYSTEM_PROPS.set(props.clone());
}

pub fn system_props() -> &'static UpstreamSystemProperties {
    static DEFAULT: Lazy<UpstreamSystemProperties> = Lazy::new(UpstreamSystemProperties::default);
    SYSTEM_PROPS.get().unwrap_or(&DEFAULT)
}

/// The upstream's `group_concat_max_len`.
pub fn group_concat_max_len() -> usize {
    system_props().group_concat_max_len
}
