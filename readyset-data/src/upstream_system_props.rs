use chrono_tz::Tz;
use once_cell::sync::OnceCell;
use readyset_errors::{internal, internal_err, ReadySetResult};
use readyset_sql::ast::SqlIdentifier;
use std::fmt;

/// MySQL's default value for `group_concat_max_len`.
pub const DEFAULT_GROUP_CONCAT_MAX_LEN: usize = 1024;

#[derive(Clone, Debug)]
pub struct UpstreamSystemProperties {
    pub search_path: Vec<SqlIdentifier>,
    pub timezone_name: SqlIdentifier,
    pub lower_case_database_names: bool,
    pub lower_case_table_names: bool,
    pub db_version: String,
    pub group_concat_max_len: usize,
}

impl Default for UpstreamSystemProperties {
    fn default() -> Self {
        Self {
            search_path: Vec::new(),
            timezone_name: SqlIdentifier::default(),
            lower_case_database_names: false,
            lower_case_table_names: false,
            db_version: String::new(),
            group_concat_max_len: DEFAULT_GROUP_CONCAT_MAX_LEN,
        }
    }
}

impl fmt::Display for UpstreamSystemProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

pub const DEFAULT_TIMEZONE_NAME: &str = "Etc/UTC";

static SYSTEM_TIME_ZONE: OnceCell<Tz> = OnceCell::new();
/// The upstream's `group_concat_max_len` value, read once at startup from the
/// upstream database's *global* setting. Per-session changes (e.g.
/// `SET SESSION group_concat_max_len = 4096`) on the upstream are NOT reflected
/// here — Readyset always uses the global value that was in effect at startup.
static GROUP_CONCAT_MAX_LEN: OnceCell<usize> = OnceCell::new();

fn init_property<T>(cell: &OnceCell<T>, value: T) -> ReadySetResult<()>
where
    T: PartialEq + Clone,
{
    if value.clone() == *cell.get_or_init(|| value) {
        Ok(())
    } else {
        internal!("upstream system properties are already set!");
    }
}

fn init_timezone(props: &UpstreamSystemProperties) -> ReadySetResult<()> {
    let tz = props.timezone_name.parse::<Tz>().map_err(|e| {
        internal_err!(
            "Error parsing system timezone name \"{}\": {}",
            props.timezone_name,
            e
        )
    })?;
    init_property(&SYSTEM_TIME_ZONE, tz)
}

pub fn init_system_props(props: &UpstreamSystemProperties) -> ReadySetResult<()> {
    init_timezone(props)?;
    init_property(&GROUP_CONCAT_MAX_LEN, props.group_concat_max_len)?;
    Ok(())
}

pub fn get_system_timezone() -> ReadySetResult<Tz> {
    SYSTEM_TIME_ZONE
        .get()
        .ok_or_else(|| internal_err!("System timezone is not set"))
        .cloned()
}

pub fn get_group_concat_max_len() -> usize {
    GROUP_CONCAT_MAX_LEN
        .get()
        .copied()
        .unwrap_or(DEFAULT_GROUP_CONCAT_MAX_LEN)
}
