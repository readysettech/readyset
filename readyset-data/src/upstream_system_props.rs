use chrono_tz::Tz;
use once_cell::sync::OnceCell;
use readyset_errors::{internal_err, ReadySetResult};
use readyset_sql::ast::SqlIdentifier;
use std::fmt;

#[derive(Default, Clone, Debug)]
pub struct UpstreamSystemProperties {
    pub search_path: Vec<SqlIdentifier>,
    pub timezone_name: SqlIdentifier,
    pub lower_case_database_names: bool,
    pub lower_case_table_names: bool,
}

impl fmt::Display for UpstreamSystemProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

pub const DEFAULT_TIMEZONE_NAME: &str = "Etc/UTC";

static SYSTEM_TIME_ZONE: OnceCell<Tz> = OnceCell::new();

pub fn init_system_props(sys_props: &UpstreamSystemProperties) -> ReadySetResult<()> {
    let timezone_name = sys_props.timezone_name.as_str();
    match SYSTEM_TIME_ZONE.get_or_try_init(|| {
        timezone_name.parse::<Tz>().map_err(|e| {
            internal_err!(
                "Error parsing system timezone name \"{}\": {}",
                timezone_name,
                e
            )
        })
    }) {
        Ok(tz) => {
            if tz.name().eq(timezone_name) {
                Ok(())
            } else {
                Err(internal_err!("System timezone name has already been set"))
            }
        }
        Err(e) => Err(e),
    }
}

pub fn get_system_timezone() -> ReadySetResult<Tz> {
    SYSTEM_TIME_ZONE
        .get()
        .ok_or_else(|| internal_err!("System timezone is not set"))
        .cloned()
}
