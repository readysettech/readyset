use chrono_tz::Tz;
use once_cell::sync::OnceCell;
use readyset_errors::{internal, internal_err, ReadySetResult};
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
static LOWER_CASE_DATABASE_NAMES: OnceCell<bool> = OnceCell::new();
static LOWER_CASE_TABLE_NAMES: OnceCell<bool> = OnceCell::new();

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
    init_property(&LOWER_CASE_DATABASE_NAMES, props.lower_case_database_names)?;
    init_property(&LOWER_CASE_TABLE_NAMES, props.lower_case_table_names)?;
    Ok(())
}

pub fn get_system_timezone() -> ReadySetResult<Tz> {
    SYSTEM_TIME_ZONE
        .get()
        .ok_or_else(|| internal_err!("System timezone is not set"))
        .cloned()
}

fn get<T>(cell: &OnceCell<T>) -> T
where
    T: Clone + Default,
{
    cell.get().cloned().unwrap_or_default()
}

pub fn lower_case_database_names() -> bool {
    get(&LOWER_CASE_DATABASE_NAMES)
}

pub fn lower_case_table_names() -> bool {
    get(&LOWER_CASE_TABLE_NAMES)
}
