use chrono_tz::Tz;
use once_cell::sync::OnceCell;
use readyset_errors::{internal_err, ReadySetResult};
use readyset_sql::ast::SqlIdentifier;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, LazyLock, RwLock};

#[derive(Default, Clone, Debug)]
pub struct UpstreamSystemProperties {
    pub search_path: Vec<SqlIdentifier>,
    pub timezone_name: SqlIdentifier,
}

impl fmt::Display for UpstreamSystemProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "schema_search_path: \"{:?}\"; timezone_name: \"{:?}\";",
            self.search_path, self.timezone_name
        )
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

static SESSION_VARIABLES_REPO: LazyLock<Arc<RwLock<HashMap<SqlIdentifier, SqlIdentifier>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

pub fn update_session_variable(
    var_name: SqlIdentifier,
    value: SqlIdentifier,
) -> ReadySetResult<()> {
    match SESSION_VARIABLES_REPO.write() {
        Ok(mut guard) => {
            guard.insert(var_name, value);
            Ok(())
        }
        Err(_) => Err(internal_err!("Internal lock is poisoned")),
    }
}

pub fn get_session_variable(var_name: &SqlIdentifier) -> ReadySetResult<Option<SqlIdentifier>> {
    match SESSION_VARIABLES_REPO.read() {
        Ok(guard) => Ok(guard.get(var_name).cloned()),
        Err(_) => Err(internal_err!("Internal lock is poisoned")),
    }
}

pub fn get_session_variables(
    var_names: &[SqlIdentifier],
) -> ReadySetResult<Vec<Option<SqlIdentifier>>> {
    match SESSION_VARIABLES_REPO.read() {
        Ok(guard) => {
            let mut result = Vec::with_capacity(var_names.len());
            for var_name in var_names {
                result.push(guard.get(var_name).cloned());
            }
            Ok(result)
        }
        Err(_) => Err(internal_err!("Internal lock is poisoned")),
    }
}
