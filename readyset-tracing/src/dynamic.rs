use std::sync::{Arc, OnceLock};

use tracing_subscriber::{EnvFilter, reload};

use crate::Error;

// This is wrapped in a `OnceLock` for simplicity, even though the `reload::Handle` should be thread
// safe and has an inner `RwLock`.
#[allow(clippy::type_complexity)]
static RELOAD_HANDLE: OnceLock<Arc<dyn Fn(&str) -> Result<(), reload::Error> + Send + Sync>> =
    OnceLock::new();

pub(crate) fn init<S>(reload_handle: reload::Handle<EnvFilter, S>) -> Result<(), Error>
where
    S: Send + Sync + 'static,
{
    RELOAD_HANDLE
        .set(Arc::new(move |directives: &str| {
            reload_handle.modify(|filter| {
                *filter = tracing_subscriber::filter::Builder::default().parse_lossy(directives);
            })
        }))
        .map_err(|_| Error::ReloadHandleNotInitialized)
}

/// Dynamically set the log level. Pass in a string of log level directives like
/// `info,my_module=debug`, to be parsed by an [`EnvFilter`]; this is the same syntax used for the
/// `LOG_LEVEL` environment variable (or `RUST_LOG` outside Readyset).
pub fn set_log_level(directives: &str) -> Result<(), Error> {
    let f = RELOAD_HANDLE
        .get()
        .ok_or(Error::ReloadHandleNotInitialized)?;
    f(directives).map_err(Into::into)
}
