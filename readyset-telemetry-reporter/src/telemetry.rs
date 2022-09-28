use derive_builder::Builder;
use readyset::query::DeniedQuery;
use serde::Serialize;
use serde_with_macros::skip_serializing_none;

/// Segment Track event types
#[derive(Debug, Serialize, Clone, Copy, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryEvent {
    /// The installer was run. Sent as soon as we have a valid API token
    InstallerRun,

    /// The creation of a new deployment was initiated by the user
    DeploymentStarted,

    /// The creation of a new deployment was initiated by the user
    DeploymentFinished,

    /// The installer exited successfully
    InstallerFinished,

    /// A deployment was torn down
    DeploymentTornDown,

    /// The adapter was run
    AdapterStart,

    /// The adapter exited successfully
    AdapterStop,

    /// The server was run
    ServerStart,

    /// The server exited successfully
    ServerStop,

    /// ReadySet failed to parse a query that upstream was able to
    QueryParseFailed,

    /// CREATE CACHE statement was executed
    CreateCache,

    /// A create statement for a schema was obtained
    Schema,

    /// SHOW CACHES statement was executed
    ShowCaches,

    /// SHOW PROXIED QUERIES statement was executed
    ShowProxiedQueries,

    /// The replicator completed a snapshot operation
    SnapshotComplete,

    /// The adapter connected to the upstream database
    UpstreamConnected,

    /// A new query was run that is proxied (not cached)
    ProxiedQuery,
}

#[derive(Clone, Copy, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentEnv {
    #[default]
    Unknown,

    InstallerCompose,
    Eks,
    Helm,
}

/// ReadySet-specific telemetry. Provide only the fields you need.
///
/// We need to keep publicly documented exactly what telemetry ReadySet gathers from its users.
/// See: https://docs.readyset.io/using/telemetry
///
/// Uses the "infallible builder" pattern to make the API simpler, described
/// [here](https://github.com/colin-kiegel/rust-derive-builder/issues/56#issuecomment-1043671602).
#[skip_serializing_none]
#[derive(Builder, Default, Serialize)]
#[builder(
    default,
    build_fn(private, name = "fallible_build"),
    setter(into, strip_option)
)]
#[derive(Debug)]
pub struct Telemetry {
    db_backend: Option<String>,
    adapter_version: Option<String>,
    server_version: Option<String>,
    query_id: Option<String>,
    schema: Option<String>,
    proxied_query: Option<DeniedQuery>,
}

impl TelemetryBuilder {
    pub fn new() -> Self {
        // future required fields can be set here
        Self::default()
    }

    pub fn build(&self) -> Telemetry {
        self.fallible_build()
            .expect("All required fields set at initialization")
    }
}

/// Wrapper for merging auto- and user-populated telemetry properties into one object
#[derive(Serialize)]
pub struct Properties<'a> {
    /// User-provided properties
    #[serde(flatten)]
    pub telemetry: &'a Telemetry,

    // Properties auto-populated by the reporter
    pub commit_id: &'a str,
    pub deployment_env: DeploymentEnv,
}

/// Top-level wrapper for ReadySet-specifc Segment Track message
///
/// See:
/// - https://segment.com/docs/connections/spec/common
/// - https://segment.com/docs/connections/spec/track
#[skip_serializing_none]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Track<'a> {
    /// https://segment.com/docs/connections/spec/identify/#user-id
    pub user_id: Option<&'a String>,

    /// Per-session generated ID
    /// https://segment.com/docs/connections/spec/identify/#anonymous-id
    pub anonymous_id: &'a String,

    /// https://segment.com/docs/connections/spec/track/#event
    pub event: TelemetryEvent,

    /// https://segment.com/docs/connections/spec/track/#properties
    pub properties: Properties<'a>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_required_fields_added_to_telemetry() {
        // This will panic if any fields have been added to Telemetry
        // (and therefore TelemetryBuilder) that lack defaults.
        let _ = TelemetryBuilder::new().build();
    }
}
