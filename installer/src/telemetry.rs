use serde::Serialize;

/// The kinds of telemetry payloads we can send as part of running the installer
#[derive(Debug, Serialize, Clone, Copy)]
pub enum Payload {
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
}
