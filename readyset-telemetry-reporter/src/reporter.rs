//! TelemetryReporter
//! The Telemetry Reporter acts asynchronously by spawning a background task that listens for
//! [`TelemetryEvent`]s sent from [`TelemetryReporter`]s. When it receives one, it forwards the
//! request to Segment.

use std::time::Duration;

use backoff::ExponentialBackoffBuilder;
use lazy_static::lazy_static;
use readyset_version::COMMIT_ID;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, RequestBuilder, Response, StatusCode, Url};
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::error::{ReporterError as Error, ReporterResult as Result};
use crate::telemetry::*;

/// Maximum time to retry sending telemetry payloads before giving up
const TIMEOUT: Duration = Duration::from_secs(2);

/// User agent to use for all telemetry payload requests
const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// URL to report telemetry to
const TELEMETRY_BASE_URL: &str = "https://api.segment.io/v1/";

/// Silently succeed if the client is None.
macro_rules! client {
    ($self: expr) => {
        if let Some(client) = &$self.client {
            client
        } else {
            return Ok(());
        }
    };
}

lazy_static! {
    /// Identifies the ReadySet Segment source. Common between all users.
    /// In priority order, the value is:
    /// - `RS_SEGMENT_WRITE_KEY` from the run-time environment
    /// - `RS_SEGMENT_WRITE_KEY` from the compile-time environment
    /// - `None`
    ///
    /// If `None`, no-op telemetry reporters will be created, which do not send HTTP requests.
    ///
    /// If `Some` but the key doesn't correspond to a valid Segment source, HTTP requests will be
    /// sent and silently ignored.
    static ref SEGMENT_WRITE_KEY: Option<String> = {
        std::env::var("RS_SEGMENT_WRITE_KEY")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| option_env!("RS_SEGMENT_WRITE_KEY").map(str::to_owned))
    };
}

fn telemetry_url(path: &str) -> Url {
    Url::parse(TELEMETRY_BASE_URL).unwrap().join(path).unwrap()
}

#[derive(Debug, Clone, Default)]
enum DeploymentEnv {
    #[default]
    Unknown,

    InstallerCompose,
    Eks,
    Helm,
}

impl From<String> for DeploymentEnv {
    fn from(s: String) -> Self {
        match s.as_str() {
            "InstallerCompose" => DeploymentEnv::InstallerCompose,
            "Eks" => DeploymentEnv::Eks,
            "Helm" => DeploymentEnv::Helm,
            _ => DeploymentEnv::Unknown,
        }
    }
}

pub struct TelemetryReporter {
    client: Option<Client>,

    rx: Receiver<(TelemetryEvent, Telemetry)>,

    /// https://segment.com/docs/connections/spec/identify/#user-id
    user_id: Option<String>,

    /// Per-session generated ID
    /// https://segment.com/docs/connections/spec/identify/#anonymous-id
    anonymous_id: String,

    /// Will shut down the run loop upon receiving a signal
    shutdown_rx: oneshot::Receiver<()>,

    /// Deployment environment, e.g. container orchestrator framework, if any
    deployment_env: DeploymentEnv,
}

impl TelemetryReporter {
    pub fn try_new(
        rx: Receiver<(TelemetryEvent, Telemetry)>,
        api_key: Option<String>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<Self> {
        Ok(Self {
            rx,
            client: SEGMENT_WRITE_KEY
                .as_ref()
                .map(|k| make_client(k).ok())
                .unwrap(),
            user_id: api_key,
            anonymous_id: Uuid::new_v4().to_string(),
            shutdown_rx,
            deployment_env: std::env::var("DEPLOYMENT_ENV").unwrap_or_default().into(),
        })
    }

    fn build_request(
        &self,
        client: &Client,
        event: TelemetryEvent,
        telemetry: &Telemetry,
    ) -> RequestBuilder {
        client.post(telemetry_url("track")).json(&Track {
            user_id: self.user_id.as_ref(),
            anonymous_id: &self.anonymous_id,
            event,
            properties: Properties {
                telemetry,
                commit_id: COMMIT_ID,
                deployment_env: &format!("{:?}", &self.deployment_env),
            },
        })
    }

    /// Send a telemetry payload to Segment. If the initial request fails for a non-permanent
    /// reason (eg, not a 4XX or IO error), this function will retry with an exponential
    /// backoff, timing out at [`TIMEOUT`].
    ///
    /// If this reporter was initialized with an API key equal to [`HARDCODED_API_KEY`], this
    /// function is a no-op.
    async fn send_event_with_payload_inner(
        &self,
        event: TelemetryEvent,
        telemetry: &Telemetry,
    ) -> Result<()> {
        handle_resp(
            self.build_request(client!(self), event, telemetry)
                .send()
                .await?,
        )
        .await
    }

    async fn send_event(&self, event: TelemetryEvent, payload: &Telemetry) {
        let backoff = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(TIMEOUT))
            .build();
        let res = tokio::time::timeout(
            TIMEOUT,
            backoff::future::retry(backoff, move || async move {
                self.send_event_with_payload_inner(event, payload)
                    .await
                    .map_err(|e| match e {
                        Error::Reqwest(_) | Error::Server(_) => e.into(),
                        e @ (Error::InvalidAPIKeyHeader(_)
                        | Error::Unauthorized
                        | Error::HTTPError { .. }
                        | Error::Timeout(_)
                        | Error::Json(_)) => backoff::Error::Permanent(e),
                    })
            }),
        )
        .await;

        if res.is_err() {
            tracing::warn!(?res, ?event, ?payload, "failed to send telemetry");
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = &mut self.shutdown_rx => {
                    tracing::info!("shutting down telemetry reporter");
                    return;
                }
                Some((event, telemetry)) = self.rx.recv() => {
                    self.send_event(event, &telemetry).await;
                }
            }
        }
    }
}

fn make_client(write_key: &str) -> Result<Client> {
    let mut headers = HeaderMap::new();

    // Authenticate using HTTP Basic Auth
    // Username is the Segment write key, password is empty
    // See: https://segment.com/docs/connections/sources/catalog/libraries/server/http-api/#authentication
    headers.insert(AUTHORIZATION, {
        // Append a colon and encode as base64
        let write_key = base64::encode(format!("{write_key}:"));
        let mut authorization = HeaderValue::from_str(&format!("Basic {write_key}"))
            .map_err(Error::InvalidAPIKeyHeader)?;
        authorization.set_sensitive(true);
        authorization
    });

    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    Ok(Client::builder()
        .default_headers(headers)
        .user_agent(APP_USER_AGENT)
        .build()?)
}

pub async fn handle_resp(resp: Response) -> Result<()> {
    match resp.status() {
        status if status.is_success() => Ok(()),
        status if status.is_server_error() => Err(Error::Server(resp.text().await?)),
        StatusCode::UNAUTHORIZED => Err(Error::Unauthorized),
        status => Err(Error::HTTPError {
            status,
            body: resp.text().await?,
        }),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::*;

    #[tokio::test(start_paused = true)]
    async fn validate_request() {
        std::env::set_var("RS_SEGMENT_WRITE_KEY", "write_key");
        let telemetry_sender = TelemetryInitializer::init(false, Some("api-key".to_string())).await;

        let (event, _telemetry): (TelemetryEvent, Telemetry) =
            (TelemetryEvent::InstallerRun, Default::default());

        // TODO(luke): We don't have a good way to inspect the event was received correctly in unit
        // tests currently
        assert!(telemetry_sender.send_event(event).await.is_ok());

        // Allow the event to propagate to the run loop
        tokio::time::sleep(Duration::from_secs(2)).await;

        telemetry_sender.shutdown().await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(telemetry_sender.send_event(event).await.is_err());
    }
}
