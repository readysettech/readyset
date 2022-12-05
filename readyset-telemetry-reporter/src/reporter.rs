#![cfg_attr(any(test, feature = "test-util"), allow(dead_code, unused_imports))]
//! TelemetryReporter
//! The Telemetry Reporter acts asynchronously by spawning a background task that listens for
//! [`TelemetryEvent`]s sent from [`TelemetryReporter`]s. When it receives one, it forwards the
//! request to Segment.
#[cfg(any(test, feature = "test-util"))]
use std::collections::HashMap;
#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backoff::ExponentialBackoffBuilder;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use lazy_static::lazy_static;
use readyset_tracing::{debug, info, trace, warn};
use readyset_version::COMMIT_ID;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, RequestBuilder, Response, StatusCode, Url};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{oneshot, Mutex};
use tokio::time::Interval;
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

impl From<String> for DeploymentEnv {
    fn from(s: String) -> Self {
        match s.as_str() {
            "installer_compose" => DeploymentEnv::InstallerCompose,
            "eks" => DeploymentEnv::Eks,
            "helm" => DeploymentEnv::Helm,
            _ => DeploymentEnv::Unknown,
        }
    }
}

#[async_trait]
pub trait PeriodicReport: Send + Sync {
    async fn report(&self) -> Result<Vec<(TelemetryEvent, Telemetry)>>;
}

pub type PeriodicReporter = Arc<dyn PeriodicReport>;

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

    /// Acknowledge that we shutdown gracefully
    shutdown_ack_tx: Option<oneshot::Sender<()>>,

    /// Deployment environment, e.g. container orchestrator framework, if any
    deployment_env: DeploymentEnv,

    /// Zero or many periodic reporters that can collect and send metrics periodically
    periodic_reporters: Arc<Mutex<Vec<PeriodicReporter>>>,

    #[cfg(any(test, feature = "test-util"))]
    received_events: Arc<Mutex<HashMap<TelemetryEvent, Vec<Telemetry>>>>,
}

impl TelemetryReporter {
    const PERIODIC_REPORT_INTERVAL: Duration = Duration::from_secs(30);

    pub fn new(
        rx: Receiver<(TelemetryEvent, Telemetry)>,
        api_key: Option<String>,
        shutdown_rx: oneshot::Receiver<()>,
        shutdown_ack_tx: oneshot::Sender<()>,
    ) -> Self {
        // If the api_key is set, use that as the user_id.
        // If not, try to get a machine uid. If that works, anonymize it by hashing it with blake2b,
        // a cryptographically secure hashing library, and use that as the id, otherwise, no
        // id will be set
        // NOTE: The machine id may not be unique across all users, since there may be many virtual
        // machines or corporate images that have the same machine id. Still, this is a decent
        // heuristic for unique users

        let user_id = api_key.or_else(|| machine_uid::get().ok().map(blake2b_string));

        Self {
            rx,
            client: SEGMENT_WRITE_KEY.as_ref().and_then(|k| make_client(k).ok()),
            user_id,
            anonymous_id: Uuid::new_v4().to_string(),
            shutdown_rx,
            shutdown_ack_tx: Some(shutdown_ack_tx),
            deployment_env: std::env::var("DEPLOYMENT_ENV").unwrap_or_default().into(),
            periodic_reporters: Arc::new(Mutex::new(vec![])),
            #[cfg(any(test, feature = "test-util"))]
            received_events: Arc::new(Mutex::new(HashMap::new())),
        }
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
                deployment_env: self.deployment_env,
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
        debug!(?event, ?payload, "sending event");
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
                        | Error::Client(_)
                        | Error::Json(_)) => backoff::Error::Permanent(e),
                    })
            }),
        )
        .await;

        if res.is_err() {
            warn!(?res, ?event, ?payload, "failed to send telemetry");
        }
    }

    #[cfg(not(any(test, feature = "test-util")))]
    async fn process_event(&self, event: TelemetryEvent, payload: &Telemetry) {
        self.send_event(event, payload).await;
    }

    #[cfg(any(test, feature = "test-util"))]
    async fn process_event(&self, event: TelemetryEvent, payload: &Telemetry) {
        let mut received_events = self.received_events.lock().await;
        let entry = received_events.entry(event).or_insert_with(|| vec![]);
        entry.push((*payload).clone());
    }

    pub async fn run(&mut self) {
        let mut interval = tokio::time::interval(Self::PERIODIC_REPORT_INTERVAL);
        loop {
            if !self.run_once(&mut interval).await {
                return;
            }
        }
    }

    /// Returns true if we are still running, false if we should shut down
    async fn run_once(&mut self, interval: &mut Interval) -> bool {
        trace!("TelemetryReporter run_once");
        tokio::select! {
            biased;
            _ = &mut self.shutdown_rx => {
                info!("shutting down telemetry reporter. will attempt to drain in-flight metrics");
                while let Ok((event, telemetry)) = self.rx.try_recv() {
                    debug!(?event, ?telemetry, "TelemetryEvent received");
                    self.process_event(event, &telemetry).await;
                }

                if let Some(shutdown_ack_tx) = self.shutdown_ack_tx.take() {
                    let _ = shutdown_ack_tx.send(());
                } else {
                    trace!("unable to acknowledge shutdown");
                };

                self.rx.close();
                return false;
            }
            Some((event, telemetry)) = Self::maybe_recv_event(&mut self.rx) => {
                self.process_event(event, &telemetry).await;
            }
            _ = interval.tick() => {
                debug!("starting periodic report");
                let periodic_reporters = self.periodic_reporters.lock().await;

                for reporter in periodic_reporters.iter() {
                    if let Ok(report) = reporter.report().await {
                        for (event, telemetry) in report {
                            self.process_event(event, &telemetry).await;
                        }
                    }
                }
            }
        }
        true
    }

    async fn maybe_recv_event(
        rx: &mut Receiver<(TelemetryEvent, Telemetry)>,
    ) -> Option<(TelemetryEvent, Telemetry)> {
        rx.recv().await
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn test_run_once(&mut self, interval: &mut Interval) -> bool {
        self.run_once(interval).await
    }

    pub async fn register_periodic_reporter(&mut self, periodic_reporter: PeriodicReporter) {
        debug!("registering periodic reporter");
        let mut periodic_reporters = self.periodic_reporters.lock().await;
        periodic_reporters.push(periodic_reporter);
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn received_events(&self) -> HashMap<TelemetryEvent, Vec<Telemetry>> {
        self.received_events.lock().await.clone()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn check_event(&self, event: TelemetryEvent) -> Vec<Telemetry> {
        self.received_events
            .lock()
            .await
            .get_mut(&event)
            .cloned()
            .unwrap_or_default()
    }

    /// Does a run() until the provided timeout is reached. Suppresses any errors if we timed out.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn run_timeout(&mut self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, self.run()).await;
    }
}

fn blake2b_string(user_id: String) -> String {
    let mut hasher = Blake2bVar::new(8).expect("8 is a valid output size for Blake2bVar");
    hasher.update(user_id.as_bytes());
    let mut buf = [0u8; 8];
    hasher
        .finalize_variable(&mut buf)
        .expect("8 is a valid output size for Blake2bVar");
    hex::encode(&buf)
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

    struct TestPeriodicReporter {}

    #[async_trait]
    impl PeriodicReport for TestPeriodicReporter {
        async fn report(&self) -> Result<Vec<(TelemetryEvent, Telemetry)>> {
            Ok(vec![(
                TelemetryEvent::QueryParseFailed,
                TelemetryBuilder::new().query_id("test".to_string()).build(),
            )])
        }
    }

    #[tokio::test(start_paused = true)]
    async fn validate_request() {
        std::env::set_var("RS_SEGMENT_WRITE_KEY", "write_key");
        let (telemetry_sender, mut telemetry_reporter) = TelemetryInitializer::test_init().await;

        let (event, telemetry): (TelemetryEvent, Telemetry) =
            (TelemetryEvent::InstallerRun, Default::default());

        assert!(telemetry_sender.send_event(event).is_ok());

        let mut interval = tokio::time::interval(Duration::from_millis(10));
        telemetry_reporter.run_once(&mut interval).await;
        assert_eq!(
            telemetry,
            *telemetry_reporter
                .check_event(TelemetryEvent::InstallerRun)
                .await
                .first()
                .unwrap()
        );

        telemetry_sender.shutdown().await;
        telemetry_reporter.run_once(&mut interval).await;

        assert!(telemetry_sender.send_event(event).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_periodic_reporter() {
        let test_periodic_reporter: PeriodicReporter = Arc::new(TestPeriodicReporter {});
        let (_sender, mut reporter) = TelemetryInitializer::test_init().await;
        reporter
            .register_periodic_reporter(test_periodic_reporter)
            .await;

        let mut interval = tokio::time::interval(Duration::from_nanos(1));
        reporter.run_once(&mut interval).await;

        assert_eq!(
            1,
            reporter
                .check_event(TelemetryEvent::QueryParseFailed)
                .await
                .len()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_shutdown_drain() {
        // Tests that the TelemetryReporter will drain any incoming requests
        let (sender, mut reporter) = TelemetryInitializer::test_init().await;

        // The biased select! will always process the shutdown first, even if we send an event ahead
        // of a shutdown.
        sender
            .send_event(TelemetryEvent::InstallerRun)
            .expect("failed to send event");
        sender.shutdown().await;

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        reporter.run_once(&mut interval).await;

        assert_eq!(
            1,
            reporter
                .check_event(TelemetryEvent::InstallerRun)
                .await
                .len()
        );
    }
}
