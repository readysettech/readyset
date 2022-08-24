//! This crate provides a reusable mechanism for reporting telemetry payloads to the
//! ReadySet Segment HTTP source endpoint.
//!
//! In the future, the plan is to extend this with support for things like background reporting,
//! more advanced API token validation, integration with `metrics`, etc.

use std::fmt::Display;
use std::time::Duration;

use backoff::ExponentialBackoffBuilder;
use lazy_static::lazy_static;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, RequestBuilder, Response, StatusCode, Url};
use uuid::Uuid;

mod error;
use crate::error::*;

mod telemetry;
use crate::telemetry::*;
pub use crate::telemetry::{TelemetryBuilder, TelemetryEvent};

/// User agent to use for all telemetry payload requests
const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// The Git commit being built, if this is a CI build
const COMMIT_ID: Option<&str> = option_env!("BUILDKITE_COMMIT");

/// URL to report telemetry to
const TELEMETRY_BASE_URL: &str = "https://api.segment.io/v1/";

/// Maximum time to retry sending telemetry payloads before giving up
const TIMEOUT: Duration = Duration::from_secs(2);

fn telemetry_url(path: &str) -> Url {
    Url::parse(TELEMETRY_BASE_URL).unwrap().join(path).unwrap()
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

/// Result type alias for the telemetry reporter crate
pub type Result<T> = std::result::Result<T, Error>;

/// A struct that can be used to report payloads containing arbitrary telemetry data to the ReadySet
/// telemetry ingress.
///
/// # Examples
///
/// Basic usage
/// ```
/// # use readyset_telemetry_reporter::{Result, TelemetryBuilder, TelemetryEvent};
/// # async fn report_telemetry(api_key: Option<String>) -> Result<()> {
/// use readyset_telemetry_reporter::TelemetryReporter;
///
/// let telemetry_reporter = TelemetryReporter::new(api_key)?;
/// telemetry_reporter.send_event_with_payload(TelemetryEvent::InstallerRun, &TelemetryBuilder::new().server_version("1.2.3").build()).await?;
/// # Ok(())
/// # }
#[derive(Debug, Clone)]
pub struct TelemetryReporter {
    client: Option<Client>,

    /// https://segment.com/docs/connections/spec/identify/#user-id
    user_id: Option<String>,

    /// Per-session generated ID
    /// https://segment.com/docs/connections/spec/identify/#anonymous-id
    anonymous_id: String,
}

async fn handle_resp(resp: Response) -> Result<()> {
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

macro_rules! client {
    ($self: expr) => {
        if let Some(client) = &$self.client {
            client
        } else {
            return Ok(());
        }
    };
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

impl TelemetryReporter {
    /// Construct a new [`TelemetryReporter`] with the given API key.
    pub fn new<K>(api_key: Option<K>) -> Result<Self>
    where
        K: Display,
        for<'a> K: PartialEq<&'a str>,
    {
        let client = SEGMENT_WRITE_KEY
            .as_ref()
            .map(|k| make_client(k))
            .transpose()?;

        Ok(Self {
            client,
            user_id: api_key.map(|k| k.to_string()),
            anonymous_id: Uuid::new_v4().to_string(),
        })
    }

    /// Create a new "no-op" telemetry reporter.
    pub fn new_no_op() -> Self {
        Self {
            client: None,
            user_id: None,
            anonymous_id: Default::default(),
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
            },
        })
    }

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

    /// Send a telemetry payload to Segment. If the initial request fails for a non-permanent
    /// reason (eg, not a 4XX or IO error), this function will retry with an exponential
    /// backoff, timing out at [`TIMEOUT`].
    ///
    /// If this reporter was initialized with an API key equal to [`HARDCODED_API_KEY`], this
    /// function is a no-op.
    pub async fn send_event_with_payload(
        &self,
        event: TelemetryEvent,
        payload: &Telemetry,
    ) -> Result<()> {
        let backoff = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(2)))
            .build();
        tokio::time::timeout(
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
        .await?
    }

    pub async fn send_event(&self, event: TelemetryEvent) -> Result<()> {
        self.send_event_with_payload(event, &TelemetryBuilder::new().build())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_request() {
        std::env::set_var("RS_SEGMENT_WRITE_KEY", "write_key");
        let reporter = TelemetryReporter::new(Some("api_key")).unwrap();
        let req = reporter
            .build_request(
                reporter.client.as_ref().unwrap(),
                TelemetryEvent::InstallerRun,
                &Default::default(),
            )
            .build()
            .unwrap();

        // This header doesn't show up, maybe for security reasons, otherwise we'd check for it
        // assert!(req.headers().contains_key(AUTHORIZATION));

        // At time of writing, Segment accepts messages with any content type
        // Including this anyway because the Segment docs ask for it
        // https://segment.com/docs/connections/sources/catalog/libraries/server/http-api/#content-type
        assert_eq!(req.headers().get(CONTENT_TYPE).unwrap(), "application/json");

        let j: serde_json::Value =
            serde_json::from_slice(req.body().unwrap().as_bytes().unwrap()).unwrap();
        let j = j.as_object().unwrap();

        // Must be true, or Segment will silently ignore the message
        assert!(j.contains_key("event"));
        assert!(j.contains_key("anonymousId") || j.contains_key("userId"));
    }
}
