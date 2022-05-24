//! This crate provides a reusable mechanism for reporting arbitrary telemetry payloads to the
//! ReadySet telemetry ingress endpoint.
//!
//! Currently things are all quite simple, and this is basically a wrapper around a
//! [`reqwest::Client`] with a little bit of default values and error handling, but in the future
//! the plan is to extend this with support for things like background reporting, retry with
//! exponential backoff, more advanced API token validation, integration with `metrics`, etc.

use std::fmt::Display;

use lazy_static::lazy_static;
use reqwest::header::{HeaderMap, HeaderValue, InvalidHeaderValue, AUTHORIZATION};
use reqwest::{Client, Response, StatusCode, Url};
use serde::Serialize;
use thiserror::Error;

/// Hardcoded API key used to verify that a user was given permission to use ReadySet, used as a
/// temporary stop-gap solution while we spin up an API key provisioning system.
///
/// If this is given as the API key to [`TelemetryReporter::new`],
/// [`TelemetryReporter::send_payload`] will be a no-op
pub const HARDCODED_API_KEY: &str = "fb1c9ee4bb847f02ec0b5546a6655835";

/// Default URL to report telemetry to. Can be overridden at build-time by setting the
/// `TELEMETRY_BASE_URL` environment variable
const DEFAULT_TELEMETRY_BASE_URL: &str = "https://telemetry.dev.readyset.io/";

fn telemetry_url(path: &str) -> Url {
    TELEMETRY_BASE_URL.join(path).unwrap()
}

lazy_static! {
    static ref TELEMETRY_BASE_URL: Url =
        Url::parse(option_env!("TELEMETRY_BASE_URL").unwrap_or(DEFAULT_TELEMETRY_BASE_URL))
            .unwrap();
}

/// Errors that can occur when reporting telemetry payloads
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid API key: {0}")]
    InvalidAPIKeyHeader(InvalidHeaderValue),

    #[error("Error received from server when sending telemetry payload: {0}")]
    ServerError(String),

    #[error("Invalid API key")]
    Unauthorized,

    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    #[error("Error sending telemetry payload (status {status}): {body}")]
    HTTPError { status: StatusCode, body: String },
}

/// Result type alias for the telemetry reporter crate
pub type Result<T> = std::result::Result<T, Error>;

/// User agent to use for all telemetry payload requests
static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// A struct that can be used to report payloads containing arbitrary telemetry data to the ReadySet
/// telemetry ingress.
///
/// # Examples
///
/// Basic usage
/// ```
/// # use readyset_telemetry_reporter::Result;
/// # async fn report_telemetry(api_key: &str) -> Result<()> {
/// use readyset_telemetry_reporter::TelemetryReporter;
///
/// let telemetry_reporter = TelemetryReporter::new(api_key)?;
/// telemetry_reporter.send_payload("Some arbitrary data goes here").await?;
/// # Ok(())
/// # }
#[derive(Debug, Clone)]
pub struct TelemetryReporter {
    client: Option<Client>,
}

async fn handle_resp(resp: Response) -> Result<()> {
    match resp.status() {
        status if status.is_success() => Ok(()),
        status if status.is_server_error() => Err(Error::ServerError(resp.text().await?)),
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

impl TelemetryReporter {
    /// Construct a new [`TelemetryReporter`] with the given API key.
    ///
    /// If the passed API key is equal to [`HARDCODED_API_KEY`], the [`send_payload`][] function
    /// will always be a no-op.
    ///
    /// [`send_payload`]: TelemetryReporter::send_payload
    pub fn new<K>(api_key: K) -> Result<Self>
    where
        K: Display,
        for<'a> K: PartialEq<&'a str>,
    {
        let client = if api_key == HARDCODED_API_KEY {
            None
        } else {
            let mut headers = HeaderMap::new();

            let mut authorization = HeaderValue::from_str(&format!("Bearer {}", api_key))
                .map_err(Error::InvalidAPIKeyHeader)?;
            authorization.set_sensitive(true);
            headers.insert(AUTHORIZATION, authorization);

            Some(
                Client::builder()
                    .default_headers(headers)
                    .user_agent(APP_USER_AGENT)
                    .build()?,
            )
        };

        Ok(Self { client })
    }

    /// Validate the configured API key for this telemetry reporter by making a request to the
    /// telemetry ingress endpoint, without actually sending a payload.
    pub async fn authenticate(&self) -> Result<()> {
        handle_resp(client!(self).get(telemetry_url("auth")).send().await?).await
    }

    /// Send a telemetry payload, which can be any arbitrary value that can be serialized to JSON,
    /// to the telemetry ingress.
    ///
    /// If this reporter was initialized with an API key equal to [`HARDCODED_API_KEY`], this
    /// function is a no-op.
    pub async fn send_payload<P>(&self, payload: &P) -> Result<()>
    where
        P: Serialize + ?Sized,
    {
        handle_resp(
            client!(self)
                .post(telemetry_url("payload"))
                .json(payload)
                .send()
                .await?,
        )
        .await
    }
}
