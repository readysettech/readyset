//! A reusable HTTP client for setting failpoints on remote Readyset processes.
//!
//! Extracted from the `failpoint` binary in `readyset-tools` so that multiple test binaries
//! (e.g. `ddl-stress`) can activate failpoints without depending on `readyset-server`.

use std::time::Duration;

use hyper::client::HttpConnector;
use hyper::{Body, Client, StatusCode};

/// Errors that can occur when interacting with the failpoint HTTP endpoint.
#[derive(Debug, thiserror::Error)]
pub enum FailpointError {
    #[error("failed to serialize failpoint request: {0}")]
    Serialize(#[from] bincode::Error),

    #[error("failed to build failpoint HTTP request: {0}")]
    BuildRequest(hyper::http::Error),

    #[error("failpoint HTTP request failed: {0}")]
    Http(hyper::Error),

    #[error(
        "failpoint request returned {status}: {body} \
         (is Readyset compiled with --features failure_injection?)"
    )]
    NonOkStatus { status: StatusCode, body: String },
}

/// HTTP client for setting failpoints on a running Readyset process.
#[derive(Debug, Clone)]
pub struct FailpointClient {
    client: Client<HttpConnector>,
    base_url: String,
}

impl FailpointClient {
    /// Create a new `FailpointClient` targeting the given base URL (e.g. `http://host:6033`).
    pub fn new(base_url: &str) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(Duration::from_secs(5)));
        let client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .build(connector);
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_owned(),
        }
    }

    /// Set a failpoint to the given action string.
    ///
    /// The action uses the same syntax as [`fail::cfg`], e.g. `"1*return"`, `"sleep(2000)"`,
    /// `"off"`, etc.
    pub async fn set(&self, name: &str, action: &str) -> Result<(), FailpointError> {
        // NOTE: Must use the same bincode version/config as readyset-server's /failpoint handler.
        // The GET-with-body protocol is a legacy quirk; see TODO to migrate to POST.
        let data = bincode::serialize(&(name, action))?;

        let url = format!("{}/failpoint", self.base_url);
        let req = hyper::Request::get(&url)
            .body(Body::from(data))
            .map_err(FailpointError::BuildRequest)?;

        let res = self
            .client
            .request(req)
            .await
            .map_err(FailpointError::Http)?;

        let status = res.status();
        if status != StatusCode::OK {
            let body_bytes = hyper::body::to_bytes(res.into_body())
                .await
                .unwrap_or_default();
            let body = String::from_utf8_lossy(&body_bytes).into_owned();
            return Err(FailpointError::NonOkStatus { status, body });
        }

        Ok(())
    }

    /// Clear a failpoint (shorthand for `set(name, "off")`).
    pub async fn clear(&self, name: &str) -> Result<(), FailpointError> {
        self.set(name, "off").await
    }
}
