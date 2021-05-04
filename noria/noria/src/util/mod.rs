//! Utilities used across noria

use crate::{ReadySetError, ReadySetResult};
use futures_util::TryFutureExt;
use serde::de::DeserializeOwned;
use std::time::Duration;

pub mod like;

/// Timeout, in seconds, applied to outgoing RPC requests sent with [`do_noria_rpc`].
pub static RPC_REQUEST_TIMEOUT_SECS: u64 = 10;

/// Make a request to a remote noria-server instance, using an already partially constructed
/// [`RequestBuilder`]. This handles sending the request with a timeout ([`RPC_REQUEST_TIMEOUT_SECS`])
/// and deserializing the result into a [`ReadySetResult<T>`], where `T` is determined by the caller.
pub async fn do_noria_rpc<T: DeserializeOwned>(req: reqwest::RequestBuilder) -> ReadySetResult<T> {
    let resp = req
        .timeout(Duration::from_secs(RPC_REQUEST_TIMEOUT_SECS))
        .send()
        .map_err(|e| ReadySetError::HttpRequestFailed(e.to_string()))
        .await?;
    let status = resp.status();
    let body = resp
        .bytes()
        .map_err(|e| ReadySetError::HttpRequestFailed(e.to_string()))
        .await?;
    if !status.is_success() {
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(ReadySetError::ServiceUnavailable);
        } else if status == reqwest::StatusCode::BAD_REQUEST {
            return Err(ReadySetError::SerializationFailed(
                "remote server returned 400".into(),
            ));
        } else {
            let err: ReadySetError = bincode::deserialize(&body)?;
            return Err(err);
        }
    }
    Ok(bincode::deserialize::<T>(&body)?)
}
