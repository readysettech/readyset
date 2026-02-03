use std::fmt::Debug;
use std::time::Duration;

use readyset_errors::{ReadySetError, ReadySetResult};
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

pub async fn rpc<T, R>(
    http: &reqwest::Client,
    url: Url,
    timeout: Duration,
    req: R,
) -> ReadySetResult<T>
where
    T: DeserializeOwned,
    R: Serialize + Debug,
{
    let http_req = http.post(url.clone()).body(bincode::serialize(&req)?);
    let resp =
        http_req
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| ReadySetError::HttpRequestFailed {
                request: format!("{req:?}"),
                message: e.to_string(),
            })?;
    let status = resp.status();
    let body = resp
        .bytes()
        .await
        .map_err(|e| ReadySetError::HttpRequestFailed {
            request: format!("{req:?}"),
            message: e.to_string(),
        })?;
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
