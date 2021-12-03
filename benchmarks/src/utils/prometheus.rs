use anyhow::Error;
use futures::future::ready;
use futures::io::AsyncBufReadExt;
use futures::Stream;
use futures::TryStreamExt;

pub async fn stream_prometheus_lines_from(
    prometheus_endpoint: &str,
) -> Result<impl Stream<Item = Result<String, std::io::Error>>, Error> {
    let stream = reqwest::get(prometheus_endpoint)
        .await?
        .error_for_status()?
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read()
        .lines();
    Ok(stream)
}

pub async fn stream_prometheus_lines_with_filter(
    prometheus_endpoint: &str,
    predicate: impl Fn(&str) -> bool,
) -> Result<impl Stream<Item = Result<String, std::io::Error>>, Error> {
    let filtered_stream = stream_prometheus_lines_from(prometheus_endpoint)
        .await?
        .try_filter(move |s| ready(predicate(s)));
    Ok(filtered_stream)
}
