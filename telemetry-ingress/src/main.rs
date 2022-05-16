//! Application providing an HTTP server which listens for HTTP POST requests on `"/payload"` and
//! uploads the request body to a configurable S3 bucket.

use std::net::SocketAddr;

use anyhow::Result;
use aws_sdk_s3 as s3;
use bytes::Bytes;
use clap::Parser;
use tracing::{error, info};
use uuid::Uuid;
use warp::http::Response;
use warp::Filter;

const MAX_BODY_SIZE_BYTES: u64 = 1024 * 1024 * 10;

#[derive(Parser)]
pub struct Options {
    /// Address to listen on
    #[clap(
        long,
        short = 'a',
        env = "LISTEN_ADDRESS",
        parse(try_from_str),
        default_value = "0.0.0.0:3030"
    )]
    address: SocketAddr,

    /// S3 bucket (without `s3://` prefix) to store uploaded telemetry payloads in
    #[clap(long, env = "S3_BUCKET")]
    s3_bucket: String,

    #[clap(flatten)]
    log_options: readyset_tracing::Options,
}

async fn handle_upload(options: &Options, s3_client: &s3::Client, body: Bytes) -> Result<Uuid> {
    let id = Uuid::new_v4();
    info!(%id, "Uploading payload");

    s3_client
        .put_object()
        .bucket(&options.s3_bucket)
        .key(id.to_string())
        .body(body.into())
        .send()
        .await?;

    Ok(id)
}

#[tokio::main]
async fn main() -> Result<()> {
    let options: &'static Options = Box::leak(Box::new(Options::parse()));
    options.log_options.init("telemetry-ingress")?;

    let config = aws_config::from_env().load().await;
    let s3_client: &'static _ = Box::leak(Box::new(s3::Client::new(&config)));

    let app = warp::path("payload")
        .and(warp::post())
        .and(warp::body::content_length_limit(MAX_BODY_SIZE_BYTES))
        .and(warp::body::bytes())
        .then(move |body| async move {
            match handle_upload(options, s3_client, body).await {
                Ok(id) => Response::builder().body(id.to_string()),
                Err(error) => {
                    error!(%error, "Error handling request");
                    Response::builder().status(500).body("".into())
                }
            }
        })
        .with(warp::trace::request());

    warp::serve(app).run(options.address).await;
    Ok(())
}
