//! Application providing an HTTP server which listens for HTTP POST requests on `"/payload"` and
//! uploads the request body to a configurable S3 bucket.

mod authentication;

use std::net::SocketAddr;

use anyhow::{Context, Result};
use aws_sdk_s3 as s3;
use bytes::Bytes;
use clap::Parser;
use tracing::{debug, error, info};
use uuid::Uuid;
use warp::http::Response;
use warp::{reject, Filter};

use crate::authentication::{load_jwks, validate_token, BearerToken, Claims, InvalidToken};

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
    authentication_options: crate::authentication::Options,

    #[clap(flatten)]
    log_options: readyset_tracing::Options,
}

async fn handle_upload(
    options: &Options,
    s3_client: &s3::Client,
    claims: Claims,
    body: Bytes,
) -> Result<Uuid> {
    let id = Uuid::new_v4();
    info!(%id, "Uploading payload");

    let path = format!("{}/{}", claims.sub, id);
    debug!(bucket = %options.s3_bucket, %path);

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

    let jwks: &'static _ = Box::leak(Box::new(
        load_jwks(&options.authentication_options)
            .await
            .context("Loading JWKS")?,
    ));

    let upload_payload = warp::path("payload")
        .and(warp::header::<BearerToken>("Authorization"))
        .and_then(move |token| async move {
            validate_token(jwks, &token).map_err(|e| reject::custom(InvalidToken(e)))
        })
        .and(warp::post())
        .and(warp::body::content_length_limit(MAX_BODY_SIZE_BYTES))
        .and(warp::body::bytes())
        .then(move |token, body| async move {
            match handle_upload(options, s3_client, token, body).await {
                Ok(id) => Response::builder().body(id.to_string()),
                Err(error) => {
                    error!(%error, "Error handling request");
                    Response::builder().status(500).body("".into())
                }
            }
        })
        .recover(|err| async move { authentication::handle_rejection(err) });

    let healthz = warp::path("healthz").and(warp::get()).map(|| "OK");

    let app = healthz.or(upload_payload).with(warp::trace::request());

    warp::serve(app).run(options.address).await;
    Ok(())
}
