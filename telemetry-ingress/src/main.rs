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
use warp::Filter;

use crate::authentication::{authenticated, load_jwks, Claims};

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

    let auth = authenticated(jwks, warp::path("auth").and(warp::get())).map(|_token| "OK");

    let upload_payload = authenticated(jwks, warp::path("payload").and(warp::post()))
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
        });

    let healthz = warp::path("healthz").and(warp::get()).map(|| "OK");

    let authenticated_routes = auth
        .or(upload_payload)
        .recover(|err| async move { authentication::handle_rejection(err) });

    let app = healthz
        .or(authenticated_routes)
        .with(warp::trace::request());

    warp::serve(app).run(options.address).await;
    Ok(())
}
