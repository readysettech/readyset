use std::convert::Infallible;
use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet};
use jsonwebtoken::{DecodingKey, Validation};
use serde::Deserialize;
use tracing::debug;
use warp::http::StatusCode;
use warp::reject::{self, InvalidHeader, MissingHeader, Reject};
use warp::{reply, Filter, Rejection, Reply};

#[derive(Parser)]
pub struct Options {
    /// Authority to use to validate tokens
    #[clap(long, env = "AUTHORITY")]
    authority: String,
}

#[derive(Debug)]
pub(crate) struct BearerToken(pub String);

impl FromStr for BearerToken {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once("Bearer ") {
            Some(("", token)) => Ok(Self(token.to_owned())),
            _ => bail!("Invalid bearer token"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Claims {
    pub(crate) sub: String,
}

/// Validate the JWT in the given `token` against one of the keys in the given set of `jwks`. If
/// it's valid (both well-formatted and is signed by one of the JWKs), returns the set of [`Claims`]
/// stored in the token
pub(crate) fn validate_token(jwks: &JwkSet, token: &BearerToken) -> Result<Claims> {
    // Mostly taken from https://github.com/Keats/jsonwebtoken/blob/master/examples/auth0.rs
    let header = jsonwebtoken::decode_header(&token.0)?;
    let key = jwks
        .find(
            header
                .kid
                .as_ref()
                .ok_or_else(|| anyhow!("No KID found in header"))?,
        )
        .ok_or_else(|| anyhow!("Specified key not found in set"))?;
    match &key.algorithm {
        AlgorithmParameters::RSA(rsa) => {
            let decoding_key = DecodingKey::from_rsa_components(&rsa.n, &rsa.e)?;
            let mut validation = Validation::new(
                key.common
                    .algorithm
                    .ok_or_else(|| anyhow!("No algorithm specified in JWK"))?,
            );
            validation.validate_exp = false;
            let token = jsonwebtoken::decode::<Claims>(&token.0, &decoding_key, &validation)?;
            Ok(token.claims)
        }
        _ => bail!("Unsupported JWK algorithm"),
    }
}

#[derive(Debug)]
pub(crate) struct InvalidToken(pub(crate) anyhow::Error);
impl Reject for InvalidToken {}

pub(crate) fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    Ok(if err.is_not_found() {
        reply::with_status("Not Found".into(), StatusCode::NOT_FOUND)
    } else if let Some(InvalidToken(e)) = err.find() {
        reply::with_status(format!("Invalid Token: {}", e), StatusCode::UNAUTHORIZED)
    } else if let Some(m) = err.find::<MissingHeader>() {
        reply::with_status(
            format!("Missing {} header", m.name()),
            StatusCode::BAD_REQUEST,
        )
    } else if let Some(m) = err.find::<InvalidHeader>() {
        reply::with_status(
            format!("Invalid {} header", m.name()),
            StatusCode::BAD_REQUEST,
        )
    } else {
        debug!(?err);
        reply::with_status(
            "Unhandled rejection".into(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })
}

/// Load the set of JWKs for the authority configured for `options` from the `.well-known` url for
/// JWKs
pub(crate) async fn load_jwks(options: &Options) -> Result<JwkSet> {
    Ok(
        reqwest::get(format!("{}.well-known/jwks.json", options.authority))
            .await?
            .json::<JwkSet>()
            .await?,
    )
}

/// Wrap the given warp filter such that it requires a valid token, and also extracts the [`Claims`]
/// within that token
pub(crate) fn authenticated<F>(
    jwks: &'static JwkSet,
    filter: F,
) -> impl Filter<Extract = (Claims,), Error = Rejection> + Clone
where
    F: Filter<Extract = (), Error = Rejection> + Clone,
{
    filter
        .and(warp::header::<BearerToken>("Authorization"))
        .and_then(move |token| async move {
            validate_token(jwks, &token).map_err(|e| reject::custom(InvalidToken(e)))
        })
}
