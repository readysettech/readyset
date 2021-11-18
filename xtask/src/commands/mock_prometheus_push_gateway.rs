use actix_web::http::StatusCode;
use actix_web::web::{BytesMut, Path, Payload};
use actix_web::{post, put, App, HttpResponse, HttpServer, Responder};
use futures::StreamExt;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Payload must be a UTF-8 string")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Failed to read request body: {0}")]
    Payload(#[from] actix_web::error::PayloadError),
}

impl actix_web::error::ResponseError for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Utf8(_) => StatusCode::BAD_REQUEST,
            Self::Payload(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).body(self.to_string())
    }
}

async fn push_with_labels(
    job: &str,
    _labels: &str,
    mut payload: Payload,
) -> Result<&'static str, Error> {
    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        body.extend_from_slice(&chunk?);
    }
    let body = String::from_utf8(body.to_vec())?;
    println!("Received metrics for job '{}':", job);
    for line in body.lines() {
        println!("\t{}", line);
    }
    println!();
    Ok("")
}

#[post("/metrics/job/{job}/{labels:.*}")]
async fn post_with_labels(params: Path<(String, String)>, payload: Payload) -> impl Responder {
    push_with_labels(&params.0, &params.1, payload).await
}

#[put("/metrics/job/{job}/{labels:.*}")]
async fn put_with_labels(params: Path<(String, String)>, payload: Payload) -> impl Responder {
    push_with_labels(&params.0, &params.1, payload).await
}

#[actix_web::main]
pub async fn run() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(post_with_labels)
            .service(put_with_labels)
    })
    .bind("0.0.0.0:9091")?
    .run()
    .await
}
