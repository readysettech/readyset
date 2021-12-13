//! Configures a failpoint for a running noria-server. This can be used to
//! dynamically trigger failure behavior at pre-defined failpoints in the
//! code.
//!
//! # Example
//!
//! ```rust
//! fail::fail_point!("critical-code-failure");
//! ```
//!
//! We can use this binary to trigger a panic when this failpoint is reached
//! in a local noria-server, 50% of the time.
//!
//! ```bash
//! cargo run --bin failpoint -- --controller-address http://127.0.0.1:6033 critical-code-failure 50%panic
//! ```
use clap::Parser;
use hyper::Client;

#[derive(Parser)]
#[clap(name = "failpoint")]
struct Failpoint {
    /// The noria-server's controller address.
    #[clap(
        short,
        long,
        env("AUTHORITY_ADDRESS"),
        default_value("http://127.0.0.1:6033")
    )]
    controller_address: String,

    /// The failpoint that is being set.
    failpoint: String,

    /// The action to set on the failpoint. See
    /// [fail::cfg](https://docs.rs/fail/latest/fail/fn.cfg.html) for more information on
    /// the actions that can be used on a failpoint.
    action: String,
}

impl Failpoint {
    pub async fn run(self) -> anyhow::Result<()> {
        let data = bincode::serialize(&(self.failpoint, self.action)).unwrap();
        let string_url = self.controller_address + "/failpoint";
        let r = hyper::Request::get(string_url)
            .body(hyper::Body::from(data))
            .unwrap();

        let client = Client::new();
        let res = client.request(r).await.unwrap();
        let status = res.status();
        assert!(status == hyper::StatusCode::OK);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let failpoint = Failpoint::parse();
    failpoint.run().await
}
