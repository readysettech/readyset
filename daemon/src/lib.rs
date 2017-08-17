extern crate bincode;
extern crate channel;
extern crate distributary;
extern crate mio;
extern crate serde;
#[macro_use]
extern crate slog;
extern crate slog_term;

mod controller;
mod worker;

pub struct Config {
    pub hostname: String,
    pub addr: String,
    pub port: u16,
    pub controller: Option<String>,
    pub heartbeat_freq: u64,
    pub healthcheck_freq: u64,
}

pub use controller::Controller;
pub use worker::Worker;
