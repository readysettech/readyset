use rustful::{Context, Handler, HttpResult, Response, Server, TreeRouter};
use rustful::server::Listening;
use rustful::server::Global;
use std::sync::{Arc, Mutex};

use flow::ControllerInner;
use core::DataType;
use serde_json::{self, Value};

struct GetEndpoint<F> {
    arguments: Vec<String>,
    f: F,
}

struct PutEndpoint<Mutator> {
    arguments: Vec<String>,
    mutator: Mutator,
}
