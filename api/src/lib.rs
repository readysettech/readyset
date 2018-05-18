//#![deny(missing_docs)]

extern crate assert_infrequent;
extern crate basics;
extern crate bincode;
extern crate channel;
extern crate consensus;
extern crate failure;
extern crate fnv;
extern crate futures;
extern crate hyper;
extern crate mio;
extern crate mio_pool;
extern crate nom;
extern crate nom_sql;
extern crate petgraph;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate slab;
extern crate slog;
extern crate slog_term;
extern crate tarpc;
extern crate timer_heap;
extern crate tokio_core;
extern crate vec_map;

use basics::*;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

mod controller;
mod getter;
mod mutator;

pub mod prelude {
    pub use super::ActivationResult;
    pub use super::ControllerHandle;
    pub use super::Mutator;
    pub use super::{ReadQuery, RemoteGetter};
}

pub use controller::{ControllerDescriptor, ControllerHandle, ControllerPointer};
pub use getter::{ReadQuery, ReadReply, RemoteGetter};
pub use mutator::{Mutator, MutatorError};

pub mod builders {
    pub use super::getter::RemoteGetterBuilder;
    pub use super::mutator::MutatorBuilder;
}

/// Marker for a handle that has an exclusive connection to the backend(s).
pub struct ExclusiveConnection;

/// Marker for a handle that shares its underlying connection with other handles owned by the same
/// thread.
pub struct SharedConnection;

/// Represents the result of a recipe activation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivationResult {
    /// Map of query names to `NodeIndex` handles for reads/writes.
    pub new_nodes: HashMap<String, NodeIndex>,
    /// List of leaf nodes that were removed.
    pub removed_leaves: Vec<NodeIndex>,
    /// Number of expressions the recipe added compared to the prior recipe.
    pub expressions_added: usize,
    /// Number of expressions the recipe removed compared to the prior recipe.
    pub expressions_removed: usize,
}

/// Serializable error type for RPC that can fail.
#[derive(Debug, Deserialize, Serialize)]
pub enum RpcError {
    /// Generic error message vessel.
    Other(String),
}

impl Error for RpcError {
    fn description(&self) -> &str {
        match *self {
            RpcError::Other(ref s) => s,
        }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}
