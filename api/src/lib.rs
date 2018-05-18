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
#[macro_use]
extern crate nom;
extern crate nom_sql;
extern crate petgraph;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate slab;
#[macro_use]
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

// TODO: remove
pub struct LocalBypass<T>(*mut T);

impl<T> LocalBypass<T> {
    pub fn make(t: Box<T>) -> Self {
        LocalBypass(Box::into_raw(t))
    }

    pub unsafe fn deref(&self) -> &T {
        &*self.0
    }

    pub unsafe fn take(self) -> Box<T> {
        Box::from_raw(self.0)
    }
}

unsafe impl<T> Send for LocalBypass<T> {}
impl<T> serde::Serialize for LocalBypass<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        (self.0 as usize).serialize(serializer)
    }
}
impl<'de, T> serde::Deserialize<'de> for LocalBypass<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        usize::deserialize(deserializer).map(|p| LocalBypass(p as *mut T))
    }
}
impl<T> Clone for LocalBypass<T> {
    fn clone(&self) -> LocalBypass<T> {
        panic!("LocalBypass types cannot be cloned");
    }
}

#[derive(Serialize, Deserialize)]
#[doc(hidden)]
pub enum LocalOrNot<T> {
    Local(LocalBypass<T>),
    Not(T),
}

impl<T> LocalOrNot<T> {
    pub fn is_local(&self) -> bool {
        if let LocalOrNot::Local(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn make(t: T, local: bool) -> Self {
        if local {
            LocalOrNot::Local(LocalBypass::make(Box::new(t)))
        } else {
            LocalOrNot::Not(t)
        }
    }

    pub unsafe fn take(self) -> T {
        match self {
            LocalOrNot::Local(l) => *l.take(),
            LocalOrNot::Not(t) => t,
        }
    }
}

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
