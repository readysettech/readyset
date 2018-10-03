//! This internal Noria crate provides types that are shared between Noria client and server, but
//! that we do not want to expose publicly through the `noria` crate.

#![deny(unused_extern_crates)]

extern crate serde;
#[macro_use]
extern crate serde_derive;

mod addressing;
mod external;
mod proto;

pub use addressing::{DomainIndex, LocalNodeIndex};
pub use external::MaterializationStatus;
pub use proto::LocalOrNot;
