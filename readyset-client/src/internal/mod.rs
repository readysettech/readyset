//! This internal ReadySet module provides types that are shared between ReadySet client and server,
//! but that we do not want to expose publicly through the `noria` crate.

mod addressing;
mod external;
mod index;

pub use self::addressing::{DomainIndex, LocalNodeIndex, ReplicaAddress};
pub use self::external::MaterializationStatus;
pub use self::index::{Index, IndexType};
