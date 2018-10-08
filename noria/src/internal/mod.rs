//! This internal Noria crate provides types that are shared between Noria client and server, but
//! that we do not want to expose publicly through the `noria` crate.

mod addressing;
mod external;
mod proto;

pub use self::addressing::{DomainIndex, LocalNodeIndex};
pub use self::external::MaterializationStatus;
pub use self::proto::LocalOrNot;
