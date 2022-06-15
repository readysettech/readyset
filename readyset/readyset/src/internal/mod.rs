//! This internal ReadySet crate provides types that are shared between ReadySet client and server, but
//! that we do not want to expose publicly through the `readyset` crate.

mod addressing;
mod external;
mod index;
mod proto;

pub use self::addressing::{DomainIndex, LocalNodeIndex};
pub use self::external::MaterializationStatus;
pub use self::index::{Index, IndexType};
pub use self::proto::LocalOrNot;
