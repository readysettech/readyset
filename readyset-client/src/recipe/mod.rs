//! This module holds the Recipe logic that needs to be shared
//! among ReadySet components.
pub mod changelist;

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

pub use crate::recipe::changelist::ChangeList;
use crate::ReplicationOffset;

/// Represents a request to extend a recipe
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExtendRecipeSpec<'a> {
    /// The list of changes to be applied
    pub changes: ChangeList,
    /// Optional replication offset if recipe is installed from replication or binlog
    pub replication_offset: Option<Cow<'a, ReplicationOffset>>,
    /// Parameter that indicates if the leader is required to be ready before handling
    /// this RecipeSpec.
    /// Defaults to true.
    pub require_leader_ready: bool,
}

impl<'a> From<ChangeList> for ExtendRecipeSpec<'a> {
    fn from(changes: ChangeList) -> Self {
        Self {
            changes,
            replication_offset: None,
            require_leader_ready: true,
        }
    }
}

/// The result of a request to extend a recipe
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum ExtendRecipeResult {
    /// The extend recipe request has completed, and the graph has been successfully modified
    Done,
    /// The extend recipe request is still running, and its status can be queried using the given
    /// token
    Pending(u64),
}

/// The status of an actively running migration
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum MigrationStatus {
    /// The migration has completed
    Done,
    /// The migration has not yet completed
    Pending,
}

impl MigrationStatus {
    /// Returns `true` if the migration status is [`Pending`].
    ///
    /// [`Pending`]: MigrationStatus::Pending
    #[must_use]
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }
}
