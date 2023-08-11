//! This module holds the Recipe logic that needs to be shared
//! among ReadySet components.
pub mod changelist;

use std::borrow::Cow;
use std::fmt::Display;

use readyset_errors::ReadySetError;
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
    ///
    /// Defaults to true.
    pub require_leader_ready: bool,
    /// Optional parameter to request a non-blocking migration. If set, the controller will return
    /// immediately with an id that can be used to query the status of the migration.
    ///
    /// Defaults to false.
    pub concurrently: bool,
}

impl<'a> ExtendRecipeSpec<'a> {
    /// Sets the `concurrently` flag, requesting that the controller returns immediately with an id
    /// that can be used to query the status of the migration.
    pub fn concurrently(mut self) -> Self {
        self.concurrently = true;
        self
    }
}

impl<'a> From<ChangeList> for ExtendRecipeSpec<'a> {
    fn from(changes: ChangeList) -> Self {
        Self {
            changes,
            replication_offset: None,
            require_leader_ready: true,
            concurrently: false,
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
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MigrationStatus {
    /// The migration has completed
    Done,
    /// The migration has completed with an error
    Failed(ReadySetError),
    /// The migration has not yet completed
    Pending,
}

impl Display for MigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationStatus::Done => f.write_str("Completed"),
            MigrationStatus::Failed(e) => write!(f, "Failed with error {e}"),
            MigrationStatus::Pending => f.write_str("Pending"),
        }
    }
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
