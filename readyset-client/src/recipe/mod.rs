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
