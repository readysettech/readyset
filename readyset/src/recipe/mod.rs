//! This module holds the Recipe logic that needs to be shared
//! among Noria components.
pub mod changelist;

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use crate::recipe::changelist::ChangeList;
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

impl<'a> Default for ExtendRecipeSpec<'a> {
    fn default() -> Self {
        ExtendRecipeSpec {
            changes: Default::default(),
            replication_offset: None,
            require_leader_ready: true,
        }
    }
}
