mod decorrelate;
mod pull_columns;

use noria_errors::ReadySetResult;

use self::decorrelate::eliminate_dependent_joins;
use self::pull_columns::pull_all_required_columns;
use crate::query::MirQuery;

impl MirQuery {
    /// Run a set of rewrite and optimization passes on self, and return the modified
    /// query
    pub fn optimize(mut self) -> ReadySetResult<Self> {
        eliminate_dependent_joins(&mut self)?;
        pull_all_required_columns(&mut self)?;
        Ok(self)
    }
}
