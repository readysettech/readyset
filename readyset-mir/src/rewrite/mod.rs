use readyset_errors::ReadySetResult;

use crate::query::MirQuery;

mod add_bogokey;
mod decorrelate;
mod pull_columns;

impl<'a> MirQuery<'a> {
    /// Run a set of rewrite and optimization passes on this [`MirQuery`], and returns the modified
    /// query.
    pub fn rewrite(mut self) -> ReadySetResult<Self> {
        decorrelate::eliminate_dependent_joins(&mut self)?;
        pull_columns::pull_all_required_columns(&mut self)?;
        Ok(self)
    }
}
