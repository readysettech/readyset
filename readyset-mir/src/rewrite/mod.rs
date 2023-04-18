use readyset_errors::ReadySetResult;

use crate::query::MirQuery;

mod add_bogokey;
mod decorrelate;
mod pull_columns;
mod pull_keys;

impl<'a> MirQuery<'a> {
    /// Run a set of rewrite and optimization passes on this [`MirQuery`], and returns the modified
    /// query.
    pub fn rewrite(mut self) -> ReadySetResult<Self> {
        pull_keys::pull_view_keys_to_leaf(&mut self)?;
        decorrelate::eliminate_dependent_joins(&mut self)?;
        add_bogokey::add_bogokey_if_necessary(&mut self)?;
        pull_columns::pull_all_required_columns(&mut self)?;
        Ok(self)
    }
}
