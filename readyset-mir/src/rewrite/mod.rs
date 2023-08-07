use readyset_errors::ReadySetResult;

use crate::query::MirQuery;

mod add_bogokey;
mod decorrelate;
mod filters_to_join_keys;
mod fuse_project;
mod pull_columns;
mod pull_keys;
mod remove_distinct_unique;

impl<'a> MirQuery<'a> {
    /// Run a set of rewrite and optimization passes on this [`MirQuery`], and returns the modified
    /// query.
    pub fn rewrite(mut self) -> ReadySetResult<Self> {
        pull_keys::pull_view_keys_to_leaf(&mut self)?;
        decorrelate::eliminate_dependent_joins(&mut self)?;
        add_bogokey::add_bogokey_if_necessary(&mut self)?;
        pull_columns::pull_all_required_columns(&mut self)?;
        remove_distinct_unique::remove_distinct_unique(&mut self)?;
        filters_to_join_keys::convert_filters_to_join_keys(&mut self)?;
        fuse_project::fuse_project_nodes(&mut self)?;
        Ok(self)
    }
}
