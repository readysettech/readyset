use readyset_errors::ReadySetResult;

use crate::query::MirQuery;

mod add_bogokey;
mod decorrelate;
mod filters_to_join_keys;
mod fuse;
mod grouped_join_parameter;
mod predicate_pushup;
mod prune_columns;
mod pull_columns;
mod pull_keys;
mod reject_self_joins;
mod zerofill;

impl MirQuery<'_> {
    /// Run a set of rewrite and optimization passes on this [`MirQuery`], and returns the modified
    /// query.
    pub fn rewrite(mut self, dialect: readyset_sql::Dialect) -> ReadySetResult<Self> {
        pull_keys::pull_view_keys_to_leaf(&mut self)?;
        decorrelate::eliminate_dependent_joins(&mut self)?;
        predicate_pushup::push_filters_up(&mut self)?;
        filters_to_join_keys::convert_filters_to_join_keys(&mut self, dialect)?;
        grouped_join_parameter::refuse_parameter_on_grouped_join_key(&self)?;
        add_bogokey::add_bogokey_if_necessary(&mut self)?;
        pull_columns::pull_all_required_columns(&mut self)?;

        loop {
            let changed = prune_columns::prune_unused_columns(&mut self)?
                | fuse::fuse_project_nodes(&mut self)?
                | fuse::fuse_filter_nodes(&mut self)?;
            if !changed {
                break;
            }
        }

        if dialect == readyset_sql::Dialect::MySQL {
            zerofill::refuse_unsupported_zerofill_uses(&self)?;
        }

        reject_self_joins::reject_self_joins(&self)?;

        Ok(self)
    }
}
