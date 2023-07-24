use readyset_errors::ReadySetResult;

use crate::query::MirQuery;

pub(crate) fn remove_distinct_unique(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    Ok(())
}
