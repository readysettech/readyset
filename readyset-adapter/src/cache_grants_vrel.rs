use std::sync::Arc;

use chrono::{DateTime, Utc};
use readyset_client::query::QueryId;
use readyset_data::DfType;
use readyset_schema::bind_vrel;
use readyset_schema::virtual_relation::{
    CacheGrantRow, CacheGrantsInfo, ShallowInfo, VrelContext, VrelRead, VrelRows,
};
use readyset_sql::ast::SqlIdentifier;

use crate::backend::AllowedUsers;
use crate::cache_acl::AclHandle;

const CACHE_GRANTS_SCHEMA: &[(&str, DfType)] = &[
    ("user", DfType::DEFAULT_TEXT),
    ("cache", DfType::DEFAULT_TEXT),
    ("verdict", DfType::DEFAULT_TEXT),
    ("probed_at", DfType::DEFAULT_TEXT),
];

/// The adapter's [`CacheGrantsInfo`]: the verdict matrix joined with the allowed-users list and
/// the live cache set, so unresolved pairs surface as `unknown` rows.
pub struct AclCacheGrants {
    pub acl: AclHandle,
    pub users: Arc<AllowedUsers>,
    pub shallow: Arc<dyn ShallowInfo>,
}

impl CacheGrantsInfo for AclCacheGrants {
    fn grants(&self) -> Vec<CacheGrantRow> {
        let matrix = self.acl.matrix();
        let mut rows: Vec<CacheGrantRow> = matrix
            .snapshot()
            .into_iter()
            .map(|(identity, cache, entry)| CacheGrantRow {
                user: identity.to_string(),
                cache: cache.to_string(),
                verdict: entry.verdict.as_str(),
                probed_at: Some(entry.probed_at),
            })
            .collect();

        let users: Vec<SqlIdentifier> = self
            .users
            .snapshot()
            .keys()
            .map(|user| SqlIdentifier::from(user.as_str()))
            .collect();
        let caches: Vec<QueryId> = self
            .shallow
            .list_caches(None, None)
            .iter()
            .map(|info| info.query_id)
            .collect();
        rows.extend(
            matrix
                .unknown_pairs(&users, &caches)
                .into_iter()
                .map(|(identity, cache)| CacheGrantRow {
                    user: identity.to_string(),
                    cache: cache.to_string(),
                    verdict: "unknown",
                    probed_at: None,
                }),
        );
        rows
    }
}

/// Backs `SELECT * FROM readyset.cache_grants`: one row per (identity, cache) pair -- the stored
/// verdicts plus the pairs the freshness worker has not resolved yet. Computed per query.
fn cache_grants_read(ctx: &VrelContext) -> VrelRead {
    let mut rows = ctx.cache_grants.grants();
    Box::pin(async move {
        rows.sort_by(|a, b| (&a.user, &a.cache).cmp(&(&b.user, &b.cache)));
        let rows: VrelRows = Box::new(rows.into_iter().map(|row| {
            vec![
                row.user.into(),
                row.cache.into(),
                row.verdict.into(),
                match row.probed_at {
                    Some(at) => DateTime::<Utc>::from(at).to_rfc3339().into(),
                    None => readyset_data::DfValue::None,
                },
            ]
        }));
        Ok(rows)
    })
}
bind_vrel!(cache_grants, CACHE_GRANTS_SCHEMA, cache_grants_read);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_acl::Verdict;

    struct NoopShallow;
    impl ShallowInfo for NoopShallow {
        fn list_caches(
            &self,
            _query_id: Option<QueryId>,
            _name: Option<&readyset_sql::ast::Relation>,
        ) -> Vec<readyset_shallow::CacheInfo> {
            vec![]
        }
        fn list_entries(
            &self,
            _query_id: Option<QueryId>,
            _limit: Option<usize>,
        ) -> Vec<readyset_shallow::CacheEntryInfo> {
            vec![]
        }
    }

    #[test]
    fn grants_include_stored_cells_and_derived_unknowns() {
        let acl = AclHandle::disabled();
        let cache = QueryId::from_unparsed_select("select 1");
        acl.matrix().record("alice".into(), cache, Verdict::Denied);

        let users = AllowedUsers::empty();
        let info = AclCacheGrants {
            acl,
            users,
            shallow: Arc::new(NoopShallow),
        };
        // With no allowed users and no caches, only the stored cell surfaces.
        let rows = info.grants();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].user, "alice");
        assert_eq!(rows[0].verdict, "denied");
        assert!(rows[0].probed_at.is_some());
    }
}
