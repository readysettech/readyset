use readyset_client_metrics::recorded;
use readyset_data::DfType;
use readyset_metrics::metrics_handle;
use readyset_schema::bind_vrel;
use readyset_schema::virtual_relation::{VrelContext, VrelRead, VrelRows};
use readyset_sql::DialectDisplay;
use readyset_sql::ast::{CacheType, TrxCachePolicy};

const DEEP_CACHES_SCHEMA: &[(&str, DfType)] = &[
    ("query_id", DfType::DEFAULT_TEXT),
    ("name", DfType::DEFAULT_TEXT),
    ("query", DfType::DEFAULT_TEXT),
    ("always", DfType::Bool),
    ("total_count", DfType::UnsignedBigInt),
];

fn deep_caches_read(ctx: &VrelContext) -> VrelRead {
    let dialect = ctx.dialect;
    let mut controller = ctx.controller.clone();
    Box::pin(async move {
        let views = controller
            .verbose_views(None, None)
            .await
            .unwrap_or_default();
        let [counts] = metrics_handle()
            .map(|h| {
                h.counters_by_label(
                    [recorded::QUERY_LOG_EXECUTION_COUNT],
                    "query_id",
                    [("database_type", "readyset")],
                )
            })
            .unwrap_or_default();

        let rows: VrelRows = Box::new(views.into_iter().filter_map(move |view| {
            if !matches!(view.cache_type, Some(CacheType::Deep) | None) {
                return None;
            }
            let query_id = view.query_id.to_string();
            let count = counts.get(&query_id);
            Some(vec![
                query_id.into(),
                view.name.display_unquoted().to_string().into(),
                view.statement.display(dialect).to_string().into(),
                matches!(view.trx_cache_policy, TrxCachePolicy::Always).into(),
                count.into(),
            ])
        }));
        Ok(rows)
    })
}
bind_vrel!(deep_caches, DEEP_CACHES_SCHEMA, deep_caches_read);
