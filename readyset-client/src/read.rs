//! End-to-end read path for cached queries.
//!
//! The adapter hands [`read_cache`] a view handle, an optional local reader
//! (the in-process fast path that bypasses RPC), the per-execute parameters,
//! and the post-lookup plan. `read_cache` builds the
//! [`ViewQuery`](crate::view::ViewQuery), dispatches to the right reader,
//! applies [`transform_schema`](crate::post_processing::post_lookup::transform_schema)
//! to the schema, and returns the rows and client-facing schema. The adapter
//! has nothing left to do but wrap the result in its own
//! `QueryResult::Select` variant.

use std::borrow::Cow;

use async_trait::async_trait;
use readyset_data::{DfValue, Dialect};
use readyset_errors::{internal_err, ReadySetError, ReadySetResult};
use readyset_post_lookup::PostLookupPlan;

use crate::post_processing::post_lookup::iter::ResultIterator;
use crate::schema::{SchemaType, SelectSchema};
use crate::view::View;
use crate::ReaderAddress;

/// In-process fast path for reader lookups.
///
/// Implemented by the server's `ReadRequestHandler` so the adapter can hand
/// the read to it without going through bincode + the multiplex layer.
/// Returns rows already passed through the server-side post-lookup pipeline
/// (filter, aggregate, dedup, recompose, limit, offset, projection); the
/// schema is rebuilt by [`read_cache`] from the reader handle separately.
#[async_trait]
pub trait LocalReader: Send {
    async fn read_local(
        &mut self,
        target: ReaderAddress,
        query: crate::view::ViewQuery,
    ) -> ReadySetResult<ResultIterator>;
}

/// Rows + schema + per-read metrics returned by a cache read.
#[derive(Debug)]
pub struct ReadResult {
    /// The row stream (post-lookup pipeline applied).
    pub rows: ResultIterator,
    /// The client-facing schema for `rows`, after recompose has been applied.
    pub schema: SelectSchema<'static>,
    /// Number of lookup keys issued (cardinality of the parameter
    /// explosion). Surfaced to the adapter for metric composition.
    pub num_keys: u64,
    /// Number of keys that missed the cache and had to be filled in.
    pub cache_misses: u64,
}

/// Drive the end-to-end read path for a cached query.
///
/// Builds the `ViewQuery` from `view` and the per-execute parameters,
/// dispatches to `local_reader` if present (in-process fast path) or to the
/// view's RPC otherwise, then applies the recompose-schema transform so the
/// returned [`ReadResult::schema`] matches the post-decompose row layout.
///
/// The adapter passes ownership of nothing besides the inputs and gets back
/// a value-typed result; no further transformation on the adapter side.
pub async fn read_cache<'a>(
    view: &'a mut View,
    local_reader: Option<&mut (dyn LocalReader + 'a)>,
    raw_keys: Vec<Cow<'a, [DfValue]>>,
    limit: Option<usize>,
    offset: Option<usize>,
    plan: &PostLookupPlan,
    dialect: Dialect,
) -> ReadySetResult<ReadResult> {
    let plan_present = !plan.is_empty();
    let plan_for_query = plan_present.then(|| plan.clone());

    let (reader_handle, mut vq) =
        match view.build_view_query(raw_keys, limit, offset, dialect, plan_for_query, None)? {
            Some(res) => res,
            None => return Err(ReadySetError::NoCacheForQuery),
        };

    // Snapshot the reader's source schema (pre-decomposition) so the server's
    // recompose stage has the source column types. Only attached when the plan
    // asks for decomposition; for plan-less reads the field stays `None` and the
    // common wire format is unchanged.
    if plan_present {
        vq.result_schema = reader_handle
            .schema()
            .map(|s| s.schema(SchemaType::ReturnedSchema).to_vec());
    }

    let num_keys = vq.key_comparisons.len() as u64;

    let rows = if let Some(rh) = local_reader {
        // In-process fast path: bypass bincode + the multiplex layer.
        let target = ReaderAddress {
            node: *reader_handle.node(),
            name: reader_handle.name().clone(),
        };
        rh.read_local(target, vq).await?
    } else {
        reader_handle.raw_lookup(vq).await?
    };

    let cache_misses = rows.total_stats().map(|s| s.cache_misses).unwrap_or(0);

    // Build the pre-decompose schema (`Cow::Owned` so the result outlives
    // `view`/`reader_handle`'s borrow) and feed it to the recompose-schema
    // transform. With an empty plan this is a pass-through.
    let raw_schema = SelectSchema {
        schema: Cow::Owned(
            reader_handle
                .schema()
                .ok_or_else(|| internal_err!("Reader missing schema for cached query"))?
                .schema(SchemaType::ReturnedSchema)
                .to_vec(),
        ),
        columns: Cow::Owned(reader_handle.columns().to_vec()),
    };
    let schema = crate::post_processing::post_lookup::transform_schema(raw_schema, plan, dialect)?;

    Ok(ReadResult {
        rows,
        schema,
        num_keys,
        cache_misses,
    })
}
