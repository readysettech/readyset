//! Post-lookup processing for reader results.
//!
//! The reader (leaf) node in the Readyset dataflow graph holds materialized
//! query state and serves lookups to the adapter. This module owns the
//! streaming pipeline driven by the reader's [`post_lookup::PostLookup`] spec:
//! merge-sort per-key runs, aggregate, filter, `DISTINCT` dedup,
//! `OFFSET`/`LIMIT`, project, empty-result defaulting, recomposition of
//! decomposed aggregates (`AVG` to `SUM`/`COUNT`).
//!
//! The end-to-end orchestrator that calls into this module from the adapter
//! lives at [`crate::read`], not here; `read_cache` is a *consumer* of
//! post-processing, not part of it.

pub mod post_lookup;

pub use post_lookup::{
    apply_post_lookup_to_prepared_schema, effective_aggregates, run_post_processing_pipeline,
    PostLookup, PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates,
    PostLookupDistinct, PostLookupPlan, ReadReplyStats, ResultIterator, Results, SharedResults,
    SharedRows,
};
