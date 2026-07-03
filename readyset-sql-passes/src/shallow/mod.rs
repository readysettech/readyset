//! Shallow-cache passes.
//!
//! Shallow caching lives entirely in the adapter. These passes rewrite a
//! shallow-cache query for storage and serving ([`rewrites`]) and decide
//! whether an in-request-path SELECT is a good auto-cache candidate
//! ([`auto_cache_eligibility`]).

mod auto_cache_eligibility;
mod rewrites;

pub use auto_cache_eligibility::{
    ShallowCacheEligibility, auto_cache_skip_reason, auto_cache_skip_reasons,
};
pub use rewrites::{
    anonymize_shallow_query, convert_placeholders_to_question_marks, literalize_shallow_prepared,
    literalize_shallow_query, rewrite_shallow,
};
