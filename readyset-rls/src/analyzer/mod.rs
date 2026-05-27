//! RLS analyzer.
//!
//! Given a parsed Postgres user query plus the in-memory [`PolicyRegistry`], the analyzer decides:
//!
//! - Whether the query is RLS-aware (touches an RLS-active relation directly or transitively
//!   through a view).
//! - Whether the active policies fit the supported grammar.
//! - Which [`SessionInputType`]s need to be folded into the shallow-cache key so per-session
//!   partitioning is correct.
//!
//! Out-of-grammar cases produce [`Cacheability::Refuse`] with a typed [`RefuseReason`] that flows
//! through to `EXPLAIN CACHE SUPPORT`.

use std::sync::Arc;

use crate::Oid;
use smallvec::SmallVec;

pub mod allowlist;
pub mod grammar;

use crate::policy_registry::PolicyRegistry;
use crate::types::SessionInputType;

/// Verdict for whether a cache may be created for a given user query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Cacheability {
    /// Cacheable result; the [`CacheSessionDeps`] tells the cache layer what to fold into the
    /// lookup key.
    #[default]
    Cacheable,
    /// Out-of-grammar policy or other refusal; surfaces via `EXPLAIN CACHE SUPPORT`.
    Refuse(RefuseReason),
}

/// Per-cache dependency descriptor produced by the analyzer.
#[derive(Debug, Clone, Default)]
pub struct CacheSessionDeps {
    /// Relations whose RLS state affects this cache (typically the query's primary table, expanded
    /// transitively through views).
    pub rls_active_for_tables: SmallVec<[Oid; 2]>,
    /// Session inputs the cache must hash into the lookup key. For `auth.jwt() ->> 'sub'` this
    /// contains `JwtClaim(["sub"])`, not the whole-blob `JwtClaim([])`.
    pub session_rls_inputs: Arc<[SessionInputType]>,
    /// Generation observed when this descriptor was produced; lets callers detect a stale analyzer
    /// result before installing the cache (a generation bump between Analyze and Install
    /// invalidates the dependency set).
    pub snapshot_generation: u64,
    /// Final verdict.
    pub cacheability: Cacheability,
}

/// Reason a cache was refused. Surfaces in `EXPLAIN CACHE SUPPORT`: each variant has a stable
/// [`RefuseReason::code`] string that downstream tooling can rely on rather than parsing the
/// human-readable `Display`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RefuseReason {
    #[error("policy on table oid={0} references another table")]
    PolicyReadsOtherTable(Oid),
    #[error("policy uses non-stable function {0}")]
    NonStableFunctionInPolicy(String),
    #[error("policy expression contains non-allowlisted constructs: {0}")]
    PolicyTooDynamic(String),
    #[error("table oid={0} is partitioned; expand to partitions (Phase 3)")]
    PartitionedTable(Oid),
    #[error("view oid={0} lacks security_invoker = true")]
    ViewWithoutSecurityInvoker(Oid),
    #[error("query calls SECURITY DEFINER function {0}")]
    SecurityDefinerFunction(String),
}

impl RefuseReason {
    /// Stable identifier per variant, so downstream consumers can switch on the reason without
    /// depending on the human-readable string.
    pub fn code(&self) -> &'static str {
        match self {
            Self::PolicyReadsOtherTable(_) => "policy_reads_other_table",
            Self::NonStableFunctionInPolicy(_) => "non_stable_function_in_policy",
            Self::PolicyTooDynamic(_) => "policy_too_dynamic",
            Self::PartitionedTable(_) => "partitioned_table",
            Self::ViewWithoutSecurityInvoker(_) => "view_without_security_invoker",
            Self::SecurityDefinerFunction(_) => "security_definer_function",
        }
    }

    /// Structured render for embedding in an EXPLAIN response payload. Format is stable:
    /// `rls_uncacheable[code=<code>, detail=<detail>]`, a single grep-able line.
    pub fn structured_display(&self) -> String {
        let detail = match self {
            Self::PolicyReadsOtherTable(oid) => format!("table_oid={oid}"),
            Self::NonStableFunctionInPolicy(name) => format!("function={name}"),
            Self::PartitionedTable(oid) => format!("table_oid={oid}"),
            Self::ViewWithoutSecurityInvoker(oid) => format!("view_oid={oid}"),
            Self::SecurityDefinerFunction(name) => format!("function={name}"),
            Self::PolicyTooDynamic(detail) => format!("detail={detail}"),
        };
        format!("rls_uncacheable[code={}, {}]", self.code(), detail)
    }
}

/// Analyzer entry point. Walks `referenced_relations` against the registry and returns a
/// [`CacheSessionDeps`], capturing the snapshot generation around the grammar analysis.
///
/// Sessions holding `BYPASSRLS` need no analyzer involvement: `SET ROLE` resolution folds the
/// bypass attribute into the scoped partition directly.
pub fn analyze_cache(
    registry: &Arc<PolicyRegistry>,
    referenced_relations: &[Oid],
) -> CacheSessionDeps {
    grammar::analyze(registry, referenced_relations, registry.generation())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuse_reason_codes_are_stable_and_distinct() {
        let codes = [
            RefuseReason::PolicyReadsOtherTable(1).code(),
            RefuseReason::NonStableFunctionInPolicy("f".into()).code(),
            RefuseReason::PolicyTooDynamic(String::new()).code(),
            RefuseReason::PartitionedTable(2).code(),
            RefuseReason::ViewWithoutSecurityInvoker(3).code(),
            RefuseReason::SecurityDefinerFunction("g".into()).code(),
        ];
        let mut sorted = codes.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), codes.len(), "codes must be distinct");
    }

    #[test]
    fn structured_display_includes_code_and_detail() {
        let s = RefuseReason::PartitionedTable(42).structured_display();
        assert!(s.contains("code=partitioned_table"));
        assert!(s.contains("table_oid=42"));
    }
}
