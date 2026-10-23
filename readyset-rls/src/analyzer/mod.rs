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
    /// Every relation the analysis reached: the query's relations, each view at every level, and
    /// every base table whether or not it is RLS-active. Caches register against all of them so a
    /// change anywhere beneath invalidates them.
    pub visited_relations: SmallVec<[Oid; 4]>,
    /// Every function the analysis reached. Caches register against all of them so a change anywhere
    /// beneath invalidates them.
    pub visited_functions: SmallVec<[Oid; 4]>,
}

impl CacheSessionDeps {
    /// Whether `other` reaches the same verdict, the same relations, and the same session inputs,
    /// ignoring the generation each was produced under. Two analyses of one query that agree here
    /// cache the same rows under the same partitioning.
    pub fn same_dependencies(&self, other: &Self) -> bool {
        fn sorted<T: Ord + Clone>(items: &[T]) -> Vec<T> {
            let mut items = items.to_vec();
            items.sort_unstable();
            items.dedup();
            items
        }
        self.cacheability == other.cacheability
            && sorted(&self.session_rls_inputs) == sorted(&other.session_rls_inputs)
            && sorted(&self.rls_active_for_tables) == sorted(&other.rls_active_for_tables)
            && sorted(&self.visited_relations) == sorted(&other.visited_relations)
            && sorted(&self.visited_functions) == sorted(&other.visited_functions)
    }
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
    #[error(
        "RLS-active table oid={0} is read as the owner of a view without security_invoker or of a \
         SECURITY DEFINER function, not as the caller"
    )]
    RlsTableInOwnerContext(Oid),
    #[error("query calls volatile function {0}")]
    VolatileFunction(String),
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
            Self::RlsTableInOwnerContext(_) => "rls_table_in_owner_context",
            Self::VolatileFunction(_) => "volatile_function",
        }
    }

    /// Structured render for embedding in an EXPLAIN response payload. Format is stable:
    /// `rls_uncacheable[code=<code>, detail=<detail>]`, a single grep-able line.
    pub fn structured_display(&self) -> String {
        let detail = match self {
            Self::PolicyReadsOtherTable(oid) => format!("table_oid={oid}"),
            Self::NonStableFunctionInPolicy(name) => format!("function={name}"),
            Self::PartitionedTable(oid) => format!("table_oid={oid}"),
            Self::RlsTableInOwnerContext(oid) => format!("table_oid={oid}"),
            Self::VolatileFunction(name) => format!("function={name}"),
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
            RefuseReason::RlsTableInOwnerContext(3).code(),
            RefuseReason::VolatileFunction("h".into()).code(),
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
