//! Query optimization rewrite pass
//!
//! This pass applies configurable optimization strategies after structural normalization.
//! It must run AFTER `derived_tables_rewrite` which produces canonical query structure.
//!
//! ## Optimization Strategies
//!
//! - **HoistParametrizableFilters**: Move parametrizable filters (`col = ?`) to top-level WHERE
//!   - Good for: prepared statements, APIs, predictable query plans
//!   - Extracts filters from JOIN ON conditions and aggregated subqueries
//!
//! - **None**: Keep canonical form without further optimization

use crate::adapter_rewrites::AdapterRewriteContext;
use crate::hoist_parametrizable_filters::hoist_parametrizable_filters;
use readyset_errors::ReadySetResult;
use readyset_sql::ast::SelectStatement;
use readyset_sql::{Dialect, DialectDisplay};
use tracing::trace;

/// Optimization strategy configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OptimizationStrategy {
    /// Move parametrizable filters to top-level WHERE clause
    HoistParametrizableFilters,

    /// Keep canonical form without further optimization
    #[default]
    None,
}

/// Trait for query optimization rewrites
pub trait QueryOptimizationRewrite: Sized {
    /// Apply optimization strategy to the query
    ///
    /// Must be called AFTER structural normalization passes:
    /// - `derived_tables_rewrite` (produces canonical structure)
    ///
    /// This pass is configurable and optional - can be skipped by using
    /// `OptimizationStrategy::None`
    fn query_optimization_rewrite<C: AdapterRewriteContext>(
        &mut self,
        context: &C,
        strategy: OptimizationStrategy,
    ) -> ReadySetResult<&mut Self>;
}

impl QueryOptimizationRewrite for SelectStatement {
    fn query_optimization_rewrite<C: AdapterRewriteContext>(
        &mut self,
        _context: &C,
        strategy: OptimizationStrategy,
    ) -> ReadySetResult<&mut Self> {
        let mut rewritten = false;

        match strategy {
            OptimizationStrategy::HoistParametrizableFilters => {
                rewritten |= hoist_parametrizable_filters(self)?;
            }

            OptimizationStrategy::None => {
                // Keep canonical form - no optimization
            }
        }

        if rewritten {
            trace!(
                name = "Optimized statement",
                "{}",
                self.display(Dialect::PostgreSQL)
            );
        }

        Ok(self)
    }
}
