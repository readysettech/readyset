//! Pre-pipeline validation pass.
//!
//! Checks structural invariants that the downstream rewrite pipeline requires.
//! Must run **after** the normalization passes (`resolve_schemas`, `expand_stars`,
//! `expand_implied_tables`, `expand_join_on_using`) and **before** the main
//! rewrite pipeline (`derived_tables_rewrite`, etc.).
//!
//! This is purely a read-only check — it never mutates the AST.
//!
//! ## Invariants checked
//!
//! 1. **No CTEs** — `WITH` clauses are not supported by the deep rewrite pipeline.
//!    Queries with CTEs skip Block B via the shallow-cache path.
//! 2. **All table columns are qualified** (`column.table` is `Some(...)`)
//!    — except for legitimate alias references in HAVING (any position),
//!    top-level bare columns in GROUP BY / ORDER BY (ordinal `ORDER BY 1` /
//!    `GROUP BY 1` references are `FieldReference::Numeric` in the AST and
//!    do not reach `visit_column`).
//! 3. **No unexpanded star expressions** (`SELECT *` or `SELECT t.*`)
//!    — `expand_stars` must have replaced all stars with concrete fields.
//! 4. **No `JOIN ... USING` constraints** — `expand_join_on_using` must have
//!    rewritten them to `ON` equalities.

use std::collections::HashSet;

use readyset_errors::{ReadySetError, ReadySetResult, internal};
use readyset_sql::analysis::visit::{Visitor, walk_function_expr, walk_select_statement};
use readyset_sql::ast::{
    Column, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr, GroupByClause, JoinConstraint,
    OrderClause, SelectStatement, SqlIdentifier, SqlQuery,
};
use readyset_sql::{Dialect, DialectDisplay};

pub trait ValidatePipelineInvariants: Sized {
    /// Check that the AST satisfies the structural invariants required by the
    /// downstream rewrite pipeline.  Returns `Ok(())` on success, or an
    /// error describing the first violation found.
    fn validate_pipeline_invariants(&self, dialect: Dialect) -> ReadySetResult<()>;
}

impl ValidatePipelineInvariants for SelectStatement {
    fn validate_pipeline_invariants(&self, dialect: Dialect) -> ReadySetResult<()> {
        let mut visitor = InvariantValidator {
            aliases: HashSet::new(),
            can_reference_aliases: false,
            dialect,
        };
        visitor.visit_select_statement(self)
    }
}

impl ValidatePipelineInvariants for SqlQuery {
    fn validate_pipeline_invariants(&self, dialect: Dialect) -> ReadySetResult<()> {
        match self {
            SqlQuery::Select(stmt) => stmt.validate_pipeline_invariants(dialect),
            SqlQuery::CompoundSelect(csq) => {
                for (_op, stmt) in &csq.selects {
                    stmt.validate_pipeline_invariants(dialect)?;
                }
                Ok(())
            }
            // Non-SELECT queries (CREATE TABLE, INSERT, etc.) are not subject
            // to the rewrite pipeline invariants.
            _ => Ok(()),
        }
    }
}

#[derive(Debug)]
struct InvariantValidator {
    /// SELECT-list aliases in the current scope.
    aliases: HashSet<SqlIdentifier>,
    /// Whether the current position allows unqualified alias references.
    can_reference_aliases: bool,
    /// Dialect for error display.
    dialect: Dialect,
}

impl<'ast> Visitor<'ast> for InvariantValidator {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast SelectStatement,
    ) -> Result<(), Self::Error> {
        // ── No CTEs ──
        // CTE support is out of scope for the deep rewrite pipeline.  Bail out
        // so Block B is skipped and the query goes through the shallow-cache path.
        if !select_statement.ctes.is_empty() {
            internal!("CTEs (WITH clauses) are not supported by the rewrite pipeline");
        }

        // ── No unexpanded stars ──
        for field in &select_statement.fields {
            match field {
                FieldDefinitionExpr::All => {
                    internal!("SELECT * should have been expanded");
                }
                FieldDefinitionExpr::AllInTable(t) => {
                    internal!(
                        "SELECT {}.* should have been expanded",
                        t.display(self.dialect)
                    );
                }
                FieldDefinitionExpr::Expr { .. } => {}
            }
        }

        // ── No JOIN ... USING ──
        for jc in &select_statement.join {
            if matches!(jc.constraint, JoinConstraint::Using(_)) {
                internal!("JOIN ... USING should have been expanded");
            }
        }

        // Scope the alias set AND the can_reference_aliases flag per subquery.
        // An inner subquery has its own alias namespace and its own clause-level
        // rules for when alias references are allowed.  Without saving/restoring
        // can_reference_aliases here, an inner query's GROUP BY/ORDER BY traversal
        // would leave the flag as `false` on exit, corrupting the outer context
        // (e.g., a HAVING clause with `<subquery> AND <alias_ref>` would reject
        // the alias reference after the subquery).
        let orig_aliases = std::mem::replace(
            &mut self.aliases,
            select_statement
                .fields
                .iter()
                .filter_map(|fde| match fde {
                    FieldDefinitionExpr::Expr {
                        alias: Some(alias), ..
                    } => Some(alias.clone()),
                    _ => None,
                })
                .collect(),
        );
        let orig_can_reference_aliases = self.can_reference_aliases;
        self.can_reference_aliases = false;

        walk_select_statement(self, select_statement)?;

        self.aliases = orig_aliases;
        self.can_reference_aliases = orig_can_reference_aliases;
        Ok(())
    }

    fn visit_having_clause(&mut self, expr: &'ast Expr) -> Result<(), Self::Error> {
        self.can_reference_aliases = true;
        self.visit_expr(expr)?;
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_order_clause(&mut self, order: &'ast OrderClause) -> Result<(), Self::Error> {
        for ord_by in &order.order_by {
            self.can_reference_aliases = matches!(
                &ord_by.field,
                FieldReference::Expr(Expr::Column(Column { table: None, .. }))
            );
            self.visit_field_reference(&ord_by.field)?;
        }
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_group_by_clause(&mut self, group_by: &'ast GroupByClause) -> Result<(), Self::Error> {
        for field in &group_by.fields {
            self.can_reference_aliases = matches!(
                field,
                FieldReference::Expr(Expr::Column(Column { table: None, .. }))
            );
            self.visit_field_reference(field)?;
        }
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_function_expr(
        &mut self,
        function_expr: &'ast FunctionExpr,
    ) -> Result<(), Self::Error> {
        // Inside function calls, columns are always table columns, never aliases.
        let saved = self.can_reference_aliases;
        self.can_reference_aliases = false;
        walk_function_expr(self, function_expr)?;
        self.can_reference_aliases = saved;
        Ok(())
    }

    // ── All table columns must be qualified ──
    fn visit_column(&mut self, column: &'ast Column) -> Result<(), Self::Error> {
        if column.table.is_none() {
            // Allow legitimate alias references.
            if self.can_reference_aliases && self.aliases.contains(&column.name) {
                return Ok(());
            }
            internal!("Unresolved column '{}'", column.display(self.dialect));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;
    use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

    use super::*;

    fn validate_postgres(sql: &str) -> ReadySetResult<()> {
        let q = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, sql)
            .unwrap();
        match q {
            SqlQuery::Select(stmt) => stmt.validate_pipeline_invariants(Dialect::PostgreSQL),
            SqlQuery::CompoundSelect(csq) => {
                for (_op, stmt) in &csq.selects {
                    stmt.validate_pipeline_invariants(Dialect::PostgreSQL)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    // ── Column qualification ──

    #[test]
    fn qualified_columns_pass() {
        assert!(validate_postgres("SELECT t.x, t.y FROM t WHERE t.x = 1").is_ok());
    }

    #[test]
    fn unqualified_column_in_select_fails() {
        assert!(validate_postgres("SELECT x FROM t").is_err());
    }

    #[test]
    fn unqualified_column_in_where_fails() {
        assert!(validate_postgres("SELECT t.x FROM t WHERE y = 1").is_err());
    }

    #[test]
    fn alias_in_having_passes() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) AS s FROM t GROUP BY t.x HAVING s > 10")
                .is_ok()
        );
    }

    #[test]
    fn non_alias_unqualified_in_having_fails() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) AS s FROM t GROUP BY t.x HAVING z > 10")
                .is_err()
        );
    }

    #[test]
    fn alias_in_group_by_passes() {
        assert!(validate_postgres("SELECT t.x AS a, SUM(t.y) FROM t GROUP BY a").is_ok());
    }

    #[test]
    fn alias_in_order_by_passes() {
        assert!(validate_postgres("SELECT t.x AS a FROM t ORDER BY a").is_ok());
    }

    #[test]
    fn unqualified_inside_function_in_order_by_fails() {
        assert!(validate_postgres("SELECT t.x AS a FROM t ORDER BY ABS(a)").is_err());
    }

    #[test]
    fn subquery_independent_scope() {
        assert!(
            validate_postgres(
                "SELECT t.x AS a FROM t WHERE t.x IN (SELECT s.y FROM s WHERE s.y = 1)"
            )
            .is_ok()
        );
    }

    #[test]
    fn unqualified_in_subquery_fails() {
        assert!(validate_postgres("SELECT t.x FROM t WHERE t.x IN (SELECT y FROM s)").is_err());
    }

    /// Regression test: a subquery inside HAVING must not corrupt can_reference_aliases
    /// for sibling expressions in the same HAVING clause.
    #[test]
    fn alias_in_having_after_subquery_passes() {
        assert!(
            validate_postgres(
                "SELECT t.x, SUM(t.y) AS s FROM t GROUP BY t.x \
                 HAVING (SELECT COUNT(*) FROM s WHERE s.z = t.x) > 0 AND s > 10"
            )
            .is_ok()
        );
    }

    /// Subquery inside ORDER BY must not corrupt alias resolution for subsequent ORDER BY items.
    #[test]
    fn alias_in_order_by_after_subquery_passes() {
        assert!(
            validate_postgres(
                "SELECT t.x AS a, t.y FROM t ORDER BY (SELECT s.z FROM s LIMIT 1), a"
            )
            .is_ok()
        );
    }

    /// Inner subquery with its own ORDER BY (which toggles can_reference_aliases) must not
    /// corrupt the outer HAVING alias resolution.  This isolates the can_reference_aliases
    /// save/restore path — the inner ORDER BY leaves the flag as `false` on exit.
    #[test]
    fn alias_in_having_after_subquery_with_order_by_passes() {
        assert!(
            validate_postgres(
                "SELECT t.x, SUM(t.y) AS s FROM t GROUP BY t.x \
                 HAVING (SELECT s.z FROM s ORDER BY s.z LIMIT 1) IS NOT NULL AND s > 10"
            )
            .is_ok()
        );
    }

    // ── No CTEs ──

    #[test]
    fn cte_in_query_fails() {
        assert!(validate_postgres("WITH cq AS (SELECT s.x FROM s) SELECT cq.x FROM cq").is_err());
    }

    // ── No unexpanded stars ──

    #[test]
    fn star_in_select_fails() {
        assert!(validate_postgres("SELECT * FROM t").is_err());
    }

    #[test]
    fn qualified_star_in_select_fails() {
        assert!(validate_postgres("SELECT t.* FROM t").is_err());
    }

    // ── No JOIN ... USING ──

    #[test]
    fn join_using_fails() {
        assert!(validate_postgres("SELECT t.x FROM t JOIN s USING (x)").is_err());
    }

    #[test]
    fn join_on_passes() {
        assert!(validate_postgres("SELECT t.x FROM t JOIN s ON t.x = s.x").is_ok());
    }
}
