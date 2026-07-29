use readyset_errors::{ReadySetError, ReadySetResult, unsupported};
use readyset_sql::Dialect;
use readyset_sql::analysis::visit_mut::{VisitorMut, walk_select_statement};
use readyset_sql::ast::{
    Expr, FieldDefinitionExpr, JoinRightSide, SelectStatement, SqlQuery, TableExprInner,
};

/// Visitor that errors if `ROW` is found in the projection or in VALUES clause expressions.
///
/// MySQL has no row type it can return to a client, so a ROW in the projection cannot be
/// serialized back. ROW in VALUES clauses would create ROW-typed columns that hit the same wall
/// when projected. ROW is allowed in other positions like predicates (e.g.
/// `WHERE (a, b) IN ((1, 2))`).
struct DisallowRowVisitor;

impl<'ast> VisitorMut<'ast> for DisallowRowVisitor {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        stmt: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        for item in &stmt.fields {
            if let FieldDefinitionExpr::Expr {
                expr: Expr::Row { .. },
                ..
            } = item
            {
                unsupported!("ROW constructor not allowed in select");
            }
        }

        // Check VALUES clauses in FROM tables and JOINs for ROW expressions.
        // ROW/tuple expressions like VALUES((1,1), (1,2)) create ROW-typed columns
        // that cannot be serialized when projected.
        for te in stmt
            .tables
            .iter()
            .chain(stmt.join.iter().flat_map(|j| match &j.right {
                JoinRightSide::Table(t) => std::slice::from_ref(t),
                JoinRightSide::Tables(ts) => ts.as_slice(),
            }))
        {
            if let TableExprInner::Values { rows } = &te.inner {
                for expr in rows.iter().flatten() {
                    if matches!(expr, Expr::Row { .. }) {
                        unsupported!("ROW/tuple expressions in VALUES clauses are not supported");
                    }
                }
            }
        }

        // Recurse into subqueries
        walk_select_statement(self, stmt)
    }
}

/// Checks for `ROW` constructor usage in positions that MySQL cannot return to a client: the
/// SELECT projection and VALUES clause expressions.
///
/// This applies to MySQL only. MySQL answers a row constructor used where a scalar is expected
/// with error 1241, "Operand should contain 1 column(s)"; refusing the query here keeps it out of
/// the cache so it proxies upstream and the client sees MySQL's own error. PostgreSQL projects
/// these as the `record` pseudo-type, which Readyset encodes field-wise.
///
/// ROW is allowed in other positions like predicates, in both engines.
pub trait DisallowRow {
    /// Checks if the `ROW` constructor is used in the projection or VALUES clauses and
    /// throws an error if found, when `dialect` is MySQL.
    ///
    /// ```sql
    /// SELECT ROW(1, 2, 3) FROM t; -- Error on MySQL: ROW in projection
    /// SELECT * FROM (VALUES((1,2))) AS v(c) JOIN t ON ...; -- Error on MySQL: ROW in VALUES
    /// ```
    ///
    /// Row is allowed in predicates:
    ///
    /// ```sql
    /// SELECT * FROM t WHERE (id, name) IN ((1, 'foo'), (2, 'bar')); -- This is allowed
    /// ```
    fn disallow_row(&mut self, dialect: Dialect) -> ReadySetResult<&mut Self>
    where
        Self: Sized;
}

impl DisallowRow for SelectStatement {
    fn disallow_row(&mut self, dialect: Dialect) -> ReadySetResult<&mut Self> {
        if dialect == Dialect::MySQL {
            let () = DisallowRowVisitor.visit_select_statement(self)?;
        }
        Ok(self)
    }
}

impl DisallowRow for SqlQuery {
    fn disallow_row(&mut self, dialect: Dialect) -> ReadySetResult<&mut Self> {
        if let SqlQuery::Select(select) = self {
            select.disallow_row(dialect)?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

    /// Runs the pass over `sql` parsed in `dialect`, gated on that same dialect.
    fn check(dialect: Dialect, sql: &str) -> ReadySetResult<()> {
        parse_query_with_config(ParsingPreset::OnlySqlparser, dialect, sql)
            .unwrap()
            .disallow_row(dialect)
            .map(|_| ())
    }

    #[test]
    fn test_disallow_row_in_projection() {
        assert!(check(Dialect::MySQL, "SELECT ROW(1, 2, 3) FROM things;").is_err());
    }

    #[test]
    fn test_allow_row_in_postgres_projection() {
        assert!(check(Dialect::PostgreSQL, "SELECT ROW(1, 2, 3) FROM things;").is_ok());
    }

    #[test]
    fn test_allow_row_elsewhere() {
        let sql = "SELECT * FROM things WHERE (id, name) IN ((1, 'foo'), (2, 'bar'));";
        assert!(check(Dialect::MySQL, sql).is_ok());
        assert!(check(Dialect::PostgreSQL, sql).is_ok());
    }

    // ARRAY(SELECT ...) and double-paren VALUES are PostgreSQL-only syntax, so those cases only
    // exercise the un-gated side of the pass. MySQL rejects both at parse time upstream.

    #[test]
    fn test_allow_row_in_array_subquery() {
        assert!(
            check(
                Dialect::PostgreSQL,
                "SELECT ARRAY(SELECT ROW(1, 2) FROM t) FROM s;"
            )
            .is_ok()
        );
    }

    #[test]
    fn test_allow_implicit_row_in_array_subquery() {
        assert!(
            check(
                Dialect::PostgreSQL,
                "SELECT ARRAY(SELECT (a, b) FROM t) FROM s;"
            )
            .is_ok()
        );
    }

    #[test]
    fn test_allow_scalar_array_subquery() {
        assert!(
            check(
                Dialect::PostgreSQL,
                "SELECT ARRAY(SELECT col FROM t) FROM s;"
            )
            .is_ok()
        );
    }

    #[test]
    fn test_disallow_row_in_from_subquery() {
        let sql = "SELECT * FROM (SELECT ROW(1, 2) FROM t) sub;";
        assert!(check(Dialect::MySQL, sql).is_err());
        assert!(check(Dialect::PostgreSQL, sql).is_ok());
    }

    #[test]
    fn test_allow_row_in_values() {
        // Double-paren VALUES produces ROW expressions, which PostgreSQL projects as `record`.
        assert!(
            check(
                Dialect::PostgreSQL,
                "SELECT * FROM (VALUES((1, 1), (1, 2))) AS v(c1, c2) JOIN t ON v.c1 = t.id",
            )
            .is_ok()
        );
    }
}
