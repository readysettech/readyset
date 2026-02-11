use readyset_errors::{ReadySetError, ReadySetResult, unsupported};
use readyset_sql::analysis::visit_mut::{VisitorMut, walk_select_statement};
use readyset_sql::ast::{
    Expr, FieldDefinitionExpr, JoinRightSide, SelectStatement, SqlQuery, TableExprInner,
};

/// Visitor that errors if `ROW` is found in the projection or in VALUES clause expressions.
///
/// ROW in the projection cannot be serialized back to the client. ROW in VALUES clauses would
/// create ROW-typed columns that also hit serialization issues when projected.
/// ROW is allowed in other positions like predicates (e.g. `WHERE (a, b) IN ((1, 2))`).
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

/// Checks for `ROW` constructor usage in positions that would cause serialization issues:
/// the SELECT projection and VALUES clause expressions.
///
/// ROW is allowed in other positions like predicates.
pub trait DisallowRow {
    /// Checks if the `ROW` constructor is used in the projection or VALUES clauses and
    /// throws an error if found.
    ///
    /// ```sql
    /// SELECT ROW(1, 2, 3) FROM t; -- Error: ROW in projection
    /// SELECT * FROM (VALUES((1,2))) AS v(c) JOIN t ON ...; -- Error: ROW in VALUES
    /// ```
    ///
    /// Row is allowed in predicates:
    ///
    /// ```sql
    /// SELECT * FROM t WHERE (id, name) IN ((1, 'foo'), (2, 'bar')); -- This is allowed
    /// ```
    fn disallow_row(&mut self) -> ReadySetResult<&mut Self>
    where
        Self: Sized;
}

impl DisallowRow for SelectStatement {
    fn disallow_row(&mut self) -> ReadySetResult<&mut Self> {
        let () = DisallowRowVisitor.visit_select_statement(self)?;
        Ok(self)
    }
}

impl DisallowRow for SqlQuery {
    fn disallow_row(&mut self) -> ReadySetResult<&mut Self> {
        if let SqlQuery::Select(select) = self {
            select.disallow_row()?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::Dialect;
    use readyset_sql_parsing::{ParsingPreset, parse_query, parse_query_with_config};

    #[test]
    fn test_disallow_row_in_projection() {
        let mut query =
            parse_query(Dialect::PostgreSQL, "SELECT ROW(1, 2, 3) FROM things;").unwrap();
        assert!(query.disallow_row().is_err());
    }

    #[test]
    fn test_allow_row_elsewhere() {
        let mut query = parse_query(
            Dialect::MySQL,
            "SELECT * FROM things WHERE (id, name) IN ((1, 'foo'), (2, 'bar'));",
        )
        .unwrap();
        assert!(query.disallow_row().is_ok());

        let mut query = parse_query(
            Dialect::PostgreSQL,
            "SELECT * FROM things WHERE (id, name) IN ((1, 'foo'), (2, 'bar'));",
        )
        .unwrap();
        assert!(query.disallow_row().is_ok());
    }

    // ARRAY(SELECT ...) is only supported by the sqlparser backend, so these tests
    // use OnlySqlparser.

    #[test]
    fn test_disallow_row_in_array_subquery() {
        let mut query = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            "SELECT ARRAY(SELECT ROW(1, 2) FROM t) FROM s;",
        )
        .unwrap();
        assert!(query.disallow_row().is_err());
    }

    #[test]
    fn test_disallow_implicit_row_in_array_subquery() {
        let mut query = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            "SELECT ARRAY(SELECT (a, b) FROM t) FROM s;",
        )
        .unwrap();
        assert!(query.disallow_row().is_err());
    }

    #[test]
    fn test_allow_scalar_array_subquery() {
        let mut query = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            "SELECT ARRAY(SELECT col FROM t) FROM s;",
        )
        .unwrap();
        assert!(query.disallow_row().is_ok());
    }

    #[test]
    fn test_disallow_row_in_from_subquery() {
        let mut query = parse_query(
            Dialect::PostgreSQL,
            "SELECT * FROM (SELECT ROW(1, 2) FROM t) sub;",
        )
        .unwrap();
        assert!(query.disallow_row().is_err());
    }

    #[test]
    fn test_disallow_row_in_values() {
        // Double-paren VALUES produces ROW expressions — must be rejected
        let mut query = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            "SELECT * FROM (VALUES((1, 1), (1, 2))) AS v(c1, c2) JOIN t ON v.c1 = t.id",
        )
        .unwrap();
        assert!(query.disallow_row().is_err());
    }
}
