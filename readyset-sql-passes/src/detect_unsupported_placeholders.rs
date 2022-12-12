use nom_sql::analysis::visit::{walk_expr, walk_select_statement, Visitor};
use nom_sql::{BinaryOperator, Expr, ItemPlaceholder, Literal, SelectStatement};
use readyset_errors::{ReadySetError, ReadySetResult};
use vec1::Vec1;

pub trait DetectUnsupportedPlaceholders {
    fn detect_unsupported_placeholders(&self) -> ReadySetResult<()>;
}

/// Collects all unsupported placeholders in a query.
///
/// This pass should be run after all rewrite passes, as some rewrite passes may modify the query in
/// such a way that it can be supported.
struct UnsupportedPlaceholderVisitor {
    /// List of unsupported placeholders. Must be Literal::Placeholder(_)
    unsupported_placeholders: Vec<u32>,
    /// Depth with respect to nested subqueries. The top level of the query is given a depth of 1
    depth: u32,
    /// Whether we are in the where clause of the query. Only placeholder in the WHERE clause and
    /// in the LIMIT/OFFSET clauses are supported.
    in_where_clause: bool,
}

impl UnsupportedPlaceholderVisitor {
    fn new() -> Self {
        Self {
            unsupported_placeholders: Vec::new(),
            depth: 0,
            in_where_clause: false,
        }
    }
}

impl<'ast> Visitor<'ast> for UnsupportedPlaceholderVisitor {
    type Error = !;
    fn visit_where_clause(&mut self, expr: &'ast nom_sql::Expr) -> Result<(), Self::Error> {
        // Only set self.in_where_clause if we are in the top level query
        if self.depth == 1 {
            self.in_where_clause = true;
        }
        let Ok(_) = self.visit_expr(expr);
        // Only set self.in_where_clause if we are in the top level query
        if self.depth == 1 {
            self.in_where_clause = false;
        }
        Ok(())
    }

    fn visit_limit_clause(
        &mut self,
        _limit_clause: &'ast nom_sql::LimitClause,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// We only call walk_expr if this expr may contain an unsupported placeholder. If this
    /// expression contains a supported placeholder, we continue.
    fn visit_expr(&mut self, expr: &'ast nom_sql::Expr) -> Result<(), Self::Error> {
        // Walk expresssion if we're not in the WHERE clause of the top-level query
        if self.depth > 1 || !self.in_where_clause {
            return walk_expr(self, expr);
        }

        match expr {
            Expr::BinaryOp { lhs, rhs, op } => {
                // The placeholder is supported if we have an equality or ordered comparison with a
                // column on the left and placeholder on the right. In that case, don't walk the
                // Expr.
                // TODO: consider mixed comparisons and whether the WHERE expression is in
                // conjunctive normal form
                if !(matches!(**lhs, Expr::Column(_))
                    && matches!(**rhs, Expr::Literal(_)) // no need to walk for any literal
                    && (matches!(op, BinaryOperator::Equal) || op.is_comparison()))
                {
                    let Ok(_) = walk_expr(self, expr);
                }
            }
            Expr::Between {
                operand, min, max, ..
            } => {
                // We only support placeholders if the operand is a column
                if !matches!(**operand, Expr::Column(_)) {
                    let Ok(_) = walk_expr(self, expr);
                } else {
                    // If min and max are expressions that contain placeholders, rather than
                    // placeholders themselves, then the placeholders are not supported.
                    //
                    // We allow a mix of non-placeholder and placeholder literals in min and max.
                    if !matches!(**min, Expr::Literal(_)) {
                        let Ok(_) = walk_expr(self, min);
                    }
                    if !matches!(**max, Expr::Literal(_)) {
                        let Ok(_) = walk_expr(self, max);
                    }
                }
            }
            _ => {
                let Ok(_) = walk_expr(self, expr);
            }
        };
        Ok(())
    }

    fn visit_literal(&mut self, literal: &'ast Literal) -> Result<(), Self::Error> {
        // We will not visit supported literals, so we should record any literal we encounter
        if let Literal::Placeholder(ItemPlaceholder::DollarNumber(p)) = literal {
            self.unsupported_placeholders.push(*p);
        }

        Ok(())
    }

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast nom_sql::SelectStatement,
    ) -> Result<(), Self::Error> {
        self.depth += 1;
        walk_select_statement(self, select_statement)?;
        self.depth -= 1;
        Ok(())
    }
}

impl DetectUnsupportedPlaceholders for SelectStatement {
    fn detect_unsupported_placeholders(&self) -> ReadySetResult<()> {
        let mut visitor = UnsupportedPlaceholderVisitor::new();
        let Ok(()) = visitor.visit_select_statement(self);
        if visitor.unsupported_placeholders.is_empty() {
            Ok(())
        } else {
            #[allow(clippy::unwrap_used)] // checked that !is_empty()
            Err(ReadySetError::UnsupportedPlaceholders {
                placeholders: Vec1::try_from(visitor.unsupported_placeholders).unwrap(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use readyset_errors::{ReadySetError, ReadySetResult};

    use super::DetectUnsupportedPlaceholders;
    use crate::util::parse_select_statement;

    // Takes the result and the list of placeholders that should be detected. If the list is empty,
    // the pass is expected to return Ok(()).
    fn extracts_placeholders(res: ReadySetResult<()>, placeholders_truth: &[u32]) {
        match res {
            Ok(_) => {
                assert_eq!(placeholders_truth.len(), 0);
            }
            Err(ReadySetError::UnsupportedPlaceholders { placeholders }) => {
                assert_eq!(&placeholders, placeholders_truth);
            }
            _ => {
                panic!("unreachable");
            }
        }
    }

    #[test]
    fn extracts_placeholder_in_fields() {
        let select = parse_select_statement("SELECT $1, a FROM t WHERE b = $2");
        let res = select.detect_unsupported_placeholders();
        extracts_placeholders(res, &[1]);
    }

    #[test]
    fn extracts_placeholder_expr_comparison() {
        let select = parse_select_statement("SELECT a FROM t WHERE b + $1 = 1 AND c + 1 = $2");
        let res = select.detect_unsupported_placeholders();
        extracts_placeholders(res, &[1, 2]);
    }

    #[test]
    fn extracts_placeholder_having_clause() {
        let select =
            parse_select_statement("SELECT a FROM t WHERE b = $1 GROUP BY d HAVING sum(d) = $2");
        let res = select.detect_unsupported_placeholders();
        extracts_placeholders(res, &[2]);
    }

    #[test]
    fn extracts_nested_subquery() {
        let select =
            parse_select_statement("SELECT a FROM t1 WHERE b IN (SELECT c FROM t2 WHERE d = $1)");
        let res = select.detect_unsupported_placeholders();
        extracts_placeholders(res, &[1]);
    }

    #[test]
    fn ignores_supported_between() {
        let select = parse_select_statement("SELECT a FROM t WHERE b BETWEEN $1 AND $2");
        let res = select.detect_unsupported_placeholders();
        extracts_placeholders(res, &[]);
    }

    #[test]
    fn ignores_supported_limit_offset() {
        let select = parse_select_statement("SELECT a FROM t WHERE b = $1 LIMIT $2 OFFSET $3");
        let res = select.detect_unsupported_placeholders();
        extracts_placeholders(res, &[]);
    }

    #[test]
    fn ignores_supported_comparisons() {
        let select = parse_select_statement("SELECT a FROM t WHERE b >= $1 AND c < $2 AND d = $3");
        let res = select.detect_unsupported_placeholders();
        extracts_placeholders(res, &[]);
    }

    #[test]
    fn ignores_supported_placeholder_after_subquery() {
        let select = parse_select_statement(
            "SELECT a FROM t WHERE b IN (SELECT c FROM t WHERE d = 1) AND e = $1",
        );
        let res = select.detect_unsupported_placeholders();
        extracts_placeholders(res, &[]);
    }
}
