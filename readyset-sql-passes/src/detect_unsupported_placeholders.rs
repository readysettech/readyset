use nom_sql::analysis::visit::{walk_expr, walk_select_statement, Visitor};
use nom_sql::{BinaryOperator, Expr, ItemPlaceholder, Literal, SelectStatement};
use readyset_errors::{ReadySetError, ReadySetResult};
use vec1::Vec1;

pub trait DetectUnsupportedPlaceholders {
    fn detect_unsupported_placeholders(&self, config: Config) -> ReadySetResult<()>;
}

#[derive(Default)]
pub struct Config {
    /// Whether to allow queries with a mix of equality and ordering omparisons. If we disallow
    /// these queries, then this pass will flag placeholders that are part of ordering comparisons
    /// as unsupported.
    ///
    /// Defaults to `false` because mixed comparisons are experimental.
    ///
    /// TODO: We could instead indicate that either the placeholders in the equality or ordering
    /// comparisons are not supported. This will give us the ability to automatically inline the
    /// query one way or the other. Even without doing so, we support manually inlining the query
    /// either way.
    pub allow_mixed_comparisons: bool,
}

/// State of the Visitor while visiting the query.
pub struct Context {
    /// Depth with respect to nested subqueries. The top level of the query is given a depth of 1
    depth: u32,
    /// Whether we are in the where clause of the query. Only placeholder in the WHERE clause and
    /// in the LIMIT/OFFSET clauses are supported.
    in_where_clause: bool,
    /// Placeholders appearing in supported = and != comparisons that we have seen.
    equality_comparisons: Vec<u32>,
    /// Placeholders appearing in supported >, <, >=, <= comparisons that we have seen.
    ordering_comparisons: Vec<u32>,
}

impl Context {
    fn new() -> Self {
        Self {
            depth: 0,
            in_where_clause: false,
            equality_comparisons: Vec::new(),
            ordering_comparisons: Vec::new(),
        }
    }
}

/// Collects all unsupported placeholders in a query.
struct UnsupportedPlaceholderVisitor {
    /// The list of unsupported placeholders returned by this pass.
    unsupported_placeholders: Vec<u32>,
    /// Context observed while visiting.
    context: Context,
    // Config to optionally allow certain placeholders.
    config: Config,
}

impl UnsupportedPlaceholderVisitor {
    fn new(config: Config) -> Self {
        Self {
            unsupported_placeholders: Vec::new(),
            context: Context::new(),
            config,
        }
    }

    /// Records placeholders used in equality or ordering comparisons if we do not allow mixed
    /// equality comparisons.
    ///
    /// Placeholders used in `Expr::Between` are handled elsewhere.
    fn record_comparison_expr(&mut self, lhs: &Expr, rhs: &Expr, op: &BinaryOperator) {
        if !self.config.allow_mixed_comparisons {
            match (lhs, rhs, op) {
                (
                    Expr::Column(_),
                    Expr::Literal(Literal::Placeholder(ItemPlaceholder::DollarNumber(n))),
                    BinaryOperator::Equal,
                ) => self.context.equality_comparisons.push(*n),
                (
                    Expr::Column(_),
                    Expr::Literal(Literal::Placeholder(ItemPlaceholder::DollarNumber(n))),
                    cmp,
                ) if cmp.is_ordering_comparison() => self.context.ordering_comparisons.push(*n),
                _ => { /* Nothing to record */ }
            }
        }
    }

    /// Consumes self and returns the unsupported placeholders found in the visit pass.
    ///
    /// If mixed comparisons are not allowed but exist in the query, we will consider all
    /// placeholders found in ordering comparisons unsupported.
    ///
    /// TODO: We could instead indicate that either the placeholders in the equality or ordering
    /// comparisons are not supported. This will give us the ability to automatically inline the
    /// query one way or the other. Even without doing so, we support manually inlining the query
    /// either way.
    pub fn unsupported_placeholders(mut self) -> Vec<u32> {
        if self.config.allow_mixed_comparisons || self.context.equality_comparisons.is_empty() {
            self.unsupported_placeholders
        } else {
            self.unsupported_placeholders
                .extend(self.context.ordering_comparisons.iter());
            self.unsupported_placeholders
        }
    }
}

impl<'ast> Visitor<'ast> for UnsupportedPlaceholderVisitor {
    type Error = !;
    fn visit_where_clause(&mut self, expr: &'ast nom_sql::Expr) -> Result<(), Self::Error> {
        // Only set Context::in_where_clause if we are in the top level query
        if self.context.depth == 1 {
            self.context.in_where_clause = true;
        }
        let Ok(_) = self.visit_expr(expr);
        // Only set Context::in_where_clause if we are in the top level query
        if self.context.depth == 1 {
            self.context.in_where_clause = false;
        }
        Ok(())
    }

    fn visit_limit_clause(
        &mut self,
        _limit_clause: &'ast nom_sql::LimitClause,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// We do nothing except record any placeholders in `Context::ordering_comparisons` or
    /// `Context::equality_comparisons` if we have:
    /// - a comparison with a `Expr::Column` on the left and a `Expr::Literal` on the right
    /// - a `Expr::Between` with a `Expr::Column` as the operand and `Expr::Literal`s for the min
    ///   and max expressions.
    ///
    /// Otherwise, walk the expression and record any placeholder values we find in
    /// `Self::unsupported_placeholders`.
    fn visit_expr(&mut self, expr: &'ast nom_sql::Expr) -> Result<(), Self::Error> {
        // Walk expresssion if we're not in the WHERE clause of the top-level query
        if self.context.depth > 1 || !self.context.in_where_clause {
            return walk_expr(self, expr);
        }

        // We do not call walk_expr() if we know all placeholders in the expr are supported.
        match expr {
            Expr::BinaryOp { lhs, rhs, op } => {
                // The placeholder is supported if we have an equality or ordering comparison with a
                // column on the left and placeholder on the right.
                if !(matches!(**lhs, Expr::Column(_))
                    && matches!(**rhs, Expr::Literal(_)) // no need to walk for any literal
                    && (matches!(op, BinaryOperator::Equal) || op.is_ordering_comparison()))
                {
                    let Ok(_) = walk_expr(self, expr);
                } else {
                    // Record placeholders in either Context::equality_comparisons or
                    // Context::ordering_comparisons.
                    self.record_comparison_expr(lhs, rhs, op);
                }
            }
            Expr::Between {
                operand, min, max, ..
            } => {
                // We only support placeholders in Expr::Between if the operand is a column.
                //
                // If min and max are expressions that contain placeholders, rather than
                // placeholders themselves, then the placeholders are not supported.
                //
                // We allow a mix of non-placeholder and placeholder literals in min and max.
                if !matches!(**operand, Expr::Column(_)) {
                    let Ok(_) = walk_expr(self, expr);
                } else {
                    if !matches!(**min, Expr::Literal(_)) {
                        let Ok(_) = walk_expr(self, min);
                    }
                    if !matches!(**max, Expr::Literal(_)) {
                        let Ok(_) = walk_expr(self, max);
                    }
                    // If we do not allow mixed equality comparisons, then we also have to register
                    // any placeholders in Context::ordering_comparisons.
                    if !self.config.allow_mixed_comparisons {
                        if let Expr::Literal(Literal::Placeholder(ItemPlaceholder::DollarNumber(
                            n,
                        ))) = **min
                        {
                            self.context.ordering_comparisons.push(n);
                        }
                        if let Expr::Literal(Literal::Placeholder(ItemPlaceholder::DollarNumber(
                            n,
                        ))) = **max
                        {
                            self.context.ordering_comparisons.push(n);
                        }
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
        // We will not call visit_literals() on supported placeholders, so we should record any
        // placeholder value that we encounter.
        if let Literal::Placeholder(ItemPlaceholder::DollarNumber(p)) = literal {
            self.unsupported_placeholders.push(*p);
        }

        Ok(())
    }

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast nom_sql::SelectStatement,
    ) -> Result<(), Self::Error> {
        self.context.depth += 1;
        walk_select_statement(self, select_statement)?;
        self.context.depth -= 1;
        Ok(())
    }
}

impl DetectUnsupportedPlaceholders for SelectStatement {
    fn detect_unsupported_placeholders(&self, config: Config) -> ReadySetResult<()> {
        let mut visitor = UnsupportedPlaceholderVisitor::new(config);
        let Ok(()) = visitor.visit_select_statement(self);

        let unsupported_placeholders = visitor.unsupported_placeholders();

        if unsupported_placeholders.is_empty() {
            Ok(())
        } else {
            #[allow(clippy::unwrap_used)] // checked that !is_empty()
            Err(ReadySetError::UnsupportedPlaceholders {
                placeholders: Vec1::try_from(unsupported_placeholders).unwrap(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use readyset_errors::{ReadySetError, ReadySetResult};

    use super::{Config, DetectUnsupportedPlaceholders};
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
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[1]);
    }

    #[test]
    fn extracts_placeholder_expr_comparison() {
        let select = parse_select_statement("SELECT a FROM t WHERE b + $1 = 1 AND c + 1 = $2");
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[1, 2]);
    }

    #[test]
    fn extracts_placeholder_having_clause() {
        let select =
            parse_select_statement("SELECT a FROM t WHERE b = $1 GROUP BY d HAVING sum(d) = $2");
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[2]);
    }

    #[test]
    fn extracts_nested_subquery() {
        let select =
            parse_select_statement("SELECT a FROM t1 WHERE b IN (SELECT c FROM t2 WHERE d = $1)");
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[1]);
    }

    #[test]
    fn extracts_ordering_placeholders() {
        let select = parse_select_statement(
            "SELECT a FROM t WHERE b BETWEEN $1 AND $2 AND c = $3 AND d <= $4",
        );
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[1, 2, 4]);
    }

    #[test]
    fn ignores_supported_between() {
        let select = parse_select_statement("SELECT a FROM t WHERE b BETWEEN $1 AND $2");
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[]);
    }

    #[test]
    fn ignores_supported_limit_offset() {
        let select = parse_select_statement("SELECT a FROM t WHERE b = $1 LIMIT $2 OFFSET $3");
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[]);
    }

    #[test]
    fn ignores_allowed_mixed_comparisons() {
        let select = parse_select_statement("SELECT a FROM t WHERE b >= $1 AND c < $2 AND d = $3");
        let res = select.detect_unsupported_placeholders(Config {
            allow_mixed_comparisons: true,
        });
        extracts_placeholders(res, &[]);
    }

    #[test]
    fn ignores_supported_placeholder_after_subquery() {
        let select = parse_select_statement(
            "SELECT a FROM t WHERE b IN (SELECT c FROM t WHERE d = 1) AND e = $1",
        );
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[]);
    }

    #[test]
    fn ignores_ordering_placeholders_only() {
        let select = parse_select_statement(
            "SELECT a FROM t WHERE b > $1 AND c BETWEEN $2 AND $3 AND d = '4'",
        );
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[]);
    }

    #[test]
    fn ignores_ordering_comparisons_when_equality_unsupported() {
        let select =
            parse_select_statement("SELECT a FROM t WHERE b > $1 AND c IN (SELECT * FROM t WHERE d = $2) GROUP BY e HAVING sum(e) = $3");
        let res = select.detect_unsupported_placeholders(Config::default());
        extracts_placeholders(res, &[2, 3]);
    }
}
