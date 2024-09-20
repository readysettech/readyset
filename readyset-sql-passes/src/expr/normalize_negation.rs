use nom_sql::analysis::visit_mut::{self, VisitorMut};
use nom_sql::{BinaryOperator, Expr, UnaryOperator};

/// Attempt to replace `expr` with the equivalent expression negated. Returns `true` if that was
/// doable, or `false` if it was impossible. If this function returns `false`, `expr` was not
/// mutated
fn negate_expr(expr: &mut Expr) -> bool {
    match expr {
        Expr::BinaryOp { op, lhs, rhs } => {
            if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                if !negate_expr(lhs) {
                    return false;
                }
                if !negate_expr(rhs) {
                    // If we can't negate the rhs, re-negate the lhs to revert it to its original
                    // state.
                    assert!(negate_expr(lhs), "negate_expr must be involutive!");
                    return false;
                }
            }

            *op = match *op {
                BinaryOperator::And => BinaryOperator::Or,
                BinaryOperator::Or => BinaryOperator::And,
                BinaryOperator::Equal => BinaryOperator::NotEqual,
                BinaryOperator::NotEqual => BinaryOperator::Equal,
                BinaryOperator::Greater => BinaryOperator::LessOrEqual,
                BinaryOperator::GreaterOrEqual => BinaryOperator::Less,
                BinaryOperator::Less => BinaryOperator::GreaterOrEqual,
                BinaryOperator::LessOrEqual => BinaryOperator::Greater,
                BinaryOperator::Like => BinaryOperator::NotLike,
                BinaryOperator::NotLike => BinaryOperator::Like,
                BinaryOperator::ILike => BinaryOperator::NotILike,
                BinaryOperator::NotILike => BinaryOperator::ILike,
                BinaryOperator::Is => BinaryOperator::IsNot,
                BinaryOperator::IsNot => BinaryOperator::Is,
                BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::HashSubtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo
                | BinaryOperator::DoublePipe
                | BinaryOperator::Arrow1
                | BinaryOperator::Arrow2
                | BinaryOperator::HashArrow1
                | BinaryOperator::HashArrow2 => return false,
                BinaryOperator::QuestionMark
                | BinaryOperator::QuestionMarkPipe
                | BinaryOperator::QuestionMarkAnd
                | BinaryOperator::AtArrowRight
                | BinaryOperator::AtArrowLeft => {
                    // Note we return true in this case to bypass the *op = ... above
                    *expr = Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        rhs: Box::new(expr.take()),
                    };
                    return true;
                }
            }
        }
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            rhs,
        } => {
            *expr = rhs.take();
        }
        Expr::Between { negated, .. } | Expr::In { negated, .. } => {
            *negated = !*negated;
        }
        _ => {
            return false;
        }
    }

    true
}

struct NormalizeNegationVisitor;
impl<'ast> VisitorMut<'ast> for NormalizeNegationVisitor {
    type Error = std::convert::Infallible;

    fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        if let Expr::UnaryOp {
            op: UnaryOperator::Not,
            rhs,
        } = expr
        {
            if !negate_expr(rhs) {
                return Ok(());
            }
            *expr = rhs.take()
        }
        visit_mut::walk_expr(self, expr)
    }
}

pub fn normalize_negation(expr: &mut Expr) {
    let Ok(()) = NormalizeNegationVisitor.visit_expr(expr);
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_expr, Dialect, DialectDisplay};

    use super::*;

    #[test]
    fn it_normalizes() {
        let mut expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            rhs: Box::new(Expr::BinaryOp {
                op: BinaryOperator::And,
                lhs: Box::new(Expr::BinaryOp {
                    op: BinaryOperator::Less,
                    lhs: Box::new(Expr::Column("a".into())),
                    rhs: Box::new(Expr::Column("b".into())),
                }),
                rhs: Box::new(Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(Expr::Column("c".into())),
                    rhs: Box::new(Expr::Column("b".into())),
                }),
            }),
        };

        let target = Expr::BinaryOp {
            op: BinaryOperator::Or,
            lhs: Box::new(Expr::BinaryOp {
                op: BinaryOperator::GreaterOrEqual,
                lhs: Box::new(Expr::Column("a".into())),
                rhs: Box::new(Expr::Column("b".into())),
            }),
            rhs: Box::new(Expr::BinaryOp {
                op: BinaryOperator::NotEqual,
                lhs: Box::new(Expr::Column("c".into())),
                rhs: Box::new(Expr::Column("b".into())),
            }),
        };

        let Ok(()) = NormalizeNegationVisitor.visit_expr(&mut expr);
        assert_eq!(
            expr,
            target,
            "expected = {}\nactual = {}",
            target.display(nom_sql::Dialect::MySQL),
            expr.display(nom_sql::Dialect::MySQL)
        );
    }

    #[test]
    fn normalize_in_with_not() {
        let mut expr = parse_expr(Dialect::MySQL, "NOT id IN (1, 2)").unwrap();
        let expected = parse_expr(Dialect::MySQL, "id NOT IN (1, 2)").unwrap();
        normalize_negation(&mut expr);
        assert_eq!(expr, expected)
    }

    #[test]
    fn normalize_in_without_not() {
        let mut expr = parse_expr(Dialect::MySQL, "id IN (1, 2)").unwrap();
        let expected = expr.clone();
        normalize_negation(&mut expr);
        assert_eq!(expr, expected)
    }

    #[test]
    fn non_negatable_rhs() {
        let mut expr = parse_expr(Dialect::MySQL, "NOT (x = 1 OR some_function(z))").unwrap();
        let expected = expr.clone();
        normalize_negation(&mut expr);
        assert_eq!(expr, expected);
    }

    #[test]
    fn normalize_question_mark() {
        let mut expr = parse_expr(Dialect::MySQL, "NOT (j ? 'key' AND NOT b)").unwrap();
        let expected = parse_expr(Dialect::MySQL, "NOT j ? 'key' OR b").unwrap();
        normalize_negation(&mut expr);
        assert_eq!(expr, expected);
    }
}
