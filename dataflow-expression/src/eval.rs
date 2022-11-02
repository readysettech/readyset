use std::borrow::Borrow;

use readyset_data::{DfType, DfValue};
use readyset_errors::{ReadySetError, ReadySetResult};
use serde_json::Value as JsonValue;

use crate::like::{CaseInsensitive, CaseSensitive, LikePattern};
use crate::{BinaryOperator, Expr};

macro_rules! non_null {
    ($df_value:expr) => {
        if let Some(val) = $df_value.non_null() {
            val
        } else {
            return Ok(DfValue::None);
        }
    };
}

mod builtins;

impl Expr {
    /// Evaluate this expression, given a source record to pull columns from
    pub fn eval<D>(&self, record: &[D]) -> ReadySetResult<DfValue>
    where
        D: Borrow<DfValue>,
    {
        use Expr::*;

        // TODO: Enforce type coercion
        match self {
            Column { index, .. } => record
                .get(*index)
                .map(|dt| dt.borrow().clone())
                .ok_or(ReadySetError::ProjectExprInvalidColumnIndex(*index)),
            Literal { val, .. } => Ok(val.clone()),
            Op {
                op, left, right, ..
            } => {
                use BinaryOperator::*;

                let left_ty = left.ty();
                let right_ty = right.ty();

                let left = left.eval(record)?;
                let right = right.eval(record)?;

                let like = |case_sensitivity, negated| -> bool {
                    match (
                        left.coerce_to(&DfType::DEFAULT_TEXT, left_ty),
                        right.coerce_to(&DfType::DEFAULT_TEXT, right_ty),
                    ) {
                        (Ok(left), Ok(right)) => {
                            // unwrap: we just coerced these to Text, so they're definitely strings.
                            let left = left.as_str().unwrap();
                            let right = right.as_str().unwrap();

                            // NOTE(grfn): At some point, we may want to optimize this to pre-cache
                            // the LikePattern if the value is constant, since constructing a new
                            // LikePattern can be kinda slow.
                            let pat = LikePattern::new(right, case_sensitivity);

                            let matches = pat.matches(left);

                            if negated {
                                !matches
                            } else {
                                matches
                            }
                        }
                        // Anything that isn't Text or text-coercible can never be LIKE anything, so
                        // we return true if not negated or false otherwise.
                        _ => !negated,
                    }
                };

                match op {
                    Add => Ok((non_null!(left) + non_null!(right))?),
                    Subtract => Ok((non_null!(left) - non_null!(right))?),
                    Multiply => Ok((non_null!(left) * non_null!(right))?),
                    Divide => Ok((non_null!(left) / non_null!(right))?),
                    And => Ok((non_null!(left).is_truthy() && non_null!(right).is_truthy()).into()),
                    Or => Ok((non_null!(left).is_truthy() || non_null!(right).is_truthy()).into()),
                    Equal => Ok((non_null!(left)
                        == &non_null!(right).coerce_to(left_ty, right_ty)?)
                        .into()),
                    NotEqual => Ok((non_null!(left)
                        != &non_null!(right).coerce_to(left_ty, right_ty)?)
                        .into()),
                    Greater => Ok((non_null!(left) > non_null!(right)).into()),
                    GreaterOrEqual => Ok((non_null!(left) >= non_null!(right)).into()),
                    Less => Ok((non_null!(left) < non_null!(right)).into()),
                    LessOrEqual => Ok((non_null!(left) <= non_null!(right)).into()),
                    Is => Ok((left == right).into()),
                    IsNot => Ok((left != right).into()),
                    Like => Ok(like(CaseSensitive, false).into()),
                    NotLike => Ok(like(CaseSensitive, true).into()),
                    ILike => Ok(like(CaseInsensitive, false).into()),
                    NotILike => Ok(like(CaseInsensitive, true).into()),

                    // JSON operators:
                    JsonExists => {
                        let json_value = left.to_json()?;
                        let key = <&str>::try_from(&right)?;

                        let result = match json_value {
                            JsonValue::Object(map) => map.contains_key(key),
                            JsonValue::Array(vec) => vec.iter().any(|v| v.as_str() == Some(key)),
                            _ => false,
                        };
                        Ok(result.into())
                    }
                }
            }
            Cast { expr, ty, .. } => {
                let res = expr.eval(record)?;
                Ok(res.coerce_to(ty, expr.ty())?)
            }
            Call { func, ty } => func.eval(ty, record),
            CaseWhen {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                if condition.eval(record)?.is_truthy() {
                    then_expr.eval(record)
                } else {
                    else_expr.eval(record)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use nom_sql::Dialect::*;
    use nom_sql::{parse_expr, SqlType};
    use readyset_data::{Collation, DfType, Dialect, PgEnumMetadata};
    use Expr::*;

    use super::*;
    use crate::lower::tests::no_op_lower_context;
    use crate::utils::{column_with_type, make_column, make_literal};

    #[track_caller]
    pub(crate) fn eval_expr(expr: &str, dialect: nom_sql::Dialect) -> DfValue {
        let ast = parse_expr(dialect, expr).unwrap();
        let expr_dialect = match dialect {
            PostgreSQL => crate::Dialect::DEFAULT_POSTGRESQL,
            MySQL => crate::Dialect::DEFAULT_MYSQL,
        };
        let expr = Expr::lower(ast, expr_dialect, no_op_lower_context()).unwrap();
        expr.eval::<DfValue>(&[]).unwrap()
    }

    #[test]
    fn eval_column() {
        let expr = make_column(1);
        assert_eq!(
            expr.eval(&[DfValue::from(1), "two".try_into().unwrap()])
                .unwrap(),
            "two".try_into().unwrap()
        )
    }

    #[test]
    fn eval_literal() {
        let expr = make_literal(1.into());
        assert_eq!(
            expr.eval(&[DfValue::from(1), "two".try_into().unwrap()])
                .unwrap(),
            1.into()
        )
    }

    #[test]
    fn eval_add() {
        let expr = Op {
            left: Box::new(make_column(0)),
            right: Box::new(Op {
                left: Box::new(make_column(1)),
                right: Box::new(make_literal(3.into())),
                op: BinaryOperator::Add,
                ty: DfType::Unknown,
            }),
            op: BinaryOperator::Add,
            ty: DfType::Unknown,
        };
        assert_eq!(
            expr.eval(&[DfValue::from(1), DfValue::from(2)]).unwrap(),
            6.into()
        );
    }

    #[test]
    fn eval_json_exists() {
        let expr = Op {
            left: Box::new(column_with_type(0, DfType::Jsonb)),
            right: Box::new(column_with_type(1, DfType::Text(Collation::default()))),
            op: BinaryOperator::JsonExists,
            ty: DfType::Bool,
        };
        assert_eq!(
            expr.eval(&[DfValue::from("{\"abc\": 42}"), DfValue::from("xyz")])
                .unwrap(),
            false.into()
        );
        assert_eq!(
            expr.eval(&[DfValue::from("{\"abc\": 42}"), DfValue::from("abc")])
                .unwrap(),
            true.into()
        );
        assert_eq!(
            expr.eval(&[DfValue::from("[\"abc\"]"), DfValue::from("abc")])
                .unwrap(),
            true.into()
        );
        assert_eq!(
            expr.eval(&[DfValue::from("[\"abc\"]"), DfValue::from("xyz")])
                .unwrap(),
            false.into()
        );
    }

    #[test]
    fn eval_json_exists_bad_types() {
        let expr = Op {
            left: Box::new(column_with_type(0, DfType::Jsonb)),
            right: Box::new(column_with_type(1, DfType::Text(Collation::default()))),
            op: BinaryOperator::JsonExists,
            ty: DfType::Bool,
        };
        assert!(expr
            .eval(&[DfValue::from("bad_json"), DfValue::from("abc")])
            .is_err());
        assert!(expr
            .eval(&[DfValue::from("{\"abc\": 42}"), DfValue::from(67)])
            .is_err());
    }

    #[test]
    fn eval_comparisons() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd(2009, 10, 17),
            NaiveTime::from_hms(12, 0, 0),
        );
        let text_dt: DfValue = "2009-10-17 12:00:00".try_into().unwrap();
        let text_less_dt: DfValue = "2009-10-16 12:00:00".try_into().unwrap();

        macro_rules! assert_op {
            ($binary_op:expr, $value:expr, $expected:expr) => {
                let expr = Op {
                    left: Box::new(column_with_type(0, DfType::DEFAULT_TEXT)),
                    right: Box::new(make_literal($value)),
                    op: $binary_op,
                    ty: DfType::Unknown,
                };
                assert_eq!(
                    expr.eval::<DfValue>(&[dt.into()]).unwrap(),
                    $expected.into()
                );
            };
        }
        assert_op!(BinaryOperator::Less, text_less_dt.clone(), 0u8);
        assert_op!(BinaryOperator::Less, text_dt.clone(), 0u8);
        assert_op!(BinaryOperator::LessOrEqual, text_less_dt.clone(), 0u8);
        assert_op!(BinaryOperator::LessOrEqual, text_dt.clone(), 1u8);
        assert_op!(BinaryOperator::Greater, text_less_dt.clone(), 1u8);
        assert_op!(BinaryOperator::Greater, text_dt.clone(), 0u8);
        assert_op!(BinaryOperator::GreaterOrEqual, text_less_dt.clone(), 1u8);
        assert_op!(BinaryOperator::GreaterOrEqual, text_dt.clone(), 1u8);
        assert_op!(BinaryOperator::Equal, text_less_dt, 0u8);
        assert_op!(BinaryOperator::Equal, text_dt, 1u8);
    }

    #[test]
    fn eval_cast() {
        let expr = Cast {
            expr: Box::new(make_column(0)),
            to_type: SqlType::Int(None),
            ty: DfType::Int,
        };
        assert_eq!(
            expr.eval::<DfValue>(&["1".try_into().unwrap(), "2".try_into().unwrap()])
                .unwrap(),
            1i32.into()
        );
    }

    #[test]
    fn cast_to_char_with_multibyte_truncation() {
        assert_eq!(
            eval_expr("CAST('é' AS CHAR(1))", nom_sql::Dialect::MySQL),
            DfValue::from("é")
        );
    }

    #[test]
    fn value_truthiness() {
        assert_eq!(
            Expr::Op {
                left: Box::new(make_literal(1.into())),
                op: BinaryOperator::And,
                right: Box::new(make_literal(3.into())),
                ty: DfType::Unknown,
            }
            .eval::<DfValue>(&[])
            .unwrap(),
            1.into()
        );

        assert_eq!(
            Expr::Op {
                left: Box::new(make_literal(1.into())),
                op: BinaryOperator::And,
                right: Box::new(make_literal(0.into())),
                ty: DfType::Unknown,
            }
            .eval::<DfValue>(&[])
            .unwrap(),
            0.into()
        );
    }

    #[test]
    fn eval_case_when() {
        let expr = Expr::CaseWhen {
            condition: Box::new(Op {
                left: Box::new(column_with_type(0, DfType::Int)),
                op: BinaryOperator::Equal,
                right: Box::new(make_literal(1.into())),
                ty: DfType::Bool,
            }),
            then_expr: Box::new(make_literal("yes".try_into().unwrap())),
            else_expr: Box::new(make_literal("no".try_into().unwrap())),
            ty: DfType::Unknown,
        };

        assert_eq!(expr.eval::<DfValue>(&[1.into()]), Ok(DfValue::from("yes")));

        assert_eq!(
            expr.eval::<DfValue>(&[DfValue::from(8)]),
            Ok(DfValue::from("no"))
        );
    }

    #[test]
    fn like_expr() {
        let expr = Expr::Op {
            left: Box::new(make_literal("foo".into())),
            op: BinaryOperator::Like,
            right: Box::new(make_literal("f%".into())),
            ty: DfType::Unknown,
        };
        let res = expr.eval::<DfValue>(&[]).unwrap();
        assert!(res.is_truthy());
    }

    #[test]
    fn enum_eq_string_postgres() {
        let expr = Expr::Op {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column {
                index: 0,
                ty: DfType::from_enum_variants(
                    ["a".into(), "b".into(), "c".into()],
                    Some(PgEnumMetadata {
                        name: "abc".into(),
                        schema: "public".into(),
                        oid: 12345,
                    }),
                ),
            }),
            right: Box::new(Expr::Literal {
                val: "a".into(),
                ty: DfType::Unknown,
            }),
            ty: DfType::Bool,
        };

        let true_res = expr.eval(&[DfValue::from(1)]).unwrap();
        assert_eq!(true_res, true.into());

        let false_res = expr.eval(&[DfValue::from(2)]).unwrap();
        assert_eq!(false_res, false.into());
    }
}
