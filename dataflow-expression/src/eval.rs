use std::borrow::Borrow;

use readyset_data::{Array, ArrayD, DfType, DfValue, IxDyn};
use readyset_errors::{invalid_err, unsupported, ReadySetError, ReadySetResult};
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
mod json;

impl Expr {
    /// Evaluate this expression, given a source record to pull columns from
    pub fn eval<D>(&self, record: &[D]) -> ReadySetResult<DfValue>
    where
        D: Borrow<DfValue>,
    {
        // TODO: Enforce type coercion
        match self {
            Expr::Column { index, .. } => record
                .get(*index)
                .map(|dt| dt.borrow().clone())
                .ok_or(ReadySetError::ProjectExprInvalidColumnIndex(*index)),
            Expr::Literal { val, .. } => Ok(val.clone()),
            Expr::Op {
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
                    json_op @ (JsonAnyExists | JsonAllExists) => {
                        let json_value = left.to_json()?;
                        let keys = right.as_array().and_then(Array::to_str_vec)?;

                        let result = match (json_op, json_value) {
                            (JsonAnyExists, JsonValue::Object(map)) => {
                                keys.into_iter().any(|k| map.contains_key(k))
                            }
                            (JsonAnyExists, JsonValue::Array(vec)) => keys
                                .into_iter()
                                .any(|k| vec.iter().any(|v| v.as_str() == Some(k))),
                            (JsonAllExists, JsonValue::Object(map)) => {
                                keys.into_iter().all(|k| map.contains_key(k))
                            }
                            (JsonAllExists, JsonValue::Array(vec)) => keys
                                .into_iter()
                                .all(|k| vec.iter().any(|v| v.as_str() == Some(k))),
                            _ => false,
                        };
                        Ok(result.into())
                    }
                    // TODO(ENG-1517)
                    JsonPathExtract |
                    // TODO(ENG-1518)
                    JsonPathExtractUnquote => {
                        // TODO: Perform `JSON_EXTRACT` conditionally followed by `JSON_UNQUOTE` for
                        // `->>`.
                        unsupported!("'{op}' operator not implemented yet for MySQL")
                    }

                    JsonKeyExtract | JsonKeyExtractText => {
                        // Both extraction operations behave the same in PostgreSQL except for the
                        // return type, which is handled during expression lowering.

                        let json = left.to_json()?;

                        let json_inner: Option<&JsonValue> = match &json {
                            JsonValue::Array(array) => {
                                isize::try_from(&right).ok().and_then(|index| {
                                    crate::utils::index_bidirectional(array, index)
                                })
                            }
                            JsonValue::Object(object) => {
                                right.as_str().and_then(|key| object.get(key))
                            }
                            // Operator type errors are handled during expression lowering.
                            _ => None,
                        };

                        Ok(json_inner.map(|inner| inner.to_string().into()).unwrap_or_default())
                    }

                    JsonContains => {
                        Ok(json::json_contains(&left.to_json()?, &right.to_json()?).into())
                    }
                    JsonContainedIn => {
                        // Evaluate `left` first for consistency.
                        let child = left.to_json()?;
                        Ok(json::json_contains(&right.to_json()?, &child).into())
                    }
                    JsonConcat => {
                        let mut left_json = left.to_json()?;
                        let mut right_json = right.to_json()?;

                        if let (Some(left_obj), Some(right_obj)) =
                            (left_json.as_object_mut(), right_json.as_object_mut())
                        {
                            // When both the left and the right side are JSON objects, merge them:
                            left_obj.append(right_obj);
                            Ok(serde_json::to_string(&left_obj)?.into())
                        } else {
                            // If both sides aren't JSON objects, concatenate arrays (after turning
                            // non-array JSON values into single-element arrays):
                            let mut res = match left_json {
                                JsonValue::Array(v) => v,
                                _ => vec![left_json],
                            };
                            match right_json {
                                JsonValue::Array(mut v) => res.append(&mut v),
                                _ => res.push(right_json)
                            };
                            Ok(serde_json::to_string(&res)?.into())
                        }
                    }
                    JsonSubtract => unsupported!("'-' operator applied to JSONB not yet supported")
                }
            }
            Expr::Cast { expr, ty, .. } => {
                let res = expr.eval(record)?;
                Ok(res.coerce_to(ty, expr.ty())?)
            }
            Expr::Call { func, ty } => func.eval(ty, record),
            Expr::CaseWhen {
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
            Expr::Array {
                elements, shape, ..
            } => {
                let elements = elements
                    .iter()
                    .map(|expr| expr.eval(record))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(DfValue::from(Array::from(
                    ArrayD::from_shape_vec(IxDyn(shape.as_slice()), elements).map_err(|e| {
                        invalid_err!("Mismatched array lengths in array expression: {e}")
                    })?,
                )))
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
    use readyset_data::{ArrayD, Collation, DfType, IxDyn, PgEnumMetadata};
    use serde_json::json;
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
    fn eval_json_any_exists() {
        let expr = Op {
            left: Box::new(column_with_type(0, DfType::Jsonb)),
            right: Box::new(column_with_type(
                1,
                DfType::Array(Box::new(DfType::Text(Collation::default()))),
            )),
            op: BinaryOperator::JsonAnyExists,
            ty: DfType::Bool,
        };
        assert_eq!(
            expr.eval(&[
                DfValue::from("{\"abc\": 42}"),
                DfValue::from(vec![DfValue::from("uvw"), DfValue::from("xyz")])
            ])
            .unwrap(),
            false.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("{\"abc\": 42}"),
                DfValue::from(vec![DfValue::from("abc"), DfValue::from("def")])
            ])
            .unwrap(),
            true.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("[\"abc\"]"),
                DfValue::from(vec![DfValue::from("uvw"), DfValue::from("xyz")])
            ])
            .unwrap(),
            false.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("[\"abc\"]"),
                DfValue::from(vec![DfValue::from("abc"), DfValue::from("def")])
            ])
            .unwrap(),
            true.into()
        );
    }

    #[test]
    fn eval_json_any_exists_bad_types() {
        let expr = Op {
            left: Box::new(column_with_type(0, DfType::Jsonb)),
            right: Box::new(column_with_type(
                1,
                DfType::Array(Box::new(DfType::Text(Collation::default()))),
            )),
            op: BinaryOperator::JsonAnyExists,
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
    fn eval_json_all_exists() {
        let expr = Op {
            left: Box::new(column_with_type(0, DfType::Jsonb)),
            right: Box::new(column_with_type(
                1,
                DfType::Array(Box::new(DfType::Text(Collation::default()))),
            )),
            op: BinaryOperator::JsonAllExists,
            ty: DfType::Bool,
        };
        assert_eq!(
            expr.eval(&[
                DfValue::from("{\"abc\": 42, \"def\": 53}"),
                DfValue::from(vec![DfValue::from("uvw"), DfValue::from("xyz")])
            ])
            .unwrap(),
            false.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("{\"abc\": 42, \"def\": 53}"),
                DfValue::from(vec![DfValue::from("abc")])
            ])
            .unwrap(),
            true.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("{\"abc\": 42, \"def\": 53}"),
                DfValue::from(vec![DfValue::from("abc"), DfValue::from("def")])
            ])
            .unwrap(),
            true.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("{\"abc\": 42}"),
                DfValue::from(vec![DfValue::from("abc"), DfValue::from("def")])
            ])
            .unwrap(),
            false.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("{\"abc\": 42, \"def\": 53}"),
                DfValue::from(vec![
                    DfValue::from("abc"),
                    DfValue::from("def"),
                    DfValue::from("ghi")
                ])
            ])
            .unwrap(),
            false.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("[\"abc\", \"def\"]"),
                DfValue::from(vec![DfValue::from("uvw"), DfValue::from("xyz")])
            ])
            .unwrap(),
            false.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("[\"abc\", \"def\"]"),
                DfValue::from(vec![DfValue::from("abc")])
            ])
            .unwrap(),
            true.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("[\"abc\", \"def\"]"),
                DfValue::from(vec![DfValue::from("abc"), DfValue::from("def")])
            ])
            .unwrap(),
            true.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("[\"abc\"]"),
                DfValue::from(vec![DfValue::from("abc"), DfValue::from("def")])
            ])
            .unwrap(),
            false.into()
        );
        assert_eq!(
            expr.eval(&[
                DfValue::from("[\"abc\", \"def\"]"),
                DfValue::from(vec![
                    DfValue::from("abc"),
                    DfValue::from("def"),
                    DfValue::from("ghi")
                ])
            ])
            .unwrap(),
            false.into()
        );
    }

    #[test]
    fn eval_json_all_exists_bad_types() {
        let expr = Op {
            left: Box::new(column_with_type(0, DfType::Jsonb)),
            right: Box::new(column_with_type(
                1,
                DfType::Array(Box::new(DfType::Text(Collation::default()))),
            )),
            op: BinaryOperator::JsonAllExists,
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
    fn eval_json_concat() {
        let expr = Op {
            left: Box::new(column_with_type(0, DfType::Jsonb)),
            right: Box::new(column_with_type(1, DfType::Jsonb)),
            op: BinaryOperator::JsonConcat,
            ty: DfType::Jsonb,
        };

        let test_eval = |left, right| {
            expr.eval(&[
                DfValue::from(serde_json::to_string(left).unwrap()),
                DfValue::from(serde_json::to_string(right).unwrap()),
            ])
            .unwrap()
            .to_json()
            .unwrap()
        };

        // Test various valid cases

        let json_obj1 = json!({"abc": 1, "def": 2});
        let json_obj2 = json!({"xyz": 3, "def": 4});
        let expected = json!({"abc": 1, "def": 4, "xyz": 3});
        assert_eq!(test_eval(&json_obj1, &json_obj2), expected);

        let json_arr1 = json!([1, 2, 3]);
        let json_arr2 = json!([4, 5, 6]);
        let expected = json!([1, 2, 3, 4, 5, 6]);
        assert_eq!(test_eval(&json_arr1, &json_arr2), expected);

        let expected = json!([1, 2, 3, json_obj1]);
        assert_eq!(test_eval(&json_arr1, &json_obj1), expected);

        let expected = json!([99, 100]);
        assert_eq!(test_eval(&json!(99), &json!(100)), expected);

        // Test error conditions

        assert!(expr
            .eval(&[DfValue::from("bad_json"), DfValue::from("42")])
            .is_err());

        assert!(expr
            .eval(&[DfValue::None, DfValue::from("\"valid_json\"")])
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
                        array_oid: 12344,
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

    #[test]
    fn array_expression() {
        let res = eval_expr(
            "ARRAY[[1, '2'::int], array[3, 4]]",
            nom_sql::Dialect::PostgreSQL,
        );

        assert_eq!(
            res,
            DfValue::from(
                readyset_data::Array::from_lower_bounds_and_contents(
                    [1, 1],
                    ArrayD::from_shape_vec(
                        IxDyn(&[2, 2]),
                        vec![
                            DfValue::from(1),
                            DfValue::from(2),
                            DfValue::from(3),
                            DfValue::from(4),
                        ]
                    )
                    .unwrap()
                )
                .unwrap()
            )
        )
    }

    /// Tests evaluation of `JsonKeyExtract` and `JsonKeyExtractText` binary ops.
    #[test]
    fn eval_json_key_extract() {
        #[track_caller]
        fn test(json: &str, key: &str, expected: &str) {
            // Both ops behave the same except for their return type.
            for op in ["->", "->>"] {
                for json_type in ["json", "jsonb"] {
                    let expr = format!("'{json}'::{json_type} {op} {key}");
                    assert_eq!(
                        eval_expr(&expr, PostgreSQL),
                        expected.into(),
                        "incorrect result for for `{expr}`"
                    );
                }
            }
        }

        let array = "[\"world\", 123]";
        test(array, "0", "\"world\"");
        test(array, "1", "123");

        let object = r#"{ "hello": "world", "abc": 123 }"#;
        test(object, "'hello'::text", "\"world\"");
        test(object, "'abc'::char(3)", "123");
    }

    /// Tests evaluation of `JsonContains` and `JsonContainedIn` binary ops.
    mod json_contains {
        use super::*;

        #[track_caller]
        fn test(parent: &str, child: &str, expected: bool) {
            for expr in [
                format!("'{parent}'::jsonb @> '{child}'::jsonb"),
                format!("'{child}'::jsonb <@ '{parent}'::jsonb"),
            ] {
                assert_eq!(
                    eval_expr(&expr, PostgreSQL),
                    expected.into(),
                    "incorrect result for `{expr}`"
                );
            }
        }

        #[test]
        fn postgresql_docs_examples() {
            // Examples in https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT
            test("\"foo\"", "\"foo\"", true);

            test("[1, 2, 3]", "[1, 3]", true);
            test("[1, 2, 3]", "[3, 1]", true);
            test("[1, 2, 3]", "[1, 2, 2]", true);

            test(
                r#"{"product": "PostgreSQL", "version": 9.4, "jsonb": true}"#,
                r#"{"version": 9.4}"#,
                true,
            );

            test("[1, 2, [1, 3]]", "[1, 3]", false);
            test("[1, 2, [1, 3]]", "[[1, 3]]", true);

            test(r#"{"foo": {"bar": "baz"}}"#, r#"{"bar": "baz"}"#, false);
            test(r#"{"foo": {"bar": "baz"}}"#, r#"{"foo": {}}"#, true);

            test(r#"["foo", "bar"]"#, "\"bar\"", true);
            test("\"bar\"", "[\"bar\"]", false);
        }

        #[test]
        fn edge_cases() {
            test("[]", "[]", true);
            test("{}", "{}", true);

            test("[[]]", "[]", true);
            test("[]", "[[]]", false);

            test("[{}]", "[]", true);
            test("[]", "[{}]", false);
        }

        #[test]
        fn float_semantics() {
            // `JsonNumber` does not handle -0.0 when `arbitrary_precision` is enabled.
            test("0.0", "-0.0", true);
            test("-0.0", "0.0", true);
            test("[0.0]", "-0.0", true);
            test("[-0.0]", "0.0", true);
            test("[0.0]", "[-0.0]", true);
            test("[-0.0]", "[0.0]", true);

            // FIXME(ENG-2080): `serde_json::Number` does not compare exponents and decimals
            // correctly when `arbitrary_precision` is enabled.
            // test("0.1", "1.0e-1", true);
        }
    }
}
