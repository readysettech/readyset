use std::borrow::Borrow;

use serde_json::Value as JsonValue;

use readyset_data::{Array, ArrayD, DfValue, IxDyn};
use readyset_errors::{invalid_query_err, unsupported, ReadySetError, ReadySetResult};

use crate::like::{CaseInsensitive, CaseSensitive, LikePattern};
use crate::{utils, BinaryOperator, CaseWhenBranch, Expr};

macro_rules! non_null {
    ($df_value:expr) => {
        match $df_value {
            DfValue::None => return Ok(DfValue::None),
            df_value => df_value,
        }
    };
}

pub(crate) mod builtins;
mod json;

fn eval_binary_op(op: BinaryOperator, left: &DfValue, right: &DfValue) -> ReadySetResult<DfValue> {
    use BinaryOperator::*;

    let like = |case_sensitivity| -> ReadySetResult<DfValue> {
        let (Some(left), Some(right)) = (non_null!(left).as_str(), non_null!(right).as_str())
        else {
            return Ok(false.into());
        };

        // NOTE(aspen): At some point, we may want to optimize this to pre-cache
        // the LikePattern if the value is constant, since constructing a new
        // LikePattern can be kinda slow.
        let pat = LikePattern::new(right, case_sensitivity);

        Ok(pat.matches(left).into())
    };

    match op {
        Add => Ok((non_null!(left) + non_null!(right))?),
        Subtract => Ok((non_null!(left) - non_null!(right))?),
        Multiply => Ok((non_null!(left) * non_null!(right))?),
        Divide => Ok((non_null!(left) / non_null!(right))?),
        Modulo => Ok((non_null!(left) % non_null!(right))?),
        And => Ok((non_null!(left).is_truthy() && non_null!(right).is_truthy()).into()),
        Or => Ok((non_null!(left).is_truthy() || non_null!(right).is_truthy()).into()),
        Equal => Ok((non_null!(left) == non_null!(right)).into()),
        Greater => Ok((non_null!(left) > non_null!(right)).into()),
        GreaterOrEqual => Ok((non_null!(left) >= non_null!(right)).into()),
        Less => Ok((non_null!(left) < non_null!(right)).into()),
        LessOrEqual => Ok((non_null!(left) <= non_null!(right)).into()),
        Is => Ok((left == right).into()),
        Like => like(CaseSensitive),
        ILike => like(CaseInsensitive),

        // JSON operators:
        JsonExists => {
            let json_value = left.to_json()?;
            let key = <&str>::try_from(right)?;

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
        // TODO(ENG-1518)
        JsonPathExtract | JsonPathExtractUnquote => {
            // TODO: Perform `JSON_EXTRACT` conditionally followed by `JSON_UNQUOTE` for
            // `->>`.
            unsupported!("'{op}' operator not implemented yet for MySQL")
        }

        JsonKeyExtract | JsonKeyExtractText => {
            // Both extraction operations behave the same in PostgreSQL except for the
            // return type, which is handled during expression lowering.

            let json = left.to_json()?;

            let json_inner: Option<&JsonValue> = match &json {
                JsonValue::Array(array) => isize::try_from(right)
                    .ok()
                    .and_then(|index| utils::index_bidirectional(array, index)),
                JsonValue::Object(object) => right.as_str().and_then(|key| object.get(key)),
                // Operator type errors are handled during expression lowering.
                _ => None,
            };

            Ok(json_inner
                .map(|inner| inner.to_string().into())
                .unwrap_or_default())
        }

        JsonKeyPathExtract | JsonKeyPathExtractText => {
            // Both extraction operations behave the same in PostgreSQL except for the
            // return type, which is handled during expression lowering.
            //
            // Type errors are also handled during expression lowering.
            json::json_extract_key_path(
                &left.to_json()?,
                // PostgreSQL docs state `text[]` but in practice it allows using
                // multi-dimensional arrays here.
                right.as_array()?.values(),
            )
        }

        JsonContains => Ok(json::json_contains(&left.to_json()?, &right.to_json()?).into()),
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

                Ok((&*left_obj).into())
            } else {
                // If both sides aren't JSON objects, concatenate arrays (after turning
                // non-array JSON values into single-element arrays):
                let mut res = match left_json {
                    JsonValue::Array(v) => v,
                    _ => vec![left_json],
                };
                match right_json {
                    JsonValue::Array(mut v) => res.append(&mut v),
                    _ => res.push(right_json),
                };

                Ok(res.into())
            }
        }
        JsonSubtract => {
            let mut json = left.to_json()?;

            fn remove_str(s: &str, vec: &mut Vec<JsonValue>) {
                vec.retain(|v| v.as_str() != Some(s));
            }

            if let Ok(key_array) = right.as_array() {
                let keys = key_array.to_str_vec()?;
                if let Some(vec) = json.as_array_mut() {
                    // TODO maybe optimize this to perform better with many keys:
                    for k in keys {
                        remove_str(k, vec);
                    }
                } else if let Some(obj) = json.as_object_mut() {
                    for k in keys {
                        obj.remove(k);
                    }
                } else {
                    return Err(invalid_query_err!(
                        "Can't subtract array from non-object, non-array JSON value"
                    ));
                }
            } else if let Some(str) = right.as_str() {
                if let Some(vec) = json.as_array_mut() {
                    remove_str(str, vec);
                } else if let Some(map) = json.as_object_mut() {
                    map.remove(str);
                } else {
                    return Err(invalid_query_err!(
                        "Can't subtract string from non-object, non-array JSON value"
                    ));
                }
            } else if let Some(index) = right.as_int() {
                if let Some(vec) = json.as_array_mut() {
                    if let Ok(index) = isize::try_from(index) {
                        utils::remove_bidirectional(vec, index);
                    }
                } else {
                    return Err(invalid_query_err!(
                        "Can't subtract integer value from non-array JSON value"
                    ));
                }
            } else {
                return Err(invalid_query_err!(
                    "Invalid type {} on right-hand side of JSONB subtract operator",
                    right.infer_dataflow_type()
                ));
            }
            Ok(json.into())
        }
        JsonSubtractPath => {
            // Type errors are handled during expression lowering, unless the type is
            // unknown.
            let keys = right.as_array()?;
            let mut json = left.to_json()?;

            // PostgreSQL docs state `text[]` but in practice it allows using
            // multi-dimensional arrays here.
            json::json_remove_path(&mut json, keys.values())?;

            Ok(serde_json::to_string(&json)?.into())
        }
    }
}

impl Expr {
    /// Evaluate this expression, given a source record to pull columns from
    pub fn eval<D>(&self, record: &[D]) -> ReadySetResult<DfValue>
    where
        D: Borrow<DfValue>,
    {
        match self {
            Expr::Column { index, .. } => record
                .get(*index)
                .map(|dt| dt.borrow().clone())
                .ok_or(ReadySetError::ProjectExprInvalidColumnIndex(*index)),
            Expr::Literal { val, .. } => Ok(val.clone()),
            Expr::Op {
                op, left, right, ..
            } => {
                let left_val = left.eval(record)?;
                let right_val = right.eval(record)?;
                eval_binary_op(*op, &left_val, &right_val)
            }
            Expr::Not { expr, .. } => Ok((!non_null!(expr.eval(record)?).is_truthy()).into()),
            Expr::OpAny {
                op, left, right, ..
            } => {
                let left_val = left.eval(record)?;
                let right_val = non_null!(right.eval(record)?);
                let mut res = DfValue::from(false);
                for member in right_val.as_array()?.values() {
                    if eval_binary_op(*op, &left_val, member)?.is_truthy() {
                        res = true.into();
                        break;
                    }
                }
                Ok(res)
            }
            Expr::OpAll {
                op, left, right, ..
            } => {
                let left_val = left.eval(record)?;
                let right_val = non_null!(right.eval(record)?);
                let mut res = DfValue::from(true);
                for member in right_val.as_array()?.values() {
                    if !eval_binary_op(*op, &left_val, member)?.is_truthy() {
                        res = false.into();
                        break;
                    }
                }
                Ok(res)
            }
            Expr::Cast {
                expr,
                ty,
                null_on_failure,
            } => {
                let res = expr.eval(record)?.coerce_to(ty, expr.ty());
                if *null_on_failure {
                    Ok(res.unwrap_or(DfValue::None))
                } else {
                    res
                }
            }
            Expr::Call { func, ty } => func.eval(ty, record),
            Expr::CaseWhen {
                branches,
                else_expr,
                ..
            } => {
                let mut res = None;
                for CaseWhenBranch { condition, body } in branches {
                    if condition.eval(record)?.is_truthy() {
                        res = Some(body.eval(record)?);
                        break;
                    }
                }
                res.map(Ok).unwrap_or_else(|| else_expr.eval(record))
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
                        invalid_query_err!("Mismatched array lengths in array expression: {e}")
                    })?,
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use serde_json::json;

    use nom_sql::parse_expr;
    use nom_sql::Dialect::*;
    use readyset_data::{ArrayD, Collation, DfType, Dialect, IxDyn, PgEnumMetadata};
    use readyset_errors::internal;
    use Expr::*;

    use crate::lower::tests::{no_op_lower_context, resolve_columns};
    use crate::utils::{column_with_type, make_column, make_literal, normalize_json};

    use super::*;

    /// Returns the value from evaluating an expression, or `ReadySetError` if evaluation fails.
    ///
    /// Note that parsing is expected to succeed, since this is strictly meant to test evaluation.
    #[track_caller]
    pub(crate) fn try_eval_expr(expr: &str, dialect: nom_sql::Dialect) -> ReadySetResult<DfValue> {
        let ast = expr_unwrap(parse_expr(dialect, expr), expr);

        let expr_dialect = match dialect {
            PostgreSQL => crate::Dialect::DEFAULT_POSTGRESQL,
            MySQL => crate::Dialect::DEFAULT_MYSQL,
        };

        expr_unwrap(Expr::lower(ast, expr_dialect, &no_op_lower_context()), expr)
            .eval::<DfValue>(&[])
    }

    /// Returns the value from evaluating an expression, panicking if parsing or evaluation fails.
    #[track_caller]
    pub(crate) fn eval_expr(expr: &str, dialect: nom_sql::Dialect) -> DfValue {
        expr_unwrap(try_eval_expr(expr, dialect), expr)
    }

    #[track_caller]
    fn expr_unwrap<T>(result: Result<T, impl std::fmt::Display>, expr: &str) -> T {
        match result {
            Ok(value) => value,
            Err(error) => panic!("Failed to evaluate `{expr}`: {error}"),
        }
    }

    #[test]
    fn eval_column() {
        let expr = make_column(1);
        assert_eq!(
            expr.eval(&[DfValue::from(1), "two".into()]).unwrap(),
            "two".into()
        )
    }

    #[test]
    fn eval_literal() {
        let expr = make_literal(1.into());
        assert_eq!(
            expr.eval(&[DfValue::from(1), "two".into()]).unwrap(),
            1.into()
        )
    }

    #[test]
    fn eval_cast_with_null_on_failure() {
        let expr = Expr::Cast {
            expr: Box::new(Expr::Literal {
                val: DfValue::from("asdfasdf"),
                ty: DfType::Unknown,
            }),
            ty: DfType::Timestamp {
                subsecond_digits: Dialect::DEFAULT_MYSQL.default_subsecond_digits(),
            },
            null_on_failure: true,
        };
        assert_eq!(expr.eval::<DfValue>(&[]).unwrap(), DfValue::None);
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
    fn eval_mod() {
        /*
         *  (10 % 7) * 3 = 9
         */
        let expr = Op {
            left: Box::new(Op {
                left: Box::new(make_column(0)),
                right: Box::new(make_column(1)),
                op: BinaryOperator::Modulo,
                ty: DfType::Unknown,
            }),
            right: Box::new(make_literal(3.into())),
            op: BinaryOperator::Multiply,
            ty: DfType::Unknown,
        };
        assert_eq!(
            expr.eval(&[DfValue::from(10), DfValue::from(7)]).unwrap(),
            9.into()
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
        expr.eval(&[DfValue::from("bad_json"), DfValue::from("abc")])
            .unwrap_err();
        expr.eval(&[DfValue::from("{\"abc\": 42}"), DfValue::from(67)])
            .unwrap_err();
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
        expr.eval(&[DfValue::from("bad_json"), DfValue::from("abc")])
            .unwrap_err();
        expr.eval(&[DfValue::from("{\"abc\": 42}"), DfValue::from(67)])
            .unwrap_err();
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
        expr.eval(&[DfValue::from("bad_json"), DfValue::from("abc")])
            .unwrap_err();
        expr.eval(&[DfValue::from("{\"abc\": 42}"), DfValue::from(67)])
            .unwrap_err();
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
            expr.eval::<DfValue>(&[From::from(left), From::from(right)])
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

        expr.eval(&[DfValue::from("bad_json"), DfValue::from("42")])
            .unwrap_err();

        expr.eval(&[DfValue::None, DfValue::from("\"valid_json\"")])
            .unwrap_err();
    }

    #[test]
    fn eval_json_subtract() {
        let expr = Op {
            left: Box::new(column_with_type(0, DfType::Jsonb)),
            right: Box::new(column_with_type(1, DfType::Unknown)),
            op: BinaryOperator::JsonSubtract,
            ty: DfType::Jsonb,
        };

        assert_eq!(
            expr.eval(&[DfValue::from(r#"["a","b","c"]"#), DfValue::from(1)])
                .unwrap(), // Subtracting index 1 should remove the second element of the array
            DfValue::from(r#"["a","c"]"#)
        );

        assert_eq!(
            expr.eval(&[DfValue::from(r#"["a","b","c"]"#), DfValue::from(-1)])
                .unwrap(), // Subtracting index -1 should remove the last element
            DfValue::from(r#"["a","b"]"#)
        );

        assert_eq!(
            expr.eval(&[DfValue::from(r#"["a","b","c"]"#), DfValue::from(3)])
                .unwrap(), // Out of bounds indexes should return same JSON as-is
            DfValue::from(r#"["a","b","c"]"#)
        );

        assert_eq!(
            expr.eval(&[DfValue::from(r#"["a","b","c"]"#), DfValue::from("b")])
                .unwrap(), // Subtracting a string should remove it from the array
            DfValue::from(r#"["a","c"]"#)
        );

        assert_eq!(
            expr.eval(&[DfValue::from(r#"["a","b","b"]"#), DfValue::from("b")])
                .unwrap(), // Subtracting a string should remove multiple copies if present
            DfValue::from(r#"["a"]"#)
        );

        assert_eq!(
            expr.eval(&[DfValue::from(r#"["a","b","c"]"#), DfValue::from("d")])
                .unwrap(), // Subtracting a string that's not present should be a no-op
            DfValue::from(r#"["a","b","c"]"#)
        );

        assert_eq!(
            expr.eval(&[DfValue::from(r#"{"a": 1, "b": 2}"#), DfValue::from("b")])
                .unwrap(), // Subtracting a string should remove that key from the object
            DfValue::from(r#"{"a":1}"#)
        );

        assert_eq!(
            expr.eval(&[DfValue::from(r#"{"a": 1, "b": 2}"#), DfValue::from("c")])
                .unwrap(), // Subtracting a string that's not a key should be a no-op
            DfValue::from(r#"{"a":1,"b":2}"#)
        );

        assert_eq!(
            expr.eval(&[
                DfValue::from(r#"["a","b","c"]"#),
                DfValue::from(vec![DfValue::from("a"), DfValue::from("c")])
            ])
            .unwrap(), // Subtracting str array from JSON array should remove all array elems
            DfValue::from(r#"["b"]"#)
        );

        assert_eq!(
            expr.eval(&[
                DfValue::from(r#"["a","a","b","c","c"]"#),
                DfValue::from(vec![DfValue::from("a"), DfValue::from("c")])
            ])
            .unwrap(), // Subtracting str array from JSON array should remove duplicate array elems
            DfValue::from(r#"["b"]"#)
        );

        assert_eq!(
            expr.eval(&[
                DfValue::from(r#"["a","b","c"]"#),
                DfValue::from(vec![DfValue::from("c"), DfValue::from("d")])
            ])
            .unwrap(), // Subtracting str array from JSON array should ignore non-present elems
            DfValue::from(r#"["a","b"]"#)
        );

        assert_eq!(
            expr.eval(&[
                DfValue::from(r#"{"a": 1, "b": 2, "c": 3}"#),
                DfValue::from(vec![DfValue::from("a"), DfValue::from("c")])
            ])
            .unwrap(), // Subtracting str array from JSON object should remove all keys
            DfValue::from(r#"{"b":2}"#)
        );

        assert_eq!(
            expr.eval(&[
                DfValue::from(r#"{"a": 1, "b": 2, "c": 3}"#),
                DfValue::from(vec![DfValue::from("c"), DfValue::from("d")])
            ])
            .unwrap(), // Subtracting str array from JSON obj should ignore non-present keys
            DfValue::from(r#"{"a":1,"b":2}"#)
        );

        expr.eval(&[DfValue::from("bad_json"), DfValue::from("abc")])
            .unwrap_err(); // Passing in bad JSON should error out

        expr.eval(&[DfValue::from(r#"["a","b","c"]"#), DfValue::Float(123.456)])
            .unwrap_err(); // Passing in a RHS that's not an int, string, or array should error out

        expr.eval(&[DfValue::from("42"), DfValue::from("abc")])
            .unwrap_err(); // Subtracting a string from a non-array, non-object JSON value is an error

        expr.eval(&[
            DfValue::from("42"),
            DfValue::from(vec![DfValue::from("c"), DfValue::from("d")]),
        ])
        .unwrap_err(); // Subtracting an array from a non-array, non-object JSON value is an error
    }

    #[test]
    fn eval_comparisons() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2009, 10, 17).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        );
        let text_dt: DfValue = "2009-10-17 12:00:00".into();
        let text_less_dt: DfValue = "2009-10-16 12:00:00".into();

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
    fn eval_op_any() {
        assert_eq!(
            eval_expr("1 = any('{1,2,3}')", nom_sql::Dialect::PostgreSQL),
            true.into()
        );
        assert_eq!(
            eval_expr("1 = any('{1,2,3}'::int[])", nom_sql::Dialect::PostgreSQL),
            true.into()
        );
        assert_eq!(
            eval_expr("4 = any('{1,2,3}')", nom_sql::Dialect::PostgreSQL),
            false.into()
        );
        assert_eq!(
            eval_expr("1 = any('{{1,2},{3,4}}')", nom_sql::Dialect::PostgreSQL),
            true.into()
        );
        assert_eq!(
            eval_expr("7 > any('{{1,2},{3,4}}')", nom_sql::Dialect::PostgreSQL),
            true.into()
        );
        assert_eq!(
            eval_expr(
                r#"'abc' ILIKE any('{"aBC","def"}')"#,
                nom_sql::Dialect::PostgreSQL
            ),
            true.into()
        );
        assert_eq!(
            eval_expr("1 = any(null)", nom_sql::Dialect::PostgreSQL),
            DfValue::None
        );
    }

    #[test]
    fn eval_op_all() {
        assert_eq!(
            eval_expr("1 = all('{1,2,3}'::int[])", nom_sql::Dialect::PostgreSQL),
            false.into()
        );
        assert_eq!(
            eval_expr("4 = all('{4,4}'::int[])", nom_sql::Dialect::PostgreSQL),
            true.into()
        );
        assert_eq!(
            eval_expr(
                "1 = all('{{1,1},{1,1}}'::int[])",
                nom_sql::Dialect::PostgreSQL
            ),
            true.into()
        );
        assert_eq!(
            eval_expr(
                "7 > all('{{1,2},{3,4}}'::int[])",
                nom_sql::Dialect::PostgreSQL
            ),
            true.into()
        );
        assert_eq!(
            eval_expr(
                r#"'abc' ILIKE all('{"aBC"}')"#,
                nom_sql::Dialect::PostgreSQL
            ),
            true.into()
        );
        assert_eq!(
            eval_expr("1 = all(null)", nom_sql::Dialect::PostgreSQL),
            DfValue::None
        );
    }

    #[test]
    fn eval_cast() {
        let expr = Cast {
            expr: Box::new(make_column(0)),
            ty: DfType::Int,
            null_on_failure: false,
        };
        assert_eq!(
            expr.eval::<DfValue>(&["1".into(), "2".into()]).unwrap(),
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
    fn eval_case_when_single_branch() {
        let expr = Expr::CaseWhen {
            branches: vec![CaseWhenBranch {
                condition: Op {
                    left: Box::new(column_with_type(0, DfType::Int)),
                    op: BinaryOperator::Equal,
                    right: Box::new(make_literal(1.into())),
                    ty: DfType::Bool,
                },
                body: make_literal("yes".into()),
            }],
            else_expr: Box::new(make_literal("no".into())),
            ty: DfType::Unknown,
        };

        assert_eq!(expr.eval::<DfValue>(&[1.into()]), Ok(DfValue::from("yes")));

        assert_eq!(
            expr.eval::<DfValue>(&[DfValue::from(8)]),
            Ok(DfValue::from("no"))
        );
    }

    #[test]
    fn eval_case_when_multiple_branches() {
        let expr = Expr::CaseWhen {
            branches: vec![
                CaseWhenBranch {
                    condition: Op {
                        left: Box::new(column_with_type(0, DfType::Int)),
                        op: BinaryOperator::Equal,
                        right: Box::new(make_literal(1.into())),
                        ty: DfType::Bool,
                    },
                    body: make_literal("one".into()),
                },
                CaseWhenBranch {
                    condition: Op {
                        left: Box::new(column_with_type(0, DfType::Int)),
                        op: BinaryOperator::Equal,
                        right: Box::new(make_literal(2.into())),
                        ty: DfType::Bool,
                    },
                    body: make_literal("two".into()),
                },
            ],
            else_expr: Box::new(make_literal("other".into())),
            ty: DfType::Unknown,
        };

        assert_eq!(expr.eval::<DfValue>(&[1.into()]), Ok(DfValue::from("one")));
        assert_eq!(expr.eval::<DfValue>(&[2.into()]), Ok(DfValue::from("two")));

        assert_eq!(
            expr.eval::<DfValue>(&[DfValue::from(8)]),
            Ok(DfValue::from("other"))
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
    fn like_null() {
        let expr = Expr::lower(
            parse_expr(nom_sql::Dialect::PostgreSQL, "a LIKE 'abc'").unwrap(),
            Dialect::DEFAULT_POSTGRESQL,
            &resolve_columns(|c| {
                if c == "a".into() {
                    Ok((0, DfType::DEFAULT_TEXT))
                } else {
                    internal!()
                }
            }),
        )
        .unwrap();
        let res = expr.eval(&[DfValue::None]).unwrap();
        assert_eq!(res, DfValue::None)
    }

    #[test]
    fn string_is_null() {
        let expr = Expr::lower(
            parse_expr(nom_sql::Dialect::MySQL, "'foobar' is null").unwrap(),
            Dialect::DEFAULT_MYSQL,
            &no_op_lower_context(),
        )
        .unwrap();
        let res = expr.eval::<&DfValue>(&[]).unwrap();
        assert_eq!(res, DfValue::Int(0))
    }

    #[test]
    fn string_is_not_null() {
        let expr = Expr::lower(
            parse_expr(nom_sql::Dialect::MySQL, "'foobar' is not null").unwrap(),
            Dialect::DEFAULT_MYSQL,
            &no_op_lower_context(),
        )
        .unwrap();
        let res = expr.eval::<&DfValue>(&[]).unwrap();
        assert_eq!(res, DfValue::Int(1))
    }

    #[test]
    fn null_is_null() {
        let expr = Expr::lower(
            parse_expr(nom_sql::Dialect::MySQL, "null is null").unwrap(),
            Dialect::DEFAULT_MYSQL,
            &no_op_lower_context(),
        )
        .unwrap();
        let res = expr.eval::<&DfValue>(&[]).unwrap();
        assert_eq!(res, DfValue::Int(1))
    }

    #[test]
    fn null_is_not_null() {
        let expr = Expr::lower(
            parse_expr(nom_sql::Dialect::MySQL, "null is not null").unwrap(),
            Dialect::DEFAULT_MYSQL,
            &no_op_lower_context(),
        )
        .unwrap();
        let res = expr.eval::<&DfValue>(&[]).unwrap();
        assert_eq!(res, DfValue::Int(0))
    }

    #[test]
    fn enum_eq_string_postgres() {
        let expr = Expr::lower(
            parse_expr(nom_sql::Dialect::PostgreSQL, "a = 'a'").unwrap(),
            Dialect::DEFAULT_POSTGRESQL,
            &resolve_columns(|c| {
                if c == "a".into() {
                    Ok((
                        0,
                        DfType::from_enum_variants(
                            ["a".into(), "b".into(), "c".into()],
                            Some(PgEnumMetadata {
                                name: "abc".into(),
                                schema: "public".into(),
                                oid: 12345,
                                array_oid: 12344,
                            }),
                        ),
                    ))
                } else {
                    internal!()
                }
            }),
        )
        .unwrap();

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
    #[ignore = "REA-4099"]
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
                        "incorrect result for `{expr}`"
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

    /// Tests evaluation of `JsonKeyPathExtract` and `JsonKeyPathExtractText` binary ops.
    #[test]
    #[ignore = "REA-4099"]
    fn eval_json_key_path_extract() {
        #[track_caller]
        fn test(json: &str, key: &str, expected: Option<&str>) {
            // Both ops behave the same except for their return type.
            for op in ["#>", "#>>"] {
                for json_type in ["json", "jsonb"] {
                    let expr = format!("'{json}'::{json_type} {op} {key}");
                    assert_eq!(
                        eval_expr(&expr, PostgreSQL),
                        expected.into(),
                        "incorrect result for `{expr}`"
                    );
                }
            }
        }

        let array = "[[\"world\", 123]]";

        test(array, "array['1']", None);
        test(array, "array[null::text]", None);

        test(array, "array['0', '0']", Some("\"world\""));
        test(array, "array['0', '1']", Some("123"));
        test(array, "array['0', '   1']", Some("123"));
        test(array, "array['0', '2']", None);
        test(array, "array['0', null::text]", None);

        let object = r#"{ "hello": ["world"], "abc": [123] }"#;

        test(object, "array[null::text]", None);
        test(object, "array['world']", None);

        test(object, "array['hello', '0']", Some("\"world\""));
        test(object, "array['hello', '1']", None);
        test(object, "array['hello', null::text]", None);

        test(object, "array['abc'::char(3), '0']", Some("123"));
        test(object, "array['abc'::char(3), null::text]", None);
    }

    /// Tests evaluation of `JsonSubtractPath` binary ops.
    #[test]
    #[ignore = "REA-4099"]
    fn eval_json_subtract_path() {
        #[track_caller]
        fn test(json: &str, key: &str, expected: &str) {
            // Normalize formatting.
            let expected = normalize_json(expected);

            let expr = format!("'{json}'::jsonb #- {key}");
            assert_eq!(
                eval_expr(&expr, PostgreSQL),
                expected.into(),
                "incorrect result for `{expr}`"
            );
        }

        let array = "[[\"world\", 123]]";
        test(array, "array['0']", "[]");
        test(array, "array['0', '0']", "[[123]]");
        test(array, "array['0', '1']", "[[\"world\"]]");
        test(array, "array['1']", array);
        test(array, "array['0', '2']", array);

        let object = r#"{ "hello": ["world"], "abc": [123] }"#;
        test(object, "array['hello']", r#"{ "abc": [123] }"#);
        test(
            object,
            "array['hello', '0']",
            r#"{ "hello": [], "abc": [123] }"#,
        );
        test(object, "array['abc'::char(3)]", r#"{ "hello": ["world"] }"#);
        test(
            object,
            "array['abc'::char(3), '0']",
            r#"{ "hello": ["world"], "abc": [] }"#,
        );
        test(object, "array['world']", object);
        test(object, "array['hello', '1']", object);

        // Remove leaf nodes:
        test("[[1]]", "array['0', '0']", "[[]]");
        test("[{ \"abc\": 1 }]", "array['0', 'abc']", "[{}]");
        test("{ \"abc\": [1] }", "array['abc', '0']", "{ \"abc\": [] }");
    }

    #[test]
    fn eval_jsonb_pretty() {
        let input = r#"[{"f1": 1, "f2": null}, 2]"#;
        let expected = r#"[
    {
        "f1": 1,
        "f2": null
    },
    2
]"#;

        let result = eval_expr(&format!("jsonb_pretty('{input}')"), PostgreSQL);
        assert_eq!(result, expected.into())
    }

    /// Tests evaluation of `JsonContains` and `JsonContainedIn` binary ops.
    mod json_contains {
        use super::*;

        #[track_caller]
        #[ignore = "REA-4099"]
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
        #[ignore = "REA-4099"]
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
        #[ignore = "REA-4099"]
        fn edge_cases() {
            test("[]", "[]", true);
            test("{}", "{}", true);

            test("[[]]", "[]", true);
            test("[]", "[[]]", false);

            test("[{}]", "[]", true);
            test("[]", "[{}]", false);
        }

        #[test]
        #[ignore = "REA-4099"]
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
