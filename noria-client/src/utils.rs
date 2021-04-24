use std::collections::HashSet;

use nom_sql::{
    Arithmetic, ArithmeticBase, ArithmeticItem, ArithmeticOperator, BinaryOperator, Column,
    ColumnConstraint, ConditionBase, ConditionExpression, ConditionTree, CreateTableStatement,
    Expression, Literal, SelectStatement, SqlQuery, TableKey, UpdateStatement,
};
use noria::errors::{bad_request_err, ReadySetResult};
use noria::{invariant, invariant_eq, unsupported, DataType, Modification, Operation};
use regex::Regex;
use std::borrow::Cow;
use std::collections::HashMap;

lazy_static! {
    pub(crate) static ref HARD_CODED_REPLIES: Vec<(Regex, Vec<(&'static str, &'static str)>)> = vec![
        (
            Regex::new(r"(?i)select version\(\) limit 1").unwrap(),
            vec![("version()", "10.1.26-MariaDB-0+deb9u1")],
        ),
        (
            Regex::new(r"(?i)show engines").unwrap(),
            vec![
                ("Engine", "InnoDB"),
                ("Support", "DEFAULT"),
                ("Comment", ""),
                ("Transactions", "YES"),
                ("XA", "YES"),
                ("Savepoints", "YES"),
            ],
        ),
        (
            Regex::new(r"SELECT 1 AS ping").unwrap(),
            vec![("ping", "1")],
        ),
        (
            Regex::new(r"(?i)show global variables like 'read_only'").unwrap(),
            vec![("Variable_name", "read_only"), ("Value", "OFF")],
        ),
        (
            Regex::new(r"(?i)select get_lock\(.*\) as lockstatus").unwrap(),
            vec![("lockstatus", "1")],
        ),
        (
            Regex::new(r"(?i)select release_lock\(.*\) as lockstatus").unwrap(),
            vec![("lockstatus", "1")],
        ),
    ];
    pub(crate) static ref COMMENTS: Vec<(Regex, &'static str)> = vec![
        (Regex::new(r"(?s)/\*.*\*/").unwrap(), ""),
        (Regex::new(r"--.*\n").unwrap(), "\n"),
    ];
    pub(crate) static ref COLLAPSE_SPACES: (Regex, &'static str) =
        (Regex::new(r" +").unwrap(), " ");
}

pub(crate) fn hash_select_query(q: &SelectStatement) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    q.hash(&mut h);
    h.finish()
}

pub(crate) fn sanitize_query(query: &str) -> String {
    let query = Cow::from(query);
    for &(ref pattern, replacement) in &*COMMENTS {
        pattern.replace_all(&query, replacement);
    }
    let query = COLLAPSE_SPACES.0.replace_all(&query, COLLAPSE_SPACES.1);
    let query = query.replace('"', "'");
    let query = query.trim();
    query.to_owned()
}

// Helper for flatten_conditional - returns true if the
// expression is "valid" (i.e. not something like `a = 1 AND a = 2`.
// Goes through the condition tree by gradually filling up primary key slots.
//
// Example:
//    (CREATE TABLE A (aid int, uid int, PRIMARY KEY(aid, uid))
//    `WHERE aid = 1 AND uid = 2` has the following tree:
//
//       +--+ AND +--+
//       |           |
//       +           +
//    aid = 1     uid = 2
//
//    After processing the left side `flattened` will look something like this: {[(aid, 1)]}
//    Then we'll check the right side, which will find a "hole" in the first key,
//    and we'll get {[(aid, 1), (uid, 2)]}.
fn do_flatten_conditional(
    cond: &ConditionExpression,
    pkey: &Vec<&Column>,
    mut flattened: &mut HashSet<Vec<(String, DataType)>>,
) -> ReadySetResult<bool> {
    Ok(match *cond {
        ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Literal(ref l)),
            right: box ConditionExpression::Base(ConditionBase::Field(ref c)),
            operator: BinaryOperator::Equal,
        })
        | ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Field(ref c)),
            right: box ConditionExpression::Base(ConditionBase::Literal(ref l)),
            operator: BinaryOperator::Equal,
        }) => {
            if !pkey.iter().any(|pk| pk.name == c.name) {
                unsupported!("UPDATE/DELETE only supports WHERE-clauses on primary keys");
            }
            if !c.table.iter().all(|n| n == pkey[0].table.as_ref().unwrap()) {
                unsupported!("UPDATE/DELETE contains references to another table")
            }

            let value = DataType::from(l);
            // We want to look through our existing keys and see if any of them
            // are missing any columns. In that case we'll add the one we're looking
            // at now there.
            let with_space = flattened
                .iter()
                .find(|key| {
                    key.len() < pkey.len() && !key.iter().any(|&(ref name, _)| name == &c.name)
                })
                // Not a very happy clone, but using a HashSet here simplifies the AND
                // logic by letting us ignore identical clauses (and we need the .clone()
                // to be able to "mutate" key).
                .and_then(|key| Some(key.clone()));

            if let Some(mut key) = with_space {
                flattened.remove(&key);
                key.push((c.name.clone(), value));
                flattened.insert(key);
            } else {
                // There were no existing keys with space, so let's create a new one:
                flattened.insert(vec![(c.name.clone(), value)]);
            }

            true
        }
        ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Literal(ref left)),
            right: box ConditionExpression::Base(ConditionBase::Literal(ref right)),
            operator: BinaryOperator::Equal,
        }) if left == right => true,
        ConditionExpression::LogicalOp(ConditionTree {
            operator: BinaryOperator::And,
            ref left,
            ref right,
        }) => {
            // When checking ANDs we want to make sure that both sides refer to the same key,
            // e.g. WHERE A.a = 1 AND A.a = 1
            // or for compound primary keys:
            // WHERE A.a = AND a.b = 2
            // but also bogus stuff like `WHERE 1 = 1 AND 2 = 2`.
            let pre_count = flattened.len();
            do_flatten_conditional(&*left, pkey, &mut flattened)? && {
                let count = flattened.len();
                let valid = do_flatten_conditional(&*right, pkey, &mut flattened)?;
                valid && (pre_count == flattened.len() || count == flattened.len())
            }
        }
        ConditionExpression::LogicalOp(ConditionTree {
            operator: BinaryOperator::Or,
            ref left,
            ref right,
        }) => {
            do_flatten_conditional(&*left, pkey, &mut flattened)?
                && do_flatten_conditional(&*right, pkey, &mut flattened)?
        }
        _ => false,
    })
}

// Takes a tree of conditional expressions for a DELETE/UPDATE statement and returns a list of all the
// keys that should be mutated.
// Panics if given a WHERE-clause containing other keys than the primary.
// DELETE FROM a WHERE key = 1 OR key = 2 -> Some([[1], [2]])
// DELETE FROM a WHERE key = 1 OR key = 2 AND key = 3 -> None // Bogus query
// DELETE FROM a WHERE key = 1 AND key = 1 -> Some([[1]])
pub(crate) fn flatten_conditional(
    cond: &ConditionExpression,
    pkey: &Vec<&Column>,
) -> ReadySetResult<Option<Vec<Vec<DataType>>>> {
    let mut flattened = HashSet::new();
    Ok(if do_flatten_conditional(cond, pkey, &mut flattened)? {
        let keys = flattened
            .into_iter()
            .map(|key| {
                // This will be the case if we got a cond without any primary keys,
                // or if we have a multi-column primary key and the cond only covers part of it.
                if key.len() != pkey.len() {
                    unsupported!(
                        "UPDATE/DELETE requires all columns of a compound key to be present"
                    );
                }

                Ok(key.into_iter().map(|(_c, v)| v).collect())
            })
            .collect::<ReadySetResult<Vec<_>>>()?;

        Some(keys)
    } else {
        None
    })
}

// Finds the primary for the given table, both by looking at constraints on individual
// columns and by searching through keys.
pub(crate) fn get_primary_key(schema: &CreateTableStatement) -> Vec<(usize, &Column)> {
    schema
        .fields
        .iter()
        .enumerate()
        .filter(|&(_, ref cs)| {
            cs.constraints.contains(&ColumnConstraint::PrimaryKey)
                || match schema.keys {
                    // Try finding PRIMARY KEY constraints in keys as well:
                    Some(ref keys) => keys.iter().any(|key| match *key {
                        TableKey::PrimaryKey(ref cols) => cols.iter().any(|c| c == &cs.column),
                        _ => false,
                    }),
                    _ => false,
                }
        })
        .map(|(i, cs)| (i, &cs.column))
        .collect()
}

fn get_parameter_columns_recurse(cond: &ConditionExpression) -> Vec<(&Column, BinaryOperator)> {
    match *cond {
        ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Field(ref c)),
            right: box ConditionExpression::Base(ConditionBase::Literal(Literal::Placeholder(_))),
            operator: binop,
        }) => vec![(c, binop)],
        ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Literal(Literal::Placeholder(_))),
            right: box ConditionExpression::Base(ConditionBase::Field(ref c)),
            operator: binop,
        }) => vec![(c, binop.flip_comparison().unwrap_or(binop))],
        ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Field(ref c)),
            right: box ConditionExpression::Base(ConditionBase::LiteralList(ref literals)),
            operator: BinaryOperator::In,
        }) if (|| {
            literals
                .iter()
                .all(|l| matches!(*l, Literal::Placeholder(_)))
        })() =>
        {
            // the weird extra closure above is due to
            // https://github.com/rust-lang/rfcs/issues/1006
            vec![(c, BinaryOperator::Equal); literals.len()]
        }
        ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Field(_)),
            right: box ConditionExpression::Base(ConditionBase::Literal(_)),
            operator: _,
        })
        | ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Literal(_)),
            right: box ConditionExpression::Base(ConditionBase::Field(_)),
            operator: _,
        }) => vec![],
        // comma joins and column equality comparisons
        ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Field(_)),
            right: box ConditionExpression::Base(ConditionBase::Field(_)),
            operator: _,
        }) => vec![],
        ConditionExpression::LogicalOp(ConditionTree {
            operator: BinaryOperator::And,
            ref left,
            ref right,
        })
        | ConditionExpression::LogicalOp(ConditionTree {
            operator: BinaryOperator::Or,
            ref left,
            ref right,
        }) => {
            let mut l = get_parameter_columns_recurse(left);
            let mut r = get_parameter_columns_recurse(right);
            l.append(&mut r);
            l
        }
        ConditionExpression::NegationOp(ref expr) | ConditionExpression::Bracketed(ref expr) => {
            get_parameter_columns_recurse(expr)
        }
        _ => unimplemented!(),
    }
}

pub(crate) fn get_select_statement_binops(
    query: &SelectStatement,
) -> Vec<(&Column, BinaryOperator)> {
    if let Some(ref wc) = query.where_clause {
        get_parameter_columns_recurse(wc)
    } else {
        vec![]
    }
}

pub(crate) fn select_statement_parameter_columns(query: &SelectStatement) -> Vec<&Column> {
    if let Some(ref wc) = query.where_clause {
        get_parameter_columns_recurse(wc)
            .into_iter()
            .map(|(c, _)| c)
            .collect()
    } else {
        vec![]
    }
}

pub(crate) fn get_parameter_columns(query: &SqlQuery) -> Vec<&Column> {
    match *query {
        SqlQuery::Select(ref query) => select_statement_parameter_columns(query),
        SqlQuery::Insert(ref query) => {
            assert_eq!(query.data.len(), 1);
            // need to find for which fields we *actually* have a parameter
            query.data[0]
                .iter()
                .enumerate()
                .filter_map(|(i, v)| match *v {
                    Literal::Placeholder(_) => Some(&query.fields.as_ref().unwrap()[i]),
                    _ => None,
                })
                .collect()
        }
        SqlQuery::Update(ref query) => {
            let field_params = query.fields.iter().filter_map(|f| {
                if let Expression::Literal(Literal::Placeholder(_)) = f.1 {
                    Some(&f.0)
                } else {
                    None
                }
            });

            let where_params = if let Some(ref wc) = query.where_clause {
                get_parameter_columns_recurse(wc)
                    .into_iter()
                    .map(|(c, _)| c)
                    .collect()
            } else {
                vec![]
            };

            field_params.chain(where_params.into_iter()).collect()
        }
        _ => unimplemented!(),
    }
}

fn walk_update_where<I>(
    col2v: &mut HashMap<String, DataType>,
    params: &mut Option<I>,
    expr: ConditionExpression,
) -> ReadySetResult<()>
where
    I: Iterator<Item = DataType>,
{
    match expr {
        ConditionExpression::ComparisonOp(ConditionTree {
            operator: BinaryOperator::Equal,
            left: box ConditionExpression::Base(ConditionBase::Field(c)),
            right: box ConditionExpression::Base(ConditionBase::Literal(l)),
        }) => {
            let v = match l {
                Literal::Placeholder(_) => params
                    .as_mut()
                    .ok_or_else(|| bad_request_err("Found placeholder in ad-hoc query"))?
                    .next()
                    .ok_or_else(|| {
                        bad_request_err("Not enough parameter values given in EXECUTE")
                    })?,
                v => DataType::from(v),
            };
            let oldv = col2v.insert(c.name, v);
            invariant!(oldv.is_none());
        }
        ConditionExpression::LogicalOp(ConditionTree {
            operator: BinaryOperator::And,
            left,
            right,
        }) => {
            // recurse
            walk_update_where(col2v, params, *left)?;
            walk_update_where(col2v, params, *right)?;
        }
        _ => unsupported!("Fancy high-brow UPDATEs are not supported"),
    }
    Ok(())
}

pub(crate) fn extract_update_params_and_fields<I>(
    q: &mut UpdateStatement,
    params: &mut Option<I>,
    schema: &CreateTableStatement,
) -> ReadySetResult<Vec<(usize, Modification)>>
where
    I: Iterator<Item = DataType>,
{
    let mut updates = Vec::new();
    for (i, field) in schema.fields.iter().enumerate() {
        if let Some(sets) = q
            .fields
            .iter()
            .position(|&(ref f, _)| f.name == field.column.name)
        {
            match q.fields.swap_remove(sets).1 {
                Expression::Literal(Literal::Placeholder(_)) => {
                    let v = params
                        .as_mut()
                        .ok_or_else(|| bad_request_err("Found placeholder in ad-hoc query"))?
                        .next()
                        .ok_or_else(|| {
                            bad_request_err("Not enough parameter values given in EXECUTE")
                        })?;
                    updates.push((i, Modification::Set(v)));
                }
                Expression::Literal(ref v) => {
                    updates.push((
                        i,
                        Modification::Set(
                            DataType::from(v)
                                .coerce_to(&field.sql_type)
                                .unwrap()
                                .into_owned(),
                        ),
                    ));
                }
                Expression::Arithmetic(ref ae) => {
                    // we only support "column = column +/- literal"
                    // TODO(grfn): Handle nested arithmetic
                    // (https://app.clubhouse.io/readysettech/story/41)
                    match ae {
                        Arithmetic {
                            op,
                            left: ArithmeticItem::Base(ArithmeticBase::Column(ref c)),
                            right: ArithmeticItem::Base(ArithmeticBase::Scalar(ref l)),
                        } => {
                            invariant_eq!(c, &field.column);
                            match op {
                                ArithmeticOperator::Add => {
                                    updates.push((i, Modification::Apply(Operation::Add, l.into())))
                                }
                                ArithmeticOperator::Subtract => {
                                    updates.push((i, Modification::Apply(Operation::Sub, l.into())))
                                }
                                _ => unsupported!(),
                            }
                        }
                        _ => unsupported!(),
                    }
                }
                _ => unsupported!(),
            }
        }
    }
    Ok(updates)
}

pub(crate) fn extract_update<I>(
    mut q: UpdateStatement,
    mut params: Option<I>,
    schema: &CreateTableStatement,
) -> ReadySetResult<(Vec<DataType>, Vec<(usize, Modification)>)>
where
    I: Iterator<Item = DataType>,
{
    let updates = extract_update_params_and_fields(&mut q, &mut params, schema);

    let pkey = get_primary_key(schema);
    let where_clause = q
        .where_clause
        .expect("UPDATE without WHERE is not supported");
    let mut col_to_val: HashMap<_, _> = HashMap::new();
    walk_update_where(&mut col_to_val, &mut params, where_clause)?;

    let key: Vec<_> = pkey
        .iter()
        .map(|&(_, c)| col_to_val.remove(&c.name).unwrap())
        .collect();

    Ok((key, updates?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::{self, SqlQuery};

    fn compare_flatten<I>(cond_query: &str, key: Vec<&str>, expected: Option<Vec<Vec<I>>>)
    where
        I: Into<DataType>,
    {
        let cond = match nom_sql::parse_query(cond_query).unwrap() {
            SqlQuery::Update(u) => u.where_clause.unwrap(),
            SqlQuery::Delete(d) => d.where_clause.unwrap(),
            _ => unreachable!(),
        };

        let pkey: Vec<Column> = key
            .into_iter()
            .map(|k| Column {
                name: String::from(k),
                table: Some(String::from("T")),
                function: None,
            })
            .collect();

        let pkey_ref = pkey.iter().map(|c| c).collect();
        if let Some(mut actual) = flatten_conditional(&cond, &pkey_ref).unwrap() {
            let mut expected: Vec<Vec<DataType>> = expected
                .unwrap()
                .into_iter()
                .map(|v| v.into_iter().map(|c| c.into()).collect())
                .collect();

            actual.sort();
            expected.sort();
            assert_eq!(actual, expected);
        } else {
            assert!(expected.is_none());
        }
    }

    fn get_schema(query: &str) -> CreateTableStatement {
        match nom_sql::parse_query(query).unwrap() {
            SqlQuery::CreateTable(c) => c,
            _ => unreachable!(),
        }
    }

    #[test]
    #[should_panic]
    fn test_flatten_conditional_different_table() {
        compare_flatten(
            "DELETE FROM T WHERE A.a = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );
    }

    #[test]
    fn test_flatten_conditional() {
        compare_flatten("DELETE FROM T WHERE a = 1", vec!["a"], Some(vec![vec![1]]));
        compare_flatten(
            "DELETE FROM T WHERE T.a = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );
        compare_flatten(
            "DELETE FROM T WHERE T.a = 1 OR T.a = 2",
            vec!["a"],
            Some(vec![vec![1], vec![2]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 OR T.a = 2",
            vec!["a"],
            Some(vec![vec![1], vec![2]]),
        );

        // Valid, but bogus, ORs:
        compare_flatten(
            "DELETE FROM T WHERE T.a = 1 OR T.a = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 OR T.a = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );

        // Valid, but bogus, ANDs:
        compare_flatten(
            "DELETE FROM T WHERE T.a = 1 AND T.a = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 AND T.a = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );
        compare_flatten(
            "DELETE FROM T WHERE T.a = 1 AND 1 = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 AND 1 = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );

        // We can't really handle these at the moment, but in the future we might want to
        // delete/update all rows:
        compare_flatten::<DataType>("DELETE FROM T WHERE 1 = 1", vec!["a"], Some(vec![]));
        compare_flatten::<DataType>("UPDATE T SET T.b = 2 WHERE 1 = 1", vec!["a"], Some(vec![]));

        // Invalid ANDs:
        compare_flatten::<DataType>("DELETE FROM T WHERE T.a = 1 AND T.a = 2", vec!["a"], None);
        compare_flatten::<DataType>(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 AND T.a = 2",
            vec!["a"],
            None,
        );
    }

    #[test]
    #[ignore]
    fn test_flatten_conditional_compound_key() {
        compare_flatten(
            "DELETE FROM T WHERE T.a = 1 AND T.b = 2",
            vec!["a", "b"],
            Some(vec![vec![1, 2]]),
        );
        compare_flatten(
            "DELETE FROM T WHERE (T.a = 1 AND T.b = 2) OR (T.a = 10 OR T.b = 20)",
            vec!["a", "b"],
            Some(vec![vec![1, 2], vec![10, 20]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 AND T.b = 2",
            vec!["a", "b"],
            Some(vec![vec![1, 2]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE (T.a = 1 AND T.b = 2) OR (T.a = 10 OR T.b = 20)",
            vec!["a", "b"],
            Some(vec![vec![1, 2], vec![10, 20]]),
        );

        // Valid, but bogus, ORs:
        compare_flatten(
            "DELETE FROM T WHERE (T.a = 1 AND T.b = 2) OR (T.a = 1 AND T.b = 2)",
            vec!["a", "b"],
            Some(vec![vec![1, 2]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE (T.a = 1 AND T.b = 2) OR (T.a = 1 AND T.b = 2)",
            vec!["a", "b"],
            Some(vec![vec![1, 2]]),
        );

        // Valid, but bogus, ANDs:
        compare_flatten(
            "DELETE FROM T WHERE (T.a = 1 AND T.b = 2) AND (T.a = 1 AND T.b = 2)",
            vec!["a", "b"],
            Some(vec![vec![1, 2]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE (T.a = 1 AND T.b = 2) AND (T.a = 1 AND T.b = 2)",
            vec!["a", "b"],
            Some(vec![vec![1, 2]]),
        );
        compare_flatten(
            "DELETE FROM T WHERE (T.a = 1 AND T.b = 2) AND 1 = 1",
            vec!["a", "b"],
            Some(vec![vec![1, 2]]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE (T.a = 1 AND T.b = 2) AND 1 = 1",
            vec!["a", "b"],
            Some(vec![vec![1, 2]]),
        );

        // Invalid ANDs:
        compare_flatten::<DataType>(
            "DELETE FROM T WHERE T.a = 1 AND T.b = 2 AND T.a = 3",
            vec!["a", "b"],
            None,
        );
        compare_flatten::<DataType>(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 AND T.b = 2 AND T.a = 3",
            vec!["a", "b"],
            None,
        );
    }

    #[test]
    fn test_get_primary_key() {
        let with_field = get_schema("CREATE TABLE A (other int, id int PRIMARY KEY)");
        assert_eq!(
            get_primary_key(&with_field),
            vec![(1, &with_field.fields[1].column)]
        );

        let with_const = get_schema("CREATE TABLE A (other int, id int, PRIMARY KEY (id))");
        assert_eq!(
            get_primary_key(&with_const),
            vec![(1, &with_const.fields[1].column)]
        );

        let with_both =
            get_schema("CREATE TABLE A (other int, id int PRIMARY KEY, PRIMARY KEY (id))");
        assert_eq!(
            get_primary_key(&with_both),
            vec![(1, &with_both.fields[1].column)]
        );

        let with_none = get_schema("CREATE TABLE A (other int, id int)");
        assert_eq!(get_primary_key(&with_none), vec![]);
    }

    #[test]
    #[should_panic]
    fn test_flatten_conditional_non_key_delete() {
        compare_flatten(
            "DELETE FROM T WHERE T.b = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );
    }

    #[test]
    #[should_panic]
    fn test_flatten_conditional_non_key_update() {
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.b = 1",
            vec!["a"],
            Some(vec![vec![1]]),
        );
    }

    #[test]
    #[should_panic]
    fn test_flatten_conditional_partial_key_delete() {
        compare_flatten(
            "DELETE FROM T WHERE T.a = 1",
            vec!["a", "b"],
            Some(vec![vec![1]]),
        );
    }

    #[test]
    #[should_panic]
    fn test_flatten_conditional_partial_key_update() {
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1",
            vec!["a", "b"],
            Some(vec![vec![1]]),
        );
    }

    #[test]
    fn test_parameter_column_extraction() {
        let query = "SELECT  `votes`.* FROM `votes` WHERE `votes`.`user_id` = 1 \
                     AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL \
                     ORDER BY `votes`.`id` ASC LIMIT 1";
        let q = nom_sql::parse_query(query).unwrap();

        let pc = get_parameter_columns(&q);

        assert_eq!(pc, vec![&Column::from("votes.story_id")]);
    }

    #[test]
    fn test_dollar_number_parameter_column_extraction() {
        let query = "SELECT  `votes`.* FROM `votes` WHERE `votes`.`user_id` = 1 \
                     AND `votes`.`story_id` = $1 AND `votes`.`comment_id` IS NULL \
                     ORDER BY `votes`.`id` ASC LIMIT 1";
        let q = nom_sql::parse_query(query).unwrap();

        let pc = get_parameter_columns(&q);

        assert_eq!(pc, vec![&Column::from("votes.story_id")]);
    }
}
