use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};

use nom_sql::{
    BinaryOperator, Column, ColumnConstraint, CreateTableStatement, DeleteStatement, Expression,
    InsertStatement, Literal, SelectStatement, SqlQuery, TableKey, UpdateStatement,
};
use noria::{Modification, Operation};
use noria_data::DataType;
use noria_errors::{
    bad_request_err, invariant, invariant_eq, unsupported, unsupported_err, ReadySetResult,
};

pub(crate) fn hash_select_query(q: &SelectStatement) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    q.hash(&mut h);
    h.finish()
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
    cond: &Expression,
    pkey: &[&Column],
    flattened: &mut HashSet<Vec<(String, DataType)>>,
) -> ReadySetResult<bool> {
    Ok(match *cond {
        Expression::BinaryOp {
            lhs: box Expression::Literal(ref l),
            rhs: box Expression::Column(ref c),
            op: BinaryOperator::Equal | BinaryOperator::Is,
        }
        | Expression::BinaryOp {
            lhs: box Expression::Column(ref c),
            rhs: box Expression::Literal(ref l),
            op: BinaryOperator::Equal | BinaryOperator::Is,
        } => {
            if !pkey.iter().any(|pk| pk.name == c.name) {
                unsupported!("UPDATE/DELETE only supports WHERE-clauses on primary keys");
            }
            if !c.table.iter().all(|n| n == pkey[0].table.as_ref().unwrap()) {
                unsupported!("UPDATE/DELETE contains references to another table")
            }

            let value = DataType::try_from(l)?;
            // We want to look through our existing keys and see if any of them
            // are missing any columns. In that case we'll add the one we're looking
            // at now there.
            let with_space = flattened
                .iter()
                .find(|key| {
                    key.len() < pkey.len() && !key.iter().any(|&(ref name, _)| name == &c.name)
                })
                .cloned();

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
        Expression::BinaryOp {
            lhs: box Expression::Literal(ref left),
            rhs: box Expression::Literal(ref right),
            op: BinaryOperator::Equal,
        } if left == right => true,
        Expression::BinaryOp {
            op: BinaryOperator::And,
            ref lhs,
            ref rhs,
        } => {
            // When checking ANDs we want to make sure that both sides refer to the same key,
            // e.g. WHERE A.a = 1 AND A.a = 1
            // or for compound primary keys:
            // WHERE A.a = AND a.b = 2
            // but also bogus stuff like `WHERE 1 = 1 AND 2 = 2`.
            let pre_count = flattened.len();
            do_flatten_conditional(&*lhs, pkey, flattened)? && {
                let count = flattened.len();
                let valid = do_flatten_conditional(&*rhs, pkey, flattened)?;
                valid && (pre_count == flattened.len() || count == flattened.len())
            }
        }
        Expression::BinaryOp {
            op: BinaryOperator::Or,
            ref lhs,
            ref rhs,
        } => {
            do_flatten_conditional(&*lhs, pkey, flattened)?
                && do_flatten_conditional(&*rhs, pkey, flattened)?
        }
        _ => false,
    })
}

// Takes a tree of conditional expressions for a DELETE/UPDATE statement and returns a list of all
// the keys that should be mutated.
// Panics if given a WHERE-clause containing other keys than the primary.
// DELETE FROM a WHERE key = 1 OR key = 2 -> Some([[1], [2]])
// DELETE FROM a WHERE key = 1 OR key = 2 AND key = 3 -> None // Bogus query
// DELETE FROM a WHERE key = 1 AND key = 1 -> Some([[1]])
pub(crate) fn flatten_conditional(
    cond: &Expression,
    pkey: &[&Column],
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
        .filter(|&(_, cs)| {
            cs.constraints.contains(&ColumnConstraint::PrimaryKey)
                || match schema.keys {
                    // Try finding PRIMARY KEY constraints in keys as well:
                    Some(ref keys) => keys.iter().any(|key| match *key {
                        TableKey::PrimaryKey { ref columns, .. } => {
                            columns.iter().any(|c| c == &cs.column)
                        }
                        _ => false,
                    }),
                    _ => false,
                }
        })
        .map(|(i, cs)| (i, &cs.column))
        .collect()
}

fn get_parameter_columns_recurse(cond: &Expression) -> Vec<(&Column, BinaryOperator)> {
    match *cond {
        Expression::BinaryOp {
            lhs: box Expression::Column(ref c),
            rhs: box Expression::Literal(Literal::Placeholder(_)),
            op: binop,
        } => vec![(c, binop)],
        Expression::BinaryOp {
            lhs: box Expression::Literal(Literal::Placeholder(_)),
            rhs: box Expression::Column(ref c),
            op: binop,
        } => vec![(c, binop.flip_comparison().unwrap_or(binop))],
        Expression::In {
            lhs: box Expression::Column(ref c),
            rhs: nom_sql::InValue::List(ref exprs),
            negated: false,
        } if exprs
            .iter()
            .all(|expr| matches!(expr, Expression::Literal(Literal::Placeholder(_)))) =>
        {
            vec![(c, BinaryOperator::Equal); exprs.len()]
        }
        Expression::In {
            lhs: box Expression::Column(ref c),
            rhs: nom_sql::InValue::List(ref exprs),
            negated: true,
        } if exprs
            .iter()
            .all(|expr| matches!(expr, Expression::Literal(Literal::Placeholder(_)))) =>
        {
            vec![(c, BinaryOperator::NotEqual); exprs.len()]
        }
        Expression::In { .. } => vec![],
        Expression::BinaryOp {
            op: BinaryOperator::And,
            ref lhs,
            ref rhs,
        }
        | Expression::BinaryOp {
            op: BinaryOperator::Or,
            ref lhs,
            ref rhs,
        } => {
            let mut l = get_parameter_columns_recurse(lhs);
            let mut r = get_parameter_columns_recurse(rhs);
            l.append(&mut r);
            l
        }
        Expression::BinaryOp { .. } => vec![],
        Expression::UnaryOp { rhs: ref expr, .. } | Expression::Cast { ref expr, .. } => {
            get_parameter_columns_recurse(expr)
        }
        Expression::Call(ref f) => f
            .arguments()
            .flat_map(get_parameter_columns_recurse)
            .collect(),
        Expression::Literal(_) => vec![],
        Expression::CaseWhen {
            ref condition,
            ref then_expr,
            ref else_expr,
        } => get_parameter_columns_recurse(condition)
            .into_iter()
            .chain(get_parameter_columns_recurse(then_expr))
            .chain(
                else_expr
                    .iter()
                    .flat_map(|expr| get_parameter_columns_recurse(expr)),
            )
            .collect(),
        Expression::Column(_) => vec![],
        Expression::Exists(_) => vec![],
        Expression::Between {
            operand: box Expression::Column(ref col),
            min: box Expression::Literal(Literal::Placeholder(_)),
            max: box Expression::Literal(Literal::Placeholder(_)),
            ..
        } => vec![
            (col, BinaryOperator::GreaterOrEqual),
            (col, BinaryOperator::LessOrEqual),
        ],

        Expression::Between {
            operand: box Expression::Column(ref col),
            min: box Expression::Literal(Literal::Placeholder(_)),
            negated: false,
            ..
        }
        | Expression::Between {
            operand: box Expression::Column(ref col),
            max: box Expression::Literal(Literal::Placeholder(_)),
            negated: true,
            ..
        } => vec![(col, BinaryOperator::GreaterOrEqual)],
        Expression::Between {
            operand: box Expression::Column(ref col),
            max: box Expression::Literal(Literal::Placeholder(_)),
            negated: false,
            ..
        }
        | Expression::Between {
            operand: box Expression::Column(ref col),
            min: box Expression::Literal(Literal::Placeholder(_)),
            negated: true,
            ..
        } => vec![(col, BinaryOperator::LessOrEqual)],
        Expression::Between { .. } => vec![],
        Expression::NestedSelect(_) => vec![],
        Expression::Variable(_) => vec![],
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

pub(crate) fn get_limit_parameters(query: &SelectStatement) -> Vec<Column> {
    let mut limit_params = vec![];
    if let Some(ref limit) = query.limit {
        if let Expression::Literal(Literal::Placeholder(_)) = limit.limit {
            limit_params.push(Column {
                name: "__row_count".to_string(),
                table: None,
                function: None,
            });
        }
        if let Some(Expression::Literal(Literal::Placeholder(_))) = limit.offset {
            limit_params.push(Column {
                name: "__offset".to_string(),
                table: None,
                function: None,
            });
        }
    }
    limit_params
}

pub(crate) fn insert_statement_parameter_columns(query: &InsertStatement) -> Vec<&Column> {
    // need to find for which fields we *actually* have a parameter
    query
        .data
        .iter()
        .flat_map(|d| {
            d.iter().enumerate().filter_map(|(i, v)| match *v {
                Literal::Placeholder(_) => Some(&query.fields.as_ref().unwrap()[i]),
                _ => None,
            })
        })
        .collect()
}

pub(crate) fn update_statement_parameter_columns(query: &UpdateStatement) -> Vec<&Column> {
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

pub(crate) fn delete_statement_parameter_columns(query: &DeleteStatement) -> Vec<&Column> {
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
        SqlQuery::Insert(ref query) => insert_statement_parameter_columns(query),
        SqlQuery::Update(ref query) => update_statement_parameter_columns(query),
        SqlQuery::Delete(ref query) => delete_statement_parameter_columns(query),
        _ => unimplemented!(),
    }
}

fn walk_pkey_where<I>(
    col2v: &mut HashMap<String, DataType>,
    params: &mut Option<I>,
    expr: Expression,
) -> ReadySetResult<()>
where
    I: Iterator<Item = DataType>,
{
    match expr {
        Expression::BinaryOp {
            op: BinaryOperator::Equal,
            lhs: box Expression::Column(c),
            rhs: box Expression::Literal(l),
        } => {
            let v = match l {
                Literal::Placeholder(_) => params
                    .as_mut()
                    .ok_or_else(|| bad_request_err("Found placeholder in ad-hoc query"))?
                    .next()
                    .ok_or_else(|| {
                        bad_request_err("Not enough parameter values given in EXECUTE")
                    })?,
                v => DataType::try_from(v)?,
            };
            let oldv = col2v.insert(c.name, v);
            invariant!(oldv.is_none());
        }
        Expression::BinaryOp {
            op: BinaryOperator::And,
            lhs,
            rhs,
        } => {
            // recurse
            walk_pkey_where(col2v, params, *lhs)?;
            walk_pkey_where(col2v, params, *rhs)?;
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
                        Modification::Set(DataType::try_from(v)?.coerce_to(&field.sql_type)?),
                    ));
                }
                Expression::BinaryOp {
                    lhs: box Expression::Column(ref c),
                    ref op,
                    rhs: box Expression::Literal(ref l),
                } => {
                    // we only support "column = column +/- literal"
                    // TODO(ENG-142): Handle nested arithmetic
                    invariant_eq!(c, &field.column);
                    match op {
                        BinaryOperator::Add => {
                            updates.push((i, Modification::Apply(Operation::Add, l.try_into()?)))
                        }
                        BinaryOperator::Subtract => {
                            updates.push((i, Modification::Apply(Operation::Sub, l.try_into()?)))
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

pub(crate) fn extract_pkey_where<I>(
    where_clause: Expression,
    mut params: Option<I>,
    schema: &CreateTableStatement,
) -> ReadySetResult<Vec<DataType>>
where
    I: Iterator<Item = DataType>,
{
    let pkey = get_primary_key(schema);
    let mut col_to_val: HashMap<_, _> = HashMap::new();
    walk_pkey_where(&mut col_to_val, &mut params, where_clause)?;
    pkey.iter()
        .map(|&(_, c)| {
            col_to_val.remove(&c.name).ok_or_else(|| {
                unsupported_err(
                    "UPDATE or DELETE on columns other than the primary key are not supported",
                )
            })
        })
        .collect()
}

type ExtractedUpdate = (Vec<DataType>, Vec<(usize, Modification)>);

pub(crate) fn extract_update<I>(
    mut q: UpdateStatement,
    mut params: Option<I>,
    schema: &CreateTableStatement,
) -> ReadySetResult<ExtractedUpdate>
where
    I: Iterator<Item = DataType>,
{
    let updates = extract_update_params_and_fields(&mut q, &mut params, schema);
    let where_clause = q
        .where_clause
        .ok_or_else(|| unsupported_err("UPDATE without WHERE is not supported"))?;
    let key = extract_pkey_where(where_clause, params, schema)?;
    Ok((key, updates?))
}

pub(crate) fn extract_delete<I>(
    q: DeleteStatement,
    params: Option<I>,
    schema: &CreateTableStatement,
) -> ReadySetResult<Vec<DataType>>
where
    I: Iterator<Item = DataType>,
{
    let where_clause = q
        .where_clause
        .ok_or_else(|| unsupported_err("DELETE without WHERE is not supported"))?;
    extract_pkey_where(where_clause, params, schema)
}

/// coerce params to correct sql types
pub(crate) fn coerce_params(
    params: Option<&[DataType]>,
    q: &SqlQuery,
    schema: &CreateTableStatement,
) -> ReadySetResult<Option<Vec<DataType>>> {
    if let Some(prms) = params {
        let mut coerced_params = vec![];
        for (i, col) in get_parameter_columns(q).iter().enumerate() {
            for field in &schema.fields {
                if col.name == field.column.name {
                    coerced_params.push(DataType::coerce_to(&prms[i], &field.sql_type)?);
                }
            }
        }
        Ok(Some(coerced_params))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{self, Dialect, SqlQuery};

    use super::*;

    fn compare_flatten<I>(cond_query: &str, key: Vec<&str>, expected: Option<Vec<Vec<I>>>)
    where
        I: Into<DataType>,
    {
        let cond = match nom_sql::parse_query(Dialect::MySQL, cond_query).unwrap() {
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

        let pkey_ref = pkey.iter().collect::<Vec<_>>();
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
        match nom_sql::parse_query(Dialect::MySQL, query).unwrap() {
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
        let q = nom_sql::parse_query(Dialect::MySQL, query).unwrap();

        let pc = get_parameter_columns(&q);

        assert_eq!(pc, vec![&Column::from("votes.story_id")]);
    }

    #[test]
    fn test_dollar_number_parameter_column_extraction() {
        let query = "SELECT  `votes`.* FROM `votes` WHERE `votes`.`user_id` = 1 \
                     AND `votes`.`story_id` = $1 AND `votes`.`comment_id` IS NULL \
                     ORDER BY `votes`.`id` ASC LIMIT 1";
        let q = nom_sql::parse_query(Dialect::MySQL, query).unwrap();

        let pc = get_parameter_columns(&q);

        assert_eq!(pc, vec![&Column::from("votes.story_id")]);
    }
}
