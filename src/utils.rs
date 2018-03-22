use std::collections::HashSet;

use distributary::DataType;
use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, Operator};
use regex::Regex;

lazy_static! {
    pub(crate) static ref HARD_CODED_REPLIES: Vec<(Regex, Vec<(&'static str, &'static str)>)> = vec![
        (Regex::new(r"(?i)select version\(\) limit 1").unwrap(),
         vec![("version()", "10.1.26-MariaDB-0+deb9u1")]),
        (Regex::new(r"(?i)show engines").unwrap(),
         vec![("Engine", "InnoDB"),
              ("Support", "DEFAULT"),
              ("Comment", ""),
              ("Transactions", "YES"),
              ("XA", "YES"),
              ("Savepoints", "YES")]),
        (Regex::new(r"SELECT 1 AS ping").unwrap(), vec![("ping", "1")]),
        (Regex::new(r"(?i)show global variables like 'read_only'").unwrap(),
         vec![("Variable_name", "read_only"), ("Value", "OFF")]),
        (Regex::new(r"(?i)select get_lock\(.*\) as lockstatus").unwrap(),
         vec![("lockstatus", "1")]),
        (Regex::new(r"(?i)select release_lock\(.*\) as lockstatus").unwrap(),
         vec![("lockstatus", "1")]),
    ];
}

pub(crate) fn sanitize_query(query: &str) -> String {
    let query = Regex::new(r"(?s)/\*.*\*/").unwrap().replace_all(query, "");
    let query = Regex::new(r"--.*\n").unwrap().replace_all(&query, "\n");
    let query = Regex::new(r" +").unwrap().replace_all(&query, " ");
    let query = query.replace('"', "'");
    let query = query.trim();
    query.to_owned()
}

// Helper for flatten_conditional - returns true if the
// expression is "valid" (i.e. not something like `a = 1 AND a = 2`.
fn do_flatten_conditional(
    cond: &ConditionExpression,
    pkey: &Column,
    mut flattened: &mut HashSet<DataType>,
) -> bool {
    match *cond {
        ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Literal(ref l)),
            right: box ConditionExpression::Base(ConditionBase::Field(ref c)),
            operator: Operator::Equal,
        })
        | ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Field(ref c)),
            right: box ConditionExpression::Base(ConditionBase::Literal(ref l)),
            operator: Operator::Equal,
        }) => {
            if c != pkey {
                panic!("UPDATE/DELETE only supports WHERE-clauses on primary keys");
            }

            flattened.insert(DataType::from(l));
            true
        }
        ConditionExpression::ComparisonOp(ConditionTree {
            left: box ConditionExpression::Base(ConditionBase::Literal(ref left)),
            right: box ConditionExpression::Base(ConditionBase::Literal(ref right)),
            operator: Operator::Equal,
        }) if left == right =>
        {
            true
        }
        ConditionExpression::LogicalOp(ConditionTree {
            operator: Operator::And,
            ref left,
            ref right,
        }) => {
            do_flatten_conditional(&*left, pkey, &mut flattened) && {
                let count = flattened.len();
                let valid = do_flatten_conditional(&*right, pkey, &mut flattened);
                // Only valid if neither of the sides contain a bad key, and if they both refer to
                // the same key value. `key = 1 AND key = 1` is okay, whereas `key = 1 AND key = 2`
                // is not.
                valid && count == flattened.len()
            }
        }
        ConditionExpression::LogicalOp(ConditionTree {
            operator: Operator::Or,
            ref left,
            ref right,
        }) => {
            do_flatten_conditional(&*left, pkey, &mut flattened)
                && do_flatten_conditional(&*right, pkey, &mut flattened)
        }
        _ => false,
    }
}

// Takes a tree of conditional expressions for a DELETE/UPDATE statement and returns a list of all the
// keys that should be mutated.
// Panics if given a WHERE-clause containing other keys than the primary.
// DELETE FROM a WHERE key = 1 OR key = 2 -> Some([1, 2])
// DELETE FROM a WHERE key = 1 OR key = 2 AND key = 3 -> None // Bogus query
// DELETE FROM a WHERE key = 1 AND key = 1 -> Some([1])
pub(crate) fn flatten_conditional(
    cond: &ConditionExpression,
    pkey: &Vec<&Column>,
) -> Option<HashSet<DataType>> {
    // below logic only works for single-column primary keys
    // a more general implementation would return a collection of tuples, where each tuple
    // consists of the primary key columns needed to retrieve a row.
    assert_eq!(pkey.len(), 1);
    let mut flattened = HashSet::new();
    if do_flatten_conditional(cond, pkey[0], &mut flattened) {
        Some(flattened)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{self, SqlQuery};
    use super::*;

    fn compare_flatten<I>(cond_query: &str, pkey_name: &str, expected: Option<Vec<I>>)
    where
        I: Into<DataType>,
    {
        let cond = match nom_sql::parse_query(cond_query).unwrap() {
            SqlQuery::Update(u) => u.where_clause.unwrap(),
            SqlQuery::Delete(d) => d.where_clause.unwrap(),
            _ => unreachable!(),
        };

        let pkey = Column {
            name: String::from(pkey_name),
            table: Some(String::from("T")),
            alias: None,
            function: None,
        };

        let flat: Option<HashSet<DataType>> =
            expected.and_then(|e| Some(e.into_iter().map(|v| v.into()).collect()));
        assert_eq!(flatten_conditional(&cond, &vec![&pkey]), flat);
    }

    #[test]
    fn test_flatten_conditional() {
        compare_flatten("DELETE FROM T WHERE T.a = 1", "a", Some(vec![1]));
        compare_flatten(
            "DELETE FROM T WHERE T.a = 1 OR T.a = 2",
            "a",
            Some(vec![1, 2]),
        );
        compare_flatten("UPDATE T SET T.b = 2 WHERE T.a = 1", "a", Some(vec![1]));
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 OR T.a = 2",
            "a",
            Some(vec![1, 2]),
        );

        // Valid, but bogus, ORs:
        compare_flatten("DELETE FROM T WHERE T.a = 1 OR T.a = 1", "a", Some(vec![1]));
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 OR T.a = 1",
            "a",
            Some(vec![1]),
        );

        // Valid, but bogus, ANDs:
        compare_flatten(
            "DELETE FROM T WHERE T.a = 1 AND T.a = 1",
            "a",
            Some(vec![1]),
        );
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 AND T.a = 1",
            "a",
            Some(vec![1]),
        );
        compare_flatten("DELETE FROM T WHERE T.a = 1 AND 1 = 1", "a", Some(vec![1]));
        compare_flatten(
            "UPDATE T SET T.b = 2 WHERE T.a = 1 AND 1 = 1",
            "a",
            Some(vec![1]),
        );

        // We can't really handle these at the moment, but in the future we might want to
        // delete/update all rows:
        compare_flatten::<DataType>("DELETE FROM T WHERE 1 = 1", "a", Some(vec![]));
        compare_flatten::<DataType>("UPDATE T SET T.b = 2 WHERE 1 = 1", "a", Some(vec![]));

        // Invalid ANDs:
        compare_flatten::<DataType>("DELETE FROM T WHERE T.a = 1 AND T.a = 2", "a", None);
        compare_flatten::<DataType>("UPDATE T SET T.b = 2 WHERE T.a = 1 AND T.a = 2", "a", None);
    }

    #[test]
    #[should_panic]
    fn test_flatten_conditional_non_key_delete() {
        compare_flatten("DELETE FROM T WHERE T.b = 1", "a", Some(vec![1]));
    }

    #[test]
    #[should_panic]
    fn test_flatten_conditional_non_key_update() {
        compare_flatten("UPDATE T SET T.b = 2 WHERE T.b = 1", "a", Some(vec![1]));
    }
}
