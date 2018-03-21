use nom_sql::{self, ConditionBase, ConditionExpression, Operator};
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

pub(crate) fn ensure_pkey_condition(
    cond: &ConditionExpression,
    pkey: &Vec<&nom_sql::Column>,
) -> bool {
    // below logic only works for single-column primary keys
    // a more general implementation would check that every component column of the pkey occurs in
    // an ANDed set of conditions
    assert_eq!(pkey.len(), 1);
    match *cond {
        ConditionExpression::LogicalOp(ref ct) => {
            match ct.operator {
                Operator::And => {
                    // if AND, recurse
                    // This only matches nonsensical queries like
                    // "WHERE pkey = 4 AND pkey = 12" or "WHERE pkey = 4 AND pkey = 4"
                    ensure_pkey_condition(&*ct.left, pkey)
                        && ensure_pkey_condition(&*ct.right, pkey)
                }
                _ => {
                    // if OR, recurse to see if both branches are on pkey
                    ensure_pkey_condition(&*ct.left, pkey)
                        && ensure_pkey_condition(&*ct.right, pkey)
                }
            }
        }
        ConditionExpression::ComparisonOp(ref ct) => {
            // either ct.left or ct.right must be the primary key column
            let left_is_pkey = match *ct.left {
                // all good
                ConditionExpression::Base(ConditionBase::Field(ref f)) => f == pkey[0],
                _ => false,
            };
            let right_is_pkey = match *ct.right {
                // all good
                ConditionExpression::Base(ConditionBase::Field(ref f)) => f == pkey[0],
                _ => false,
            };

            // one side has to be the pkey column, and it can't be a comma join, so XOR
            left_is_pkey ^ right_is_pkey
        }
        ConditionExpression::NegationOp(_) => unimplemented!(),
        _ => unreachable!(),
    }
}
