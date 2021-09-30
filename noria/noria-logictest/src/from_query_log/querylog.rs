use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::mem;
use std::str::FromStr;

use anyhow::{bail, Error};
use chrono::{DateTime, FixedOffset, Utc};
use enum_display_derive::Display;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, Lines};

use nom_sql::{
    Expression, FieldDefinitionExpression, FunctionExpression, InValue, ItemPlaceholder,
    JoinConstraint, JoinRightSide, Literal, SelectStatement, SqlQuery,
};

#[derive(Clone, Debug, Display, Eq, PartialEq)]
pub enum Command {
    Connect,
    Query,
    Prepare,
    Execute,
    CloseStmt,
    Quit,
}

impl FromStr for Command {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "Connect" => Ok(Self::Connect),
            "Query" => Ok(Self::Query),
            "Prepare" => Ok(Self::Prepare),
            "Execute" => Ok(Self::Execute),
            "Close stmt" => Ok(Self::CloseStmt),
            "Quit" => Ok(Self::Quit),
            _ => bail!("Unknown command '{}'", s),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub timestamp: DateTime<Utc>,
    pub id: u32,
    pub command: Command,
    pub arguments: String,
}

impl FromStr for Entry {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        let parts = s.splitn(3, '\t').collect::<Vec<&str>>();
        if parts.len() != 3 {
            bail!("Wrong number of tab-delimited parts: {}", parts.len());
        }
        let middle_parts = parts[1].trim().splitn(2, ' ').collect::<Vec<&str>>();
        Ok(Self {
            timestamp: DateTime::<FixedOffset>::parse_from_rfc3339(parts[0])?.into(),
            id: middle_parts[0].parse()?,
            command: Command::from_str(middle_parts[1])?,
            arguments: parts[2].to_string(),
        })
    }
}

trait ReplaceLiteralsWithPlaceholders {
    fn replace_literals(&mut self) -> Vec<Literal>;
}

impl ReplaceLiteralsWithPlaceholders for SelectStatement {
    fn replace_literals(&mut self) -> Vec<Literal> {
        let mut values = vec![];
        for cte in self.ctes.iter_mut() {
            values.append(&mut cte.statement.replace_literals());
        }
        for field in self.fields.iter_mut() {
            if let FieldDefinitionExpression::Expression { expr, .. } = field {
                values.append(&mut expr.replace_literals());
            }
        }
        for join in self.join.iter_mut() {
            if let JoinRightSide::NestedSelect(subselect, _) = &mut join.right {
                values.append(&mut subselect.replace_literals());
            }
            match &mut join.constraint {
                JoinConstraint::On(expr) => values.append(&mut expr.replace_literals()),
                JoinConstraint::Using(cols) => {
                    for col in cols.iter_mut() {
                        if let Some(expr) = col.function.as_mut() {
                            values.append(&mut expr.replace_literals());
                        }
                    }
                }
            };
        }
        if let Some(expr) = self.where_clause.as_mut() {
            values.append(&mut expr.replace_literals());
        }
        if let Some(clause) = self.group_by.as_mut() {
            if let Some(having) = clause.having.as_mut() {
                values.append(&mut having.replace_literals());
            }
        }
        if let Some(order) = self.order.as_mut() {
            for (col, _) in order.columns.iter_mut() {
                if let Some(expr) = col.function.as_mut() {
                    values.append(&mut expr.replace_literals());
                }
            }
        }
        values
    }
}

impl ReplaceLiteralsWithPlaceholders for FunctionExpression {
    fn replace_literals(&mut self) -> Vec<Literal> {
        match self {
            FunctionExpression::Avg { expr, .. } => expr.replace_literals(),
            FunctionExpression::Count { expr, .. } => expr.replace_literals(),
            FunctionExpression::CountStar => vec![],
            FunctionExpression::Sum { expr, .. } => expr.replace_literals(),
            FunctionExpression::Max(expr) => expr.replace_literals(),
            FunctionExpression::Min(expr) => expr.replace_literals(),
            FunctionExpression::GroupConcat { expr, .. } => expr.replace_literals(),
            FunctionExpression::Call { arguments, .. } => {
                let mut values = vec![];
                for expr in arguments.iter_mut() {
                    values.append(&mut expr.replace_literals());
                }
                values
            }
        }
    }
}

impl ReplaceLiteralsWithPlaceholders for Expression {
    fn replace_literals(&mut self) -> Vec<Literal> {
        match self {
            Expression::Call(v) => v.replace_literals(),
            // THIS IS THE PLACE WHERE SOMETHING HAPPENS
            Expression::Literal(v) => vec![mem::replace(
                v,
                Literal::Placeholder(ItemPlaceholder::QuestionMark),
            )],
            Expression::BinaryOp { lhs, rhs, .. } => {
                let mut values = lhs.replace_literals();
                values.append(&mut rhs.replace_literals());
                values
            }
            Expression::UnaryOp { rhs: expr, .. } | Expression::Cast { expr, .. } => {
                expr.replace_literals()
            }
            Expression::CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => {
                let mut values = condition.replace_literals();
                values.append(&mut then_expr.replace_literals());
                if let Some(expr) = else_expr {
                    values.append(&mut expr.replace_literals());
                }
                values
            }
            Expression::Column(c) => match c.function.as_mut() {
                Some(expr) => expr.replace_literals(),
                None => vec![],
            },
            Expression::Exists(select) => select.replace_literals(),
            Expression::Between {
                operand, min, max, ..
            } => {
                let mut values = operand.replace_literals();
                values.append(&mut min.replace_literals());
                values.append(&mut max.replace_literals());
                values
            }
            Expression::NestedSelect(select) => select.replace_literals(),
            Expression::In { lhs, rhs, .. } => {
                let mut values = lhs.replace_literals();
                match rhs {
                    InValue::Subquery(select) => values.append(&mut select.replace_literals()),
                    InValue::List(list) => {
                        for expr in list.iter_mut() {
                            values.append(&mut expr.replace_literals());
                        }
                    }
                };
                values
            }
        }
    }
}

#[derive(Debug)]
pub struct Session {
    pub entries: Vec<Entry>,
    pub prepared_statements: HashSet<SqlQuery>,
}

impl Session {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            prepared_statements: HashSet::new(),
        }
    }

    fn query_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| matches!(e.command, Command::Query | Command::Execute))
            .count()
    }

    /// Accepts a query with embedded literals and searches for an existing prepared statement with
    /// all the literals replaced with ?s; if a match is found, returns the prepared statement
    /// along with a list of the literals that were pulled out.
    pub fn find_prepared_statement(&self, query: &SqlQuery) -> Option<(&SqlQuery, Vec<Literal>)> {
        let mut values = vec![];
        // TODO:  We should flip this around to do a comparison where placeholders in a prepared
        // statement match any literal in the input query, then pull out just the literals that
        // matched with a placeholder.  This will allow us to support queries where the prepared
        // statement itself contains literals, without this early escape hack making it
        // all-or-nothing.
        // TODO:  We probably need to support placeholders other than ?
        if let Some(q) = self.prepared_statements.get(query) {
            return Some((q, vec![]));
        }
        let mut query = query.clone();
        match &mut query {
            SqlQuery::Insert(q) => q.data.iter_mut().for_each(|row| {
                row.iter_mut().for_each(|v| {
                    values.push(mem::replace(
                        v,
                        Literal::Placeholder(ItemPlaceholder::QuestionMark),
                    ))
                })
            }),
            SqlQuery::Update(q) => {
                for (_, v) in q.fields.iter_mut() {
                    values.append(&mut v.replace_literals());
                }
                if let Some(clause) = q.where_clause.as_mut() {
                    values.append(&mut clause.replace_literals());
                }
            }
            SqlQuery::Select(q) => values.append(&mut q.replace_literals()),
            SqlQuery::Delete(q) => {
                if let Some(clause) = q.where_clause.as_mut() {
                    values.append(&mut clause.replace_literals());
                }
            }
            _ => (),
        }
        self.prepared_statements.get(&query).map(|s| (s, values))
    }
}

pub struct Stream<R: AsyncBufRead + Unpin> {
    // Include this to emulate the functionality of .enumerate()
    current_idx: usize,
    inner: Lines<R>,
    partial_sessions: BTreeMap<u32, Session>,
    current_entry: Option<Entry>,
    split_sessions: bool,
}

// Query logs have a format that looks like this:
//    Time                 Id Command    Argument
//    2021-08-17T16:28:35.019845Z        43 Quit
//    2021-08-17T16:28:35.374604Z        44 Connect   root@172.17.0.1 on test using TCP/IP
//    2021-08-17T16:28:35.374879Z        44 Query     SET autocommit=0
//    2021-08-17T16:28:35.382943Z        44 Query     SELECT DISTINCT TABLE_NAME, CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL
//    2021-08-17T16:28:35.388404Z        44 Query     SHOW TABLES
//    2021-08-17T16:28:35.693179Z        45 Connect   root@172.17.0.1 on test using TCP/IP
//    2021-08-17T16:28:40.559222Z        44 Quit
// Where the "Id" field is a unique identifier for a given client connection,
// and various connections are interleaved.
// The code to read them is, in essence, a state machine.  We want to yield one
// Session for each of those connections.  To do this, we read the input
// line-by-line, buffering entries for each connection we find until we get a
// "Quit" command for that connection.  Once we do, we remove it from the
// "buffer" and return it.  Upon getting EOF on the input, we yield all the
// "incomplete" sessions that are still in the buffer.
impl<R: AsyncBufRead + Unpin> Stream<R> {
    pub fn new(input: R, split_sessions: bool) -> Self {
        Self {
            current_idx: 0,
            inner: input.lines(),
            partial_sessions: BTreeMap::new(),
            current_entry: None,
            split_sessions,
        }
    }

    pub async fn next(&mut self) -> Option<(usize, Session)> {
        let result = self.next_inner().await.map(|s| (self.current_idx, s));
        self.current_idx += 1;
        result
    }

    async fn next_inner(&mut self) -> Option<Session> {
        if self.split_sessions {
            self.next_split().await
        } else {
            self.next_unsplit().await
        }
    }

    async fn next_split(&mut self) -> Option<Session> {
        while let Ok(Some(line)) = self.inner.next_line().await {
            let completed_session = match Entry::from_str(&line) {
                Ok(v) => {
                    let old_entry = self.current_entry.replace(v);
                    if let Some(entry) = old_entry {
                        self.partial_sessions
                            .entry(entry.id)
                            .or_insert_with(Session::new)
                            .entries
                            .push(entry.clone());
                        if entry.command == Command::Quit {
                            self.partial_sessions.remove(&entry.id)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                Err(_) => {
                    if let Some(entry) = self.current_entry.as_mut() {
                        entry.arguments.push('\n');
                        entry.arguments.push_str(&line);
                    };
                    None
                }
            };
            if let Some(session) = completed_session {
                if session.query_count() > 0 {
                    return Some(session);
                }
            }
        }
        while let Some(k) = self.partial_sessions.keys().cloned().next() {
            if let Some(session) = self.partial_sessions.remove(&k) {
                if session.query_count() > 0 {
                    return Some(session);
                }
            }
        }
        None
    }

    async fn next_unsplit(&mut self) -> Option<Session> {
        let mut session = Session::new();
        // TODO:  Queries can contain non-UTF8 bytes; this will skip those lines, which will cause
        // incorrect conversion (best case) or a completely broken logictest (worst case).  We
        // should handle these as just byte arrays instead of strings, but mysql_async accepts
        // queries as AsRef<str>, so at this time we can only support UTF8 anyway.
        while let Ok(Some(line)) = self.inner.next_line().await {
            if let Ok(v) = Entry::from_str(&line) {
                if let Some(entry) = self.current_entry.replace(v) {
                    session.entries.push(entry);
                }
            } else if let Some(entry) = self.current_entry.as_mut() {
                entry.arguments.push('\n');
                entry.arguments.push_str(&line);
            }
        }
        if session.entries.is_empty() {
            None
        } else {
            Some(session)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::{parse_query, Dialect, Literal};

    /// The primary purpose of this test is to validate that, when we extract literals from
    /// queries, they come out in left-to-right order.  Anything else it covers is a happy
    /// accident.
    #[test]
    fn test_prepared_statement_value_extraction() {
        use Literal::*;

        let common_insert = parse_query(
            Dialect::MySQL,
            "INSERT INTO payment (customer_id, amount, account_name) VALUES (?, ?, ?)",
        )
        .unwrap();
        let common_update = parse_query(
            Dialect::MySQL,
            "UPDATE payment SET account_name = ? WHERE (customer_id = ?)",
        )
        .unwrap();
        let common_delete = parse_query(
            Dialect::MySQL,
            "DELETE FROM payment WHERE (customer_id = ?)",
        )
        .unwrap();
        let select_information_schema = parse_query(Dialect::MySQL, "SELECT column_name AS column_name, data_type AS data_type, column_type AS full_data_type, character_maximum_length AS character_maximum_length, numeric_precision AS numeric_precision, numeric_scale AS numeric_scale, datetime_precision AS datetime_precision, column_default AS column_default, is_nullable AS is_nullable, extra AS extra, table_name AS table_name FROM information_schema.columns WHERE (table_schema = ?) ORDER BY ordinal_position ASC").unwrap();
        let get_weird = parse_query(Dialect::MySQL, "SELECT ?, field1, MyFunc(?) FROM t1 INNER JOIN t2 ON t1.id = t2.id AND t2.name = ? WHERE t1.id = ?").unwrap();

        let mut session = Session::new();
        session.prepared_statements.insert(common_insert.clone());
        session.prepared_statements.insert(common_update.clone());
        session.prepared_statements.insert(common_delete.clone());
        session
            .prepared_statements
            .insert(select_information_schema.clone());
        session.prepared_statements.insert(get_weird.clone());

        let (parsed, values) = session
            .find_prepared_statement(
                &parse_query(
                    Dialect::MySQL,
                    "INSERT INTO payment (customer_id, amount, account_name) VALUES (1, 2, 3)",
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(parsed, &common_insert);
        assert_eq!(values, vec![Integer(1), Integer(2), Integer(3)]);
        let (parsed, values) = session.find_prepared_statement(&parse_query(Dialect::MySQL, "INSERT INTO payment (customer_id, amount, account_name) VALUES (1, 'str', NULL)").unwrap()).unwrap();
        assert_eq!(parsed, &common_insert);
        assert_eq!(values, vec![Integer(1), String("str".to_string()), Null]);
        let (parsed, values) = session
            .find_prepared_statement(
                &parse_query(
                    Dialect::MySQL,
                    "UPDATE payment SET account_name = 'test' WHERE (customer_id = 123)",
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(parsed, &common_update);
        assert_eq!(values, vec![String("test".to_string()), Integer(123)]);
        let (parsed, values) = session
            .find_prepared_statement(
                &parse_query(
                    Dialect::MySQL,
                    "DELETE FROM payment WHERE (customer_id = 9)",
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(parsed, &common_delete);
        assert_eq!(values, vec![Integer(9)]);
        let (parsed, values) = session.find_prepared_statement(&parse_query(Dialect::MySQL, "SELECT column_name AS column_name, data_type AS data_type, column_type AS full_data_type, character_maximum_length AS character_maximum_length, numeric_precision AS numeric_precision, numeric_scale AS numeric_scale, datetime_precision AS datetime_precision, column_default AS column_default, is_nullable AS is_nullable, extra AS extra, table_name AS table_name FROM information_schema.columns WHERE (table_schema = 'dbtest') ORDER BY ordinal_position ASC").unwrap()).unwrap();
        assert_eq!(parsed, &select_information_schema);
        assert_eq!(values, vec![String("dbtest".to_string())]);
        let (parsed, values) = session.find_prepared_statement(&parse_query(Dialect::MySQL, "SELECT 3.14, field1, MyFunc(NULL) FROM t1 INNER JOIN t2 ON t1.id = t2.id AND t2.name = 'somebody' WHERE t1.id = 867").unwrap()).unwrap();
        assert_eq!(parsed, &get_weird);
        assert_eq!(
            values,
            vec![
                Literal::Double(nom_sql::Double {
                    value: 3.14,
                    precision: 2,
                }),
                Null,
                String("somebody".to_string()),
                Integer(867)
            ]
        );
    }

    #[test]
    fn test_close_stmt_parse() {
        let line = "2021-09-22T11:06:44.749911Z	   11 Close stmt	";
        let parsed = Entry::from_str(line).unwrap();
        assert_eq!(
            parsed,
            Entry {
                timestamp: DateTime::<FixedOffset>::parse_from_rfc3339(
                    "2021-09-22T11:06:44.749911Z"
                )
                .unwrap()
                .into(),
                id: 11,
                command: Command::CloseStmt,
                arguments: "".to_string(),
            }
        );
    }
}
