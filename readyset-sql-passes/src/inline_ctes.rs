//! Inlining a statement's `WITH` entries at the references that read them.
//!
//! A statement still carrying a `WITH` clause fails the deep pipeline's invariants and takes the
//! shallow path, where each entry becomes a view.  Substituting each entry's body at the reference
//! that reads it clears the clause instead and leaves an ordinary derived table, which the later
//! passes absorb as they would any other.
//!
//! An entry read more than once would be substituted more than once, so a body is taken at the
//! reference that reads it: a second reference finding it gone is what refuses the statement.

use readyset_errors::{ReadySetError, ReadySetResult, unsupported};
use readyset_sql::analysis::visit_mut::{self as visit_mut, VisitorMut};
use std::ops::Range;

use readyset_sql::Dialect;
use readyset_sql::ast::{SelectStatement, SqlIdentifier, TableExpr, TableExprInner};

/// Inline every `WITH` entry into the statement that defines it, clearing the `WITH` clauses.
///
/// An entry read once becomes a derived table at the reference that reads it, under the name it
/// was defined by, so the columns it projects go on resolving unchanged.  An entry nothing reads
/// is dropped.  A statement where any entry is read more than once is refused: inlining copies a
/// body into the statement once per read, and the copies do not stay alike -- later passes absorb
/// one and leave another standing, so the statement ends up keyed on the same literals twice.
///
/// Bodies are inlined before the statement that reads them, so a chain collapses innermost-first.
///
/// # Recursion
///
/// This assumes a `WITH RECURSIVE` entry is distinguishable from a plain one.  It is not yet:
/// `CommonTableExpr` carries no marker and the conversion from the parser's own `Cte` drops the
/// `recursive` flag, so a recursive entry arrives looking ordinary and its self-reference reads as
/// a reference to a relation of that name.  Inlining one would leave that self-reference behind,
/// pointing at whatever else answers to the name.  Carrying the marker is a separate change; this
/// pass must not be wired into the pipeline before it lands.
pub fn inline_ctes(statement: &mut SelectStatement, dialect: Dialect) -> ReadySetResult<()> {
    let mut inliner = Inliner {
        dialect,
        depth: 0,
        scope: Vec::new(),
        lists: Vec::new(),
        hidden: Vec::new(),
    };
    inliner.visit_select_statement(statement)
}

/// The deepest chain of entries the pass will inline.
///
/// A chain where each entry reads the one before it is flat where it is written and nests once
/// per link when inlined, so the written shape bounds nothing.  The parser admits derived tables
/// nested about this far -- `sqlparser`'s depth budget is 50 and a nesting costs two -- so
/// refusing past it keeps the pass from handing any later pass a shape the parser could not have
/// produced, and keeps this walk from running the stack out on the way there.
const MAX_CHAIN_DEPTH: usize = 24;

/// Substitutes each entry's body at the one reference that reads it.
///
/// A body is taken rather than copied, so the slot empties as the walk proceeds and a second
/// reference finds it already taken.  A body is walked only when it is taken, which is what keeps
/// an entry nothing reads from consuming the entries its own body names.
///
/// Every entry in scope sits in one list in the order it was declared, innermost statement last.
/// That order is the visibility rule: what a body can see is what stands before it, which is the
/// prefix of the list ending where the body itself is bound.
#[derive(Debug)]
struct Inliner {
    /// Whose scoping the statement is resolved under.
    dialect: Dialect,
    /// Each entry in scope under the name it was defined by, holding its body until the one
    /// reference that reads it takes it.  An entry whose body is gone has already been read.
    scope: Vec<(SqlIdentifier, Option<Box<SelectStatement>>)>,
    /// Where each statement being walked begins its own `WITH` list, outermost first.
    lists: Vec<usize>,
    /// How many bodies are being walked, so a chain cannot nest past what the rest of the
    /// pipeline has been given before.
    depth: usize,
    /// What each body currently being walked cannot reach, innermost walk last.  An entry is
    /// visible only where no frame hides it, so a body walked inside another keeps the outer
    /// body's limits as well as its own.
    hidden: Vec<Range<usize>>,
}

impl Inliner {
    /// Where the `WITH` list binding the entry at `index` ends.
    fn list_end(&self, index: usize) -> usize {
        self.lists
            .iter()
            .copied()
            .find(|start| *start > index)
            .unwrap_or(self.scope.len())
    }

    /// Whether an entry bound as `bound` answers to `name`.
    ///
    /// MySQL reads an entry name without regard to case.  PostgreSQL folds an unquoted name as
    /// it parses and leaves a quoted one as written, so by here it compares as it stands.
    fn answers_to(&self, bound: &SqlIdentifier, name: &SqlIdentifier) -> bool {
        match self.dialect {
            Dialect::MySQL => bound.as_str().eq_ignore_ascii_case(name.as_str()),
            Dialect::PostgreSQL => bound == name,
        }
    }

    /// Where `name` is bound, among the entries the body being walked can reach.
    ///
    /// The last match wins: an entry of that name bound closer to the reference shadows one
    /// bound further out.
    fn find(&self, name: &SqlIdentifier) -> Option<usize> {
        self.scope
            .iter()
            .enumerate()
            .rev()
            .find(|(index, (bound, _))| {
                self.answers_to(bound, name)
                    && !self.hidden.iter().any(|frame| frame.contains(index))
            })
            .map(|(index, _)| index)
    }

    /// Walks the body bound at `index` against the entries its dialect lets it reach.
    ///
    /// PostgreSQL resolves a body where it is written, so nothing standing after it is in reach.
    /// MySQL resolves it where it is read, so only the rest of its own list is out of reach --
    /// no dialect lets a body read forward within the list that binds it.  Either way a `WITH`
    /// written inside the body is bound past the limit and stays readable there.
    fn walk_body(&mut self, body: &mut SelectStatement, index: usize) -> ReadySetResult<()> {
        if self.depth == MAX_CHAIN_DEPTH {
            unsupported!(
                "common table expressions are nested more than {MAX_CHAIN_DEPTH} deep once \
                 each is read at the reference that reads it"
            )
        }
        self.depth += 1;
        self.hidden.push(match self.dialect {
            Dialect::PostgreSQL => index..self.scope.len(),
            Dialect::MySQL => index..self.list_end(index),
        });
        let walked = self.visit_select_statement(body);
        self.hidden.pop();
        self.depth -= 1;
        walked
    }

    /// The body `name` reads, taken out of the scope that binds it and walked in its own right.
    ///
    /// Refuses the statement where the body has already been taken: that reference is the second
    /// to read the entry, and substituting at both copies the body into the statement twice.
    fn take(&mut self, name: &SqlIdentifier) -> ReadySetResult<Option<Box<SelectStatement>>> {
        let Some(index) = self.find(name) else {
            return Ok(None);
        };
        let Some(mut body) = self.scope[index].1.take() else {
            unsupported!(
                "common table expression `{name}` is read more than once, and inlining copies \
                 its body into the statement once per read"
            )
        };
        self.walk_body(&mut body, index)?;
        Ok(Some(body))
    }
}

impl<'ast> VisitorMut<'ast> for Inliner {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        let outer = self.scope.len();
        self.lists.push(outer);

        // Taking the list first is what keeps the walk below from reaching the bodies a second
        // time, and it is also what clears the clause the pipeline refuses to see.
        let entries = std::mem::take(&mut statement.ctes);
        self.scope.extend(
            entries
                .into_iter()
                .map(|cte| (cte.name, Some(Box::new(cte.statement)))),
        );

        let walked = visit_mut::walk_select_statement(self, statement);
        self.scope.truncate(outer);
        self.lists.pop();
        walked
    }

    fn visit_table_expr(&mut self, table_expr: &'ast mut TableExpr) -> Result<(), Self::Error> {
        // A `WITH` entry is never schema-qualified, so a qualified relation cannot name one.
        let name = match &table_expr.inner {
            TableExprInner::Table(relation) if relation.schema.is_none() => relation.name.clone(),
            _ => return visit_mut::walk_table_expr(self, table_expr),
        };
        let Some(body) = self.take(&name)? else {
            return visit_mut::walk_table_expr(self, table_expr);
        };

        // The entry's own name becomes the derived table's alias, which is what leaves the columns
        // it projects resolving without rewriting a single reference to them.
        if table_expr.alias.is_none() {
            table_expr.alias = Some(name);
        }
        table_expr.inner = TableExprInner::Subquery(body);
        // The body was walked as it was taken, against the scopes it was written under, so it is
        // resolved already and must not be walked a second time here.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::{Dialect, DialectDisplay};

    use crate::util::parse_select_statement;
    use readyset_sql_parsing::parse_select;

    /// Inlines `sql`, giving the rewritten statement or the refusal.
    fn inlined_as(dialect: Dialect, sql: &str) -> Result<SelectStatement, String> {
        let mut statement =
            parse_select(dialect, sql).unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"));
        match inline_ctes(&mut statement, dialect) {
            Ok(()) => Ok(statement),
            Err(error) => Err(error.to_string()),
        }
    }

    #[track_caller]
    fn assert_inlined_as(dialect: Dialect, sql: &str, expected: &str) {
        let statement = inlined_as(dialect, sql).expect("the statement should inline");
        let expected = parse_select_statement(expected);
        assert_eq!(
            statement,
            expected,
            "got: {}\nexpected: {}",
            statement.display(Dialect::MySQL),
            expected.display(Dialect::MySQL)
        );
    }

    fn inlined(sql: &str) -> Result<SelectStatement, String> {
        let mut statement = parse_select_statement(sql);
        match inline_ctes(&mut statement, Dialect::PostgreSQL) {
            Ok(()) => Ok(statement),
            Err(error) => Err(error.to_string()),
        }
    }

    /// Inlines `sql` and checks the result against `expected`, parsed the same way so the check
    /// is on the statement rather than on how either is written.
    #[track_caller]
    fn assert_inlined(sql: &str, expected: &str) {
        let statement = inlined(sql).expect("the statement should inline");
        let expected = parse_select_statement(expected);
        assert_eq!(
            statement,
            expected,
            "got: {}\nexpected: {}",
            statement.display(Dialect::MySQL),
            expected.display(Dialect::MySQL)
        );
    }

    /// A reference may name the entry under an alias of its own.  The derived table takes that
    /// alias, not the entry's name, or every column reference through it dangles.
    #[test]
    fn an_entry_read_under_another_alias_keeps_that_alias() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t) SELECT l.x FROM a AS l",
            "SELECT l.x FROM (SELECT t.x FROM t) AS l",
        );
    }

    /// A `WITH` entry is never schema-qualified, so a qualified relation names a base table even
    /// where an entry answers to the same bare name.  The entry goes unread and is dropped.
    #[test]
    fn a_schema_qualified_reference_reads_the_base_table() {
        assert_inlined_as(
            Dialect::PostgreSQL,
            "WITH a AS (SELECT t.x FROM t) SELECT a.x FROM public.a",
            "SELECT a.x FROM public.a",
        );
    }

    /// PostgreSQL folds an unquoted name as it parses and leaves a quoted one as written, so a
    /// quoted entry is not read by a reference in another case.  MySQL's twin is the opposite.
    #[test]
    fn a_postgresql_quoted_entry_is_not_read_in_another_case() {
        assert_inlined_as(
            Dialect::PostgreSQL,
            r#"WITH "Orders" AS (SELECT t.x FROM t) SELECT orders.x FROM orders"#,
            "SELECT orders.x FROM orders",
        );
    }

    /// A chain of entries each reading the one before is flat where it is written, and nesting
    /// once inlined.  Nothing else in the pipeline can produce a statement deeper than the parser
    /// accepts, so the chain is refused past that depth rather than handed on as a shape no
    /// consumer has ever been given -- and rather than recursing until the stack runs out.
    #[test]
    fn a_chain_deeper_than_the_parser_accepts_is_refused() {
        let n = 400;
        let mut sql = String::from("WITH c0 AS (SELECT t.x FROM t)");
        for i in 1..n {
            sql.push_str(&format!(", c{i} AS (SELECT c{}.x FROM c{})", i - 1, i - 1));
        }
        sql.push_str(&format!(" SELECT c{}.x FROM c{}", n - 1, n - 1));

        let error = inlined(&sql).expect_err("a chain this deep should be refused");
        assert!(error.contains("nested"), "got: {error}");
    }

    /// PostgreSQL resolves a body where it is written, so a name bound again between where the
    /// body is written and where it is read keeps its written meaning.
    #[test]
    fn postgresql_resolves_a_rebound_name_where_the_body_is_written() {
        assert_inlined_as(
            Dialect::PostgreSQL,
            "WITH b AS (SELECT t.x FROM t), a AS (SELECT b.x FROM b) \
             SELECT s.x FROM (WITH b AS (SELECT u.x FROM u) SELECT a.x FROM a) AS s",
            "SELECT s.x FROM (SELECT a.x FROM (SELECT b.x FROM (SELECT t.x FROM t) AS b) AS a) AS s",
        );
    }

    /// MySQL resolves it where it is read.  Measured on MySQL 8.0.42: the statement gives the
    /// rows of `u`.
    #[test]
    fn mysql_resolves_a_rebound_name_where_the_body_is_read() {
        assert_inlined_as(
            Dialect::MySQL,
            "WITH b AS (SELECT t.x FROM t), a AS (SELECT b.x FROM b) \
             SELECT s.x FROM (WITH b AS (SELECT u.x FROM u) SELECT a.x FROM a) AS s",
            "SELECT s.x FROM (SELECT a.x FROM (SELECT b.x FROM (SELECT u.x FROM u) AS b) AS a) AS s",
        );
    }

    /// A later sibling of an entry's own list is a base table inside that entry's body, at any
    /// depth and under either dialect: no `WITH` list is visible to the entries before it.
    #[test]
    fn a_nested_body_does_not_see_its_outer_entrys_later_siblings() {
        for dialect in [Dialect::PostgreSQL, Dialect::MySQL] {
            assert_inlined_as(
                dialect,
                "WITH b AS (SELECT s.x FROM (WITH i AS (SELECT c.x FROM c) SELECT i.x FROM i) AS s), \
                 c AS (SELECT u.x FROM u) SELECT b.x FROM b",
                "SELECT b.x FROM (SELECT s.x FROM \
                 (SELECT i.x FROM (SELECT c.x FROM c) AS i) AS s) AS b",
            );
        }
    }

    /// MySQL common table expression names are case insensitive, so a reference written in
    /// another case reads the entry rather than a relation that answers to the same name.
    #[test]
    fn a_mysql_reference_in_another_case_reads_the_entry() {
        assert_inlined_as(
            Dialect::MySQL,
            "WITH Orders AS (SELECT t.x FROM t) SELECT orders.x FROM orders",
            "SELECT orders.x FROM (SELECT t.x FROM t) AS orders",
        );
    }

    #[test]
    fn a_single_reference_becomes_a_derived_table_under_its_own_name() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t) SELECT a.x FROM a",
            "SELECT a.x FROM (SELECT t.x FROM t) AS a",
        );
    }

    #[test]
    fn an_unreferenced_entry_is_dropped() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t) SELECT u.x FROM u",
            "SELECT u.x FROM u",
        );
    }

    #[test]
    fn a_second_reference_is_refused() {
        let error = inlined(
            "WITH a AS (SELECT t.x FROM t) SELECT l.x FROM a AS l JOIN a AS r ON l.x = r.x",
        )
        .unwrap_err();
        assert!(
            error.contains("is read more than once"),
            "expected the refusal to name the second read, got: {error}"
        );
    }

    /// Two sibling bodies reading one entry is the same refusal, though the outer statement names
    /// it nowhere.  The counterpart, where only one of the two bodies is read, is
    /// `a_statement_whose_only_second_read_is_dead_inlines`.
    #[test]
    fn a_second_reference_from_a_sibling_body_is_refused() {
        let error = inlined(
            "WITH a AS (SELECT t.x FROM t), \
                  b AS (SELECT a.x FROM a), \
                  c AS (SELECT a.x FROM a) \
             SELECT b.x FROM b JOIN c ON b.x = c.x",
        )
        .unwrap_err();
        assert!(error.contains("is read more than once"), "got: {error}");
    }

    /// A chain collapses innermost-first, so the inner body sits inside the outer one.
    #[test]
    fn a_chain_nests_innermost_first() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t), \
                  b AS (SELECT a.x FROM a) \
             SELECT b.x FROM b",
            "SELECT b.x FROM (SELECT a.x FROM (SELECT t.x FROM t) AS a) AS b",
        );
    }

    #[test]
    fn a_nested_entry_is_inlined_in_the_body_that_defines_it() {
        assert_inlined(
            "SELECT s.x FROM (WITH a AS (SELECT t.x FROM t) SELECT a.x FROM a) AS s",
            "SELECT s.x FROM (SELECT a.x FROM (SELECT t.x FROM t) AS a) AS s",
        );
    }

    /// A body naming a *later* sibling is not reading it: a `WITH` list makes only earlier
    /// siblings visible, and PostgreSQL rejects the forward reference outright.  The name has to
    /// survive inlining as the plain relation it was read as, rather than picking up the sibling's
    /// body once that sibling is bound.
    #[test]
    fn a_forward_reference_to_a_later_sibling_is_left_alone() {
        assert_inlined(
            "WITH a AS (SELECT b.x FROM b), \
                  b AS (SELECT u.x FROM u) \
             SELECT a.x FROM a",
            "SELECT a.x FROM (SELECT b.x FROM b) AS a",
        );
    }

    #[test]
    fn an_entry_read_from_a_with_nested_in_a_derived_table_is_inlined() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t) \
             SELECT s.x FROM (WITH i AS (SELECT a.x FROM a) SELECT i.x FROM i) AS s",
            "SELECT s.x FROM (SELECT i.x FROM (SELECT a.x FROM (SELECT t.x FROM t) AS a) AS i) AS s",
        );
    }

    #[test]
    fn an_entry_a_body_defines_for_itself_is_inlined() {
        assert_inlined(
            "WITH a AS (WITH i AS (SELECT t.x FROM t) SELECT i.x FROM i) SELECT a.x FROM a",
            "SELECT a.x FROM (SELECT i.x FROM (SELECT t.x FROM t) AS i) AS a",
        );
    }

    /// A reference inside a subquery is the one reference, and inlining reaches it there.
    #[test]
    fn a_reference_inside_a_subquery_is_inlined() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t) SELECT u.x FROM u WHERE u.x IN (SELECT a.x FROM a)",
            "SELECT u.x FROM u WHERE u.x IN (SELECT a.x FROM (SELECT t.x FROM t) AS a)",
        );
    }

    /// `c` is never read, so its body is never walked and never takes `a`, leaving `a` read once
    /// and inlinable.  The counterpart, where `c` is read too, is
    /// `a_second_reference_from_a_sibling_body_is_refused`.
    #[test]
    fn a_statement_whose_only_second_read_is_dead_inlines() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t), b AS (SELECT a.x FROM a), \
                  c AS (SELECT a.x FROM a) \
             SELECT b.x FROM b",
            "SELECT b.x FROM (SELECT a.x FROM (SELECT t.x FROM t) AS a) AS b",
        );
    }

    /// A `WITH` nested inside a body sees what that body sees, so a sibling declared after it is
    /// a base table down there too, however deep the nesting goes.
    #[test]
    fn a_later_sibling_named_inside_a_nested_body_reads_the_base_table() {
        assert_inlined(
            "WITH b AS (SELECT s.x FROM (WITH i AS (SELECT c.x FROM c) SELECT i.x FROM i) AS s), \
                  c AS (SELECT u.x FROM u) \
             SELECT b.x FROM b",
            "SELECT b.x FROM (SELECT s.x FROM (SELECT i.x FROM (SELECT c.x FROM c) AS i) AS s) AS b",
        );
    }

    /// The same for the name the body is itself bound to.
    #[test]
    fn an_entrys_own_name_inside_a_nested_body_reads_the_base_table() {
        assert_inlined(
            "WITH b AS (SELECT s.x FROM (WITH i AS (SELECT b.x FROM b) SELECT i.x FROM i) AS s) \
             SELECT b.x FROM b",
            "SELECT b.x FROM (SELECT s.x FROM (SELECT i.x FROM (SELECT b.x FROM b) AS i) AS s) AS b",
        );
    }

    /// A body nested in a body resolves where it was written: the inner `WITH` binds `b` after
    /// `a`, so `a`'s body still reads the `b` of the statement above.
    #[test]
    fn a_body_nested_in_a_body_keeps_the_outer_sibling_rule() {
        assert_inlined(
            "WITH b AS (SELECT v.x FROM v) \
             SELECT s.x FROM (WITH a AS (WITH i AS (SELECT b.x FROM b) SELECT i.x FROM i), \
                                   b AS (SELECT u.x FROM u) \
                              SELECT a.x FROM a) AS s",
            "SELECT s.x FROM (SELECT a.x FROM (SELECT i.x FROM \
             (SELECT b.x FROM (SELECT v.x FROM v) AS b) AS i) AS a) AS s",
        );
    }

    /// An entry nothing reads is dropped before anything is bound.  Binding its body first would
    /// let it take the body of the entry it names, which the statement itself still reads.
    #[test]
    fn an_unread_entry_declared_first_leaves_the_read_one_intact() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t), c AS (SELECT a.x FROM a) SELECT a.x FROM a",
            "SELECT a.x FROM (SELECT t.x FROM t) AS a",
        );
    }

    /// The same, with the reader a later sibling rather than the statement.
    #[test]
    fn an_unread_entry_declared_between_two_read_ones_leaves_them_intact() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t), c AS (SELECT a.x FROM a), \
                  b AS (SELECT a.x FROM a) \
             SELECT b.x FROM b",
            "SELECT b.x FROM (SELECT a.x FROM (SELECT t.x FROM t) AS a) AS b",
        );
    }

    /// The unread body reaches the entry through a derived table of its own.
    #[test]
    fn an_unread_entry_reading_through_a_derived_table_leaves_the_read_one_intact() {
        assert_inlined(
            "WITH a AS (SELECT t.x FROM t), c AS (SELECT d.x FROM (SELECT a.x FROM a) AS d) \
             SELECT a.x FROM a",
            "SELECT a.x FROM (SELECT t.x FROM t) AS a",
        );
    }

    /// Bind order is per level, so an unread entry nested below the top level prunes there too.
    #[test]
    fn an_unread_entry_below_the_top_level_leaves_the_read_one_intact() {
        assert_inlined(
            "SELECT s.x FROM (WITH a AS (SELECT t.x FROM t), c AS (SELECT a.x FROM a) \
                              SELECT a.x FROM a) AS s",
            "SELECT s.x FROM (SELECT a.x FROM (SELECT t.x FROM t) AS a) AS s",
        );
    }
}
