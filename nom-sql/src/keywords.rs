use common::eof;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::peek;
use nom::sequence::terminated;
use nom::IResult;

// NOTE: Each keyword_$start_letter_to_$end_letter function uses `alt`,
// which is implemented for tuples sizes up to 21. Because of this constraint
// on maximum tuple sizes, keywords are aggregated into groups of 20

fn keyword_follow_char(i: &[u8]) -> IResult<&[u8], &[u8]> {
    peek(alt((
        tag(" "),
        tag("\n"),
        tag(";"),
        tag("("),
        tag(")"),
        tag("\t"),
        tag(","),
        tag("="),
        eof,
    )))(i)
}

fn keyword_a_to_c(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("ABORT"), keyword_follow_char),
        terminated(tag_no_case("ACTION"), keyword_follow_char),
        terminated(tag_no_case("ADD"), keyword_follow_char),
        terminated(tag_no_case("AFTER"), keyword_follow_char),
        terminated(tag_no_case("ALL"), keyword_follow_char),
        terminated(tag_no_case("ALTER"), keyword_follow_char),
        terminated(tag_no_case("ANALYZE"), keyword_follow_char),
        terminated(tag_no_case("AND"), keyword_follow_char),
        terminated(tag_no_case("AS"), keyword_follow_char),
        terminated(tag_no_case("ASC"), keyword_follow_char),
        terminated(tag_no_case("ATTACH"), keyword_follow_char),
        terminated(tag_no_case("AUTOINCREMENT"), keyword_follow_char),
        terminated(tag_no_case("BEFORE"), keyword_follow_char),
        terminated(tag_no_case("BEGIN"), keyword_follow_char),
        terminated(tag_no_case("BETWEEN"), keyword_follow_char),
        terminated(tag_no_case("BY"), keyword_follow_char),
        terminated(tag_no_case("CASCADE"), keyword_follow_char),
        terminated(tag_no_case("CASE"), keyword_follow_char),
        terminated(tag_no_case("CAST"), keyword_follow_char),
        terminated(tag_no_case("CHECK"), keyword_follow_char),
        terminated(tag_no_case("COLLATE"), keyword_follow_char),
    ))(i)
}

fn keyword_c_to_e(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("COLUMN"), keyword_follow_char),
        terminated(tag_no_case("COMMIT"), keyword_follow_char),
        terminated(tag_no_case("CONFLICT"), keyword_follow_char),
        terminated(tag_no_case("CONSTRAINT"), keyword_follow_char),
        terminated(tag_no_case("CREATE"), keyword_follow_char),
        terminated(tag_no_case("CROSS"), keyword_follow_char),
        terminated(tag_no_case("CURRENT_DATE"), keyword_follow_char),
        terminated(tag_no_case("CURRENT_TIME"), keyword_follow_char),
        terminated(tag_no_case("CURRENT_TIMESTAMP"), keyword_follow_char),
        terminated(tag_no_case("DATABASE"), keyword_follow_char),
        terminated(tag_no_case("DEFAULT"), keyword_follow_char),
        terminated(tag_no_case("DEFERRABLE"), keyword_follow_char),
        terminated(tag_no_case("DEFERRED"), keyword_follow_char),
        terminated(tag_no_case("DELETE"), keyword_follow_char),
        terminated(tag_no_case("DESC"), keyword_follow_char),
        terminated(tag_no_case("DETACH"), keyword_follow_char),
        terminated(tag_no_case("DISTINCT"), keyword_follow_char),
        terminated(tag_no_case("DROP"), keyword_follow_char),
        terminated(tag_no_case("EACH"), keyword_follow_char),
        terminated(tag_no_case("ELSE"), keyword_follow_char),
        terminated(tag_no_case("END"), keyword_follow_char),
    ))(i)
}

fn keyword_e_to_i(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("ESCAPE"), keyword_follow_char),
        terminated(tag_no_case("EXCEPT"), keyword_follow_char),
        terminated(tag_no_case("EXCLUSIVE"), keyword_follow_char),
        terminated(tag_no_case("EXISTS"), keyword_follow_char),
        terminated(tag_no_case("EXPLAIN"), keyword_follow_char),
        terminated(tag_no_case("FAIL"), keyword_follow_char),
        terminated(tag_no_case("FOR"), keyword_follow_char),
        terminated(tag_no_case("FOREIGN"), keyword_follow_char),
        terminated(tag_no_case("FROM"), keyword_follow_char),
        terminated(tag_no_case("FULL"), keyword_follow_char),
        terminated(tag_no_case("FULLTEXT"), keyword_follow_char),
        terminated(tag_no_case("GLOB"), keyword_follow_char),
        terminated(tag_no_case("GROUP"), keyword_follow_char),
        terminated(tag_no_case("HAVING"), keyword_follow_char),
        terminated(tag_no_case("IF"), keyword_follow_char),
        terminated(tag_no_case("IGNORE"), keyword_follow_char),
        terminated(tag_no_case("IMMEDIATE"), keyword_follow_char),
        terminated(tag_no_case("IN"), keyword_follow_char),
        terminated(tag_no_case("INDEX"), keyword_follow_char),
        terminated(tag_no_case("INDEXED"), keyword_follow_char),
        terminated(tag_no_case("INITIALLY"), keyword_follow_char),
    ))(i)
}

fn keyword_i_to_o(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("INNER"), keyword_follow_char),
        terminated(tag_no_case("INSERT"), keyword_follow_char),
        terminated(tag_no_case("INSTEAD"), keyword_follow_char),
        terminated(tag_no_case("INTERSECT"), keyword_follow_char),
        terminated(tag_no_case("INTO"), keyword_follow_char),
        terminated(tag_no_case("IS"), keyword_follow_char),
        terminated(tag_no_case("ISNULL"), keyword_follow_char),
        terminated(tag_no_case("ORDER"), keyword_follow_char),
        terminated(tag_no_case("JOIN"), keyword_follow_char),
        terminated(tag_no_case("KEY"), keyword_follow_char),
        terminated(tag_no_case("LEFT"), keyword_follow_char),
        terminated(tag_no_case("LIKE"), keyword_follow_char),
        terminated(tag_no_case("LIMIT"), keyword_follow_char),
        terminated(tag_no_case("MATCH"), keyword_follow_char),
        terminated(tag_no_case("NATURAL"), keyword_follow_char),
        terminated(tag_no_case("NO"), keyword_follow_char),
        terminated(tag_no_case("NOT"), keyword_follow_char),
        terminated(tag_no_case("NOTNULL"), keyword_follow_char),
        terminated(tag_no_case("NULL"), keyword_follow_char),
        terminated(tag_no_case("OF"), keyword_follow_char),
        terminated(tag_no_case("OFFSET"), keyword_follow_char),
    ))(i)
}

fn keyword_o_to_s(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("ON"), keyword_follow_char),
        terminated(tag_no_case("OR"), keyword_follow_char),
        terminated(tag_no_case("OUTER"), keyword_follow_char),
        terminated(tag_no_case("PLAN"), keyword_follow_char),
        terminated(tag_no_case("PRAGMA"), keyword_follow_char),
        terminated(tag_no_case("PRIMARY"), keyword_follow_char),
        terminated(tag_no_case("QUERY"), keyword_follow_char),
        terminated(tag_no_case("RAISE"), keyword_follow_char),
        terminated(tag_no_case("RECURSIVE"), keyword_follow_char),
        terminated(tag_no_case("REFERENCES"), keyword_follow_char),
        terminated(tag_no_case("REGEXP"), keyword_follow_char),
        terminated(tag_no_case("REINDEX"), keyword_follow_char),
        terminated(tag_no_case("RELEASE"), keyword_follow_char),
        terminated(tag_no_case("RENAME"), keyword_follow_char),
        terminated(tag_no_case("REPLACE"), keyword_follow_char),
        terminated(tag_no_case("RESTRICT"), keyword_follow_char),
        terminated(tag_no_case("RIGHT"), keyword_follow_char),
        terminated(tag_no_case("ROLLBACK"), keyword_follow_char),
        terminated(tag_no_case("ROW"), keyword_follow_char),
        terminated(tag_no_case("SAVEPOINT"), keyword_follow_char),
        terminated(tag_no_case("SELECT"), keyword_follow_char),
    ))(i)
}

fn keyword_s_to_z(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("SET"), keyword_follow_char),
        terminated(tag_no_case("TABLE"), keyword_follow_char),
        terminated(tag_no_case("TEMP"), keyword_follow_char),
        terminated(tag_no_case("TEMPORARY"), keyword_follow_char),
        terminated(tag_no_case("THEN"), keyword_follow_char),
        terminated(tag_no_case("TO"), keyword_follow_char),
        terminated(tag_no_case("TRANSACTION"), keyword_follow_char),
        terminated(tag_no_case("TRIGGER"), keyword_follow_char),
        terminated(tag_no_case("UNION"), keyword_follow_char),
        terminated(tag_no_case("UNIQUE"), keyword_follow_char),
        terminated(tag_no_case("UPDATE"), keyword_follow_char),
        terminated(tag_no_case("USING"), keyword_follow_char),
        terminated(tag_no_case("VACUUM"), keyword_follow_char),
        terminated(tag_no_case("VALUES"), keyword_follow_char),
        terminated(tag_no_case("VIEW"), keyword_follow_char),
        terminated(tag_no_case("VIRTUAL"), keyword_follow_char),
        terminated(tag_no_case("WHEN"), keyword_follow_char),
        terminated(tag_no_case("WHERE"), keyword_follow_char),
        terminated(tag_no_case("WITH"), keyword_follow_char),
        terminated(tag_no_case("WITHOUT"), keyword_follow_char),
    ))(i)
}

// Matches any SQL reserved keyword
pub fn sql_keyword(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        keyword_a_to_c,
        keyword_c_to_e,
        keyword_e_to_i,
        keyword_i_to_o,
        keyword_o_to_s,
        keyword_s_to_z,
    ))(i)
}

pub fn escape_if_keyword(s: &str) -> String {
    if sql_keyword(s.as_bytes()).is_ok() {
        format!("`{}`", s)
    } else {
        s.to_owned()
    }
}
