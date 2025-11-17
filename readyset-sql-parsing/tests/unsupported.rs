//! Tests for queries that should be rejected as unsupported.

use readyset_sql::Dialect;
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

mod utils;

#[test]
fn postgres_like_escape_char() {
    for query in &[
        "SELECT * FROM users WHERE name LIKE 'John#_Doe' ESCAPE '#'",
        "SELECT * FROM users WHERE name ILIKE 'John!%' ESCAPE '!'",
        "SELECT * FROM products WHERE sku NOT LIKE '!%OFF' ESCAPE '!'",
        "SELECT * FROM products WHERE name NOT ILIKE 'test#%' ESCAPE '#'",
        "SELECT * FROM paths WHERE path LIKE 'C:\\Users\\%' ESCAPE ''",
        "SELECT * FROM users WHERE name LIKE ANY(ARRAY['test#%', 'demo#_']) ESCAPE '#'",
        "SELECT * FROM users WHERE name ILIKE ANY(ARRAY['test!%']) ESCAPE '!'",
        // Various escape characters
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '!'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '#'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '@'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '$'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '^'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '&'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '*'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '~'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '|'",
    ] {
        check_parse_fails!(
            Dialect::PostgreSQL,
            query,
            "LIKE/ILIKE with custom ESCAPE character"
        );
    }
}

#[test]
fn mysql_like_escape_char() {
    for query in &[
        "SELECT * FROM users WHERE name LIKE 'John#_Doe' ESCAPE '#'",
        "SELECT * FROM products WHERE sku NOT LIKE '!%OFF' ESCAPE '!'",
        "SELECT * FROM paths WHERE path LIKE 'C:\\Users\\%' ESCAPE ''",
        // Various escape characters
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '!'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '#'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '@'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '$'",
    ] {
        check_parse_fails!(
            Dialect::MySQL,
            query,
            "LIKE/ILIKE with custom ESCAPE character"
        );
    }
}
