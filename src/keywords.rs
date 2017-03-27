use nom::{IResult, Err, ErrorKind, Needed};

use std::str;

named!(keyword_follow_char<&[u8], char>,
       peek!(one_of!(" \n;(\t,="))
);

named!(keyword_a_to_c<&[u8], &[u8]>,
       alt_complete!(
          terminated!(caseless_tag!("ABORT"), keyword_follow_char)
        | terminated!(caseless_tag!("ACTION"), keyword_follow_char)
        | terminated!(caseless_tag!("ADD"), keyword_follow_char)
        | terminated!(caseless_tag!("AFTER"), keyword_follow_char)
        | terminated!(caseless_tag!("ALL"), keyword_follow_char)
        | terminated!(caseless_tag!("ALTER"), keyword_follow_char)
        | terminated!(caseless_tag!("ANALYZE"), keyword_follow_char)
        | terminated!(caseless_tag!("AND"), keyword_follow_char)
        | terminated!(caseless_tag!("AS"), keyword_follow_char)
        | terminated!(caseless_tag!("ASC"), keyword_follow_char)
        | terminated!(caseless_tag!("ATTACH"), keyword_follow_char)
        | terminated!(caseless_tag!("AUTOINCREMENT"), keyword_follow_char)
        | terminated!(caseless_tag!("BEFORE"), keyword_follow_char)
        | terminated!(caseless_tag!("BEGIN"), keyword_follow_char)
        | terminated!(caseless_tag!("BETWEEN"), keyword_follow_char)
        | terminated!(caseless_tag!("BY"), keyword_follow_char)
        | terminated!(caseless_tag!("CASCADE"), keyword_follow_char)
        | terminated!(caseless_tag!("CASE"), keyword_follow_char)
        | terminated!(caseless_tag!("CAST"), keyword_follow_char)
        | terminated!(caseless_tag!("CHECK"), keyword_follow_char)
        | terminated!(caseless_tag!("COLLATE"), keyword_follow_char)
        | terminated!(caseless_tag!("COLUMN"), keyword_follow_char)
        | terminated!(caseless_tag!("COMMIT"), keyword_follow_char)
        | terminated!(caseless_tag!("CONFLICT"), keyword_follow_char)
        | terminated!(caseless_tag!("CONSTRAINT"), keyword_follow_char)
        | terminated!(caseless_tag!("CREATE"), keyword_follow_char)
        | terminated!(caseless_tag!("CROSS"), keyword_follow_char)
        | terminated!(caseless_tag!("CURRENT_DATE"), keyword_follow_char)
        | terminated!(caseless_tag!("CURRENT_TIME"), keyword_follow_char)
        | terminated!(caseless_tag!("CURRENT_TIMESTAMP"), keyword_follow_char)
    )
);

named!(keyword_d_to_i<&[u8], &[u8]>,
    alt_complete!(
          terminated!(caseless_tag!("DATABASE"), keyword_follow_char)
        | terminated!(caseless_tag!("DEFAULT"), keyword_follow_char)
        | terminated!(caseless_tag!("DEFERRABLE"), keyword_follow_char)
        | terminated!(caseless_tag!("DEFERRED"), keyword_follow_char)
        | terminated!(caseless_tag!("DELETE"), keyword_follow_char)
        | terminated!(caseless_tag!("DESC"), keyword_follow_char)
        | terminated!(caseless_tag!("DETACH"), keyword_follow_char)
        | terminated!(caseless_tag!("DISTINCT"), keyword_follow_char)
        | terminated!(caseless_tag!("DROP"), keyword_follow_char)
        | terminated!(caseless_tag!("EACH"), keyword_follow_char)
        | terminated!(caseless_tag!("ELSE"), keyword_follow_char)
        | terminated!(caseless_tag!("END"), keyword_follow_char)
        | terminated!(caseless_tag!("ESCAPE"), keyword_follow_char)
        | terminated!(caseless_tag!("EXCEPT"), keyword_follow_char)
        | terminated!(caseless_tag!("EXCLUSIVE"), keyword_follow_char)
        | terminated!(caseless_tag!("EXISTS"), keyword_follow_char)
        | terminated!(caseless_tag!("EXPLAIN"), keyword_follow_char)
        | terminated!(caseless_tag!("FAIL"), keyword_follow_char)
        | terminated!(caseless_tag!("FOR"), keyword_follow_char)
        | terminated!(caseless_tag!("FOREIGN"), keyword_follow_char)
        | terminated!(caseless_tag!("FROM"), keyword_follow_char)
        | terminated!(caseless_tag!("FULL"), keyword_follow_char)
        | terminated!(caseless_tag!("GLOB"), keyword_follow_char)
        | terminated!(caseless_tag!("GROUP"), keyword_follow_char)
        | terminated!(caseless_tag!("HAVING"), keyword_follow_char)
        | terminated!(caseless_tag!("IF"), keyword_follow_char)
        | terminated!(caseless_tag!("IGNORE"), keyword_follow_char)
        | terminated!(caseless_tag!("IMMEDIATE"), keyword_follow_char)
        | terminated!(caseless_tag!("IN"), keyword_follow_char)
        | terminated!(caseless_tag!("INDEX"), keyword_follow_char)
        | terminated!(caseless_tag!("INDEXED"), keyword_follow_char)
        | terminated!(caseless_tag!("INITIALLY"), keyword_follow_char)
        | terminated!(caseless_tag!("INNER"), keyword_follow_char)
        | terminated!(caseless_tag!("INSERT"), keyword_follow_char)
        | terminated!(caseless_tag!("INSTEAD"), keyword_follow_char)
        | terminated!(caseless_tag!("INTERSECT"), keyword_follow_char)
        | terminated!(caseless_tag!("INTO"), keyword_follow_char)
        | terminated!(caseless_tag!("IS"), keyword_follow_char)
        | terminated!(caseless_tag!("ISNULL"), keyword_follow_char)
    )
);

named!(keyword_j_to_s<&[u8], &[u8]>,
    alt_complete!(
          terminated!(caseless_tag!("ORDER"), keyword_follow_char)
        | terminated!(caseless_tag!("JOIN"), keyword_follow_char)
        | terminated!(caseless_tag!("KEY"), keyword_follow_char)
        | terminated!(caseless_tag!("LEFT"), keyword_follow_char)
        | terminated!(caseless_tag!("LIKE"), keyword_follow_char)
        | terminated!(caseless_tag!("LIMIT"), keyword_follow_char)
        | terminated!(caseless_tag!("MATCH"), keyword_follow_char)
        | terminated!(caseless_tag!("NATURAL"), keyword_follow_char)
        | terminated!(caseless_tag!("NO"), keyword_follow_char)
        | terminated!(caseless_tag!("NOT"), keyword_follow_char)
        | terminated!(caseless_tag!("NOTNULL"), keyword_follow_char)
        | terminated!(caseless_tag!("NULL"), keyword_follow_char)
        | terminated!(caseless_tag!("OF"), keyword_follow_char)
        | terminated!(caseless_tag!("OFFSET"), keyword_follow_char)
        | terminated!(caseless_tag!("ON"), keyword_follow_char)
        | terminated!(caseless_tag!("OR"), keyword_follow_char)
        | terminated!(caseless_tag!("OUTER"), keyword_follow_char)
        | terminated!(caseless_tag!("PLAN"), keyword_follow_char)
        | terminated!(caseless_tag!("PRAGMA"), keyword_follow_char)
        | terminated!(caseless_tag!("PRIMARY"), keyword_follow_char)
        | terminated!(caseless_tag!("QUERY"), keyword_follow_char)
        | terminated!(caseless_tag!("RAISE"), keyword_follow_char)
        | terminated!(caseless_tag!("RECURSIVE"), keyword_follow_char)
        | terminated!(caseless_tag!("REFERENCES"), keyword_follow_char)
        | terminated!(caseless_tag!("REGEXP"), keyword_follow_char)
        | terminated!(caseless_tag!("REINDEX"), keyword_follow_char)
        | terminated!(caseless_tag!("RELEASE"), keyword_follow_char)
        | terminated!(caseless_tag!("RENAME"), keyword_follow_char)
        | terminated!(caseless_tag!("REPLACE"), keyword_follow_char)
        | terminated!(caseless_tag!("RESTRICT"), keyword_follow_char)
        | terminated!(caseless_tag!("RIGHT"), keyword_follow_char)
        | terminated!(caseless_tag!("ROLLBACK"), keyword_follow_char)
        | terminated!(caseless_tag!("ROW"), keyword_follow_char)
        | terminated!(caseless_tag!("SAVEPOINT"), keyword_follow_char)
        | terminated!(caseless_tag!("SELECT"), keyword_follow_char)
        | terminated!(caseless_tag!("SET"), keyword_follow_char)
    )
);

named!(keyword_t_to_z<&[u8], &[u8]>,
    alt_complete!(
          terminated!(caseless_tag!("TABLE"), keyword_follow_char)
        | terminated!(caseless_tag!("TEMP"), keyword_follow_char)
        | terminated!(caseless_tag!("TEMPORARY"), keyword_follow_char)
        | terminated!(caseless_tag!("THEN"), keyword_follow_char)
        | terminated!(caseless_tag!("TO"), keyword_follow_char)
        | terminated!(caseless_tag!("TRANSACTION"), keyword_follow_char)
        | terminated!(caseless_tag!("TRIGGER"), keyword_follow_char)
        | terminated!(caseless_tag!("UNION"), keyword_follow_char)
        | terminated!(caseless_tag!("UNIQUE"), keyword_follow_char)
        | terminated!(caseless_tag!("UPDATE"), keyword_follow_char)
        | terminated!(caseless_tag!("USING"), keyword_follow_char)
        | terminated!(caseless_tag!("VACUUM"), keyword_follow_char)
        | terminated!(caseless_tag!("VALUES"), keyword_follow_char)
        | terminated!(caseless_tag!("VIEW"), keyword_follow_char)
        | terminated!(caseless_tag!("VIRTUAL"), keyword_follow_char)
        | terminated!(caseless_tag!("WHEN"), keyword_follow_char)
        | terminated!(caseless_tag!("WHERE"), keyword_follow_char)
        | terminated!(caseless_tag!("WITH"), keyword_follow_char)
        | terminated!(caseless_tag!("WITHOUT"), keyword_follow_char)
    )
);

/// Matches any SQL reserved keyword
named!(pub sql_keyword<&[u8], &[u8]>,
    complete!(chain!(
        kw: alt_complete!(
              keyword_a_to_c
            | keyword_d_to_i
            | keyword_j_to_s
            | keyword_t_to_z),
        || { kw }
    ))
);
