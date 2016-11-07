use nom::{alphanumeric, digit, eof, line_ending, multispace, space};
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;
use std::str::FromStr;

/// Parse an unsigned integer.
named!(pub unsigned_number<&[u8], u64>,
    map_res!(
        map_res!(digit, str::from_utf8),
        FromStr::from_str
    )
);

/// Parse a terminator that ends a SQL statement.
named!(pub statement_terminator,
    delimited!(opt!(multispace),
               alt_complete!(tag!(";") | line_ending | eof),
               opt!(multispace)
    )
);

/// Parse binary comparison operators
named!(pub binary_comparison_operator<&[u8], &str>,
    dbg_dmp!(map_res!(
        dbg_dmp!(alt_complete!(  tag_s!(b"=")
             | tag_s!(b"<")
             | tag_s!(b">")
             | tag_s!(b"<>")
             | tag_s!(b">=")
             | tag_s!(b"<=")
             | tag_s!(b"LIKE")
             | tag_s!(b"NOT_LIKE")
        )),
        str::from_utf8
    )
));

/// Parse logical operators
named!(pub binary_logical_operator<&[u8], &str>,
    map_res!(
        alt_complete!(
               tag_s!(b"and")
             | tag_s!(b"or")
        ),
        str::from_utf8
    )
);

/// Parse unary comparison operators
named!(pub unary_comparison_operator<&[u8], &str>,
    map_res!(
        alt_complete!(
               tag_s!(b"NOT")
             | tag_s!(b"-") // ??? (number neg)
             | tag_s!(b"ISNULL")
        ),
        str::from_utf8
    )
);

/// Parse unary comparison operators
named!(pub unary_negation_operator<&[u8], &str>,
    map_res!(
        alt_complete!(
              tag_s!(b"not")
            | tag_s!(b"!")
        ),
        str::from_utf8
    )
);

/// Parse rule for a comma-separated list.
named!(pub csvlist<&[u8], Vec<&str> >,
       many0!(
           map_res!(
               chain!(
                   fieldname: alphanumeric ~
                   opt!(
                       chain!(
                           tag!(",") ~
                           multispace?,
                           ||{}
                       )
                   ),
                   ||{ fieldname }
               ),
               str::from_utf8
           )
       )
);

/// Parse list of columns/fields.
/// XXX(malte): add support for named table notation
named!(pub fieldlist<&[u8], Vec<&str> >,
       alt_complete!(
           tag!("*") => { |_| vec!["ALL".into()] }
         | csvlist
       )
);

/// Parse a list of values (e.g., for INSERT syntax).
/// XXX(malte): support non-placeholder values
named!(pub valuelist<&[u8], Vec<&str> >,
       many0!(
           map_res!(
               chain!(
                   val: alt_complete!(tag_s!(b"?") | alphanumeric) ~
                   opt!(
                       chain!(
                           tag!(",") ~
                           multispace?,
                           ||{}
                       )
                   ),
                   || { val }
               ),
               str::from_utf8
           )
       )
);

/// Parse a reference to a named table
/// XXX(malte): add support for schema.table notation
named!(pub table_reference<&[u8], &str>,
    chain!(
        table: map_res!(alphanumeric, str::from_utf8) ~
        opt!(
            chain!(
                space ~
                caseless_tag!("as") ~
                space ~
                alias: map_res!(alphanumeric, str::from_utf8),
                || { println!("got alias: {} -> {}", table, alias); alias }
            )
        ),
        || { table }
    )
);
