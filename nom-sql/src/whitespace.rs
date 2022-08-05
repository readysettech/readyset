//! This module contains multiple useful parsers that can identify what we consider whitespace
//! from a given parseable input.
//!
//! The parsers exposed here are generic and they might seem to be very restrictive in terms of the
//! types they accept, but all traits required by them are already implemented by the two types that
//! we care for: [`&str`] and [`&[u8]`].

use std::ops::{Range, RangeFrom, RangeTo};

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until};
use nom::character::complete::{line_ending, not_line_ending};
use nom::combinator::{map, map_res, value};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{many0, many1};
use nom::sequence::delimited;
use nom::{
    AsChar, Compare, FindSubstring, FindToken, IResult, InputIter, InputLength, InputTake,
    InputTakeAtPosition, Slice,
};

use crate::{NomSqlResult, Span};

/// Recognizes a multiline comment of the form `/* ... */`, skipping the `/*` and `*/`,
/// to return the comment content.
///
/// # Example
///
/// ```
/// # use nom::error::ErrorKind;
/// # use nom::{Err, IResult, Needed};
/// use nom_sql::whitespace::multiline_comment;
///
/// fn parser(input: &str) -> IResult<&str, &str> {
///     multiline_comment(input)
/// }
///
/// assert_eq!(
///     parser("/* this is an example comment    */21c"),
///     Ok(("21c", " this is an example comment    "))
/// );
/// assert_eq!(parser("/* comment */Z21c"), Ok(("Z21c", " comment ")));
/// assert_eq!(
///     parser("Z21c"),
///     Err(Err::Error(nom::error::Error::new("Z21c", ErrorKind::Tag)))
/// );
/// ```
pub fn multiline_comment(input: Span) -> NomSqlResult<&[u8]> {
    map(
        delimited(tag_no_case("/*"), take_until("*/"), tag_no_case("*/")),
        |x: Span| *x,
    )(input)
}

/// Recognizes a EOL style comment of the form `# ...` (in case the start tag is '#'), skipping the
/// `#` to return the comment content.
///
/// # Example
///
/// ```
/// # use nom::{Err, error::ErrorKind, IResult, Needed};
/// use nom_sql::whitespace::eol_comment;
///
/// fn parser(input: &str) -> IResult<&str, &str> {
///     eol_comment("#")(input)
/// }
///
/// assert_eq!(
///     parser("# this is an example comment    \r\n21c"),
///     Ok(("21c", " this is an example comment    "))
/// );
/// assert_eq!(parser("# comment\nZ21c"), Ok(("Z21c", " comment")));
/// assert_eq!(
///     parser("\rZ21c"),
///     Err(Err::Error(nom::error::Error::new("\rZ21c", ErrorKind::Tag)))
/// );
/// ```
pub fn eol_comment(tag: &'static str) -> impl Fn(Span) -> NomSqlResult<&[u8]> {
    move |input| {
        delimited(
            tag_no_case(tag),
            map(not_line_ending, |x: Span| *x),
            line_ending,
        )(input)
    }
}

/// Recognizes what we consider a whitespace in SQL. A whitespace can be:
/// - A single space.
/// - A tabulation.
/// - A carriage return.
/// - A new line.
/// - A multiline comment of the form `/* ... */`.
/// - An end of line comment of the form `# ...`.
/// - An end of line comment of the form `-- ...`.
///
/// The whitespace is to be consumed, unless it is a comment. In that case, the comment
/// content is the output.
///
/// # Example
///
/// ```
/// # use nom::error::ErrorKind;
/// # use nom::{Err, IResult, Needed};
/// use nom_sql::whitespace::whitespace;
///
/// fn parser(input: &str) -> IResult<&str, &str> {
///     whitespace(input)
/// }
///
/// assert_eq!(parser(" \t\r\n21c"), Ok(("\t\r\n21c", "")));
/// assert_eq!(parser("\t\r\n21c"), Ok(("\r\n21c", "")));
/// assert_eq!(parser("\r\n21c"), Ok(("\n21c", "")));
/// assert_eq!(parser("\n21c"), Ok(("21c", "")));
/// assert_eq!(parser("/* comment */Z21c"), Ok(("Z21c", " comment ")));
/// assert_eq!(
///     parser("Z21c"),
///     Err(Err::Error(nom::error::Error::new("Z21c", ErrorKind::Tag)))
/// );
/// assert_eq!(parser("# comment\nZ21c"), Ok(("Z21c", " comment")));
/// ```
pub fn whitespace(input: Span) -> NomSqlResult<&[u8]> {
    alt((
        multiline_comment,
        eol_comment("#"),
        eol_comment("--"),
        value(Default::default(), tag(" ")),
        value(Default::default(), tag("\t")),
        value(Default::default(), tag("\r")),
        value(Default::default(), tag("\n")),
    ))(input)
}

/// Recognizes zero or more whitespaces. See `whitespace` for more information.
///
/// # Example
///
/// ```
/// # use nom::error::ErrorKind;
/// # use nom::{Err, IResult, Needed};
/// use nom_sql::whitespace::whitespace0;
///
/// fn parser(input: &str) -> IResult<&str, Vec<&str>> {
///     whitespace0(input)
/// }
///
/// assert_eq!(parser(" \t\r\n21c"), Ok(("21c", vec![])));
/// assert_eq!(
///     parser("/* comment */\nZ21c"),
///     Ok(("Z21c", vec![" comment "]))
/// );
/// assert_eq!(parser("Z21c"), Ok(("Z21c", vec![])));
/// assert_eq!(
///     parser("# comment\n/* other comment */Z21c"),
///     Ok(("Z21c", vec![" comment", " other comment "]))
/// );
/// ```
pub fn whitespace0(input: Span) -> NomSqlResult<Vec<&[u8]>> {
    many0(whitespace)(input).map(|(remaining, mut output)| {
        output.retain(|item| item.input_len() > 0);
        (remaining, output)
    })
}

/// Recognizes one or more whitespaces. See `whitespace` for more information.
///
/// # Example
///
/// ```
/// # use nom::error::ErrorKind;
/// # use nom::{Err, IResult, Needed};
/// use nom_sql::whitespace::whitespace1;
///
/// fn parser(input: &str) -> IResult<&str, Vec<&str>> {
///     whitespace1(input)
/// }
///
/// assert_eq!(parser(" \t\r\n21c"), Ok(("21c", vec![])));
/// assert_eq!(
///     parser("/* comment */\nZ21c"),
///     Ok(("Z21c", vec![" comment "]))
/// );
/// assert_eq!(
///     parser("Z21c"),
///     Err(Err::Error(nom::error::Error::new("Z21c", ErrorKind::Tag)))
/// );
/// assert_eq!(
///     parser("# comment\n/* other comment */Z21c"),
///     Ok(("Z21c", vec![" comment", " other comment "]))
/// );
/// ```
pub fn whitespace1(input: Span) -> NomSqlResult<Vec<&[u8]>> {
    many1(whitespace)(input).map(|(remaining, mut output)| {
        output.retain(|item| item.input_len() > 0);
        (remaining, output)
    })
}

#[cfg(test)]
mod tests {
    use nom::error::ErrorKind;

    use super::*;

    macro_rules! error {
        ($input:expr, $err_kind: expr) => {
            Err(nom::Err::Error(nom::error::Error::new($input, $err_kind)))
        };
    }

    #[test]
    fn test_multiline_comment() {
        fn parser(input: &str) -> IResult<&str, &str> {
            multiline_comment(input)
        }

        assert_eq!(
            parser("/* this is an example comment   */Z21c"),
            Ok(("Z21c", " this is an example comment   "))
        );
        assert_eq!(parser("/* comment */Z21c"), Ok(("Z21c", " comment ")));
        assert_eq!(
            parser("/* multiline \n comment */Z21c"),
            Ok(("Z21c", " multiline \n comment "))
        );
        assert_eq!(parser("Z21c"), error!("Z21c", ErrorKind::Tag));
    }

    #[test]
    fn test_eol_comment() {
        fn parser(tag: &'static str) -> impl Fn(Span) -> NomSqlResult<&[u8]> {
            move |input| eol_comment(tag)(Span::new(&input))
        }

        // New line
        assert_eq!(
            parser("#")("# this is an example comment    \n21c"),
            Ok(("21c", " this is an example comment    "))
        );
        assert_eq!(parser("#")("# comment\nZ21c"), Ok(("Z21c", " comment")));
        assert_eq!(parser("#")("Z21c"), error!("Z21c", ErrorKind::Tag));

        assert_eq!(
            parser("--")("-- this is an example comment    \n21c"),
            Ok(("21c", " this is an example comment    "))
        );
        assert_eq!(parser("--")("-- comment\nZ21c"), Ok(("Z21c", " comment")));

        // Carriage return
        assert_eq!(
            parser("#")("# this is an example comment    \r21c"),
            error!(" this is an example comment    \r21c", ErrorKind::Tag)
        );
        assert_eq!(
            parser("#")("# comment\rZ21c"),
            error!(" comment\rZ21c", ErrorKind::Tag)
        );
        assert_eq!(parser("#")("Z21c"), error!("Z21c", ErrorKind::Tag));

        assert_eq!(
            parser("--")("-- this is an example comment    \r21c"),
            error!(" this is an example comment    \r21c", ErrorKind::Tag)
        );
        assert_eq!(
            parser("--")("-- comment\rZ21c"),
            error!(" comment\rZ21c", ErrorKind::Tag)
        );

        // Carriage return, new line
        assert_eq!(
            parser("#")("# this is an example comment    \r\n21c"),
            Ok(("21c", " this is an example comment    "))
        );
        assert_eq!(parser("#")("# comment\r\nZ21c"), Ok(("Z21c", " comment")));
        assert_eq!(parser("#")("Z21c"), error!("Z21c", ErrorKind::Tag));

        assert_eq!(
            parser("--")("-- this is an example comment    \r\n21c"),
            Ok(("21c", " this is an example comment    "))
        );
        assert_eq!(parser("--")("-- comment\r\nZ21c"), Ok(("Z21c", " comment")));

        assert_eq!(parser("--")("Z21c"), error!("Z21c", ErrorKind::Tag));
    }

    #[test]
    fn test_whitespace() {
        fn parser(input: &str) -> IResult<&str, &str> {
            whitespace(input)
        }
        // whitespace characters
        assert_eq!(parser(" \t\n\r21c"), Ok(("\t\n\r21c", "")));
        assert_eq!(parser("\t\n\r21c"), Ok(("\n\r21c", "")));
        assert_eq!(parser("\n\r21c"), Ok(("\r21c", "")));
        assert_eq!(parser("\r21c"), Ok(("21c", "")));
        assert_eq!(parser("21c"), error!("21c", ErrorKind::Tag));

        // multiline comments
        assert_eq!(
            parser("/* this is an example comment */21c"),
            Ok(("21c", " this is an example comment "))
        );
        assert_eq!(parser("/* comment */Z21c"), Ok(("Z21c", " comment ")));
        assert_eq!(
            parser("/* multiline \n comment */Z21c"),
            Ok(("Z21c", " multiline \n comment "))
        );
        assert_eq!(parser("Z21c"), error!("Z21c", ErrorKind::Tag));

        // eol comments
        assert_eq!(
            parser("# this is an example comment    \r21c"),
            error!("# this is an example comment    \r21c", ErrorKind::Tag)
        );
        assert_eq!(parser("# comment\nZ21c"), Ok(("Z21c", " comment")));
        assert_eq!(parser("# comment\r\nZ21c"), Ok(("Z21c", " comment")));
        assert_eq!(parser("Z21c"), error!("Z21c", ErrorKind::Tag));
    }

    #[test]
    fn test_whitespace0() {
        fn parser(input: &str) -> IResult<&str, Vec<&str>> {
            whitespace0(input)
        }
        assert_eq!(parser(" \t\n\r21c"), Ok(("21c", vec![])));
        assert_eq!(parser("21c"), Ok(("21c", vec![])));

        // multiline comments
        assert_eq!(
            parser("/* this is an example comment */# comment2\r21c"),
            Ok(("# comment2\r21c", vec![" this is an example comment "]))
        );
        assert_eq!(parser("/* comment */Z21c"), Ok(("Z21c", vec![" comment "])));
        assert_eq!(
            parser("/* multiline \n comment */Z21c"),
            Ok(("Z21c", vec![" multiline \n comment "]))
        );

        // eol comments
        assert_eq!(
            parser("# this is an example comment    \r21c"),
            Ok(("# this is an example comment    \r21c", vec![]))
        );
        assert_eq!(parser("# comment\nZ21c"), Ok(("Z21c", vec![" comment"])));
        assert_eq!(parser("# comment\r\nZ21c"), Ok(("Z21c", vec![" comment"])));
    }

    #[test]
    fn test_whitespace1() {
        fn parser(input: &str) -> IResult<&str, Vec<&str>> {
            whitespace1(input)
        }
        assert_eq!(parser(" \t\r\n21c"), Ok(("21c", vec![])));
        assert_eq!(parser("21c"), error!("21c", ErrorKind::Tag));

        // multiline comments
        assert_eq!(
            parser("/* this is an example comment */# comment2\r21c"),
            Ok(("# comment2\r21c", vec![" this is an example comment "]))
        );
        assert_eq!(parser("/* comment */Z21c"), Ok(("Z21c", vec![" comment "])));
        assert_eq!(
            parser("/* multiline \n comment */Z21c"),
            Ok(("Z21c", vec![" multiline \n comment "]))
        );

        // eol comments
        assert_eq!(
            parser("# this is an example comment    \r21c"),
            error!("# this is an example comment    \r21c", ErrorKind::Tag)
        );
        assert_eq!(parser("# comment\nZ21c"), Ok(("Z21c", vec![" comment"])));
        assert_eq!(parser("# comment\r\nZ21c"), Ok(("Z21c", vec![" comment"])));
        assert_eq!(parser("Z21c"), error!("Z21c", ErrorKind::Tag));
    }
}
