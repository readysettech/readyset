use nom::bytes::complete::tag_no_case;
use nom::character::complete::{multispace0, multispace1};
use std::{fmt, str};

use common::{literal, sql_identifier, statement_terminator, Literal};
use nom::IResult;
use nom::sequence::tuple;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SetStatement {
    pub variable: String,
    pub value: Literal,
}

impl fmt::Display for SetStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SET ")?;
        write!(f, "{} = {}", self.variable, self.value.to_string())?;
        Ok(())
    }
}

pub fn set(i: &[u8]) -> IResult<&[u8], SetStatement> {
    let (input, (_, _, var, _, _, _, value, _)) =
        tuple((tag_no_case("set"), multispace1, sql_identifier, multispace0, tag_no_case("="),
                    multispace0, literal, statement_terminator))(i)?;
    let variable = String::from(str::from_utf8(var).unwrap());
    Ok((input, SetStatement { variable, value }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_set() {
        let qstring = "SET SQL_AUTO_IS_NULL = 0;";
        let res = set(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SetStatement {
                variable: "SQL_AUTO_IS_NULL".to_owned(),
                value: 0.into(),
            }
        );
    }

    #[test]
    fn user_defined_vars() {
        let qstring = "SET @var = 123;";
        let res = set(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SetStatement {
                variable: "@var".to_owned(),
                value: 123.into(),
            }
        );
    }

    #[test]
    fn format_set() {
        let qstring = "set autocommit=1";
        let expected = "SET autocommit = 1";
        let res = set(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }
}
