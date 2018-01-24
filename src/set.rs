use nom::multispace;
use std::{fmt, str};

use common::{literal, sql_identifier, Literal};

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
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

named!(pub set<&[u8], SetStatement>,
    do_parse!(
        tag_no_case!("set") >>
        multispace >>
        var: map_res!(sql_identifier, str::from_utf8) >>
        multispace? >>
        tag_no_case!("=") >>
        multispace? >>
        val: literal >>
        (SetStatement {
            variable: String::from(var),
            value: val,
        })
    )
);

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
    fn format_set() {
        let qstring = "set autocommit=1";
        let expected = "SET autocommit = 1";
        let res = set(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }
}
