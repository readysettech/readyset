use std::{fmt, str};

use itertools::Itertools;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::multi::separated_list1;
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::common::{schema_table_reference, ws_sep_comma};
use crate::table::Table;
use crate::Dialect;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RenameTableStatement {
    pub ops: Vec<RenameTableOperation>,
}

impl fmt::Display for RenameTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RENAME TABLE {}",
            self.ops.iter().map(|op| op.to_string()).join(", ")
        )
    }
}

pub fn rename_table(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], RenameTableStatement> {
    move |i| {
        let (i, _) = tag_no_case("rename")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, _) = tag_no_case("table")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, ops) = separated_list1(ws_sep_comma, rename_table_operation(dialect))(i)?;
        Ok((i, RenameTableStatement { ops }))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RenameTableOperation {
    pub from: Table,
    pub to: Table,
}

impl fmt::Display for RenameTableOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} TO {}", self.from, self.to)
    }
}

fn rename_table_operation(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], RenameTableOperation> {
    move |i| {
        let (i, from) = schema_table_reference(dialect)(i)?;
        let (i, _) = multispace1(i)?;
        let (i, _) = tag_no_case("to")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, to) = schema_table_reference(dialect)(i)?;
        Ok((i, RenameTableOperation { from, to }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Table;

    #[test]
    fn simple_rename_table() {
        let qstring = b"RENAME TABLE t1 TO t2";
        let res = test_parse!(rename_table(Dialect::MySQL), qstring);
        assert_eq!(
            res,
            RenameTableStatement {
                ops: vec![RenameTableOperation {
                    from: Table::from("t1"),
                    to: Table::from("t2")
                }]
            }
        );
        assert_eq!(res.to_string(), "RENAME TABLE `t1` TO `t2`");
    }

    #[test]
    fn escaped_rename_table() {
        let qstring = b"RENAME TABLE `from` TO `to`";
        let res = test_parse!(rename_table(Dialect::MySQL), qstring);
        assert_eq!(
            res,
            RenameTableStatement {
                ops: vec![RenameTableOperation {
                    from: Table::from("from"),
                    to: Table::from("to")
                }]
            }
        );
        assert_eq!(res.to_string(), "RENAME TABLE `from` TO `to`");
    }

    #[test]
    fn compound_rename_table() {
        let qstring = b"RENAME TABLE t1 TO t2, `change` to t3, t4 to `select`";
        let res = test_parse!(rename_table(Dialect::MySQL), qstring);
        assert_eq!(
            res,
            RenameTableStatement {
                ops: vec![
                    RenameTableOperation {
                        from: Table::from("t1"),
                        to: Table::from("t2"),
                    },
                    RenameTableOperation {
                        from: Table::from("change"),
                        to: Table::from("t3"),
                    },
                    RenameTableOperation {
                        from: Table::from("t4"),
                        to: Table::from("select")
                    }
                ]
            }
        );
        assert_eq!(
            res.to_string(),
            "RENAME TABLE `t1` TO `t2`, `change` TO `t3`, `t4` TO `select`"
        );
    }

    #[test]
    fn flarum_rename_1() {
        let qstring = b"rename table `posts_likes` to `post_likes`";
        let res = test_parse!(rename_table(Dialect::MySQL), qstring);
        assert_eq!(
            res,
            RenameTableStatement {
                ops: vec![RenameTableOperation {
                    from: Table::from("posts_likes"),
                    to: Table::from("post_likes"),
                }]
            }
        );
        assert_eq!(
            res.to_string(),
            "RENAME TABLE `posts_likes` TO `post_likes`"
        );
    }
}
