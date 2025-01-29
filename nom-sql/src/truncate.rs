use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::sequence::tuple;
use nom::Parser;
use nom_locate::LocatedSpan;
use readyset_sql::Dialect;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::ws_sep_comma;
use crate::table::relation;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{DialectDisplay, NomSqlResult, Relation};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]

pub struct TruncateTable {
    pub relation: Relation,
    pub only: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct TruncateStatement {
    pub tables: Vec<TruncateTable>,
    pub restart_identity: bool,
    pub cascade: bool,
}

impl DialectDisplay for TruncateStatement {
    fn display(&self, dialect: Dialect) -> impl std::fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "TRUNCATE ")?;

            write!(
                f,
                "{}",
                self.tables
                    .iter()
                    .map(|t| format!(
                        "{}{}",
                        if t.only { "ONLY " } else { "" },
                        t.relation.display(dialect)
                    ))
                    .join(", ")
            )?;

            if self.restart_identity {
                write!(f, " RESTART IDENTITY")?;
            }

            if self.cascade {
                write!(f, " CASCADE")?;
            }

            Ok(())
        })
    }
}

/// Parse a TRUNCATE statement.
///
/// The grammar for [MySQL][]:
///
/// ```sql
/// TRUNCATE [TABLE] tbl_name
/// ```
///
/// The grammar for [Postgres][]:
///
/// ```sql
/// TRUNCATE [ TABLE ] [ ONLY ] name [ * ] [, ... ]
///     [ RESTART IDENTITY | CONTINUE IDENTITY ] [ CASCADE | RESTRICT ]
/// ```
///
/// [MySQL]: https://dev.mysql.com/doc/refman/8.0/en/truncate-table.html
/// [Postgres]: https://www.postgresql.org/docs/current/sql-truncate.html
pub fn truncate(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TruncateStatement> {
    move |i| {
        let (i, _) = tag_no_case("truncate")(i)?;
        let (i, _) = whitespace1(i)?;

        let (i, _) = opt(tuple((tag_no_case("table"), whitespace1)))(i)?;

        let (i, tables) = match dialect {
            Dialect::MySQL => {
                let (i, rel) = relation(dialect)(i)?;
                (
                    i,
                    vec![TruncateTable {
                        relation: rel,
                        only: false,
                    }],
                )
            }
            Dialect::PostgreSQL => separated_list1(ws_sep_comma, |i| {
                let (i, only) = opt(tuple((tag_no_case("only"), whitespace1)))(i)?;
                let (i, rel) = relation(dialect)(i)?;
                let i = if only.is_none() {
                    opt(tuple((whitespace0, tag("*"))))(i)?.0
                } else {
                    i
                };

                Ok((
                    i,
                    TruncateTable {
                        relation: rel,
                        only: only.is_some(),
                    },
                ))
            })(i)?,
        };

        let (i, restart_identity) = if matches!(dialect, Dialect::PostgreSQL) {
            opt(tuple((
                whitespace1,
                alt((
                    tag_no_case("continue").map(|_| false),
                    tag_no_case("restart").map(|_| true),
                )),
                whitespace1,
                tag_no_case("identity"),
            )))
            .map(|r| r.map(|t| t.1).unwrap_or(false))
            .parse(i)?
        } else {
            (i, false)
        };

        let (i, cascade) = if matches!(dialect, Dialect::PostgreSQL) {
            opt(tuple((
                whitespace1,
                alt((
                    tag_no_case("restrict").map(|_| false),
                    tag_no_case("cascade").map(|_| true),
                )),
            )))
            .map(|r| r.map(|t| t.1).unwrap_or(false))
            .parse(i)?
        } else {
            (i, false)
        };

        Ok((
            i,
            TruncateStatement {
                tables,
                restart_identity,
                cascade,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn truncate_table_mysql() {
        assert_eq!(
            truncate(Dialect::MySQL)(LocatedSpan::new(b"truncate mytable"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: false,
                }],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::MySQL)(LocatedSpan::new(b"tRUNcate mydb.mytable"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation {
                        name: "mytable".into(),
                        schema: Some("mydb".into())
                    },
                    only: false,
                }],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::MySQL)(LocatedSpan::new(b"truncate table mytable"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: false,
                }],
                restart_identity: false,
                cascade: false,
            }
        );
        let leftover_i = truncate(Dialect::MySQL)(LocatedSpan::new(b"truncate t1, t2"))
            .unwrap()
            .0;
        assert_eq!(*leftover_i, b", t2");
        let leftover_i =
            truncate(Dialect::MySQL)(LocatedSpan::new(b"truncate t1 restart identity"))
                .unwrap()
                .0;
        assert_eq!(*leftover_i, b" restart identity");
        let leftover_i = truncate(Dialect::MySQL)(LocatedSpan::new(b"truncate t1 cascade"))
            .unwrap()
            .0;
        assert_eq!(*leftover_i, b" cascade");
    }

    #[test]
    fn truncate_table_postgres() {
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"trunCATe mytable"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: false,
                }],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate mydb.mytable"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation {
                        name: "mytable".into(),
                        schema: Some("mydb".into())
                    },
                    only: false,
                }],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate table mytable"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: false,
                }],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate table table1, table2"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![
                    TruncateTable {
                        relation: Relation::from("table1"),
                        only: false,
                    },
                    TruncateTable {
                        relation: Relation::from("table2"),
                        only: false,
                    }
                ],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate ONLY mytable"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: true,
                }],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate t1, only t2"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![
                    TruncateTable {
                        relation: Relation::from("t1"),
                        only: false,
                    },
                    TruncateTable {
                        relation: Relation::from("t2"),
                        only: true,
                    }
                ],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate mytable continue identity"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: false,
                }],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate mytable restart identity"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: false,
                }],
                restart_identity: true,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate mytable restrict"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: false,
                }],
                restart_identity: false,
                cascade: false,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate mytable cascade"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: false,
                }],
                restart_identity: false,
                cascade: true,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(
                b"truncate mytable restart identity cascade"
            ))
            .unwrap()
            .1,
            TruncateStatement {
                tables: vec![TruncateTable {
                    relation: Relation::from("mytable"),
                    only: false,
                }],
                restart_identity: true,
                cascade: true,
            }
        );
        assert_eq!(
            truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate t1 *, t2*, t3"))
                .unwrap()
                .1,
            TruncateStatement {
                tables: vec![
                    TruncateTable {
                        relation: Relation::from("t1"),
                        only: false,
                    },
                    TruncateTable {
                        relation: Relation::from("t2"),
                        only: false,
                    },
                    TruncateTable {
                        relation: Relation::from("t3"),
                        only: false,
                    }
                ],
                restart_identity: false,
                cascade: false,
            }
        );
        // Can't combine explicit descendant truncation (*) with ONLY
        let leftover_i = truncate(Dialect::PostgreSQL)(LocatedSpan::new(b"truncate only t1*, t2"))
            .unwrap()
            .0;
        assert_eq!(*leftover_i, b"*, t2");
    }

    #[test]
    fn display() {
        let s = TruncateStatement {
            tables: vec![
                TruncateTable {
                    relation: Relation::from("t1"),
                    only: false,
                },
                TruncateTable {
                    relation: Relation::from("t2"),
                    only: true,
                },
            ],
            restart_identity: true,
            cascade: true,
        };
        assert_eq!(
            s.display(Dialect::PostgreSQL).to_string().as_str(),
            r#"TRUNCATE "t1", ONLY "t2" RESTART IDENTITY CASCADE"#
        );
    }
}
