use crate::ast::{Column, Relation, Variable};
use crate::{Dialect, DialectDisplay};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use test_strategy::Arbitrary;

#[derive(Clone, Debug, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct CreateRlsStatement {
    pub table: Relation,
    pub policy: Vec<(Column, Variable)>,
    pub if_not_exists: bool,
}

#[derive(
    Clone, Debug, PartialOrd, Ord, Default, Serialize, Deserialize, Arbitrary, PartialEq, Eq, Hash,
)]
pub struct DropRlsStatement {
    pub table: Option<Relation>,
}

impl CreateRlsStatement {
    pub fn new(table: Relation, if_not_exists: bool) -> Self {
        CreateRlsStatement {
            table,
            policy: vec![],
            if_not_exists,
        }
    }
}

impl DialectDisplay for CreateRlsStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        macro_rules! append_to {
            ($str:expr, $item:expr) => {
                if !$str.is_empty() {
                    $str.push_str(", ");
                }
                $str.push_str($item.as_str());
            };
        }

        let mut str_cols: String = String::new();
        let mut str_vars: String = String::new();
        for (col, var) in &self.policy {
            append_to!(str_cols, format!("\"{}\"", col.name));
            append_to!(str_vars, var.name);
        }

        fmt_with(move |f| {
            write!(
                f,
                "CREATE RLS ON {} {}USING ({}) = ({})",
                self.table.display(dialect),
                if self.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                },
                str_cols,
                str_vars
            )?;
            Ok(())
        })
    }
}

impl PartialEq<Self> for CreateRlsStatement {
    fn eq(&self, other: &Self) -> bool {
        self.table == other.table
            && self.if_not_exists == other.if_not_exists
            && self.policy == other.policy
    }
}

impl Eq for CreateRlsStatement {}

impl Hash for CreateRlsStatement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table.hash(state);
        self.if_not_exists.hash(state);
        self.policy.hash(state);
    }
}

impl DropRlsStatement {
    pub fn new(table: Option<Relation>) -> Self {
        DropRlsStatement { table }
    }
}

impl DialectDisplay for DropRlsStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            match &self.table {
                Some(table) => write!(f, "DROP RLS ON {}", table.display(dialect),),
                None => write!(f, "DROP ALL RLS"),
            }?;
            Ok(())
        })
    }
}
