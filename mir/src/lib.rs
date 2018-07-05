#![deny(unused_extern_crates)]

extern crate basics;
extern crate dataflow;
extern crate nom_sql;
extern crate regex;
#[macro_use]
extern crate slog;

use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

use nom_sql::FunctionExpression;

use basics::*;

pub mod node;
mod optimize;
pub mod query;
pub mod reuse;
mod rewrite;
pub mod visualize;

pub type MirNodeRef = Rc<RefCell<node::MirNode>>;

#[derive(Clone, Debug, Eq, Hash)]
pub struct Column {
    pub table: Option<String>,
    pub name: String,
    pub function: Option<Box<FunctionExpression>>,
    pub aliases: Vec<Column>,
}

impl From<nom_sql::Column> for Column {
    fn from(c: nom_sql::Column) -> Column {
        Column {
            aliases: match &c.alias {
                Some(_) => vec![Column::from(nom_sql::Column {
                    name: c.name.to_owned(),
                    table: c.table.clone(),
                    alias: None,
                    function: c.function.clone(),
                })],
                None => vec![],
            },
            table: c.table,
            name: match c.alias {
                Some(a) => a,
                None => c.name,
            },
            function: c.function,
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Column) -> bool {
        (self.name == other.name && self.table == other.table && self.function == other.function)
            || self.aliases.contains(other) || other.aliases.contains(self)
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Column) -> Ordering {
        if self.table.is_some() && other.table.is_some() {
            match self.table.cmp(&other.table) {
                Ordering::Equal => self.name.cmp(&other.name),
                x => x,
            }
        } else {
            self.name.cmp(&other.name)
        }
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Column) -> Option<Ordering> {
        if self.table.is_some() && other.table.is_some() {
            match self.table.cmp(&other.table) {
                Ordering::Equal => Some(self.name.cmp(&other.name)),
                x => Some(x),
            }
        } else if self.table.is_none() && other.table.is_none() {
            Some(self.name.cmp(&other.name))
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub enum FlowNode {
    New(NodeIndex),
    Existing(NodeIndex),
}
impl FlowNode {
    pub fn address(&self) -> NodeIndex {
        match *self {
            FlowNode::New(na) | FlowNode::Existing(na) => na,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_from_nomsql_column() {
        let nsc = nom_sql::Column::from("t.col");
        assert_eq!(
            Column::from(nsc),
            Column {
                table: Some("t".into()),
                name: "col".into(),
                function: None,
                aliases: vec![],
            }
        );

        let nsc_with_alias = nom_sql::Column {
            name: "colwa".into(),
            table: Some("t".into()),
            alias: Some("al".into()),
            function: None,
        };
        assert_eq!(
            Column::from(nsc_with_alias),
            Column {
                table: Some("t".into()),
                name: "al".into(),
                function: None,
                aliases: vec![Column {
                    table: Some("t".into()),
                    name: "colwa".into(),
                    function: None,
                    aliases: vec![],
                }],
            }
        );
    }

    #[test]
    fn column_equality() {
        let c = Column::from(nom_sql::Column::from("t.col"));
        let ac = Column {
            table: Some("t".into()),
            name: "al".into(),
            function: None,
            aliases: vec![c.clone()],
        };

        // column is equal to itself
        assert_eq!(c, c);

        // column is equal to its aliases
        assert_eq!(ac, c);
    }
}
