pub mod alias_removal;

use nom_sql::parser::SqlQuery;

pub trait Pass {
    fn apply(&mut self, q: SqlQuery) -> SqlQuery;
}
