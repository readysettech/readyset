pub mod alias_removal;

use nom_sql::SqlQuery;

pub trait Pass {
    fn apply(&mut self, q: SqlQuery) -> SqlQuery;
}
