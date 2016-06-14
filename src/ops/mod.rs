use query;

pub mod base;
pub mod aggregate;
pub mod join;
pub mod union;

#[derive(Clone)]
pub enum Record {
    Positive(Vec<query::DataType>),
    Negative(Vec<query::DataType>),
}

#[derive(Clone)]
pub enum Update {
    Records(Vec<Record>),
}
