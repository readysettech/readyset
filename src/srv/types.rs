use std::collections::HashMap;

pub use flow::NodeIndex;
pub use query::DataType;

pub enum Request {
    Read(NodeIndex, Vec<DataType>),
    Write(NodeIndex, Vec<DataType>),
    List,
}

pub enum Response {
    NodeMapping(HashMap<String, NodeIndex>),
    Records(Vec<Vec<DataType>>),
    Ok,
}
