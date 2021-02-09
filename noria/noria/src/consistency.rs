//! Primitives and structs related to maintaining different consistency
//! models within the Noria dataflow graph.

/// Representation of a timestamp used to enforce read-your-write
/// consistency.
// TODO(justin): Support for comparing against tickets used in client
// read requests.
// TODO(justin): Implement Ord operator s.t. t1 < t2 means that every base
// table timestamp in t1 is less than t2.
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct Timestamp {}
