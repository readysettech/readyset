//! Primitives and structs related to maintaining different consistency
//! models within the Noria dataflow graph.

/// Representation of a timestamp used to enforce read-your-write
/// consistency.
// TODO(justin): Support for comparing against tickets used in client
// read requests.
#[derive(Clone, Default, Debug)]
pub struct Timestamp {}
