/// Types of (key-value) data structures we can use as indices in Noria.
///
/// See [the design doc][0] for more information
///
/// [0]: https://www.notion.so/readyset/Index-Selection-f91b2a873dda4b63a4b5d9d14bbee266
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    /// An index backed by a [`HashMap`](std::collections::HashMap).
    HashMap,
    /// An index backed by a [`BTreeMap`](std::collections::BTreeMap)
    BTreeMap,
}
