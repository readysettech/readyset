/// Describe the materialization state of an operator.
#[derive(Debug, Serialize, Deserialize)]
pub enum MaterializationStatus {
    /// Operator's state is not materialized.
    Not,
    /// Operator's state is fully materialized.
    Full,
    /// Operator's state is partially materialized.
    Partial {
        beyond_materialization_frontier: bool,
    },
}
