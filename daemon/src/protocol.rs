#[derive(Debug, Deserialize, Serialize)]
pub enum CoordinationMessage {
    Register,
    Deregister,
    Heartbeat,
    AssignDomain,
    RemoveDomain,
}
