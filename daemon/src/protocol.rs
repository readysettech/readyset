use std::net::SocketAddr;

#[derive(Debug, Deserialize, Serialize)]
pub struct CoordinationMessage {
    pub source: SocketAddr,
    pub payload: CoordinationPayload,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum CoordinationPayload {
    Register,
    Deregister,
    Heartbeat,
    AssignDomain,
    RemoveDomain,
}
