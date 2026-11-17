use std::net::SocketAddr;

use readyset_adapter::ConnectionInfo;

#[test]
fn connection_info_ordering_by_id() {
    let addr1: SocketAddr = "127.0.0.1:1000".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:2000".parse().unwrap();

    let c1 = ConnectionInfo::new(1, addr2, "alice".to_string());
    let c2 = ConnectionInfo::new(2, addr1, "alice".to_string());

    assert!(c1 < c2, "Lower id should sort first, whatever the address");
}

#[test]
fn connection_info_ordering_by_username() {
    let addr: SocketAddr = "127.0.0.1:1000".parse().unwrap();

    let c1 = ConnectionInfo::new(1, addr, "alice".to_string());
    let c2 = ConnectionInfo::new(1, addr, "bob".to_string());

    assert!(c1 < c2, "Same id and addr, 'alice' < 'bob' lexicographically");
}
