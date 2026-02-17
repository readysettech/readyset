use std::net::SocketAddr;

use readyset_adapter::ConnectionInfo;

#[test]
fn connection_info_ordering_by_addr() {
    let addr1: SocketAddr = "127.0.0.1:1000".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:2000".parse().unwrap();

    let c1 = ConnectionInfo::new(addr1, "alice".to_string());
    let c2 = ConnectionInfo::new(addr2, "alice".to_string());

    assert!(c1 < c2, "Lower port should sort first");
}

#[test]
fn connection_info_ordering_by_username() {
    let addr: SocketAddr = "127.0.0.1:1000".parse().unwrap();

    let c1 = ConnectionInfo::new(addr, "alice".to_string());
    let c2 = ConnectionInfo::new(addr, "bob".to_string());

    assert!(c1 < c2, "Same addr, 'alice' < 'bob' lexicographically");
}
