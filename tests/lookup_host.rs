use rusty_tokio::io::{lookup_host, AsyncReadExt, AsyncWriteExt, TcpListener, TcpStream};
use rusty_tokio::Runtime;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[test]
fn a_numeric_address_and_port_resolves_without_any_real_lookup() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let addrs: Vec<SocketAddr> = lookup_host("127.0.0.1:9999").await.unwrap().collect();
        assert_eq!(
            addrs,
            vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                9999
            )]
        );
    });
}

#[test]
fn a_host_port_tuple_resolves_the_same_way_as_a_string() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let addrs: Vec<SocketAddr> = lookup_host(("127.0.0.1", 9999)).await.unwrap().collect();
        assert_eq!(
            addrs,
            vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                9999
            )]
        );
    });
}

#[test]
fn localhost_resolves_to_at_least_one_loopback_address() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let addrs: Vec<SocketAddr> = lookup_host("localhost:0").await.unwrap().collect();
        assert!(!addrs.is_empty());
        assert!(addrs.iter().all(|addr| addr.ip().is_loopback()));
    });
}

#[test]
fn a_malformed_host_string_is_an_error_not_a_panic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Missing a port -- `ToSocketAddrs` rejects this outright, no
        // network access involved.
        assert!(lookup_host("not a valid host string").await.is_err());
    });
}

#[test]
fn a_resolved_address_can_actually_be_connected_to() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = listener.local_addr().unwrap().port();

        let mut addrs = lookup_host(format!("127.0.0.1:{port}")).await.unwrap();
        let addr = addrs.next().expect("at least one resolved address");

        let (accepted, connected) = rusty_tokio::try_join!(
            async { listener.accept().await.map(|(stream, _)| stream) },
            TcpStream::connect(addr),
        )
        .unwrap();
        let (mut server, mut client) = (accepted, connected);

        client
            .write_all(b"hello over a resolved addr")
            .await
            .unwrap();
        client.shutdown().await.unwrap();
        let mut received = Vec::new();
        server.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, b"hello over a resolved addr");
    });
}
