use rusty_tokio::io::{TcpListener, TcpStream, UdpSocket};
use rusty_tokio::Runtime;

#[test]
fn tcp_stream_connect_accepts_a_plain_ip_port_string() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        // The fast path: this string already parses directly as a
        // `SocketAddr`, so no DNS/`lookup_host` round trip happens.
        let client = TcpStream::connect(format!("127.0.0.1:{}", addr.port()))
            .await
            .unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        server.await.unwrap();
    });
}

#[test]
fn tcp_stream_connect_accepts_a_str_tuple() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (_stream, _peer) = listener.accept().await.unwrap();
        });

        let _client = TcpStream::connect(("127.0.0.1", addr.port()))
            .await
            .unwrap();

        server.await.unwrap();
    });
}

#[test]
fn tcp_stream_connect_resolves_a_hostname_via_dns() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        // "localhost" isn't a plain IP literal, so this genuinely
        // exercises the `lookup_host`/`spawn_blocking` DNS path rather
        // than the direct-parse fast path.
        let client = TcpStream::connect(format!("localhost:{}", addr.port()))
            .await
            .unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        server.await.unwrap();
    });
}

#[test]
fn tcp_stream_connect_addr_still_takes_a_concrete_socket_addr() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (_stream, _peer) = listener.accept().await.unwrap();
        });

        let _client = TcpStream::connect_addr(addr).await.unwrap();

        server.await.unwrap();
    });
}

#[test]
fn tcp_listener_bind_addrs_accepts_a_str() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind_addrs("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");

        let client = TcpStream::connect_addr(addr).await.unwrap();
        let (stream, _peer) = listener.accept().await.unwrap();
        drop(client);
        drop(stream);
    });
}

#[test]
fn udp_socket_bind_addrs_and_connect_addrs_accept_str_forms() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind_addrs("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b_addr = b.local_addr().unwrap();

        a.connect_addrs(format!("127.0.0.1:{}", b_addr.port()))
            .await
            .unwrap();

        a.send(b"ping").await.unwrap();
        let mut buf = [0u8; 32];
        let (n, peer) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");
        assert_eq!(peer, a.local_addr().unwrap());
    });
}
