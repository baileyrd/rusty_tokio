use rusty_tokio::io::{TcpListener, TcpStream, UdpSocket};
use rusty_tokio::Runtime;

#[test]
fn tcp_listener_from_std_accepts_a_connection() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = std_listener.local_addr().unwrap();
        let listener = TcpListener::from_std(std_listener).unwrap();

        let client = rusty_tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(b"adopted listener").await.unwrap();
        });

        let (stream, _peer) = listener.accept().await.unwrap();
        let mut buf = [0u8; 16];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"adopted listener");

        client.await.unwrap();
    });
}

#[test]
fn tcp_listener_into_std_can_still_accept_blocking() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let std_listener = listener.into_std().unwrap();

        let accepted = rusty_tokio::spawn_blocking(move || {
            use std::io::Read;
            let (mut stream, _peer) = std_listener.accept().unwrap();
            let mut buf = [0u8; 15];
            stream.read_exact(&mut buf).unwrap();
            buf
        });

        let client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"blocking accept").await.unwrap();

        let received = accepted.await.unwrap();
        assert_eq!(&received, b"blocking accept");
    });
}

#[test]
fn tcp_stream_from_std_adopts_an_already_connected_socket() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 7];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        // Connect with a plain blocking std socket (in a blocking-safe
        // spot, since connecting to a local loopback listener that's
        // already bound completes essentially immediately), then adopt
        // it as an async `TcpStream`.
        let std_stream = rusty_tokio::spawn_blocking(move || std::net::TcpStream::connect(addr))
            .await
            .unwrap()
            .unwrap();
        let stream = TcpStream::from_std(std_stream).unwrap();

        stream.write_all(b"adopted").await.unwrap();
        let mut buf = [0u8; 7];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"adopted");

        server.await.unwrap();
    });
}

#[test]
fn tcp_stream_into_std_keeps_the_same_connection_alive() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(b"still connected").await.unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let std_stream = stream.into_std().unwrap();

        let received = rusty_tokio::spawn_blocking(move || {
            use std::io::Read;
            let mut std_stream = std_stream;
            let mut buf = [0u8; 15];
            std_stream.read_exact(&mut buf).unwrap();
            buf
        })
        .await
        .unwrap();
        assert_eq!(&received, b"still connected");

        server.await.unwrap();
    });
}

#[test]
fn udp_socket_from_std_and_into_std_round_trip() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let std_a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let std_b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr_a = std_a.local_addr().unwrap();
        let addr_b = std_b.local_addr().unwrap();

        let a = UdpSocket::from_std(std_a).unwrap();
        let b = UdpSocket::from_std(std_b).unwrap();

        a.send_to(b"hello udp", addr_b).await.unwrap();
        let mut buf = [0u8; 9];
        let (n, from) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello udp");
        assert_eq!(from, addr_a);

        // Hand `b` back out as a plain std socket and confirm the same
        // underlying port still works, via a blocking recv this time.
        let std_b_again = b.into_std().unwrap();
        let recv_task = rusty_tokio::spawn_blocking(move || {
            let mut buf = [0u8; 11];
            let (n, from) = std_b_again.recv_from(&mut buf).unwrap();
            (buf, n, from)
        });

        a.send_to(b"back to std", addr_b).await.unwrap();
        let (buf, n, from) = recv_task.await.unwrap();
        assert_eq!(&buf[..n], b"back to std");
        assert_eq!(from, addr_a);
    });
}
