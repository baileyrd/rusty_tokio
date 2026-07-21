use rusty_tokio::io::{AsyncReadExt, AsyncWriteExt, TcpListener, TcpStream, UdpSocket};
use rusty_tokio::Runtime;
use std::net::SocketAddr;

#[test]
fn tcp_echo_roundtrip() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let n = stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf[..n]).await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"hello reactor").await.unwrap();
        let mut buf = [0u8; 64];
        client.read_exact(&mut buf[..13]).await.unwrap();
        assert_eq!(&buf[..13], b"hello reactor");

        server.await.unwrap();
    });
}

#[test]
fn many_concurrent_tcp_connections() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            for _ in 0..50 {
                let (stream, _peer) = listener.accept().await.unwrap();
                rusty_tokio::spawn(async move {
                    let mut buf = [0u8; 8];
                    let n = stream.read(&mut buf).await.unwrap();
                    stream.write_all(&buf[..n]).await.unwrap();
                });
            }
        });

        let mut clients = Vec::new();
        for i in 0..50u8 {
            clients.push(rusty_tokio::spawn(async move {
                let stream = TcpStream::connect(addr).await.unwrap();
                stream.write_all(&[i]).await.unwrap();
                let mut buf = [0u8; 1];
                stream.read_exact(&mut buf).await.unwrap();
                assert_eq!(buf[0], i);
            }));
        }
        for c in clients {
            c.await.unwrap();
        }
        server.await.unwrap();
    });
}

#[test]
fn into_split_moves_owned_halves_into_separate_tasks() {
    // The whole point of `into_split` over plain `&TcpStream` usage: the
    // two halves are independently `'static` and can be handed to two
    // different spawned tasks with no `Arc` wrapping at the call site.
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let (mut read_half, mut write_half) = client.into_split();

        let writer_task = rusty_tokio::spawn(async move { write_half.write_all(b"hello").await });

        let mut buf = [0u8; 5];
        read_half.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        writer_task.await.unwrap().unwrap();
        server.await.unwrap();
    });
}

#[test]
fn split_borrows_read_and_write_halves_from_one_task() {
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

        let mut client = TcpStream::connect(addr).await.unwrap();
        let (mut read_half, mut write_half) = client.split();

        write_half.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        read_half.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        server.await.unwrap();
    });
}

#[test]
fn udp_send_and_recv() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b_addr: SocketAddr = b.local_addr().unwrap();

        let responder = rusty_tokio::spawn(async move {
            let mut buf = [0u8; 32];
            let (n, peer) = b.recv_from(&mut buf).await.unwrap();
            b.send_to(&buf[..n], peer).await.unwrap();
        });

        a.send_to(b"ping", b_addr).await.unwrap();
        let mut buf = [0u8; 32];
        let (n, peer) = a.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");
        assert_eq!(peer, b_addr);

        responder.await.unwrap();
    });
}

#[test]
fn udp_connect_allows_send_and_recv_without_addressing() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let a_addr = a.local_addr().unwrap();
        let b_addr = b.local_addr().unwrap();

        a.connect(b_addr).unwrap();
        b.connect(a_addr).unwrap();

        let responder = rusty_tokio::spawn(async move {
            let mut buf = [0u8; 32];
            let n = b.recv(&mut buf).await.unwrap();
            b.send(&buf[..n]).await.unwrap();
        });

        a.send(b"ping").await.unwrap();
        let mut buf = [0u8; 32];
        let n = a.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");

        responder.await.unwrap();
    });
}
