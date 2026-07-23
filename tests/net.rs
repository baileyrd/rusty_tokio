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

#[cfg(unix)]
#[test]
fn tcp_stream_raw_fd_roundtrip_preserves_functionality() {
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};

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
        assert!(client.as_raw_fd() >= 0);
        let fd = client.into_raw_fd();
        let client = unsafe { TcpStream::from_raw_fd(fd) };

        client.write_all(b"roundtrip").await.unwrap();
        let mut buf = [0u8; 64];
        client.read_exact(&mut buf[..9]).await.unwrap();
        assert_eq!(&buf[..9], b"roundtrip");

        server.await.unwrap();
    });
}

#[cfg(unix)]
#[test]
fn tcp_listener_raw_fd_roundtrip_can_still_accept() {
    use std::os::fd::{FromRawFd, IntoRawFd};

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let fd = listener.into_raw_fd();
        let listener = unsafe { TcpListener::from_raw_fd(fd) };

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 16];
            let n = stream.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"still works");
        });

        let client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"still works").await.unwrap();
        server.await.unwrap();
    });
}

#[cfg(unix)]
#[test]
fn udp_socket_raw_fd_roundtrip_preserves_functionality() {
    use std::os::fd::{FromRawFd, IntoRawFd};

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let a_addr = a.local_addr().unwrap();

        let fd = b.into_raw_fd();
        let b = unsafe { UdpSocket::from_raw_fd(fd) };
        let b_addr = b.local_addr().unwrap();

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
        let _ = a_addr;

        responder.await.unwrap();
    });
}

#[cfg(unix)]
#[test]
fn tcp_socket_raw_fd_roundtrip_still_binds_and_listens() {
    use rusty_tokio::io::TcpSocket;
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();
        assert!(socket.as_raw_fd() >= 0);
        let fd = socket.into_raw_fd();
        let socket = unsafe { TcpSocket::from_raw_fd(fd) };

        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(128).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move { listener.accept().await.unwrap() });
        let _client = TcpStream::connect(addr).await.unwrap();
        server.await.unwrap();
    });
}

#[cfg(target_os = "linux")]
#[test]
fn udp_socket_bind_device_set_and_read_back_then_cleared() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        assert_eq!(socket.device().unwrap(), None);

        socket.bind_device(Some(b"lo")).unwrap();
        assert_eq!(socket.device().unwrap().as_deref(), Some(&b"lo"[..]));

        socket.bind_device(None).unwrap();
        assert_eq!(socket.device().unwrap(), None);
    });
}

#[cfg(unix)]
#[test]
fn udp_multicast_v4_join_and_leave_succeed() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = UdpSocket::bind("0.0.0.0:0".parse().unwrap()).unwrap();
        let multiaddr: std::net::Ipv4Addr = "224.0.0.113".parse().unwrap();
        let interface = std::net::Ipv4Addr::UNSPECIFIED;

        socket.join_multicast_v4(multiaddr, interface).unwrap();
        socket.leave_multicast_v4(multiaddr, interface).unwrap();
    });
}

#[cfg(unix)]
#[test]
fn udp_multicast_v6_join_and_leave_succeed() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Some sandboxed/CI environments have no IPv6 stack at all --
        // skip rather than fail on an environment limitation this test
        // isn't meant to exercise.
        let Ok(socket) = UdpSocket::bind("[::]:0".parse().unwrap()) else {
            eprintln!("skipping: no IPv6 support in this environment");
            return;
        };
        let multiaddr: std::net::Ipv6Addr = "ff02::1234".parse().unwrap();

        socket.join_multicast_v6(&multiaddr, 0).unwrap();
        socket.leave_multicast_v6(&multiaddr, 0).unwrap();
    });
}

#[cfg(unix)]
#[test]
fn udp_multicast_loop_v4_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = UdpSocket::bind("0.0.0.0:0".parse().unwrap()).unwrap();
        socket.set_multicast_loop_v4(false).unwrap();
        assert!(!socket.multicast_loop_v4().unwrap());
        socket.set_multicast_loop_v4(true).unwrap();
        assert!(socket.multicast_loop_v4().unwrap());
    });
}

#[cfg(unix)]
#[test]
fn udp_multicast_loop_v6_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let Ok(socket) = UdpSocket::bind("[::]:0".parse().unwrap()) else {
            eprintln!("skipping: no IPv6 support in this environment");
            return;
        };
        socket.set_multicast_loop_v6(false).unwrap();
        assert!(!socket.multicast_loop_v6().unwrap());
        socket.set_multicast_loop_v6(true).unwrap();
        assert!(socket.multicast_loop_v6().unwrap());
    });
}

#[cfg(unix)]
#[test]
fn udp_multicast_ttl_v4_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = UdpSocket::bind("0.0.0.0:0".parse().unwrap()).unwrap();
        socket.set_multicast_ttl_v4(5).unwrap();
        assert_eq!(socket.multicast_ttl_v4().unwrap(), 5);
    });
}

#[test]
fn udp_broadcast_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = UdpSocket::bind("0.0.0.0:0".parse().unwrap()).unwrap();
        assert!(!socket.broadcast().unwrap());
        socket.set_broadcast(true).unwrap();
        assert!(socket.broadcast().unwrap());
        socket.set_broadcast(false).unwrap();
        assert!(!socket.broadcast().unwrap());
    });
}

#[cfg(unix)]
#[test]
fn udp_multicast_v4_round_trip_over_loopback() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let multiaddr: std::net::Ipv4Addr = "224.0.0.114".parse().unwrap();
        let interface = std::net::Ipv4Addr::UNSPECIFIED;

        let receiver = UdpSocket::bind("0.0.0.0:0".parse().unwrap()).unwrap();
        let port = receiver.local_addr().unwrap().port();
        receiver.join_multicast_v4(multiaddr, interface).unwrap();

        let sender = UdpSocket::bind("0.0.0.0:0".parse().unwrap()).unwrap();
        sender.set_multicast_loop_v4(true).unwrap();
        sender
            .send_to(b"multicast hello", SocketAddr::new(multiaddr.into(), port))
            .await
            .unwrap();

        let mut buf = [0u8; 32];
        let (n, _from) = rusty_tokio::time::timeout(
            std::time::Duration::from_secs(2),
            receiver.recv_from(&mut buf),
        )
        .await
        .expect("should receive the looped-back multicast datagram")
        .unwrap();
        assert_eq!(&buf[..n], b"multicast hello");

        receiver.leave_multicast_v4(multiaddr, interface).unwrap();
    });
}
