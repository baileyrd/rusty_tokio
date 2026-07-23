use rusty_tokio::io::{TcpListener, TcpSocket, TcpStream};
use rusty_tokio::Runtime;

#[test]
fn bind_then_listen_accepts_a_connection() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(128).unwrap();
        let addr = listener.local_addr().unwrap();

        let client = rusty_tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(b"via TcpSocket::listen").await.unwrap();
        });

        let (stream, _peer) = listener.accept().await.unwrap();
        let mut buf = [0u8; 21];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"via TcpSocket::listen");

        client.await.unwrap();
    });
}

#[test]
fn connect_reaches_a_real_listener() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 22];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let socket = TcpSocket::new_v4().unwrap();
        let stream = socket.connect(addr).await.unwrap();
        stream.write_all(b"via TcpSocket::connect").await.unwrap();
        let mut buf = [0u8; 22];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"via TcpSocket::connect");

        server.await.unwrap();
    });
}

#[test]
fn reuseaddr_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();

        socket.set_reuseaddr(true).unwrap();
        assert!(socket.reuseaddr().unwrap());

        socket.set_reuseaddr(false).unwrap();
        assert!(!socket.reuseaddr().unwrap());
    });
}

#[test]
fn reuseport_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();

        socket.set_reuseport(true).unwrap();
        assert!(socket.reuseport().unwrap());

        socket.set_reuseport(false).unwrap();
        assert!(!socket.reuseport().unwrap());
    });
}

#[test]
fn buffer_sizes_set_and_read_back_at_least_the_requested_size() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();

        // The kernel doesn't necessarily echo back the exact value
        // requested (Linux, notably, doubles it for its own
        // bookkeeping) -- only that it applied *at least* what was
        // asked for.
        let requested = 64 * 1024;

        socket.set_send_buffer_size(requested).unwrap();
        assert!(socket.send_buffer_size().unwrap() >= requested);

        socket.set_recv_buffer_size(requested).unwrap();
        assert!(socket.recv_buffer_size().unwrap() >= requested);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn bind_device_defaults_to_none() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();
        assert_eq!(socket.device().unwrap(), None);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn bind_device_set_and_read_back_then_cleared() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();

        socket.bind_device(Some(b"lo")).unwrap();
        assert_eq!(socket.device().unwrap().as_deref(), Some(&b"lo"[..]));

        socket.bind_device(None).unwrap();
        assert_eq!(socket.device().unwrap(), None);
    });
}

#[test]
fn keepalive_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();

        socket.set_keepalive(true).unwrap();
        assert!(socket.keepalive().unwrap());

        socket.set_keepalive(false).unwrap();
        assert!(!socket.keepalive().unwrap());
    });
}

#[test]
fn linger_defaults_to_none_then_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();
        assert_eq!(socket.linger().unwrap(), None);

        socket
            .set_linger(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        assert_eq!(
            socket.linger().unwrap(),
            Some(std::time::Duration::from_secs(5))
        );

        socket.set_linger(None).unwrap();
        assert_eq!(socket.linger().unwrap(), None);
    });
}

#[test]
fn set_zero_linger_enables_lingering_with_a_zero_timeout() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_zero_linger().unwrap();
        assert_eq!(socket.linger().unwrap(), Some(std::time::Duration::ZERO));
    });
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn quickack_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();

        socket.set_quickack(true).unwrap();
        assert!(socket.quickack().unwrap());

        socket.set_quickack(false).unwrap();
        assert!(!socket.quickack().unwrap());
    });
}

#[test]
fn nodelay_set_and_read_back_on_tcp_socket() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();

        socket.set_nodelay(true).unwrap();
        assert!(socket.nodelay().unwrap());

        socket.set_nodelay(false).unwrap();
        assert!(!socket.nodelay().unwrap());
    });
}

#[test]
fn ttl_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_ttl(48).unwrap();
        assert_eq!(socket.ttl().unwrap(), 48);
    });
}

#[test]
fn tos_v4_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_tos_v4(0x10).unwrap();
        assert_eq!(socket.tos_v4().unwrap(), 0x10);
    });
}

#[test]
fn tclass_v6_set_and_read_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Some sandboxed/CI environments have no IPv6 stack at all --
        // skip rather than fail on an environment limitation this test
        // isn't meant to exercise.
        let Ok(socket) = TcpSocket::new_v6() else {
            eprintln!("skipping: no IPv6 support in this environment");
            return;
        };
        socket.set_tclass_v6(0x20).unwrap();
        assert_eq!(socket.tclass_v6().unwrap(), 0x20);
    });
}

#[test]
fn tcp_socket_take_error_is_none_on_a_fresh_socket() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = TcpSocket::new_v4().unwrap();
        assert!(socket.take_error().unwrap().is_none());
    });
}

#[test]
fn tcp_stream_take_error_is_none_on_a_healthy_connection() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream
        });

        let client = TcpStream::connect(addr).await.unwrap();
        assert!(client.take_error().unwrap().is_none());

        let stream = server.await.unwrap();
        assert!(stream.take_error().unwrap().is_none());
    });
}

#[test]
fn tcp_stream_nodelay_getter_reflects_the_setter() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move { listener.accept().await.unwrap() });
        let client = TcpStream::connect(addr).await.unwrap();
        let (_stream, _peer) = server.await.unwrap();

        client.set_nodelay(true).unwrap();
        assert!(client.nodelay().unwrap());

        client.set_nodelay(false).unwrap();
        assert!(!client.nodelay().unwrap());
    });
}

#[cfg(unix)]
#[test]
fn from_std_stream_adopts_an_already_connected_socket() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 22];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let std_stream = std::net::TcpStream::connect(addr).unwrap();
        let socket = TcpSocket::from_std_stream(std_stream).unwrap();
        // Still a bare TcpSocket, not yet reactor-registered -- its own
        // options are still readable/writable on it directly.
        socket.set_nodelay(true).unwrap();
        assert!(socket.nodelay().unwrap());

        // Wrap it back into a std stream to actually exercise the
        // underlying connection, since TcpSocket itself has no read/
        // write methods (only connect()/listen() consume it into a
        // TcpStream/TcpListener that do) -- via IntoRawFd/FromRawFd,
        // since TcpSocket has no dedicated into_std of its own.
        use std::os::fd::{FromRawFd, IntoRawFd};
        let std_stream = unsafe { std::net::TcpStream::from_raw_fd(socket.into_raw_fd()) };
        std::io::Write::write_all(&mut &std_stream, b"via from_std_stream()!").unwrap();
        let mut buf = [0u8; 22];
        std::io::Read::read_exact(&mut &std_stream, &mut buf).unwrap();
        assert_eq!(&buf, b"via from_std_stream()!");

        server.await.unwrap();
    });
}
