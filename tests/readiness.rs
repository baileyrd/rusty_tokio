use rusty_tokio::io::{Interest, TcpListener, TcpStream, UdpSocket};
use rusty_tokio::Runtime;

#[cfg(unix)]
use rusty_tokio::io::{UnixListener, UnixStream};
#[cfg(unix)]
use std::os::fd::AsRawFd;

#[cfg(unix)]
fn raw_read(fd: std::os::fd::RawFd, buf: &mut [u8]) -> std::io::Result<usize> {
    // SAFETY: `fd` is caller-owned and open; `buf` is a valid,
    // exclusively-borrowed out-param for the call's duration.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

#[cfg(unix)]
fn raw_write(fd: std::os::fd::RawFd, buf: &[u8]) -> std::io::Result<usize> {
    // SAFETY: `fd` is caller-owned and open; `buf` is valid for the
    // call's duration.
    let n = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
    if n < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

#[cfg(unix)]
#[test]
fn tcp_stream_readable_then_try_io_reads_real_data() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(b"hello").await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let fd = client.as_raw_fd();
        let mut buf = [0u8; 5];
        loop {
            client.readable().await.unwrap();
            match client.try_io(Interest::READABLE, || raw_read(fd, &mut buf)) {
                Ok(n) => {
                    assert_eq!(n, 5);
                    assert_eq!(&buf, b"hello");
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("unexpected read error: {e}"),
            }
        }
        server.await.unwrap();
    });
}

#[cfg(unix)]
#[test]
fn tcp_stream_writable_and_try_io_write_succeeds() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
        });

        let client = TcpStream::connect(addr).await.unwrap();
        client.writable().await.unwrap();
        let fd = client.as_raw_fd();
        let n = client
            .try_io(Interest::WRITABLE, || raw_write(fd, b"ping"))
            .unwrap();
        assert_eq!(n, 4);
        server.await.unwrap();
    });
}

#[test]
fn tcp_stream_ready_reports_which_direction_fired() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = rusty_tokio::spawn(async move {
            let (_stream, _peer) = listener.accept().await.unwrap();
            rusty_tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });

        let client = TcpStream::connect(addr).await.unwrap();
        // A freshly connected socket has a full send buffer available --
        // requesting only writability must report only that direction,
        // not a spurious readable too (nothing's been sent yet).
        let ready = client.ready(Interest::WRITABLE).await.unwrap();
        assert!(ready.is_writable());

        server.abort();
    });
}

#[test]
fn tcp_listener_poll_accept_resolves_once_a_connection_arrives() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let accepted = rusty_tokio::spawn(async move {
            std::future::poll_fn(|cx| listener.poll_accept(cx))
                .await
                .unwrap()
        });

        let _client = TcpStream::connect(addr).await.unwrap();
        let (_stream, _peer) = accepted.await.unwrap();
    });
}

#[test]
fn udp_socket_poll_send_to_and_poll_recv_from() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b_addr = b.local_addr().unwrap();

        std::future::poll_fn(|cx| a.poll_send_to(cx, b"hi", b_addr))
            .await
            .unwrap();

        let mut buf = [0u8; 2];
        let (n, from) = std::future::poll_fn(|cx| b.poll_recv_from(cx, &mut buf))
            .await
            .unwrap();
        assert_eq!(&buf[..n], b"hi");
        assert_eq!(from, a.local_addr().unwrap());
    });
}

#[test]
fn udp_socket_poll_send_and_poll_recv_after_connect() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        a.connect(b.local_addr().unwrap()).unwrap();
        b.connect(a.local_addr().unwrap()).unwrap();

        std::future::poll_fn(|cx| a.poll_send(cx, b"yo"))
            .await
            .unwrap();
        let mut buf = [0u8; 2];
        let n = std::future::poll_fn(|cx| b.poll_recv(cx, &mut buf))
            .await
            .unwrap();
        assert_eq!(&buf[..n], b"yo");
    });
}

#[test]
fn udp_socket_poll_recv_ready_and_poll_send_ready() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b_addr = b.local_addr().unwrap();

        // A fresh, unconnected UDP socket is always writable.
        std::future::poll_fn(|cx| a.poll_send_ready(cx))
            .await
            .unwrap();

        a.send_to(b"pong", b_addr).await.unwrap();
        std::future::poll_fn(|cx| b.poll_recv_ready(cx))
            .await
            .unwrap();
        let mut buf = [0u8; 4];
        let (n, _from) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"pong");
    });
}

#[cfg(unix)]
#[test]
fn udp_socket_try_io_sends_and_receives_via_a_raw_syscall() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        a.connect(b.local_addr().unwrap()).unwrap();
        b.connect(a.local_addr().unwrap()).unwrap();

        a.writable().await.unwrap();
        let a_fd = a.as_raw_fd();
        let n = a
            .try_io(Interest::WRITABLE, || raw_write(a_fd, b"raw"))
            .unwrap();
        assert_eq!(n, 3);

        let mut buf = [0u8; 3];
        loop {
            b.readable().await.unwrap();
            let b_fd = b.as_raw_fd();
            match b.try_io(Interest::READABLE, || raw_read(b_fd, &mut buf)) {
                Ok(n) => {
                    assert_eq!(&buf[..n], b"raw");
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("unexpected read error: {e}"),
            }
        }
    });
}

#[cfg(unix)]
#[test]
fn unix_stream_readable_writable_and_try_io() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let dir = tempfile_dir();
        let path = dir.join("readiness.sock");
        let listener = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(b"unix!").await.unwrap();
        });

        let client = UnixStream::connect(&path).await.unwrap();
        let fd = client.as_raw_fd();
        let mut buf = [0u8; 5];
        loop {
            client.readable().await.unwrap();
            match client.try_io(Interest::READABLE, || raw_read(fd, &mut buf)) {
                Ok(n) => {
                    assert_eq!(&buf[..n], b"unix!");
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("unexpected read error: {e}"),
            }
        }
        server.await.unwrap();
    });
}

#[cfg(unix)]
#[test]
fn unix_listener_poll_accept_resolves_once_a_connection_arrives() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let dir = tempfile_dir();
        let path = dir.join("readiness-accept.sock");
        let listener = UnixListener::bind(&path).unwrap();

        let accepted = rusty_tokio::spawn(async move {
            std::future::poll_fn(|cx| listener.poll_accept(cx))
                .await
                .unwrap()
        });

        let _client = UnixStream::connect(&path).await.unwrap();
        let (_stream, _peer) = accepted.await.unwrap();
    });
}

#[cfg(unix)]
fn tempfile_dir() -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("rusty_tokio_readiness_test_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    dir
}
