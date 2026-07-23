use rusty_tokio::io::{TcpListener, TcpStream, UdpSocket};
use rusty_tokio::Runtime;
use std::io::{IoSlice, IoSliceMut};

#[cfg(unix)]
use rusty_tokio::io::{UnixListener, UnixStream};

#[test]
fn tcp_try_read_fails_would_block_then_succeeds_once_data_arrives() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            rusty_tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            stream.write_all(b"data").await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 4];
        let err = client.try_read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);

        client.readable().await.unwrap();
        let n = client.try_read(&mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"data");

        server.await.unwrap();
    });
}

#[test]
fn tcp_try_write_succeeds_once_writable() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello");
        });

        let client = TcpStream::connect(addr).await.unwrap();
        client.writable().await.unwrap();
        let n = client.try_write(b"hello").unwrap();
        assert_eq!(n, 5);

        server.await.unwrap();
    });
}

#[test]
fn tcp_try_read_vectored_scatters_across_buffers() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(b"0123456789").await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        client.readable().await.unwrap();

        let mut a = [0u8; 4];
        let mut b = [0u8; 6];
        let mut bufs = [IoSliceMut::new(&mut a), IoSliceMut::new(&mut b)];
        let mut total = 0;
        loop {
            match client.try_read_vectored(&mut bufs) {
                Ok(n) => {
                    total += n;
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    client.readable().await.unwrap();
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert_eq!(total, 10);
        assert_eq!(&a, b"0123");
        assert_eq!(&b, b"456789");

        server.await.unwrap();
    });
}

#[test]
fn tcp_try_write_vectored_gathers_from_buffers() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 10];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"0123456789");
        });

        let client = TcpStream::connect(addr).await.unwrap();
        client.writable().await.unwrap();

        let bufs = [IoSlice::new(b"0123"), IoSlice::new(b"456789")];
        let n = client.try_write_vectored(&bufs).unwrap();
        assert_eq!(n, 10);

        server.await.unwrap();
    });
}

#[test]
fn tcp_owned_split_halves_expose_try_read_and_try_write() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(b"owned").await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"pong");
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = client.into_split();

        // `readable`/`writable` aren't exposed on the split halves
        // themselves, so a short retry loop stands in for waiting on
        // readiness directly.
        let mut buf = [0u8; 5];
        let n = loop {
            match read_half.try_read(&mut buf) {
                Ok(n) => break n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    rusty_tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        };
        assert_eq!(n, 5);
        assert_eq!(&buf, b"owned");

        loop {
            match write_half.try_write(b"pong") {
                Ok(n) => {
                    assert_eq!(n, 4);
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    rusty_tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        server.await.unwrap();
    });
}

#[test]
fn udp_try_send_to_and_try_recv_from() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let a_addr = a.local_addr().unwrap();
        let b_addr = b.local_addr().unwrap();

        let err = b.try_recv_from(&mut [0u8; 8]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);

        let n = a.try_send_to(b"hello", b_addr).unwrap();
        assert_eq!(n, 5);

        b.readable().await.unwrap();
        let mut buf = [0u8; 8];
        let (n, from) = b.try_recv_from(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");
        assert_eq!(from, a_addr);
    });
}

#[test]
fn udp_try_send_and_try_recv_after_connect() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        a.connect(b.local_addr().unwrap()).unwrap();
        b.connect(a.local_addr().unwrap()).unwrap();

        let n = a.try_send(b"yo").unwrap();
        assert_eq!(n, 2);

        b.readable().await.unwrap();
        let mut buf = [0u8; 2];
        let n = b.try_recv(&mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf, b"yo");
    });
}

#[cfg(unix)]
#[test]
fn unix_stream_try_read_and_try_write() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!(
            "rusty_tokio_try_read_write_test_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("try.sock");
        let listener = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(b"unix-data").await.unwrap();
        });

        let client = UnixStream::connect(&path).await.unwrap();
        let mut buf = [0u8; 9];
        let n = loop {
            client.readable().await.unwrap();
            match client.try_read(&mut buf) {
                Ok(n) => break n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("unexpected error: {e}"),
            }
        };
        assert_eq!(n, 9);
        assert_eq!(&buf, b"unix-data");

        server.await.unwrap();
    });
}

#[cfg(unix)]
#[test]
fn unix_stream_try_read_vectored_and_try_write_vectored() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!(
            "rusty_tokio_try_read_write_vectored_test_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("try_vectored.sock");
        let listener = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.writable().await.unwrap();
            let bufs = [IoSlice::new(b"ab"), IoSlice::new(b"cd")];
            let n = stream.try_write_vectored(&bufs).unwrap();
            assert_eq!(n, 4);
        });

        let client = UnixStream::connect(&path).await.unwrap();
        let mut x = [0u8; 2];
        let mut y = [0u8; 2];
        let mut bufs = [IoSliceMut::new(&mut x), IoSliceMut::new(&mut y)];
        let n = loop {
            client.readable().await.unwrap();
            match client.try_read_vectored(&mut bufs) {
                Ok(n) => break n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("unexpected error: {e}"),
            }
        };
        assert_eq!(n, 4);
        assert_eq!(&x, b"ab");
        assert_eq!(&y, b"cd");

        server.await.unwrap();
    });
}
