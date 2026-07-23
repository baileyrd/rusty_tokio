#![cfg(unix)]

use rusty_tokio::io::{UnixDatagram, UnixListener, UnixSocket, UnixStream};
use rusty_tokio::Runtime;

fn temp_socket_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rusty_tokio-test-unix_socket-{}-{}-{}.sock",
        std::process::id(),
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn bind_then_listen_accepts_a_connection() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("bind-listen");
    rt.block_on(async {
        let socket = UnixSocket::new_stream().unwrap();
        socket.bind(&path).unwrap();
        let listener = socket.listen(128).unwrap();

        let client = rusty_tokio::spawn({
            let path = path.clone();
            async move {
                let stream = UnixStream::connect(&path).await.unwrap();
                stream.write_all(b"via UnixSocket::listen").await.unwrap();
            }
        });

        let (stream, _peer) = listener.accept().await.unwrap();
        let mut buf = [0u8; 22];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"via UnixSocket::listen");

        client.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn connect_reaches_a_real_listener() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("connect");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 23];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let socket = UnixSocket::new_stream().unwrap();
        let stream = socket.connect(&path).await.unwrap();
        stream.write_all(b"via UnixSocket::connect").await.unwrap();
        let mut buf = [0u8; 23];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"via UnixSocket::connect");

        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn new_datagram_then_bind_and_datagram_can_exchange_with_a_peer() {
    let rt = Runtime::new().unwrap();
    let path_a = temp_socket_path("datagram-a");
    let path_b = temp_socket_path("datagram-b");
    rt.block_on(async {
        let socket_a = UnixSocket::new_datagram().unwrap();
        socket_a.bind(&path_a).unwrap();
        let a = socket_a.datagram().unwrap();

        let b = UnixDatagram::bind(&path_b).unwrap();

        a.send_to(b"ping", &path_b).await.unwrap();
        let mut buf = [0u8; 32];
        let (n, peer) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");
        assert_eq!(peer.as_pathname(), Some(path_a.as_path()));
    });
    let _ = std::fs::remove_file(&path_a);
    let _ = std::fs::remove_file(&path_b);
}

#[test]
fn listen_fails_on_a_datagram_socket() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("listen-on-datagram");
    rt.block_on(async {
        let socket = UnixSocket::new_datagram().unwrap();
        socket.bind(&path).unwrap();
        let Err(err) = socket.listen(128) else {
            panic!("expected listen to fail on a datagram socket");
        };
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn connect_fails_on_a_datagram_socket() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("connect-on-datagram-target");
    rt.block_on(async {
        // A real listener to connect *at* -- irrelevant which kind, since
        // the error here comes from `socket`'s own type, checked before
        // the connect(2) call is even attempted.
        let _listener = UnixListener::bind(&path).unwrap();

        let socket = UnixSocket::new_datagram().unwrap();
        let Err(err) = socket.connect(&path).await else {
            panic!("expected connect to fail on a datagram socket");
        };
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn datagram_fails_on_a_stream_socket() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let socket = UnixSocket::new_stream().unwrap();
        let Err(err) = socket.datagram() else {
            panic!("expected datagram to fail on a stream socket");
        };
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    });
}
