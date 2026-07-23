#![cfg(unix)]
// `AF_UNIX` is Unix-only -- `UnixListener`/`UnixStream` are gated out of
// `rusty_tokio::io` entirely on Windows (see `io/mod.rs`'s docs), so this
// whole file is gated rather than every individual item.

use rusty_tokio::io::{AsyncReadExt, AsyncWriteExt, UnixListener, UnixStream};
use rusty_tokio::Runtime;

fn temp_socket_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rusty_tokio-test-{}-{}-{}.sock",
        std::process::id(),
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn unix_into_split_moves_owned_halves_into_separate_tasks() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    let path = temp_socket_path("into_split");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let client = UnixStream::connect(&path).await.unwrap();
        let (mut read_half, mut write_half) = client.into_split();

        let writer_task = rusty_tokio::spawn(async move { write_half.write_all(b"hello").await });

        let mut buf = [0u8; 5];
        read_half.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        writer_task.await.unwrap().unwrap();
        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unix_split_borrows_read_and_write_halves_from_one_task() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("split");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let mut client = UnixStream::connect(&path).await.unwrap();
        let (mut read_half, mut write_half) = client.split();

        write_half.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        read_half.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unix_echo_roundtrip() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("echo");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let n = stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf[..n]).await.unwrap();
        });

        let client = UnixStream::connect(&path).await.unwrap();
        client.write_all(b"hello unix").await.unwrap();
        let mut buf = [0u8; 64];
        client.read_exact(&mut buf[..10]).await.unwrap();
        assert_eq!(&buf[..10], b"hello unix");

        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn many_concurrent_unix_connections() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let path = temp_socket_path("many");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();

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
            let path = path.clone();
            clients.push(rusty_tokio::spawn(async move {
                let stream = UnixStream::connect(&path).await.unwrap();
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
    let _ = std::fs::remove_file(&path);
}

#[test]
fn stale_socket_file_is_reclaimed_on_rebind() {
    // A listener that's dropped without an explicit unlink leaves its
    // socket file behind on disk -- rustils' own `unix_listen` detects
    // that the path is stale (nothing is listening there anymore, via a
    // throwaway probe connect) and reclaims it rather than failing with
    // `AddrInUse`, the same behavior a re-run of a crashed daemon relies
    // on. This exercises that behavior through this crate's wrapper.
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("stale");
    rt.block_on(async {
        {
            let first = UnixListener::bind(&path).unwrap();
            drop(first);
        }
        assert!(path.exists(), "the stale socket file should still exist");

        // Rebinding at the same path should succeed by reclaiming the
        // stale file, not fail with AddrInUse.
        let second = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = second.accept().await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let client = UnixStream::connect(&path).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn a_live_listener_rejects_a_second_bind_at_the_same_path() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("live");
    rt.block_on(async {
        let _first = UnixListener::bind(&path).unwrap();
        match UnixListener::bind(&path) {
            Err(e) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::AddrInUse,
                "binding over a still-live listener's path should fail, not steal it"
            ),
            Ok(_) => panic!("binding over a still-live listener's path should fail, not steal it"),
        }
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unix_stream_raw_fd_roundtrip_preserves_functionality() {
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};

    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("stream_raw_fd");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();
        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let n = stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf[..n]).await.unwrap();
        });

        let client = UnixStream::connect(&path).await.unwrap();
        assert!(client.as_raw_fd() >= 0);
        let fd = client.into_raw_fd();
        let client = unsafe { UnixStream::from_raw_fd(fd) };

        client.write_all(b"roundtrip").await.unwrap();
        let mut buf = [0u8; 64];
        client.read_exact(&mut buf[..9]).await.unwrap();
        assert_eq!(&buf[..9], b"roundtrip");

        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unix_listener_raw_fd_roundtrip_can_still_accept() {
    use std::os::fd::{FromRawFd, IntoRawFd};

    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("listener_raw_fd");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();
        let fd = listener.into_raw_fd();
        let listener = unsafe { UnixListener::from_raw_fd(fd) };

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 16];
            let n = stream.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"still works");
        });

        let client = UnixStream::connect(&path).await.unwrap();
        client.write_all(b"still works").await.unwrap();
        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}
