#![cfg(unix)]
// `AF_UNIX` is Unix-only -- `UnixListener`/`UnixStream` are gated out of
// `rusty_tokio::io` entirely on Windows (see `io/mod.rs`'s docs), so this
// whole file is gated rather than every individual item.

use rusty_tokio::io::{AsyncReadExt, AsyncWriteExt, UnixListener, UnixSocketAddr, UnixStream};
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
fn unix_reunite_succeeds_for_halves_from_the_same_stream_and_it_still_works() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("reunite-ok");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let client = UnixStream::connect(&path).await.unwrap();
        let (read_half, write_half) = client.into_split();
        let reunited = read_half.reunite(write_half).unwrap();

        reunited.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        reunited.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unix_reunite_fails_for_halves_from_different_streams_and_hands_them_back() {
    let rt = Runtime::new().unwrap();
    // Two separate listener paths (rather than one listener accepting
    // both connections) so which accepted stream corresponds to which
    // client connection is deterministic by construction, not by
    // accept-ordering.
    let path_a = temp_socket_path("reunite-mismatch-a");
    let path_b = temp_socket_path("reunite-mismatch-b");
    rt.block_on(async {
        let _listener_a = UnixListener::bind(&path_a).unwrap();
        let listener_b = UnixListener::bind(&path_b).unwrap();

        // `a`'s connection is deliberately never `accept()`-ed here --
        // `connect` below already completed the handshake, so it stays
        // a live, open connection from the client's perspective for as
        // long as `listener_a` itself stays alive (kept alive in this
        // outer scope, not dropped until the whole test body finishes
        // -- dropping the *listener* while a connection still sits
        // unaccepted in its backlog resets that connection, which
        // would defeat the point here).
        let server = rusty_tokio::spawn(async move {
            let (b, _peer) = listener_b.accept().await.unwrap();
            let mut buf = [0u8; 2];
            b.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hi");
        });

        let a = UnixStream::connect(&path_a).await.unwrap();
        let b = UnixStream::connect(&path_b).await.unwrap();
        let (read_a, _write_a) = a.into_split();
        let (_read_b, write_b) = b.into_split();

        let Err(err) = read_a.reunite(write_b) else {
            panic!("expected reunite to fail for halves from different streams");
        };
        let (read_a, mut write_b) = (err.0, err.1);
        write_b.write_all(b"hi").await.unwrap();
        assert_eq!(
            read_a.try_read(&mut [0u8; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "read_a should still be a live, functioning half with nothing sent to it"
        );

        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path_a);
    let _ = std::fs::remove_file(&path_b);
}

#[test]
fn unix_owned_halves_as_ref_exposes_the_underlying_stream() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("as-ref");
    rt.block_on(async {
        let _listener = UnixListener::bind(&path).unwrap();
        let client = UnixStream::connect(&path).await.unwrap();
        let (read_half, write_half) = client.into_split();

        // A connecting client never `bind`s its own end, so both halves'
        // local address is the unnamed address -- `UnixSocketAddr`
        // itself isn't `PartialEq` (nor is the `std::os::unix::net::
        // SocketAddr` it wraps), so this checks `is_unnamed()` rather
        // than comparing the two addresses directly.
        let read_local = AsRef::<UnixStream>::as_ref(&read_half)
            .local_addr()
            .unwrap();
        let write_local = AsRef::<UnixStream>::as_ref(&write_half)
            .local_addr()
            .unwrap();
        assert!(read_local.is_unnamed());
        assert!(write_local.is_unnamed());
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
fn pair_gives_two_ends_already_connected_to_each_other() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (a, b) = UnixStream::pair().unwrap();

        let task = rusty_tokio::spawn(async move {
            let mut buf = [0u8; 4];
            b.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            b.write_all(b"pong").await.unwrap();
        });

        a.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        a.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");

        task.await.unwrap();
    });
}

#[test]
fn pair_ends_report_the_unnamed_address_on_both_sides() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (a, b) = UnixStream::pair().unwrap();
        // `socketpair(2)` sockets have no filesystem path -- neither
        // end ever `bind`s one, unlike a `connect`/`accept` pair
        // through a real `UnixListener`.
        assert!(a.local_addr().unwrap().is_unnamed());
        assert!(a.peer_addr().unwrap().is_unnamed());
        assert!(b.local_addr().unwrap().is_unnamed());
        assert!(b.peer_addr().unwrap().is_unnamed());
    });
}

#[test]
fn bind_addr_then_connect_addr_round_trip_over_a_pathname() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("bind-addr-pathname");
    rt.block_on(async {
        let addr = UnixSocketAddr::from_pathname(&path).unwrap();
        assert_eq!(addr.as_pathname(), Some(path.as_path()));
        assert!(!addr.is_unnamed());

        let listener = UnixListener::bind_addr(&addr).unwrap();
        assert_eq!(
            listener.local_addr().unwrap().as_pathname(),
            Some(path.as_path())
        );

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let addr = UnixSocketAddr::from_pathname(&path).unwrap();
        let client = UnixStream::connect_addr(&addr).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        server.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
#[cfg(any(target_os = "linux", target_os = "android"))]
fn bind_addr_then_connect_addr_round_trip_over_an_abstract_name() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Unique per test run (and per repeated run of this same test)
        // the same way `temp_socket_path` is for real filesystem paths --
        // an abstract name is a global, kernel-wide namespace, not
        // scoped to a directory the way a temp path is.
        let name = format!(
            "rusty_tokio-test-abstract-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let addr = UnixSocketAddr::from_abstract_name(name.as_bytes()).unwrap();
        assert_eq!(addr.as_abstract_name(), Some(name.as_bytes()));
        assert_eq!(addr.as_pathname(), None);
        assert!(!addr.is_unnamed());

        let listener = UnixListener::bind_addr(&addr).unwrap();
        assert_eq!(
            listener.local_addr().unwrap().as_abstract_name(),
            Some(name.as_bytes())
        );

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let addr = UnixSocketAddr::from_abstract_name(name.as_bytes()).unwrap();
        let client = UnixStream::connect_addr(&addr).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        server.await.unwrap();
    });
}

#[test]
fn unix_take_error_is_none_on_healthy_sockets() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("take_error");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();
        assert!(listener.take_error().unwrap().is_none());

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream
        });

        let client = UnixStream::connect(&path).await.unwrap();
        assert!(client.take_error().unwrap().is_none());

        let stream = server.await.unwrap();
        assert!(stream.take_error().unwrap().is_none());
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unix_peer_cred_reports_this_same_process_on_both_ends() {
    // Both ends of this connection are the current process (it both
    // `connect`s and `accept`s), so the peer credentials reported on
    // either end are exactly this process's own -- verifiable directly
    // against `libc::getuid`/`getgid`/`getpid` rather than needing a
    // separate real peer process.
    let expected_uid = unsafe { libc::getuid() };
    let expected_gid = unsafe { libc::getgid() };
    let expected_pid = std::process::id() as i32;

    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("peer_cred");
    rt.block_on(async {
        let listener = UnixListener::bind(&path).unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream
        });

        let client = UnixStream::connect(&path).await.unwrap();
        let client_cred = client.peer_cred().unwrap();
        assert_eq!(client_cred.uid(), expected_uid);
        assert_eq!(client_cred.gid(), expected_gid);
        assert_eq!(client_cred.pid(), Some(expected_pid));

        let stream = server.await.unwrap();
        let server_cred = stream.peer_cred().unwrap();
        assert_eq!(server_cred.uid(), expected_uid);
        assert_eq!(server_cred.gid(), expected_gid);
        assert_eq!(server_cred.pid(), Some(expected_pid));
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
