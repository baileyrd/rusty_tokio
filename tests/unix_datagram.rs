use rusty_tokio::io::UnixDatagram;
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
fn send_and_recv_via_bound_paths() {
    let rt = Runtime::new().unwrap();
    let a_path = temp_socket_path("dgram-a");
    let b_path = temp_socket_path("dgram-b");
    rt.block_on(async {
        let a = UnixDatagram::bind(&a_path).unwrap();
        let b = UnixDatagram::bind(&b_path).unwrap();

        let b_path_for_task = b_path.clone();
        let a_path_for_task = a_path.clone();
        let responder = rusty_tokio::spawn(async move {
            let mut buf = [0u8; 32];
            let (n, peer) = b.recv_from(&mut buf).await.unwrap();
            assert_eq!(peer.as_pathname(), Some(a_path_for_task.as_path()));
            b.send_to(&buf[..n], peer.as_pathname().unwrap())
                .await
                .unwrap();
        });

        a.send_to(b"ping", &b_path_for_task).await.unwrap();
        let mut buf = [0u8; 32];
        let (n, peer) = a.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");
        assert_eq!(peer.as_pathname(), Some(b_path_for_task.as_path()));

        responder.await.unwrap();
    });
    std::fs::remove_file(&a_path).ok();
    std::fs::remove_file(&b_path).ok();
}

#[test]
fn connect_allows_send_and_recv_without_addressing() {
    let rt = Runtime::new().unwrap();
    let a_path = temp_socket_path("connect-a");
    let b_path = temp_socket_path("connect-b");
    rt.block_on(async {
        let a = UnixDatagram::bind(&a_path).unwrap();
        let b = UnixDatagram::bind(&b_path).unwrap();

        a.connect(&b_path).unwrap();
        b.connect(&a_path).unwrap();

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
    std::fs::remove_file(&a_path).ok();
    std::fs::remove_file(&b_path).ok();
}

#[test]
fn an_unbound_socket_can_send_to_a_bound_peer() {
    let rt = Runtime::new().unwrap();
    let peer_path = temp_socket_path("unbound-peer");
    rt.block_on(async {
        let peer = UnixDatagram::bind(&peer_path).unwrap();
        let sender = UnixDatagram::unbound().unwrap();

        sender
            .send_to(b"hi from unbound", &peer_path)
            .await
            .unwrap();
        let mut buf = [0u8; 32];
        let (n, from) = peer.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hi from unbound");
        // An unbound sender has no path of its own to report.
        assert_eq!(from.as_pathname(), None);
    });
    std::fs::remove_file(&peer_path).ok();
}

#[test]
fn local_addr_reports_the_bound_path() {
    let rt = Runtime::new().unwrap();
    let path = temp_socket_path("local-addr");
    rt.block_on(async {
        let socket = UnixDatagram::bind(&path).unwrap();
        let addr = socket.local_addr().unwrap();
        assert_eq!(addr.as_pathname(), Some(path.as_path()));
    });
    std::fs::remove_file(&path).ok();
}
