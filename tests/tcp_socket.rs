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
