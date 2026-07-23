use rusty_tokio::io::{TcpListener, TcpStream, UdpSocket};
use rusty_tokio::Runtime;
use std::time::Duration;

#[test]
fn tcp_peek_sees_data_without_consuming_it() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(b"hello").await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut peek_buf = [0u8; 5];
        let n = client.peek(&mut peek_buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&peek_buf, b"hello");

        // A real read afterward still sees the whole thing -- peek
        // didn't consume anything.
        let mut read_buf = [0u8; 5];
        client.read_exact(&mut read_buf).await.unwrap();
        assert_eq!(&read_buf, b"hello");

        server.await.unwrap();
    });
}

#[test]
fn tcp_poll_peek_resolves_once_data_arrives() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            rusty_tokio::time::sleep(Duration::from_millis(20)).await;
            stream.write_all(b"peek").await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 4];
        let n = std::future::poll_fn(|cx| client.poll_peek(cx, &mut buf))
            .await
            .unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"peek");

        server.await.unwrap();
    });
}

#[test]
fn tcp_try_peek_fails_would_block_before_data_arrives_then_succeeds_after() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            rusty_tokio::time::sleep(Duration::from_millis(50)).await;
            stream.write_all(b"late").await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 4];
        let err = client.try_peek(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);

        client.readable().await.unwrap();
        let n = client.try_peek(&mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"late");

        server.await.unwrap();
    });
}

#[test]
fn udp_peek_from_sees_the_datagram_without_dequeuing_it() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let a_addr = a.local_addr().unwrap();

        a.send_to(b"datagram", b.local_addr().unwrap())
            .await
            .unwrap();

        let mut peek_buf = [0u8; 8];
        let (n, from) = b.peek_from(&mut peek_buf).await.unwrap();
        assert_eq!(n, 8);
        assert_eq!(&peek_buf, b"datagram");
        assert_eq!(from, a_addr);

        // The datagram is still there for a real recv_from afterward.
        let mut recv_buf = [0u8; 8];
        let (n2, from2) = b.recv_from(&mut recv_buf).await.unwrap();
        assert_eq!(n2, 8);
        assert_eq!(&recv_buf, b"datagram");
        assert_eq!(from2, a_addr);
    });
}

#[test]
fn udp_peek_sender_reports_the_source_without_consuming_the_datagram() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let a_addr = a.local_addr().unwrap();

        a.send_to(b"who's there", b.local_addr().unwrap())
            .await
            .unwrap();

        let sender = b.peek_sender().await.unwrap();
        assert_eq!(sender, a_addr);

        let mut buf = [0u8; 11];
        let (n, from) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"who's there");
        assert_eq!(from, a_addr);
    });
}

#[test]
fn udp_try_peek_from_fails_would_block_before_a_datagram_arrives() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut buf = [0u8; 8];
        let err = b.try_peek_from(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    });
}

#[test]
fn udp_try_peek_sender_fails_would_block_before_a_datagram_arrives() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let err = b.try_peek_sender().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    });
}

#[test]
fn udp_poll_peek_from_resolves_once_a_datagram_arrives() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let a_addr = a.local_addr().unwrap();
        let b_addr = b.local_addr().unwrap();

        let receiver = rusty_tokio::spawn(async move {
            let mut buf = [0u8; 2];
            std::future::poll_fn(|cx| b.poll_peek_from(cx, &mut buf))
                .await
                .unwrap()
        });

        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        a.send_to(b"hi", b_addr).await.unwrap();

        let (n, from) = receiver.await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(from, a_addr);
    });
}
