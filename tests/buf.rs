use bytes::{Buf, BufMut, BytesMut};
use rusty_tokio::io::{self, AsyncReadExt, AsyncWriteExt, TcpListener, TcpStream, UdpSocket};
use rusty_tokio::Runtime;

#[test]
fn read_buf_fills_a_bytesmut_and_advances_its_len() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut a, mut b) = io::duplex(64);
        a.write_all(b"hello buf").await.unwrap();

        let mut dst = BytesMut::with_capacity(64);
        let n = b.read_buf(&mut dst).await.unwrap();
        assert_eq!(n, 9);
        assert_eq!(&dst[..], b"hello buf");
    });
}

#[test]
fn read_buf_returns_zero_immediately_when_the_buffer_has_no_capacity_left() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (_a, mut b) = io::duplex(64);
        // Unlike `BytesMut` (which auto-grows -- `remaining_mut()` is
        // effectively unbounded), a plain `&mut [u8]` has a true fixed
        // capacity that actually reaches zero once fully written.
        let mut storage = [0u8; 4];
        let mut dst: &mut [u8] = &mut storage;
        dst.put_slice(&[1, 2, 3, 4]);
        assert_eq!(dst.remaining_mut(), 0);

        let n = b.read_buf(&mut dst).await.unwrap();
        assert_eq!(n, 0);
    });
}

#[test]
fn read_buf_can_be_called_repeatedly_to_accumulate_a_larger_message() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut a, mut b) = io::duplex(4096);
        let payload = vec![7u8; 20_000];
        let write_task = {
            let payload = payload.clone();
            rusty_tokio::spawn(async move {
                a.write_all(&payload).await.unwrap();
            })
        };

        let mut dst = BytesMut::with_capacity(20_000);
        while dst.len() < payload.len() {
            let n = b.read_buf(&mut dst).await.unwrap();
            assert!(n > 0, "should not hit EOF before the full payload arrives");
        }
        assert_eq!(&dst[..], &payload[..]);
        write_task.await.unwrap();
    });
}

#[test]
fn write_buf_writes_one_chunk_and_advances_the_source_buf() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut a, mut b) = io::duplex(64);
        let mut src = bytes::Bytes::from_static(b"chunked");
        let n = a.write_buf(&mut src).await.unwrap();
        assert_eq!(n, 7);
        assert_eq!(src.remaining(), 0);

        let mut recv = [0u8; 7];
        b.read_exact(&mut recv).await.unwrap();
        assert_eq!(&recv, b"chunked");
    });
}

#[test]
fn write_all_buf_drains_the_whole_buffer_even_past_one_duplex_chunk() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut a, mut b) = io::duplex(16);
        let payload = vec![9u8; 5000];
        let mut src = bytes::Bytes::from(payload.clone());

        let write_task = rusty_tokio::spawn(async move {
            a.write_all_buf(&mut src).await.unwrap();
            assert_eq!(src.remaining(), 0);
        });

        let mut received = Vec::new();
        while received.len() < payload.len() {
            let mut chunk = [0u8; 512];
            let n = b.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            received.extend_from_slice(&chunk[..n]);
        }
        assert_eq!(received, payload);
        write_task.await.unwrap();
    });
}

#[test]
fn try_read_buf_fills_a_bytesmut_once_readable() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(b"try read buf").await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut dst = BytesMut::with_capacity(64);
        let n = loop {
            client.readable().await.unwrap();
            match client.try_read_buf(&mut dst) {
                Ok(n) => break n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("unexpected error: {e}"),
            }
        };
        assert_eq!(n, 12);
        assert_eq!(&dst[..], b"try read buf");

        server.await.unwrap();
    });
}

#[test]
fn recv_buf_from_fills_a_bytesmut_and_reports_the_sender() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let a_addr = a.local_addr().unwrap();
        let b_addr = b.local_addr().unwrap();

        a.send_to(b"datagram payload", b_addr).await.unwrap();

        let mut dst = BytesMut::with_capacity(64);
        let (n, from) = b.recv_buf_from(&mut dst).await.unwrap();
        assert_eq!(n, 16);
        assert_eq!(&dst[..], b"datagram payload");
        assert_eq!(from, a_addr);
    });
}

#[test]
fn recv_buf_fills_a_bytesmut_after_connect() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        a.connect(b.local_addr().unwrap()).unwrap();
        b.connect(a.local_addr().unwrap()).unwrap();

        a.send(b"connected payload").await.unwrap();

        let mut dst = BytesMut::with_capacity(64);
        let n = b.recv_buf(&mut dst).await.unwrap();
        assert_eq!(n, 17);
        assert_eq!(&dst[..], b"connected payload");
    });
}

#[test]
fn recv_buf_from_truncates_to_the_buffers_remaining_capacity_like_a_real_datagram_read() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b_addr = b.local_addr().unwrap();

        a.send_to(b"0123456789", b_addr).await.unwrap();

        // Only 4 bytes of capacity -- the rest of the 10-byte datagram
        // must be discarded, matching real `recvfrom(2)` truncation
        // semantics, not held back for a later read.
        let mut storage = [0u8; 4];
        let mut dst: &mut [u8] = &mut storage;
        let (n, _from) = b.recv_buf_from(&mut dst).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(&storage, b"0123");
    });
}
