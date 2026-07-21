use rusty_tokio::io::{copy, AsyncReadExt, AsyncWriteExt, TcpListener, TcpStream};
use rusty_tokio::Runtime;
use std::sync::Arc;

#[test]
fn asyncreadext_and_asyncwriteext_roundtrip_via_ufcs() {
    // `TcpStream` also has pre-existing inherent `read`/`write_all`
    // methods (taking `&self`), and inherent methods always win over
    // trait methods for plain dot-call syntax -- so `stream.read(...)`
    // on a concrete `TcpStream` would silently call the inherent method,
    // not the trait's. UFCS (`AsyncReadExt::read(...)`) sidesteps that
    // and unambiguously calls the trait method, which is what this test
    // means to exercise.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (mut stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 13];
            AsyncReadExt::read_exact(&mut stream, &mut buf)
                .await
                .unwrap();
            AsyncWriteExt::write_all(&mut stream, &buf).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        AsyncWriteExt::write_all(&mut client, b"hello traits")
            .await
            .unwrap();
        AsyncWriteExt::write_all(&mut client, b"!").await.unwrap();
        let mut buf = [0u8; 13];
        AsyncReadExt::read_exact(&mut client, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf, b"hello traits!");

        server.await.unwrap();
    });
}

#[test]
fn generic_function_over_asyncread_asyncwrite_dispatches_through_the_trait() {
    // Unlike calling `.read()`/`.write_all()` directly on a concrete
    // `TcpStream` (which the previous test's UFCS works around), a
    // function generic over `T: AsyncRead`/`AsyncWrite` has no inherent
    // methods to consider at all -- only the trait bound is visible --
    // so plain dot-call syntax inside it unambiguously means the trait.
    // This is the shape real generic code (this crate's own `copy`
    // included) actually uses these traits through.
    async fn echo_once<S>(stream: &mut S) -> std::io::Result<()>
    where
        S: rusty_tokio::io::AsyncRead + rusty_tokio::io::AsyncWrite + Unpin + Send,
    {
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await?;
        stream.write_all(&buf).await
    }

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (mut stream, _peer) = listener.accept().await.unwrap();
            echo_once(&mut stream).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        AsyncWriteExt::write_all(&mut client, b"ping")
            .await
            .unwrap();
        let mut buf = [0u8; 4];
        AsyncReadExt::read_exact(&mut client, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf, b"ping");

        server.await.unwrap();
    });
}

#[test]
fn shared_ref_impl_allows_concurrent_read_and_write_through_the_trait() {
    // Using `&TcpStream` (not an owned `&mut TcpStream`) through the
    // trait, from two different tasks at once -- the whole point of
    // implementing AsyncRead/AsyncWrite for `&TcpStream` rather than
    // only for owned `TcpStream`.
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let client = Arc::new(TcpStream::connect(addr).await.unwrap());
        let client_for_writer = client.clone();

        let writer_task = rusty_tokio::spawn(async move {
            let mut writer = &*client_for_writer;
            AsyncWriteExt::write_all(&mut writer, b"hello").await
        });

        let mut reader = &*client;
        let mut buf = [0u8; 5];
        // If the &TcpStream impl secretly needed exclusive access this
        // would deadlock against the concurrent write task above.
        AsyncReadExt::read_exact(&mut reader, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf, b"hello");

        writer_task.await.unwrap().unwrap();
        server.await.unwrap();
    });
}

#[test]
fn copy_streams_all_bytes_into_an_in_memory_sink() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let payload = vec![0xABu8; 200_000]; // several times the internal copy buffer size
        let payload_for_server = payload.clone();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(&payload_for_server).await.unwrap();
            // Half-close so the client's `copy` sees EOF instead of
            // hanging forever waiting for more.
            AsyncWriteExt::shutdown(&mut &stream).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut sink: Vec<u8> = Vec::new();
        let n = copy(&mut client, &mut sink).await.unwrap();

        assert_eq!(n as usize, payload.len());
        assert_eq!(sink, payload);
        server.await.unwrap();
    });
}

#[test]
fn read_to_end_reads_until_eof() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(b"hello, world!").await.unwrap();
            AsyncWriteExt::shutdown(&mut &stream).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut buf = Vec::new();
        // Pre-existing content should be appended to, not overwritten.
        buf.extend_from_slice(b"prefix:");
        let n = client.read_to_end(&mut buf).await.unwrap();

        assert_eq!(n, b"hello, world!".len());
        assert_eq!(buf, b"prefix:hello, world!");
        server.await.unwrap();
    });
}

#[test]
fn read_to_string_reads_valid_utf8_until_eof() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all("héllo".as_bytes()).await.unwrap();
            AsyncWriteExt::shutdown(&mut &stream).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut buf = String::new();
        let n = client.read_to_string(&mut buf).await.unwrap();

        assert_eq!(n, "héllo".len());
        assert_eq!(buf, "héllo");
        server.await.unwrap();
    });
}

#[test]
fn read_to_string_reports_invalid_utf8_without_modifying_the_buffer() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            stream.write_all(&[0xFF, 0xFE]).await.unwrap(); // not valid UTF-8
            AsyncWriteExt::shutdown(&mut &stream).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut buf = String::from("unchanged");
        let result = client.read_to_string(&mut buf).await;

        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(buf, "unchanged");
        server.await.unwrap();
    });
}

#[test]
fn write_vectored_writes_from_multiple_buffers() {
    // The default `poll_write_vectored` only ever writes from the first
    // non-empty buffer per call (see its doc comment), so getting
    // everything across both buffers sent takes a loop that advances
    // past however many bytes each call actually wrote -- the same
    // shape `write_all` already needs for a single buffer.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 12];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello, world");
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut first: &[u8] = b"hello";
        let mut second: &[u8] = b", world";
        while !first.is_empty() || !second.is_empty() {
            let bufs = [std::io::IoSlice::new(first), std::io::IoSlice::new(second)];
            let mut n = client.write_vectored(&bufs).await.unwrap();
            assert!(n > 0, "write_vectored made no progress");
            let take = n.min(first.len());
            first = &first[take..];
            n -= take;
            let take = n.min(second.len());
            second = &second[take..];
        }

        server.await.unwrap();
    });
}
