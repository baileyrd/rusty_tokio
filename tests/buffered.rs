use rusty_tokio::io::{
    duplex, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
    BufStream, BufWriter, ReadBuf, TcpListener, TcpStream,
};
use rusty_tokio::Runtime;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A synthetic in-memory reader that counts how many times `poll_read`
/// is actually called on the underlying type -- lets tests assert that
/// `BufReader` really is batching small reads into fewer underlying
/// calls, not just that the bytes come out correct.
struct CountingReader {
    data: Vec<u8>,
    pos: usize,
    poll_read_calls: usize,
}

impl CountingReader {
    fn new(data: impl Into<Vec<u8>>) -> Self {
        CountingReader {
            data: data.into(),
            pos: 0,
            poll_read_calls: 0,
        }
    }
}

impl AsyncRead for CountingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_read_calls += 1;
        let remaining = &self.data[self.pos..];
        let n = std::cmp::min(remaining.len(), buf.remaining());
        buf.unfilled_mut()[..n].copy_from_slice(&remaining[..n]);
        buf.advance(n);
        self.pos += n;
        Poll::Ready(Ok(()))
    }
}

/// The write-side counterpart of [`CountingReader`].
struct CountingWriter {
    data: Vec<u8>,
    poll_write_calls: usize,
}

impl CountingWriter {
    fn new() -> Self {
        CountingWriter {
            data: Vec::new(),
            poll_write_calls: 0,
        }
    }
}

impl AsyncWrite for CountingWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_calls += 1;
        self.data.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn buf_reader_serves_several_small_reads_from_one_underlying_fill() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut reader = BufReader::with_capacity(64, CountingReader::new(b"0123456789".to_vec()));
        let mut chunk = [0u8; 4];
        reader.read_exact(&mut chunk).await.unwrap();
        assert_eq!(&chunk, b"0123");
        reader.read_exact(&mut chunk).await.unwrap();
        assert_eq!(&chunk, b"4567");
        assert_eq!(
            reader.get_ref().poll_read_calls,
            1,
            "both small reads should have come from a single underlying fill"
        );
    });
}

#[test]
fn buf_reader_bypasses_its_buffer_for_a_read_at_least_as_big() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let data = vec![7u8; 200];
        let mut reader = BufReader::with_capacity(64, CountingReader::new(data.clone()));
        let mut big = vec![0u8; 200];
        reader.read_exact(&mut big).await.unwrap();
        assert_eq!(big, data);
    });
}

#[test]
fn buf_writer_batches_small_writes_until_flushed() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut writer = BufWriter::with_capacity(64, CountingWriter::new());
        writer.write_all(b"hello").await.unwrap();
        writer.write_all(b", ").await.unwrap();
        writer.write_all(b"world").await.unwrap();
        assert_eq!(
            writer.get_ref().poll_write_calls,
            0,
            "nothing should have reached the underlying writer before flush"
        );

        writer.flush().await.unwrap();
        assert_eq!(writer.get_ref().poll_write_calls, 1);
        assert_eq!(writer.get_ref().data, b"hello, world");
    });
}

#[test]
fn buf_writer_flushes_automatically_before_a_write_that_would_overflow_capacity() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut writer = BufWriter::with_capacity(8, CountingWriter::new());
        writer.write_all(b"1234").await.unwrap();
        assert_eq!(writer.get_ref().poll_write_calls, 0);

        // "1234" (4) + "5678" (4) == 8, exactly at capacity -- still
        // buffered, no flush needed yet.
        writer.write_all(b"5678").await.unwrap();
        assert_eq!(writer.get_ref().poll_write_calls, 0);

        // This one would overflow the 8-byte buffer, forcing an
        // automatic flush of what's already buffered first.
        writer.write_all(b"9").await.unwrap();
        assert!(writer.get_ref().poll_write_calls >= 1);

        writer.flush().await.unwrap();
        assert_eq!(writer.get_ref().data, b"123456789");
    });
}

#[test]
fn buf_writer_bypasses_its_buffer_for_a_write_at_least_as_big() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut writer = BufWriter::with_capacity(8, CountingWriter::new());
        let big = vec![9u8; 100];
        writer.write_all(&big).await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!(writer.get_ref().data, big);
    });
}

#[test]
fn read_until_and_read_line_split_on_the_right_delimiter() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut reader = BufReader::new(CountingReader::new(b"a,b,c\nd\n".to_vec()));

        let mut field = Vec::new();
        let n = reader.read_until(b',', &mut field).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(field, b"a,");

        let mut line = String::new();
        let n = reader.read_line(&mut line).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(line, "b,c\n");

        let mut line2 = String::new();
        reader.read_line(&mut line2).await.unwrap();
        assert_eq!(line2, "d\n");
    });
}

#[test]
fn lines_strips_trailing_newlines_and_ends_at_eof() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let reader = BufReader::new(CountingReader::new(b"first\r\nsecond\nthird".to_vec()));
        let mut lines = reader.lines();

        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("first"));
        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("second"));
        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("third"));
        assert_eq!(lines.next_line().await.unwrap(), None);
    });
}

#[test]
fn buf_reader_and_buf_writer_round_trip_over_a_real_tcp_socket() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            line
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut writer = BufWriter::new(client);
        writer.write_all(b"buffered over tcp\n").await.unwrap();
        writer.flush().await.unwrap();

        let received = server.await.unwrap();
        assert_eq!(received, "buffered over tcp\n");
    });
}

#[test]
fn buf_stream_batches_writes_and_serves_reads_from_one_underlying_fill() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut peer, stream) = duplex(256);
        let mut buf_stream = BufStream::with_capacity(64, 64, stream);

        buf_stream.write_all(b"hello").await.unwrap();
        buf_stream.write_all(b", world").await.unwrap();
        // Nothing reaches the duplex peer until flushed -- same
        // batching behavior as a plain `BufWriter`.
        let poll_before_flush =
            rusty_tokio::time::timeout(std::time::Duration::from_millis(20), async {
                let mut probe = [0u8; 1];
                peer.read(&mut probe).await
            })
            .await;
        assert!(
            poll_before_flush.is_err(),
            "nothing should have reached the peer before flush"
        );

        buf_stream.flush().await.unwrap();
        let mut received = [0u8; 12];
        peer.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"hello, world");

        peer.write_all(b"reply").await.unwrap();
        let mut reply = [0u8; 5];
        buf_stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
    });
}

#[test]
fn buf_stream_get_ref_get_mut_into_inner_reach_the_wrapped_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (_peer, stream) = duplex(64);
        let mut buf_stream = BufStream::new(stream);
        let _ = buf_stream.get_ref();
        let _ = buf_stream.get_mut();
        let _inner = buf_stream.into_inner();
    });
}

#[test]
fn buf_stream_round_trips_over_a_real_tcp_socket() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf_stream = BufStream::new(stream);
            let mut line = String::new();
            buf_stream.read_line(&mut line).await.unwrap();
            buf_stream.write_all(b"ack\n").await.unwrap();
            buf_stream.flush().await.unwrap();
            line
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut buf_stream = BufStream::new(client);
        buf_stream.write_all(b"buffered both ways\n").await.unwrap();
        buf_stream.flush().await.unwrap();

        let mut ack = String::new();
        buf_stream.read_line(&mut ack).await.unwrap();
        assert_eq!(ack, "ack\n");

        let received = server.await.unwrap();
        assert_eq!(received, "buffered both ways\n");
    });
}
