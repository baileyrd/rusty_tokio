use rusty_tokio::io::{
    copy_bidirectional, copy_bidirectional_with_sizes, AsyncRead, AsyncReadExt, AsyncWrite,
    AsyncWriteExt, ReadBuf, TcpListener, TcpStream,
};
use rusty_tokio::Runtime;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

#[test]
fn copy_bidirectional_relays_both_directions_with_independent_half_close() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let client_listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let client_addr = client_listener.local_addr().unwrap();
        let server_listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let server_addr = server_listener.local_addr().unwrap();

        // Client sends "ping" then half-closes its write side while
        // still expecting a response.
        let client = rusty_tokio::spawn(async move {
            let mut stream = TcpStream::connect(client_addr).await.unwrap();
            stream.write_all(b"ping").await.unwrap();
            AsyncWriteExt::shutdown(&mut stream).await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            received
        });

        // Server reads until EOF (sees the client's half-close relayed
        // through), then sends "pong" and half-closes its own write side.
        let server = rusty_tokio::spawn(async move {
            let (mut stream, _peer) = server_listener.accept().await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            stream.write_all(b"pong").await.unwrap();
            AsyncWriteExt::shutdown(&mut stream).await.unwrap();
            received
        });

        let (mut relay_a, _peer) = client_listener.accept().await.unwrap();
        let mut relay_b = TcpStream::connect(server_addr).await.unwrap();

        let (a_to_b, b_to_a) = copy_bidirectional(&mut relay_a, &mut relay_b)
            .await
            .unwrap();
        assert_eq!(a_to_b, 4, "client -> server byte count (\"ping\")");
        assert_eq!(b_to_a, 4, "server -> client byte count (\"pong\")");

        assert_eq!(server.await.unwrap(), b"ping");
        assert_eq!(client.await.unwrap(), b"pong");
    });
}

#[test]
fn copy_bidirectional_with_sizes_relays_correctly_with_tiny_asymmetric_buffers() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let client_listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let client_addr = client_listener.local_addr().unwrap();
        let server_listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let server_addr = server_listener.local_addr().unwrap();

        let client = rusty_tokio::spawn(async move {
            let mut stream = TcpStream::connect(client_addr).await.unwrap();
            // Bigger than the 1-byte a_to_b buffer below, so relaying it
            // correctly requires looping over several small reads/writes
            // rather than moving it in one shot.
            stream
                .write_all(b"a longer message than one byte")
                .await
                .unwrap();
            AsyncWriteExt::shutdown(&mut stream).await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            received
        });

        let server = rusty_tokio::spawn(async move {
            let (mut stream, _peer) = server_listener.accept().await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            stream.write_all(b"ok").await.unwrap();
            AsyncWriteExt::shutdown(&mut stream).await.unwrap();
            received
        });

        let (mut relay_a, _peer) = client_listener.accept().await.unwrap();
        let mut relay_b = TcpStream::connect(server_addr).await.unwrap();

        // Deliberately tiny, and different in each direction, to
        // exercise the "with_sizes" part specifically (not just that
        // some default size relays correctly).
        let (a_to_b, b_to_a) = copy_bidirectional_with_sizes(&mut relay_a, &mut relay_b, 1, 2)
            .await
            .unwrap();
        assert_eq!(a_to_b, 30);
        assert_eq!(b_to_a, 2);

        assert_eq!(server.await.unwrap(), b"a longer message than one byte");
        assert_eq!(client.await.unwrap(), b"ok");
    });
}

/// A reader that yields `data` once (on its very first poll) and then
/// EOF forever after -- used as the "has data ready" side of the error
/// propagation test below, where the *other* direction's write always
/// fails immediately.
struct OnceReader {
    data: Option<Vec<u8>>,
}

impl AsyncRead for OnceReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(data) = self.data.take() {
            let n = std::cmp::min(data.len(), buf.remaining());
            buf.unfilled_mut()[..n].copy_from_slice(&data[..n]);
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for OnceReader {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// A duplex whose read side never resolves (always `Pending`) and whose
/// write side always fails immediately -- the "broken" half of the
/// error-propagation test.
struct BrokenDuplex;

impl AsyncRead for BrokenDuplex {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for BrokenDuplex {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::other("boom")))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn copy_bidirectional_propagates_an_error_without_waiting_on_a_permanently_pending_direction() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // `a_to_b` (read BrokenDuplex, write OnceReader) can never make
        // progress -- `BrokenDuplex::poll_read` is permanently Pending.
        // `b_to_a` (read OnceReader, write BrokenDuplex) fails on its
        // very first write. `copy_bidirectional` must return that error
        // immediately rather than waiting for `a_to_b` to ever resolve.
        let mut a = BrokenDuplex;
        let mut b = OnceReader {
            data: Some(b"data".to_vec()),
        };

        let result = rusty_tokio::time::timeout(
            std::time::Duration::from_secs(2),
            copy_bidirectional(&mut a, &mut b),
        )
        .await
        .expect("copy_bidirectional should not hang waiting on the permanently-pending direction");

        let err = result.expect_err("the broken direction's write should fail");
        assert_eq!(err.to_string(), "boom");
    });
}
