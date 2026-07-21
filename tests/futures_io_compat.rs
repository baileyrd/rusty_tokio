use rusty_tokio::io::{Compat, TcpListener, TcpStream};
use rusty_tokio::Runtime;
use std::future::poll_fn;
use std::pin::Pin;
use std::task::Context;

async fn futures_io_read(
    r: &mut (impl futures_io::AsyncRead + Unpin),
    buf: &mut [u8],
) -> std::io::Result<usize> {
    poll_fn(|cx: &mut Context<'_>| Pin::new(&mut *r).poll_read(cx, buf)).await
}

async fn futures_io_read_exact(
    r: &mut (impl futures_io::AsyncRead + Unpin),
    mut buf: &mut [u8],
) -> std::io::Result<()> {
    while !buf.is_empty() {
        let n = futures_io_read(r, buf).await?;
        assert!(n > 0, "unexpected eof");
        buf = &mut buf[n..];
    }
    Ok(())
}

async fn futures_io_write_all(
    w: &mut (impl futures_io::AsyncWrite + Unpin),
    mut buf: &[u8],
) -> std::io::Result<()> {
    while !buf.is_empty() {
        let n = poll_fn(|cx: &mut Context<'_>| Pin::new(&mut *w).poll_write(cx, buf)).await?;
        assert!(n > 0, "write returned 0");
        buf = &buf[n..];
    }
    Ok(())
}

#[test]
fn compat_wrapped_tcpstream_roundtrips_through_futures_io_traits() {
    // Proves the actual point of `Compat`: a `TcpStream` wrapped in it
    // is usable via *only* `futures_io`'s trait methods, the ones a
    // third-party codec/framing crate built against `futures-io` (not
    // this crate's own `AsyncRead`/`AsyncWrite`) would call.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut compat = Compat::new(stream);
            let mut buf = [0u8; 13];
            futures_io_read_exact(&mut compat, &mut buf).await.unwrap();
            futures_io_write_all(&mut compat, &buf).await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut compat = Compat::new(client);
        futures_io_write_all(&mut compat, b"hello compat!")
            .await
            .unwrap();
        let mut buf = [0u8; 13];
        futures_io_read_exact(&mut compat, &mut buf).await.unwrap();
        assert_eq!(&buf, b"hello compat!");

        server.await.unwrap();
    });
}

#[test]
fn compat_get_mut_and_into_inner_reach_the_wrapped_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = rusty_tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let compat = Compat::new(client);
        // Reach the inner TcpStream's own inherent methods through
        // get_ref/get_mut/into_inner -- Compat shouldn't hide the
        // wrapped type's own API, just add the futures_io traits.
        assert!(compat.get_ref().local_addr().is_ok());
        let client = compat.into_inner();
        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        server.await.unwrap();
    });
}
