use rusty_tokio::io::{self, AsyncReadExt, AsyncWriteExt, DuplexStream};
use rusty_tokio::Runtime;

/// A `DuplexStream` whose peer has already written `data` and shut down,
/// so reading it drains `data` and then reports EOF -- a simple stand-in
/// for "any `AsyncRead` source", since this crate has no `AsyncRead for
/// &[u8]` impl to reach for directly.
async fn reader_with(data: &[u8]) -> DuplexStream {
    let (mut writer, reader) = io::duplex(data.len().max(1));
    let data = data.to_vec();
    rusty_tokio::spawn(async move {
        writer.write_all(&data).await.unwrap();
        writer.shutdown().await.unwrap();
    });
    reader
}

#[test]
fn chain_reads_the_first_reader_fully_then_the_second() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let first = reader_with(b"hello ").await;
        let second = reader_with(b"world").await;
        let mut chained = first.chain(second);

        let mut out = Vec::new();
        chained.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello world");
    });
}

#[test]
fn chain_get_ref_get_mut_into_inner_reach_both_readers() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let first = reader_with(b"a").await;
        let second = reader_with(b"b").await;
        let mut chained = first.chain(second);

        let _ = chained.get_ref();
        let _ = chained.get_mut();
        let (_a, _b) = chained.into_inner();
    });
}

#[test]
fn take_limits_total_bytes_read_even_when_the_inner_reader_has_more() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let inner = reader_with(b"0123456789").await;
        let mut limited = inner.take(4);

        let mut out = Vec::new();
        limited.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"0123");
    });
}

#[test]
fn take_limit_getter_and_setter() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let inner = reader_with(b"0123456789").await;
        let mut limited = inner.take(4);
        assert_eq!(limited.limit(), 4);
        limited.set_limit(2);
        assert_eq!(limited.limit(), 2);
    });
}

#[test]
fn take_get_ref_get_mut_into_inner_reach_the_wrapped_reader() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let inner = reader_with(b"abc").await;
        let mut limited = inner.take(2);
        let _ = limited.get_ref();
        let _ = limited.get_mut();
        let _inner = limited.into_inner();
    });
}

#[test]
fn take_reads_gradually_across_multiple_small_calls() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let inner = reader_with(b"0123456789").await;
        let mut limited = inner.take(5);

        let mut buf = [0u8; 2];
        let n1 = limited.read(&mut buf).await.unwrap();
        assert_eq!(n1, 2);
        assert_eq!(&buf, b"01");

        let n2 = limited.read(&mut buf).await.unwrap();
        assert_eq!(n2, 2);
        assert_eq!(&buf, b"23");

        let n3 = limited.read(&mut buf).await.unwrap();
        assert_eq!(n3, 1);
        assert_eq!(&buf[..1], b"4");

        let n4 = limited.read(&mut buf).await.unwrap();
        assert_eq!(n4, 0, "limit reached -- should report EOF");
    });
}
