use rusty_tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use rusty_tokio::Runtime;

#[test]
fn empty_reports_eof_immediately() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut buf = [0u8; 8];
        let n = io::empty().read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    });
}

#[test]
fn empty_read_to_end_produces_no_bytes() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut out = Vec::new();
        io::empty().read_to_end(&mut out).await.unwrap();
        assert!(out.is_empty());
    });
}

#[test]
fn repeat_fills_the_whole_buffer_with_the_given_byte() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut buf = [0u8; 16];
        let n = io::repeat(b'x').read(&mut buf).await.unwrap();
        assert_eq!(n, buf.len());
        assert!(buf.iter().all(|&b| b == b'x'));
    });
}

#[test]
fn repeat_never_reports_eof_across_repeated_reads() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut source = io::repeat(7);
        for _ in 0..5 {
            let mut buf = [0u8; 4];
            let n = source.read(&mut buf).await.unwrap();
            assert_eq!(n, 4);
            assert_eq!(buf, [7, 7, 7, 7]);
        }
    });
}

#[test]
fn sink_accepts_and_discards_everything_written() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut sink = io::sink();
        sink.write_all(b"whatever, this goes nowhere")
            .await
            .unwrap();
        sink.flush().await.unwrap();
        sink.shutdown().await.unwrap();
    });
}
