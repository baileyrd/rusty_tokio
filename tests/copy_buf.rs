use rusty_tokio::io::{self, copy_buf, AsyncWriteExt, BufReader};
use rusty_tokio::Runtime;

#[test]
fn copy_buf_copies_everything_from_a_buffered_reader_to_a_writer() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut writer, reader) = io::duplex(64);
        writer.write_all(b"hello copy_buf world").await.unwrap();
        writer.shutdown().await.unwrap();

        let mut buffered = BufReader::new(reader);
        let mut out: Vec<u8> = Vec::new();
        let n = copy_buf(&mut buffered, &mut out).await.unwrap();

        assert_eq!(n, "hello copy_buf world".len() as u64);
        assert_eq!(out, b"hello copy_buf world");
    });
}

#[test]
fn copy_buf_handles_data_arriving_across_several_separate_fills() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut writer, reader) = io::duplex(4);
        let mut buffered = BufReader::with_capacity(4, reader);

        let sender = rusty_tokio::spawn(async move {
            for chunk in [b"0123", b"4567", b"89ab"] {
                writer.write_all(chunk).await.unwrap();
            }
            writer.shutdown().await.unwrap();
        });

        let mut out: Vec<u8> = Vec::new();
        let n = copy_buf(&mut buffered, &mut out).await.unwrap();

        assert_eq!(n, 12);
        assert_eq!(out, b"0123456789ab");
        sender.await.unwrap();
    });
}

#[test]
fn copy_buf_on_an_already_empty_reader_copies_nothing() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (writer, reader) = io::duplex(64);
        drop(writer);

        let mut buffered = BufReader::new(reader);
        let mut out: Vec<u8> = Vec::new();
        let n = copy_buf(&mut buffered, &mut out).await.unwrap();

        assert_eq!(n, 0);
        assert!(out.is_empty());
    });
}
