use rusty_tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use rusty_tokio::Runtime;
use std::time::Duration;

#[test]
fn basic_roundtrip_one_direction() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut writer, mut reader) = io::simplex(64);
        writer.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    });
}

#[test]
fn write_blocks_until_a_read_frees_up_room() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut writer, mut reader) = io::simplex(4);

        writer.write_all(b"abcd").await.unwrap();

        let blocked = rusty_tokio::time::timeout(Duration::from_millis(100), async {
            writer.write_all(b"e").await.unwrap();
        })
        .await;
        assert!(
            blocked.is_err(),
            "write_all should still be pending with a full buffer and no reader"
        );

        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ab");

        writer.write_all(b"f").await.unwrap();
        let mut rest = [0u8; 3];
        reader.read_exact(&mut rest).await.unwrap();
        assert_eq!(&rest, b"cdf");
    });
}

#[test]
fn dropping_the_writer_gives_the_reader_an_eof_after_draining_whats_left() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut writer, mut reader) = io::simplex(64);
        writer.write_all(b"last words").await.unwrap();
        drop(writer);

        let mut contents = Vec::new();
        reader.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"last words");

        let mut buf = [0u8; 1];
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    });
}

#[test]
fn writing_after_the_reader_dropped_fails_fast() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut writer, reader) = io::simplex(64);
        drop(reader);

        let err =
            rusty_tokio::time::timeout(Duration::from_millis(100), writer.write_all(b"anyone?"))
                .await
                .expect("a write against a dropped peer must fail immediately, not hang")
                .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    });
}

#[test]
fn shutdown_marks_eof_for_the_reader() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut writer, mut reader) = io::simplex(64);

        writer.write_all(b"done writing").await.unwrap();
        writer.shutdown().await.unwrap();

        let mut contents = Vec::new();
        reader.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"done writing");
    });
}

#[test]
fn empty_pipe_read_is_pending_until_a_write_arrives() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut writer, mut reader) = io::simplex(16);

        let read_task = rusty_tokio::spawn(async move {
            let mut buf = [0u8; 3];
            reader.read_exact(&mut buf).await.unwrap();
            buf
        });

        // Give the reader a moment to actually register as pending
        // before anything's been written.
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        writer.write_all(b"hey").await.unwrap();

        let buf = read_task.await.unwrap();
        assert_eq!(&buf, b"hey");
    });
}
