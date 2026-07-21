use rusty_tokio::fs::File;
use rusty_tokio::io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use rusty_tokio::Runtime;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// A fresh, unique path under the OS temp dir for each test -- avoids
/// collisions between tests (and between repeated runs of the same
/// test) without depending on a `tempfile`-style crate.
fn temp_path(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rusty_tokio_fs_test_{}_{name}_{n}",
        std::process::id()
    ))
}

#[test]
fn create_write_then_open_and_read_back() {
    let path = temp_path("roundtrip");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"hello file").await.unwrap();
        drop(file);

        let mut file = File::open(&path).await.unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"hello file");
    });
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn seek_moves_the_read_cursor_on_the_same_open_file() {
    // `File::create` opens write-only (matching `std::fs::File::create`
    // exactly), so the write and the seek/read below deliberately use
    // two separate `File`s on the same path, the same as plain
    // `std::fs::File` would require -- this is exercising `AsyncSeek`
    // interleaved with multiple reads on *one* open (read-only) File,
    // not read-after-write on a single handle.
    let path = temp_path("seek");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"0123456789").await.unwrap();
        drop(file);

        let mut file = File::open(&path).await.unwrap();
        let mut buf = [0u8; 10];
        file.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"0123456789");

        let pos = file.seek(SeekFrom::Start(3)).await.unwrap();
        assert_eq!(pos, 3);
        let mut buf = [0u8; 4];
        file.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"3456");

        // Seeking relative to the current position (7, after the read
        // above) backward by 2 should land at 5.
        let pos = file.seek(SeekFrom::Current(-2)).await.unwrap();
        assert_eq!(pos, 5);
        let mut buf = [0u8; 2];
        file.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"56");
    });
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn open_a_missing_file_reports_not_found() {
    let path = temp_path("does-not-exist");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        match File::open(&path).await {
            Ok(_) => panic!("expected NotFound, got a File for a path that shouldn't exist"),
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound),
        }
    });
}

#[test]
fn generic_copy_works_between_two_files() {
    let src_path = temp_path("copy-src");
    let dst_path = temp_path("copy-dst");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut src = File::create(&src_path).await.unwrap();
        src.write_all(b"copied through the generic io::copy")
            .await
            .unwrap();
        drop(src);

        let mut src = File::open(&src_path).await.unwrap();
        let mut dst = File::create(&dst_path).await.unwrap();
        let copied = io::copy(&mut src, &mut dst).await.unwrap();
        assert_eq!(copied, "copied through the generic io::copy".len() as u64);
        drop(dst);

        let mut dst = File::open(&dst_path).await.unwrap();
        let mut contents = Vec::new();
        dst.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"copied through the generic io::copy");
    });
    std::fs::remove_file(&src_path).unwrap();
    std::fs::remove_file(&dst_path).unwrap();
}
