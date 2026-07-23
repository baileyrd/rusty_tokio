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
fn stream_position_reports_the_cursor_without_moving_it() {
    let path = temp_path("stream-position");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"0123456789").await.unwrap();
        drop(file);

        let mut file = File::open(&path).await.unwrap();
        assert_eq!(file.stream_position().await.unwrap(), 0);

        let mut buf = [0u8; 4];
        file.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"0123");

        // Reading a further two bytes right after should pick up where
        // the position report said it was, not from the start.
        assert_eq!(file.stream_position().await.unwrap(), 4);
        let mut buf = [0u8; 2];
        file.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"45");
        assert_eq!(file.stream_position().await.unwrap(), 6);
    });
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn rewind_seeks_back_to_the_start() {
    let path = temp_path("rewind");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"0123456789").await.unwrap();
        drop(file);

        let mut file = File::open(&path).await.unwrap();
        let mut buf = [0u8; 6];
        file.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"012345");
        assert_eq!(file.stream_position().await.unwrap(), 6);

        file.rewind().await.unwrap();
        assert_eq!(file.stream_position().await.unwrap(), 0);

        let mut buf = [0u8; 10];
        file.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"0123456789");
    });
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn create_dir_makes_a_single_new_directory() {
    let path = temp_path("create-dir");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        rusty_tokio::fs::create_dir(&path).await.unwrap();
        assert!(path.is_dir());
    });
    std::fs::remove_dir(&path).unwrap();
}

#[test]
fn create_dir_fails_if_the_parent_is_missing() {
    let parent = temp_path("create-dir-missing-parent");
    let child = parent.join("child");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let err = rusty_tokio::fs::create_dir(&child).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    });
}

#[test]
fn create_dir_all_makes_every_missing_parent() {
    let root = temp_path("create-dir-all");
    let nested = root.join("a").join("b").join("c");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        rusty_tokio::fs::create_dir_all(&nested).await.unwrap();
        assert!(nested.is_dir());

        // Succeeds again without complaint, unlike `create_dir`.
        rusty_tokio::fs::create_dir_all(&nested).await.unwrap();
    });
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn remove_dir_removes_an_empty_directory() {
    let path = temp_path("remove-dir");
    std::fs::create_dir(&path).unwrap();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        rusty_tokio::fs::remove_dir(&path).await.unwrap();
    });
    assert!(!path.exists());
}

#[test]
fn remove_dir_fails_on_a_non_empty_directory() {
    let path = temp_path("remove-dir-non-empty");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("file"), b"content").unwrap();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let err = rusty_tokio::fs::remove_dir(&path).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::DirectoryNotEmpty);
    });
    std::fs::remove_dir_all(&path).unwrap();
}

#[test]
fn remove_dir_all_removes_a_directory_and_its_contents() {
    let root = temp_path("remove-dir-all");
    let nested = root.join("a").join("b");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("file"), b"content").unwrap();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        rusty_tokio::fs::remove_dir_all(&root).await.unwrap();
    });
    assert!(!root.exists());
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

#[test]
fn set_len_truncates_and_extends() {
    let path = temp_path("set_len");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"0123456789").await.unwrap();

        file.set_len(4).await.unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 4);

        file.set_len(8).await.unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 8);
    });
    std::fs::remove_file(&path).unwrap();
}

#[cfg(unix)]
#[test]
fn set_permissions_applies_to_the_underlying_file() {
    use std::os::unix::fs::PermissionsExt;

    let path = temp_path("set_permissions");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        let mut perm = std::fs::metadata(&path).unwrap().permissions();
        perm.set_mode(0o600);
        file.set_permissions(perm).await.unwrap();
    });
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn sync_all_and_sync_data_do_not_error_on_a_writable_file() {
    let path = temp_path("sync");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"durability").await.unwrap();
        file.sync_all().await.unwrap();
        file.sync_data().await.unwrap();
    });
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn try_clone_gives_an_independent_handle_onto_the_same_file() {
    let path = temp_path("try_clone");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"shared-").await.unwrap();

        let mut cloned = file.try_clone().await.unwrap();
        // Both handles share one cursor (same open file description,
        // like `std::fs::File::try_clone`/`dup(2)`) -- a write through
        // the clone continues from wherever the original's cursor left
        // off, rather than starting over at offset 0.
        cloned.write_all(b"continued").await.unwrap();
    });
    let contents = std::fs::read(&path).unwrap();
    assert_eq!(contents, b"shared-continued");
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn try_into_std_succeeds_when_idle() {
    let path = temp_path("try_into_std");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"idle now").await.unwrap();

        // Idle (the write above already completed) -- should succeed.
        // `File` isn't `Debug`, so `Result::unwrap` doesn't apply to the
        // `Err(Self)` case -- go through `Option` instead.
        let std_file = file.try_into_std().ok().expect("file should be idle");
        drop(std_file);
    });
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn max_buf_size_defaults_then_can_be_changed_and_caps_a_single_read() {
    let path = temp_path("max_buf_size");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        assert_eq!(file.max_buf_size(), 2 * 1024 * 1024);
        file.write_all(b"0123456789").await.unwrap();
        drop(file);

        let mut file = File::open(&path).await.unwrap();
        file.set_max_buf_size(4);
        assert_eq!(file.max_buf_size(), 4);

        let mut buf = [0u8; 10];
        let n = file.read(&mut buf).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf[..4], b"0123");
    });
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn metadata_reports_file_length() {
    let path = temp_path("metadata");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"0123456789").await.unwrap();
        drop(file);

        let meta = rusty_tokio::fs::metadata(&path).await.unwrap();
        assert_eq!(meta.len(), 10);
        assert!(meta.is_file());
    });
    std::fs::remove_file(&path).unwrap();
}

#[cfg(unix)]
#[test]
fn symlink_metadata_does_not_follow_the_symlink_itself() {
    let target = temp_path("symlink-metadata-target");
    let link = temp_path("symlink-metadata-link");
    std::fs::write(&target, b"hello").unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let meta = rusty_tokio::fs::symlink_metadata(&link).await.unwrap();
        assert!(meta.file_type().is_symlink());

        // Plain `metadata` follows the link through to the real file.
        let followed = rusty_tokio::fs::metadata(&link).await.unwrap();
        assert!(followed.is_file());
        assert_eq!(followed.len(), 5);
    });
    std::fs::remove_file(&link).unwrap();
    std::fs::remove_file(&target).unwrap();
}

#[test]
fn try_exists_reports_true_for_a_real_path_and_false_for_a_missing_one() {
    let path = temp_path("try-exists");
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        assert!(!rusty_tokio::fs::try_exists(&path).await.unwrap());

        std::fs::write(&path, b"here").unwrap();
        assert!(rusty_tokio::fs::try_exists(&path).await.unwrap());
    });
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn canonicalize_resolves_to_an_absolute_path() {
    let path = temp_path("canonicalize");
    std::fs::write(&path, b"content").unwrap();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let canonical = rusty_tokio::fs::canonicalize(&path).await.unwrap();
        assert!(canonical.is_absolute());
        assert!(canonical.ends_with(path.file_name().unwrap()));
    });
    std::fs::remove_file(&path).unwrap();
}

#[cfg(unix)]
#[test]
fn free_set_permissions_applies_without_an_open_file() {
    use std::os::unix::fs::PermissionsExt;

    let path = temp_path("free-set-permissions");
    std::fs::write(&path, b"content").unwrap();
    let mut perm = std::fs::metadata(&path).unwrap().permissions();
    perm.set_mode(0o640);

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        rusty_tokio::fs::set_permissions(&path, perm).await.unwrap();
    });

    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o640);
    std::fs::remove_file(&path).unwrap();
}
