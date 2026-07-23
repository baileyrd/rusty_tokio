#![cfg(unix)]

use rusty_tokio::io::{pipe, AsyncReadExt, AsyncWriteExt, PipeOpenOptions};
use rusty_tokio::Runtime;
use std::os::fd::AsRawFd;

fn temp_fifo_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rusty_tokio-test-pipe-{}-{}-{}.fifo",
        std::process::id(),
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn mkfifo(path: &std::path::Path) {
    let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    // SAFETY: `c_path` is a valid, NUL-terminated C string for the
    // duration of this call.
    let r = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    assert_eq!(r, 0, "mkfifo failed: {}", std::io::Error::last_os_error());
}

#[test]
fn anonymous_pipe_sends_and_receives_data() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut sender, mut receiver) = pipe().unwrap();

        let task = rusty_tokio::spawn(async move {
            sender.write_all(b"hello pipe").await.unwrap();
        });

        let mut buf = [0u8; 10];
        receiver.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello pipe");

        task.await.unwrap();
    });
}

#[test]
fn anonymous_pipe_try_write_then_try_read() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (sender, receiver) = pipe().unwrap();

        sender.writable().await.unwrap();
        let n = sender.try_write(b"ping").unwrap();
        assert_eq!(n, 4);

        receiver.readable().await.unwrap();
        let mut buf = [0u8; 4];
        let n = receiver.try_read(&mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"ping");
    });
}

#[test]
fn named_pipe_open_receiver_and_sender_round_trip() {
    let rt = Runtime::new().unwrap();
    let path = temp_fifo_path("round-trip");
    mkfifo(&path);
    rt.block_on(async {
        // A `Receiver` opened alone would block (at the OS level) until
        // a writer also opens the same FIFO -- `O_NONBLOCK` (always
        // applied by `PipeOpenOptions`) turns that into an immediate,
        // successful-but-empty open instead, so opening receiver-then-
        // sender sequentially like this is safe.
        let mut receiver = PipeOpenOptions::new().open_receiver(&path).unwrap();
        let mut sender = PipeOpenOptions::new().open_sender(&path).unwrap();

        sender.write_all(b"named pipe").await.unwrap();
        let mut buf = [0u8; 10];
        receiver.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"named pipe");
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn open_receiver_fails_on_a_non_fifo_file() {
    let rt = Runtime::new().unwrap();
    let path = temp_fifo_path("not-a-fifo");
    std::fs::write(&path, b"plain file").unwrap();
    rt.block_on(async {
        let Err(err) = PipeOpenOptions::new().open_receiver(&path) else {
            panic!("expected open_receiver to reject a plain file");
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn open_receiver_unchecked_skips_the_fifo_check_for_a_real_fifo() {
    // `unchecked` only skips the `fstat`-based FIFO-type check itself --
    // registering a genuinely non-pollable fd (e.g. a plain regular
    // file) with the reactor still fails regardless of this flag,
    // `epoll_ctl`/`kevent` reject those outright (`EPERM`) no matter
    // what userspace believes about the fd. So this exercises `unchecked`
    // against a real FIFO instead, confirming the flag doesn't otherwise
    // change behavior for the case it's actually meant for.
    let rt = Runtime::new().unwrap();
    let path = temp_fifo_path("unchecked");
    mkfifo(&path);
    rt.block_on(async {
        let mut receiver = PipeOpenOptions::new()
            .unchecked(true)
            .open_receiver(&path)
            .unwrap();
        let mut sender = PipeOpenOptions::new()
            .unchecked(true)
            .open_sender(&path)
            .unwrap();

        sender.write_all(b"unchecked!").await.unwrap();
        let mut buf = [0u8; 10];
        receiver.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"unchecked!");
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn into_nonblocking_fd_then_into_blocking_fd_toggle_o_nonblock() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (sender, _receiver) = pipe().unwrap();

        let nonblocking_fd = sender.into_nonblocking_fd().unwrap();
        // SAFETY: `F_GETFL` takes no further argument; `nonblocking_fd`
        // is a valid, currently-open fd.
        let flags = unsafe { libc::fcntl(nonblocking_fd.as_raw_fd(), libc::F_GETFL) };
        assert_ne!(
            flags & libc::O_NONBLOCK,
            0,
            "expected O_NONBLOCK to still be set"
        );

        let (sender, _receiver) = pipe().unwrap();
        let blocking_fd = sender.into_blocking_fd().unwrap();
        // SAFETY: same as above.
        let flags = unsafe { libc::fcntl(blocking_fd.as_raw_fd(), libc::F_GETFL) };
        assert_eq!(
            flags & libc::O_NONBLOCK,
            0,
            "expected O_NONBLOCK to be cleared"
        );
    });
}
