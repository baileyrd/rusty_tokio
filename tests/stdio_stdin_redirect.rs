//! Deliberately the *only* test in this file: it temporarily redirects
//! the real process-wide fd 0 (stdin) to a pipe it controls, via raw
//! `libc` calls -- a global process resource, so this needs to run with
//! nothing else in the same test binary/process concurrently reading
//! real stdin (or, worse, some other test's own status output racing
//! through a *different* fd while this one's mid-redirect). Each
//! `tests/*.rs` file is its own separate process, so keeping this test
//! alone here is what actually guarantees that isolation, not anything
//! `#[test]`-attribute-level.
//!
//! `Stdin` itself is cross-platform (`io::stdio` has no fd-specific code
//! at all), but this test's *redirection mechanism* -- raw
//! `pipe`/`dup`/`dup2` against the well-known small-integer fd `0` -- is
//! POSIX-only; Windows has no equivalent notion of a stdin handle
//! addressable that way. Gating the whole file rather than trying to
//! find a Windows equivalent redirection trick for this one test.
#![cfg(unix)]

use rusty_tokio::io::{AsyncReadExt, Stdin};
use rusty_tokio::Runtime;

#[test]
fn stdin_reads_exactly_what_was_written_to_the_redirected_fd() {
    const REAL_STDIN_FD: i32 = 0;

    let mut pipe_fds = [0i32; 2];
    assert_eq!(
        unsafe { libc::pipe(pipe_fds.as_mut_ptr()) },
        0,
        "pipe() failed"
    );
    let (read_fd, write_fd) = (pipe_fds[0], pipe_fds[1]);

    let saved_stdin = unsafe { libc::dup(REAL_STDIN_FD) };
    assert!(saved_stdin >= 0, "dup(stdin) failed");
    assert_eq!(
        unsafe { libc::dup2(read_fd, REAL_STDIN_FD) },
        REAL_STDIN_FD,
        "dup2(pipe read end, stdin) failed"
    );
    unsafe { libc::close(read_fd) };

    let message = b"hello from a redirected stdin\n";
    let mut written = 0;
    while written < message.len() {
        let n = unsafe {
            libc::write(
                write_fd,
                message[written..].as_ptr() as *const _,
                message.len() - written,
            )
        };
        assert!(n > 0, "write() to the pipe failed");
        written += n as usize;
    }
    // EOF once the write end closes -- otherwise `read_to_end` below
    // would wait forever for more input that's never coming.
    unsafe { libc::close(write_fd) };

    let rt = Runtime::new().unwrap();
    let contents = rt.block_on(async {
        let mut stdin: Stdin = rusty_tokio::io::stdin();
        let mut buf = Vec::new();
        stdin.read_to_end(&mut buf).await.unwrap();
        buf
    });

    unsafe {
        libc::dup2(saved_stdin, REAL_STDIN_FD);
        libc::close(saved_stdin);
    }

    assert_eq!(contents, message);
}
