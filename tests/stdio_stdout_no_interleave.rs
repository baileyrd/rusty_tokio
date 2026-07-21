//! Deliberately the *only* test in this file -- see
//! `tests/stdio_stdin_redirect.rs`'s own module docs for why a raw-fd
//! redirection test needs to run alone in its process, not just alone
//! within one `#[test]` at a time.
//!
//! This is a best-effort, real-OS-scheduling-dependent check, not a
//! guaranteed reproduction of a missing lock: it can only actually
//! observe interleaving if the underlying blocking-pool threads truly
//! run concurrently (CPU count, scheduler behavior, and container/CPU
//! limits all affect that) at the moment each one's `write(2)` call is
//! large enough to risk it. The deterministic half of this guarantee --
//! that a single `poll_write` call always reports the full length, never
//! a partial count -- is checked directly and unconditionally in
//! `tests/stdio.rs`'s `a_single_write_call_always_reports_the_full_length_never_partial`.

use rusty_tokio::io::AsyncWriteExt;
use rusty_tokio::Runtime;

#[test]
fn concurrent_stdout_writes_do_not_interleave() {
    const REAL_STDOUT_FD: i32 = 1;

    let mut pipe_fds = [0i32; 2];
    assert_eq!(
        unsafe { libc::pipe(pipe_fds.as_mut_ptr()) },
        0,
        "pipe() failed"
    );
    let (read_fd, write_fd) = (pipe_fds[0], pipe_fds[1]);

    let saved_stdout = unsafe { libc::dup(REAL_STDOUT_FD) };
    assert!(saved_stdout >= 0, "dup(stdout) failed");
    assert_eq!(
        unsafe { libc::dup2(write_fd, REAL_STDOUT_FD) },
        REAL_STDOUT_FD,
        "dup2(pipe write end, stdout) failed"
    );
    unsafe { libc::close(write_fd) };

    // Long and distinct enough per task that a torn/interleaved write
    // would very likely land in the middle of one of these, not just
    // get lucky and land on a boundary. Deliberately bigger than
    // `PIPE_BUF` (4096 on Linux, the size under which POSIX guarantees
    // a single `write(2)` to a pipe is atomic on its own) -- below that
    // threshold the kernel's own guarantee alone would mask a missing
    // lock here, giving a false pass. Total size (6 * ~8000 =~ 48000
    // bytes) stays comfortably under a default pipe's ~64KB capacity so
    // the writes below don't block waiting for a concurrent reader that
    // doesn't exist yet (this test only reads the pipe back afterward).
    let messages: Vec<String> = (0..6)
        .map(|i| format!("[[task-{i}:{}]]", "x".repeat(8000)))
        .collect();
    let expected_len: usize = messages.iter().map(String::len).sum();

    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let handles: Vec<_> = messages
            .iter()
            .map(|msg| {
                let msg = msg.clone();
                rusty_tokio::spawn(async move {
                    rusty_tokio::io::stdout()
                        .write_all(msg.as_bytes())
                        .await
                        .unwrap();
                })
            })
            .collect();
        for h in handles {
            h.await.unwrap();
        }
    });

    // Restore the real stdout *before* reading the pipe back -- nothing
    // else should be writing to fd 1 by this point (this test runs
    // alone in its process), but there's no reason to leave the
    // redirect in place a moment longer than the writes above need it.
    unsafe {
        libc::dup2(saved_stdout, REAL_STDOUT_FD);
        libc::close(saved_stdout);
    }

    let mut captured = Vec::new();
    let mut chunk = [0u8; 8192];
    while captured.len() < expected_len {
        let n = unsafe { libc::read(read_fd, chunk.as_mut_ptr() as *mut _, chunk.len()) };
        assert!(n > 0, "unexpected EOF/error reading back captured stdout");
        captured.extend_from_slice(&chunk[..n as usize]);
    }
    unsafe { libc::close(read_fd) };

    assert_eq!(
        captured.len(),
        expected_len,
        "captured more bytes than every message's combined length -- unexpected extra output"
    );
    for msg in &messages {
        assert!(
            captured
                .windows(msg.len())
                .any(|window| window == msg.as_bytes()),
            "message {msg:?} did not appear intact and contiguous in captured stdout -- \
             a concurrent write must have interleaved with it"
        );
    }
}
