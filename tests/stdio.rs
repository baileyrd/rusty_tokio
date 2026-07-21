use rusty_tokio::io::{AsyncWriteExt, Stderr, Stdin, Stdout};
use rusty_tokio::Runtime;

#[test]
fn stdout_write_all_flush_and_shutdown_all_succeed() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut out = rusty_tokio::io::stdout();
        out.write_all(b"rusty_tokio stdio smoke test (stdout)\n")
            .await
            .unwrap();
        out.flush().await.unwrap();
        out.shutdown().await.unwrap();
    });
}

#[test]
fn stderr_write_all_flush_and_shutdown_all_succeed() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut err = rusty_tokio::io::stderr();
        err.write_all(b"rusty_tokio stdio smoke test (stderr)\n")
            .await
            .unwrap();
        err.flush().await.unwrap();
        err.shutdown().await.unwrap();
    });
}

#[test]
fn a_single_write_call_always_reports_the_full_length_never_partial() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut out = rusty_tokio::io::stdout();
        // `poll_write` always does a `write_all` internally, so even a
        // buffer big enough to need several underlying syscalls is
        // reported back as one atomic, fully-written call -- never a
        // partial count a caller's own retry loop might otherwise
        // interleave with someone else's write in between. Checked
        // directly and deterministically here (unlike actually
        // reproducing concurrent interleaving, which depends on real OS
        // thread scheduling -- see `tests/stdio_stdout_no_interleave.rs`
        // for that best-effort end-to-end check).
        let buf = vec![b'.'; 200_000];
        let n = out.write(&buf).await.unwrap();
        assert_eq!(n, buf.len());
    });
}

#[test]
fn constructing_multiple_handles_is_cheap_and_independent() {
    // Every call just builds a fresh, `Idle`-state handle -- no shared
    // construction-time cost, and no reason a second call should fail
    // just because a first one exists (unlike, say, holding two mutable
    // borrows of the same value).
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let _a: Stdin = rusty_tokio::io::stdin();
        let _b: Stdin = rusty_tokio::io::stdin();
        let mut out_a: Stdout = rusty_tokio::io::stdout();
        let mut out_b: Stdout = rusty_tokio::io::stdout();
        let mut err: Stderr = rusty_tokio::io::stderr();

        out_a.write_all(b"a\n").await.unwrap();
        out_b.write_all(b"b\n").await.unwrap();
        err.write_all(b"c\n").await.unwrap();
    });
}
