#![cfg(unix)]
// `process`'s own module is Unix-only (see `src/process/mod.rs`'s docs),
// and this file also uses `std::os::unix::process::ExitStatusExt`
// directly -- gating the whole file rather than every individual item.

use rusty_tokio::io::{AsyncReadExt, AsyncWriteExt};
use rusty_tokio::process::{Command, Stdio};
use rusty_tokio::Runtime;
use std::os::unix::process::ExitStatusExt;
use std::time::Duration;

#[test]
fn spawn_and_wait_reports_the_exit_code() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exit 3")
            .spawn()
            .unwrap();
        let status = child.wait().await.unwrap();
        assert_eq!(status.code(), Some(3));
    });
}

#[test]
fn piped_stdout_is_read_asynchronously() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut child = Command::new("echo")
            .arg("hello from child")
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        let mut stdout = child.stdout.take().unwrap();
        let mut contents = Vec::new();
        stdout.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"hello from child\n");

        let status = child.wait().await.unwrap();
        assert!(status.success());
    });
}

#[test]
fn piped_stdin_is_written_and_cat_echoes_it_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        let mut stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();

        stdin.write_all(b"round trip through cat").await.unwrap();
        // `cat` keeps echoing until it sees EOF on its stdin -- dropping
        // our write end delivers that.
        drop(stdin);

        let mut contents = Vec::new();
        stdout.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"round trip through cat");

        let status = child.wait().await.unwrap();
        assert!(status.success());
    });
}

#[test]
fn stderr_is_captured_separately_from_stdout() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("echo to-stdout; echo to-stderr >&2")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();

        let mut out = Vec::new();
        stdout.read_to_end(&mut out).await.unwrap();
        let mut err = Vec::new();
        stderr.read_to_end(&mut err).await.unwrap();

        assert_eq!(out, b"to-stdout\n");
        assert_eq!(err, b"to-stderr\n");

        child.wait().await.unwrap();
    });
}

#[test]
fn try_wait_reports_none_while_running_then_some_after_exit() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("sleep 0.2")
            .spawn()
            .unwrap();

        assert!(child.try_wait().unwrap().is_none());

        let status = child.wait().await.unwrap();
        assert!(status.success());

        // Already reaped -- try_wait keeps reporting the same status
        // rather than erroring on an already-waited-for child.
        assert_eq!(child.try_wait().unwrap().unwrap().code(), status.code());
    });
}

#[test]
fn kill_terminates_a_running_child() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("sleep 30")
            .spawn()
            .unwrap();

        assert!(child.try_wait().unwrap().is_none());
        child.kill().unwrap();

        let status = rusty_tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("a killed child should exit promptly")
            .unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL));

        // Killing an already-reaped child is a no-op, not an error.
        child.kill().unwrap();
    });
}

#[test]
fn id_stays_available_after_the_child_has_been_waited_on() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exit 0")
            .spawn()
            .unwrap();
        let id = child.id();
        assert!(id > 0);
        child.wait().await.unwrap();
        assert_eq!(child.id(), id);
    });
}

#[test]
fn arg0_sets_argv_zero_without_changing_which_binary_actually_runs() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut child = Command::new("/bin/sh")
            .arg0("totally-not-sh")
            .arg("-c")
            .arg("echo $0")
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        let mut stdout = child.stdout.take().unwrap();
        let mut contents = Vec::new();
        stdout.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"totally-not-sh\n");

        let status = child.wait().await.unwrap();
        assert!(status.success());
    });
}
