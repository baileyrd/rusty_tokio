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

#[test]
fn process_group_zero_makes_the_child_its_own_group_leader() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("sleep 5")
            .process_group(0)
            .spawn()
            .unwrap();

        let pid = child.id() as libc::pid_t;
        let pgid = unsafe { libc::getpgid(pid) };
        assert_eq!(
            pgid, pid,
            "process_group(0) should make the child its own group leader"
        );

        child.kill().unwrap();
        child.wait().await.unwrap();
    });
}

#[test]
fn kill_on_drop_kills_a_still_running_child_when_dropped() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("sleep 30");
        cmd.kill_on_drop(true);
        assert!(cmd.get_kill_on_drop());

        let child = cmd.spawn().unwrap();
        let pid = child.id() as libc::pid_t;

        drop(child);

        // Give the detached reap task a moment to actually run.
        rusty_tokio::time::sleep(Duration::from_millis(200)).await;

        // ESRCH: no such process -- killed *and* reaped, so it isn't
        // even lingering as a zombie a plain `kill` alone would leave.
        let err = unsafe { libc::kill(pid, 0) };
        assert_eq!(err, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    });
}

#[test]
fn kill_on_drop_defaults_to_false_and_leaves_a_running_child_alone() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("sleep 30");
        assert!(!cmd.get_kill_on_drop());

        let child = cmd.spawn().unwrap();
        let pid = child.id() as libc::pid_t;
        drop(child);

        rusty_tokio::time::sleep(Duration::from_millis(100)).await;

        // Still alive -- a plain drop (kill_on_drop left at its default)
        // just orphans it, same as dropping a std::process::Child.
        let err = unsafe { libc::kill(pid, 0) };
        assert_eq!(err, 0, "child should still be running after a plain drop");

        // Clean up so it doesn't linger as a zombie for the rest of
        // this test process's life.
        unsafe {
            libc::kill(pid, libc::SIGKILL);
            libc::waitpid(pid, std::ptr::null_mut(), 0);
        }
    });
}

#[test]
fn as_std_reflects_the_builder_state_set_so_far() {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg("exit 0");
    assert_eq!(cmd.as_std().get_program(), "/bin/sh");
    assert_eq!(
        cmd.as_std().get_args().collect::<Vec<_>>(),
        vec!["-c", "exit 0"]
    );
}

#[test]
fn as_std_mut_setters_apply_to_the_spawned_child() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("echo $FROM_AS_STD_MUT");
        cmd.as_std_mut().env("FROM_AS_STD_MUT", "yes");
        cmd.stdout(Stdio::piped());

        let mut child = cmd.spawn().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut contents = Vec::new();
        stdout.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"yes\n");

        let status = child.wait().await.unwrap();
        assert!(status.success());
    });
}

#[test]
fn status_spawns_and_waits_reporting_the_exit_code() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let status = Command::new("/bin/sh")
            .arg("-c")
            .arg("exit 7")
            .status()
            .await
            .unwrap();
        assert_eq!(status.code(), Some(7));
    });
}

#[test]
fn output_captures_stdout_and_stderr_and_reports_the_exit_code() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let output = Command::new("/bin/sh")
            .arg("-c")
            .arg("echo to stdout; echo to stderr 1>&2; exit 5")
            .output()
            .await
            .unwrap();
        assert_eq!(output.status.code(), Some(5));
        assert_eq!(output.stdout, b"to stdout\n");
        assert_eq!(output.stderr, b"to stderr\n");
    });
}

#[test]
fn wait_with_output_does_not_deadlock_on_a_chatty_child() {
    // A child that writes enough to fill an OS pipe buffer on *both*
    // stdout and stderr before exiting would deadlock a naive
    // sequential drain (read all of stdout, then all of stderr, then
    // wait) -- this only passes if both streams are actually drained
    // concurrently with the wait.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let output = rusty_tokio::time::timeout(
            std::time::Duration::from_secs(10),
            Command::new("/bin/sh")
                .arg("-c")
                .arg("yes stdout-line | head -c 200000; yes stderr-line | head -c 200000 1>&2")
                .output(),
        )
        .await
        .expect("draining both piped streams concurrently with wait must not deadlock")
        .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout.len(), 200_000);
        assert_eq!(output.stderr.len(), 200_000);
    });
}

#[test]
fn wait_with_output_works_when_only_stdout_is_piped() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("echo only stdout");
        cmd.stdout(Stdio::piped());
        let child = cmd.spawn().unwrap();

        let output = child.wait_with_output().await.unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"only stdout\n");
        assert_eq!(output.stderr, b"");
    });
}
