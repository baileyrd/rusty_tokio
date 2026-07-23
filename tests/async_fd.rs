#![cfg(unix)]

use rusty_tokio::io::{AsyncFd, Interest};
use rusty_tokio::Runtime;
use std::os::fd::AsRawFd;

fn set_nonblocking(fd: std::os::fd::RawFd) {
    // SAFETY: `fd` is a valid, open fd for the duration of this call.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
    }
}

fn raw_read(fd: std::os::fd::RawFd, buf: &mut [u8]) -> std::io::Result<usize> {
    // SAFETY: `fd` is caller-owned and open; `buf` is a valid,
    // exclusively-borrowed out-param for the call's duration.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

fn raw_write(fd: std::os::fd::RawFd, buf: &[u8]) -> std::io::Result<usize> {
    // SAFETY: `fd` is caller-owned and open; `buf` is valid for the
    // call's duration.
    let n = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
    if n < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

#[test]
fn async_fd_readable_then_try_io_reads_real_data_from_a_pipe() {
    let (reader, writer) = std::io::pipe().unwrap();
    set_nonblocking(reader.as_raw_fd());

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let async_fd = AsyncFd::new(reader).unwrap();

        let write_task = rusty_tokio::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            raw_write(writer.as_raw_fd(), b"hello").unwrap();
        });

        let mut buf = [0u8; 5];
        loop {
            let mut guard = async_fd.readable().await.unwrap();
            match guard.try_io(|inner| raw_read(inner.get_ref().as_raw_fd(), &mut buf)) {
                Ok(Ok(n)) => {
                    assert_eq!(n, 5);
                    assert_eq!(&buf, b"hello");
                    break;
                }
                Ok(Err(e)) => panic!("unexpected read error: {e}"),
                Err(_would_block) => continue,
            }
        }

        write_task.await.unwrap();
    });
}

#[test]
fn async_fd_writable_resolves_and_write_succeeds() {
    let (_reader, writer) = std::io::pipe().unwrap();
    set_nonblocking(writer.as_raw_fd());

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let async_fd = AsyncFd::new(writer).unwrap();
        let mut guard = async_fd.writable().await.unwrap();
        let n = guard
            .try_io(|inner| raw_write(inner.get_ref().as_raw_fd(), b"ping"))
            .unwrap()
            .unwrap();
        assert_eq!(n, 4);
    });
}

#[test]
fn async_fd_get_ref_get_mut_into_inner_reach_the_wrapped_value() {
    let (reader, _writer) = std::io::pipe().unwrap();
    let raw_fd = reader.as_raw_fd();

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut async_fd = AsyncFd::new(reader).unwrap();
        assert_eq!(async_fd.get_ref().as_raw_fd(), raw_fd);
        assert_eq!(async_fd.get_mut().as_raw_fd(), raw_fd);
        assert_eq!(async_fd.interest(), Interest::READABLE | Interest::WRITABLE);

        let inner = async_fd.into_inner();
        assert_eq!(inner.as_raw_fd(), raw_fd);
    });
}

#[test]
#[should_panic(expected = "Interest::WRITABLE")]
fn async_fd_writable_panics_if_write_interest_was_not_declared() {
    let (reader, _writer) = std::io::pipe().unwrap();
    set_nonblocking(reader.as_raw_fd());

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let async_fd = AsyncFd::with_interest(reader, Interest::READABLE).unwrap();
        let _ = async_fd.writable().await;
    });
}
