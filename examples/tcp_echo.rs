//! A concurrent TCP echo server: each connection gets its own spawned
//! task, so slow/idle clients never block anyone else.
//!
//! Run it, then in another terminal: `nc 127.0.0.1 7878`

use rusty_tokio::io::TcpListener;
use rusty_tokio::Runtime;

fn main() -> std::io::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:7878".parse().unwrap())?;
        println!("listening on {}", listener.local_addr()?);

        loop {
            let (stream, peer) = listener.accept().await?;
            println!("accepted connection from {peer}");
            rusty_tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) => {
                            println!("{peer} disconnected");
                            return;
                        }
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            eprintln!("read error from {peer}: {e}");
                            return;
                        }
                    }
                }
            });
        }
    })
}
