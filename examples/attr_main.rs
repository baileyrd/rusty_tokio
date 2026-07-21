//! Demonstrates `#[rusty_tokio::main]` -- run with
//! `cargo run --example attr_main`.

#[rusty_tokio::main]
async fn main() {
    let (tx, rx) = rusty_tokio::sync::oneshot::channel::<&str>();
    rusty_tokio::spawn(async move {
        let _ = tx.send("hello from a spawned task");
    });
    println!("{}", rx.await.unwrap());
}
